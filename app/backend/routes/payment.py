"""DataPulse v5 — İyzico Ödeme Routes (routes/payment.py)

Endpoints:
  POST /api/v1/payment/checkout  → Ödeme başlat, form token al
  POST /api/v1/payment/callback  → İyzico webhook (3DS callback)
  POST /api/v1/payment/webhook   → İyzico sunucu bildirimi
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.core.config import settings
from backend.services.supabase_service import SupabaseService, get_current_user
from backend.services.preview_service import PreviewService
from backend.services.telegram_service import TelegramService

logger = logging.getLogger(__name__)
router = APIRouter()

supabase_svc = SupabaseService()
preview_svc = PreviewService()
telegram_svc = TelegramService()


# ── Schemas ───────────────────────────────────────────────────
class CheckoutRequest(BaseModel):
    job_id: str
    plan: Optional[str] = None  # plan yükseltme için


class CheckoutResponse(BaseModel):
    checkout_form_content: str
    token: str


# ── Plan fiyatları (TL kuruş cinsinden) ──────────────────────
PLAN_PRICES = {
    "starter": {"price": "29900", "name": "DataPulse Starter"},  # ₺299
    "pro": {"price": "69900", "name": "DataPulse Pro"},          # ₺699
}

JOB_UNLOCK_PRICE = "100"  # ₺1 — tek job unlock (test)


# ── İyzico yardımcı fonksiyonlar ─────────────────────────────
def _iyzico_auth_header() -> dict:
    """İyzico authorization header üret."""
    import base64
    import random
    import string

    nonce = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    ts = str(int(time.time() * 1000))
    raw = f"{settings.IYZICO_SECRET_KEY}&{nonce}&{ts}&"
    digest = base64.b64encode(
        hashlib.sha256(raw.encode()).digest()
    ).decode()

    return {
        "Authorization": f"IYZWS {settings.IYZICO_API_KEY}:{digest}",
        "x-iyzi-rnd": nonce,
        "x-iyzi-client-version": "iyzipay-python-1.0.46",
        "Content-Type": "application/json",
    }


async def _iyzico_post(path: str, body: dict) -> dict:
    """İyzico API'ye POST isteği gönder."""
    url = f"{settings.IYZICO_BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=body, headers=_iyzico_auth_header())
        resp.raise_for_status()
        return resp.json()


# ── Endpoints ─────────────────────────────────────────────────
@router.post("/checkout", response_model=CheckoutResponse)
async def checkout(
    body: CheckoutRequest,
    current_user: dict = Depends(get_current_user),
) -> CheckoutResponse:
    """İyzico checkout form başlat — job unlock için ödeme."""
    user_id = current_user["id"]
    job_id = body.job_id

    # Job var mı?
    job = await supabase_svc.get_job(job_id=job_id, user_id=user_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job bulunamadı.")
    if job.get("unlocked"):
        raise HTTPException(status_code=400, detail="Job zaten açık.")

    payload = {
        "locale": "tr",
        "conversationId": job_id,
        "price": JOB_UNLOCK_PRICE,
        "paidPrice": JOB_UNLOCK_PRICE,
        "currency": "TRY",
        "basketId": job_id,
        "paymentGroup": "PRODUCT",
        "callbackUrl": f"{settings.ALLOWED_ORIGINS[0]}/api/v1/payment/callback",
        "enabledInstallments": [1, 2, 3],
        "buyer": {
            "id": str(user_id),
            "name": current_user.get("email", "User").split("@")[0],
            "surname": "DataPulse",
            "email": current_user.get("email", "user@datapulse.io"),
            "identityNumber": "11111111111",
            "registrationAddress": "Türkiye",
            "city": "Istanbul",
            "country": "Turkey",
            "ip": "85.34.78.112",
        },
        "shippingAddress": {
            "contactName": "DataPulse User",
            "city": "Istanbul",
            "country": "Turkey",
            "address": "Türkiye",
        },
        "billingAddress": {
            "contactName": "DataPulse User",
            "city": "Istanbul",
            "country": "Turkey",
            "address": "Türkiye",
        },
        "basketItems": [
            {
                "id": job_id,
                "name": f"DataPulse Job Unlock: {job['task_type']}",
                "category1": "Yazılım",
                "itemType": "VIRTUAL",
                "price": JOB_UNLOCK_PRICE,
            }
        ],
    }

    result = await _iyzico_post("/payment/iyzipos/initialize", payload)
    if result.get("status") != "success":
        logger.error(f"İyzico checkout hatası: {result}")
        raise HTTPException(status_code=502, detail="Ödeme sistemi hatası.")

    return CheckoutResponse(
        checkout_form_content=result.get("checkoutFormContent", ""),
        token=result.get("token", ""),
    )


@router.post("/callback")
async def payment_callback(
    request: Request,
    background_tasks: BackgroundTasks,
) -> dict:
    """İyzico 3DS callback — ödeme sonucu."""
    form = await request.form()
    token = form.get("token", "")
    status = form.get("status", "")

    if status != "success" or not token:
        logger.warning(f"Ödeme başarısız: token={token} status={status}")
        return {"status": "failed", "message": "Ödeme başarısız."}

    # Ödemeyi doğrula
    result = await _iyzico_post(
        "/payment/iyzipos/detail",
        {"locale": "tr", "token": token},
    )

    if result.get("paymentStatus") != "SUCCESS":
        logger.error(f"İyzico doğrulama hatası: {result}")
        raise HTTPException(status_code=402, detail="Ödeme doğrulanamadı.")

    job_id = result.get("basketId", "")
    if not job_id:
        raise HTTPException(status_code=400, detail="Job ID bulunamadı.")

    # Unlock token oluştur ve job'a kaydet
    background_tasks.add_task(_finalize_payment, job_id=job_id)

    return {"status": "success", "job_id": job_id}


@router.post("/webhook")
async def payment_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
) -> dict:
    """İyzico sunucu bildirimi (IPN)."""
    try:
        data = await request.json()
    except Exception:
        data = {}

    job_id = data.get("conversationId") or data.get("basketId", "")
    event_type = data.get("iyziEventType", "")
    payment_status = data.get("paymentStatus", "")

    logger.info(f"İyzico webhook: job={job_id} event={event_type} status={payment_status}")

    if payment_status == "SUCCESS" and job_id:
        background_tasks.add_task(_finalize_payment, job_id=job_id)

    return {"received": True}


async def _finalize_payment(job_id: str) -> None:
    """Ödeme sonrası: unlock token oluştur, Telegram bildir."""
    try:
        job = await supabase_svc.get_job_by_id(job_id)
        if not job:
            return

        token = await preview_svc.create_unlock_token(
            job_id=job_id,
            user_id=job["user_id"],
        )
        await supabase_svc.set_payment_token(job_id=job_id, token=token)
        logger.info(f"Ödeme tamamlandı: job={job_id} token={token[:8]}...")

        await telegram_svc.send_payment_success(
            job_id=job_id,
            task_type=job.get("task_type", "?"),
            user_id=job["user_id"],
        )
    except Exception as e:
        logger.error(f"_finalize_payment hatası: {e}", exc_info=True)
