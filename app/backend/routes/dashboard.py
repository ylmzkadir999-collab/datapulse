"""DataPulse v5 — Dashboard Routes (JWT korumalı)

Endpoints:
  POST /api/v1/jobs          → Job oluştur + Celery'ye kuyruğa al
  GET  /api/v1/jobs/{id}     → Job durumu + preview
  GET  /api/v1/jobs          → Kullanıcının job listesi
  POST /api/v1/unlock/{id}   → Token ile tam dosyayı aç
  GET  /api/v1/me            → Mevcut kullanıcı + plan
"""
from __future__ import annotations

import logging
from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend.core.config import settings
from backend.services.supabase_service import SupabaseService, get_current_user
from backend.services.preview_service import PreviewService
from backend.worker.tasks import dispatch_job

logger = logging.getLogger(__name__)
router = APIRouter()

supabase_svc = SupabaseService()
preview_svc = PreviewService()


# ── Schemas ───────────────────────────────────────────────────
class JobCreateResponse(BaseModel):
    job_id: str
    status: str
    message: str


class JobStatusResponse(BaseModel):
    job_id: str
    task_type: str
    status: str
    progress: int
    logs: list[str]
    preview: Optional[dict] = None
    unlock_required: bool = True
    created_at: str


class UnlockRequest(BaseModel):
    token: str


# ── Allowed task types ────────────────────────────────────────
TASK_TYPES = {
    "pdf2excel",
    "scrape",
    "email_scrape",
    "clean",
    "bulk_clean",
    "excel_merge",
    "sheets_sync",
    "full_pipeline",
}


# ── Endpoints ─────────────────────────────────────────────────
@router.post("/jobs", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job(
    task_type: Annotated[str, Form()],
    file: Annotated[Optional[UploadFile], File()] = None,
    config_json: Annotated[Optional[str], Form()] = None,
    current_user: dict = Depends(get_current_user),
) -> JobCreateResponse:
    """Yeni job oluştur ve kuyruğa al."""
    if task_type not in TASK_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Geçersiz task_type. Seçenekler: {sorted(TASK_TYPES)}",
        )

    user_id = current_user["id"]
    plan = current_user.get("plan", "free")

    # Plan limiti kontrolü
    monthly_count = await supabase_svc.get_monthly_job_count(user_id)
    limit = settings.PLAN_FREE_JOBS_PER_MONTH if plan == "free" else (
        settings.PLAN_STARTER_JOBS_PER_MONTH if plan == "starter" else -1
    )
    if limit != -1 and monthly_count >= limit:
        raise HTTPException(
            status_code=429,
            detail=f"Aylık {limit} iş limitine ulaştınız. Plan yükseltin.",
        )

    # Dosyayı Supabase Storage'a yükle
    file_url: Optional[str] = None
    if file:
        file_bytes = await file.read()
        file_url = await supabase_svc.upload_file(
            user_id=user_id,
            filename=file.filename or "upload",
            content=file_bytes,
            content_type=file.content_type or "application/octet-stream",
        )

    # Job kaydı oluştur
    job_id = await supabase_svc.create_job(
        user_id=user_id,
        task_type=task_type,
        file_url=file_url,
        config=config_json,
    )

    # Celery'ye gönder (veya thread fallback)
    dispatch_job(
        job_id=job_id,
        task_type=task_type,
        user_id=user_id,
        plan=plan,
        file_url=file_url,
        config_json=config_json,
    )

    logger.info(f"Job oluşturuldu: {job_id} type={task_type} user={user_id}")
    return JobCreateResponse(
        job_id=job_id,
        status="queued",
        message=f"Job kuyruğa alındı. WebSocket: /ws/jobs/{job_id}",
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(
    job_id: str,
    current_user: dict = Depends(get_current_user),
) -> JobStatusResponse:
    """Job durumu ve preview verisi."""
    job = await supabase_svc.get_job(job_id=job_id, user_id=current_user["id"])
    if not job:
        raise HTTPException(status_code=404, detail="Job bulunamadı.")

    preview = None
    if job.get("status") == "done" and job.get("result_url"):
        preview = await preview_svc.get_preview(job["result_url"])

    return JobStatusResponse(
        job_id=job["id"],
        task_type=job["task_type"],
        status=job["status"],
        progress=job.get("progress", 0),
        logs=job.get("logs", []),
        preview=preview,
        unlock_required=not job.get("unlocked", False),
        created_at=job["created_at"],
    )


@router.get("/jobs")
async def list_jobs(
    current_user: dict = Depends(get_current_user),
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """Kullanıcının job geçmişi."""
    jobs = await supabase_svc.list_jobs(
        user_id=current_user["id"], limit=limit, offset=offset
    )
    return {"jobs": jobs, "total": len(jobs)}


@router.post("/unlock/{job_id}")
async def unlock_job(
    job_id: str,
    body: UnlockRequest,
    current_user: dict = Depends(get_current_user),
) -> dict:
    """Ödeme token'ı ile tam dosyayı aç (tek kullanımlık)."""
    job = await supabase_svc.get_job(job_id=job_id, user_id=current_user["id"])
    if not job:
        raise HTTPException(status_code=404, detail="Job bulunamadı.")

    # Token doğrula + tek kullanımlık işaretle (replay koruması)
    valid = await preview_svc.validate_and_consume_token(
        token=body.token,
        job_id=job_id,
        user_id=current_user["id"],
    )
    if not valid:
        raise HTTPException(
            status_code=400,
            detail="Geçersiz veya zaten kullanılmış token.",
        )

    # Job'ı unlock et
    download_url = await supabase_svc.unlock_job(job_id=job_id)
    logger.info(f"Job unlocked: {job_id} user={current_user['id']}")

    return {
        "job_id": job_id,
        "status": "unlocked",
        "download_url": download_url,
    }


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user)) -> dict:
    """Mevcut kullanıcı bilgisi + plan."""
    monthly_count = await supabase_svc.get_monthly_job_count(current_user["id"])
    return {
        "id": current_user["id"],
        "email": current_user.get("email"),
        "plan": current_user.get("plan", "free"),
        "monthly_jobs_used": monthly_count,
    }
