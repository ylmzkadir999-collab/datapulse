"""DataPulse v5 — Supabase Auth + DB + Storage (services/supabase_service.py)

İşlevler:
  - JWT doğrulama (get_current_user dependency)
  - Job CRUD (create, get, list, update)
  - Dosya yükleme / signed URL
  - Aylık job sayacı
  - Unlock token yönetimi
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from supabase import Client, create_client

from backend.core.config import settings

logger = logging.getLogger(__name__)
bearer_scheme = HTTPBearer(auto_error=True)


class SupabaseService:
    """Supabase Client wrapper — singleton pattern."""

    def __init__(self) -> None:
        self._client: Optional[Client] = None
        self._service_client: Optional[Client] = None
        if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
            logger.warning("Supabase URL/KEY eksik — demo modunda çalışıyor")
            return
        try:
            self._client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
            self._service_client = create_client(
                settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY
            )
            logger.info("Supabase bağlantısı kuruldu.")
        except Exception as e:
            # Hatalı key ile app crash olmayacak — /health endpoint çalışmaya devam eder
            logger.error(f"Supabase başlatılamadı: {e} — demo modunda çalışıyor")
            self._client = None
            self._service_client = None

    def _db(self) -> Client:
        if not self._service_client:
            raise RuntimeError("Supabase bağlantısı yapılandırılmamış.")
        return self._service_client

    # ── Auth ─────────────────────────────────────────────────
    def verify_jwt(self, token: str) -> dict:
        """Supabase JWT token'ı doğrula, user dict döndür."""
        if not self._client:
            # Demo mode — geliştirme ortamı
            return {"id": "demo-user", "email": "demo@datapulse.io", "plan": "pro"}
        try:
            resp = self._client.auth.get_user(token)
            user = resp.user
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Geçersiz token.",
                )
            # Kullanıcı profilini çek (plan bilgisi)
            profile = (
                self._db()
                .table("profiles")
                .select("plan")
                .eq("id", str(user.id))
                .maybe_single()
                .execute()
            )
            plan = (profile.data or {}).get("plan", "free")
            return {
                "id": str(user.id),
                "email": user.email,
                "plan": plan,
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"JWT doğrulama hatası: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token doğrulanamadı.",
            )

    # ── Job CRUD ─────────────────────────────────────────────
    async def create_job(
        self,
        user_id: str,
        task_type: str,
        file_url: Optional[str],
        config: Optional[str],
    ) -> str:
        """Yeni job kaydı oluştur, job_id döndür."""
        job_id = str(uuid.uuid4())
        row = {
            "id": job_id,
            "user_id": user_id,
            "task_type": task_type,
            "status": "queued",
            "progress": 0,
            "logs": [],
            "file_url": file_url,
            "config": config,
            "unlocked": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._db().table("jobs").insert(row).execute()
        return job_id

    async def get_job(self, job_id: str, user_id: str) -> Optional[dict]:
        """Kullanıcıya ait job'ı getir (RLS)."""
        result = (
            self._db()
            .table("jobs")
            .select("*")
            .eq("id", job_id)
            .eq("user_id", user_id)
            .maybe_single()
            .execute()
        )
        return result.data

    async def get_job_by_id(self, job_id: str) -> Optional[dict]:
        """Admin: job_id ile direkt getir (webhook için)."""
        result = (
            self._db()
            .table("jobs")
            .select("*")
            .eq("id", job_id)
            .maybe_single()
            .execute()
        )
        return result.data

    async def list_jobs(self, user_id: str, limit: int = 20, offset: int = 0) -> list:
        result = (
            self._db()
            .table("jobs")
            .select("id,task_type,status,progress,created_at,unlocked")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return result.data or []

    async def update_job(self, job_id: str, **kwargs) -> None:
        """Job alanlarını güncelle (status, progress, logs, result_url...)."""
        self._db().table("jobs").update(kwargs).eq("id", job_id).execute()

    async def append_log(self, job_id: str, log_entry: str) -> None:
        """Job log listesine satır ekle."""
        current = await self.get_job_by_id(job_id)
        logs = (current or {}).get("logs", [])
        logs.append(log_entry)
        await self.update_job(job_id, logs=logs)

    async def unlock_job(self, job_id: str) -> str:
        """Job'ı unlock et, signed download URL döndür."""
        job = await self.get_job_by_id(job_id)
        if not job or not job.get("result_url"):
            raise ValueError("Job result_url yok.")

        await self.update_job(job_id, unlocked=True)

        # Signed URL oluştur (1 saat geçerli)
        signed = self._db().storage.from_(settings.SUPABASE_BUCKET).create_signed_url(
            job["result_url"], 3600
        )
        return signed.get("signedURL", "")

    async def set_payment_token(self, job_id: str, token: str) -> None:
        """Ödeme unlock token'ını job'a kaydet."""
        await self.update_job(job_id, payment_token=token)

    # ── Storage ───────────────────────────────────────────────
    async def upload_file(
        self,
        user_id: str,
        filename: str,
        content: bytes,
        content_type: str,
    ) -> str:
        """Dosyayı Supabase Storage'a yükle, path döndür."""
        path = f"{user_id}/{uuid.uuid4()}_{filename}"
        self._db().storage.from_(settings.SUPABASE_BUCKET).upload(
            path=path,
            file=content,
            file_options={"content-type": content_type},
        )
        return path

    async def download_file(self, path: str) -> bytes:
        """Storage'dan dosya indir."""
        return self._db().storage.from_(settings.SUPABASE_BUCKET).download(path)

    # ── Counters ─────────────────────────────────────────────
    async def get_monthly_job_count(self, user_id: str) -> int:
        """Bu ay kullanıcının kaç job'ı var?"""
        now = datetime.now(timezone.utc)
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0).isoformat()
        result = (
            self._db()
            .table("jobs")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .gte("created_at", start_of_month)
            .execute()
        )
        return result.count or 0


# ── FastAPI Dependency ────────────────────────────────────────
_supabase_svc = SupabaseService()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    """JWT Bearer token'dan user dict çıkar (FastAPI Depends)."""
    return _supabase_svc.verify_jwt(credentials.credentials)
