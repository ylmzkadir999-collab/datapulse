"""DataPulse v5 — Celery Job Sistemi (worker/tasks.py)

8 görev tipi:
  pdf2excel, scrape, email_scrape, clean,
  bulk_clean, excel_merge, sheets_sync, full_pipeline

Celery yoksa ThreadPoolExecutor fallback.
Redis'te job state saklanır (restart-safe).
"""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

logger = logging.getLogger(__name__)

# ── Celery kurulumu (graceful fallback) ───────────────────────
try:
    from celery import Celery
    from backend.core.config import settings

    celery_app = Celery(
        "datapulse",
        broker=settings.REDIS_URL,
        backend=settings.REDIS_URL,
    )
    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="Europe/Istanbul",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,          # restart-safe
        worker_prefetch_multiplier=1,
        result_expires=86400,         # 24 saat
    )
    CELERY_AVAILABLE = True
    logger.info("Celery başlatıldı")
except Exception as e:
    logger.warning(f"Celery başlatılamadı ({e}) — thread fallback aktif")
    celery_app = None
    CELERY_AVAILABLE = False

# Thread pool for fallback
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="datapulse")


# ── Job runner core ───────────────────────────────────────────
def _run_job_sync(
    job_id: str,
    task_type: str,
    user_id: str,
    plan: str,
    file_url: Optional[str],
    config_json: Optional[str],
) -> dict:
    """
    Senkron job runner (Celery task içinden çağrılır).
    Her script class + run() metoduna sahiptir.
    stop_on_error=False → bir adım hata verse bile pipeline devam eder.
    """
    import json
    from backend.services.supabase_service import SupabaseService
    from backend.services.telegram_service import TelegramService

    svc = SupabaseService()
    tg = TelegramService()
    config = json.loads(config_json) if config_json else {}

    def update(status: str, progress: int, log: str = "") -> None:
        asyncio.run(svc.update_job(
            job_id,
            status=status,
            progress=progress,
        ))
        if log:
            asyncio.run(svc.append_log(job_id, log))
        logger.info(f"[{job_id[:8]}] {status} {progress}% {log}")

    try:
        update("running", 5, f"Görev başlatıldı: {task_type}")
        result = _dispatch_to_script(
            task_type=task_type,
            job_id=job_id,
            user_id=user_id,
            plan=plan,
            file_url=file_url,
            config=config,
            update_fn=update,
        )
        update("done", 100, f"Tamamlandı. Satır: {result.get('row_count', '?')}")

        # Sonucu kaydet
        asyncio.run(svc.update_job(
            job_id,
            status="done",
            progress=100,
            result_url=result.get("output_url"),
        ))

        asyncio.run(tg.send_job_done(
            job_id=job_id,
            task_type=task_type,
            user_id=user_id,
            row_count=result.get("row_count"),
        ))
        return result

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Job hatası [{job_id[:8]}]: {error_msg}", exc_info=True)
        asyncio.run(svc.update_job(job_id, status="failed", progress=0))
        asyncio.run(svc.append_log(job_id, f"HATA: {error_msg}"))
        asyncio.run(tg.send_job_failed(
            job_id=job_id,
            task_type=task_type,
            user_id=user_id,
            error=error_msg,
        ))
        return {"error": error_msg}


def _dispatch_to_script(
    task_type: str,
    job_id: str,
    user_id: str,
    plan: str,
    file_url: Optional[str],
    config: dict,
    update_fn,
) -> dict:
    """task_type'a göre script modülünü çağır."""

    if task_type == "pdf2excel":
        from scripts.pdf.extractor import PDFExtractor
        runner = PDFExtractor(config=config, plan=plan)

    elif task_type == "scrape":
        from scripts.scraping.scraper import WebScraper
        runner = WebScraper(config=config, plan=plan)

    elif task_type == "email_scrape":
        from scripts.scraping.email_scraper import EmailScraper
        runner = EmailScraper(config=config, plan=plan)

    elif task_type == "clean":
        from scripts.cleaning.cleaner import DataCleaner
        runner = DataCleaner(config=config, plan=plan)

    elif task_type == "bulk_clean":
        from scripts.cleaning.bulk_cleaner import BulkCleaner
        runner = BulkCleaner(config=config, plan=plan)

    elif task_type == "excel_merge":
        from scripts.excel.merger import ExcelMerger
        runner = ExcelMerger(config=config, plan=plan)

    elif task_type == "sheets_sync":
        from scripts.sheets.sync import SheetsSync
        runner = SheetsSync(config=config, plan=plan)

    elif task_type == "full_pipeline":
        from scripts.pipeline import FullPipeline
        runner = FullPipeline(config=config, plan=plan)

    else:
        raise ValueError(f"Bilinmeyen task_type: {task_type}")

    update_fn("running", 15, f"Script yüklendi: {task_type}")

    # Her script run() metoduna sahip
    result = runner.run(
        file_url=file_url,
        user_id=user_id,
        job_id=job_id,
        progress_callback=update_fn,
    )
    return result


# ── Celery Tasks ──────────────────────────────────────────────
if CELERY_AVAILABLE and celery_app:

    @celery_app.task(
        bind=True,
        name="datapulse.run_job",
        max_retries=2,
        default_retry_delay=30,
        soft_time_limit=600,  # 10 dakika
        time_limit=660,
    )
    def celery_run_job(
        self,
        job_id: str,
        task_type: str,
        user_id: str,
        plan: str,
        file_url: Optional[str],
        config_json: Optional[str],
    ) -> dict:
        """Celery task — tüm job tiplerini çalıştırır."""
        try:
            return _run_job_sync(
                job_id=job_id,
                task_type=task_type,
                user_id=user_id,
                plan=plan,
                file_url=file_url,
                config_json=config_json,
            )
        except Exception as exc:
            raise self.retry(exc=exc)


# ── dispatch_job — public API ─────────────────────────────────
def dispatch_job(
    job_id: str,
    task_type: str,
    user_id: str,
    plan: str,
    file_url: Optional[str] = None,
    config_json: Optional[str] = None,
) -> None:
    """
    Job'ı kuyruğa al.
    Celery varsa → Celery kuyruğu
    Celery yoksa → ThreadPoolExecutor fallback
    """
    kwargs = dict(
        job_id=job_id,
        task_type=task_type,
        user_id=user_id,
        plan=plan,
        file_url=file_url,
        config_json=config_json,
    )

    if CELERY_AVAILABLE and celery_app:
        celery_run_job.apply_async(kwargs=kwargs)
        logger.info(f"Job Celery kuyruğuna alındı: {job_id[:8]} type={task_type}")
    else:
        # Thread fallback
        _executor.submit(_run_job_sync, **kwargs)
        logger.info(f"Job thread'e gönderildi: {job_id[:8]} type={task_type}")
