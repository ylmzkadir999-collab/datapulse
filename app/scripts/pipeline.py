"""DataPulse v5 — Full Pipeline Orkestrasyonu (scripts/pipeline.py)

8 görev tipinin tümünü sırayla çalıştırır.
stop_on_error: false → bir adım hata verse bile devam eder.

Config:
  steps: [
    {"task": "pdf2excel", "config": {...}},
    {"task": "clean",     "config": {...}},
    {"task": "sheets_sync", "config": {...}},
    ...
  ]
  stop_on_error: false (varsayılan)
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Optional

from backend.services.ai_service import AIService

logger = logging.getLogger(__name__)

STEP_MAP = {
    "pdf2excel": ("scripts.pdf.extractor", "PDFExtractor"),
    "scrape": ("scripts.scraping.scraper", "WebScraper"),
    "email_scrape": ("scripts.scraping.email_scraper", "EmailScraper"),
    "clean": ("scripts.cleaning.cleaner", "DataCleaner"),
    "bulk_clean": ("scripts.cleaning.bulk_cleaner", "BulkCleaner"),
    "excel_merge": ("scripts.excel.merger", "ExcelMerger"),
    "sheets_sync": ("scripts.sheets.sync", "SheetsSync"),
}


class FullPipeline:
    """Çok adımlı pipeline — hata toleranslı."""

    def __init__(self, config: dict, plan: str = "pro") -> None:
        self.config = config
        self.plan = plan
        self._ai = AIService()

    def run(
        self,
        file_url: Optional[str],
        user_id: str,
        job_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> dict:
        def log(msg: str, pct: int = 0) -> None:
            logger.info(f"[Pipeline] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        steps: list[dict] = self.config.get("steps", [])
        stop_on_error: bool = self.config.get("stop_on_error", False)

        if not steps:
            raise ValueError("Config'de 'steps' listesi zorunlu.")

        log(f"Pipeline başlıyor: {len(steps)} adım.", 5)

        results: list[dict] = []
        current_file_url = file_url
        total_steps = len(steps)

        for i, step in enumerate(steps):
            task_type = step.get("task")
            step_config = step.get("config", {})
            step_start = time.time()

            base_pct = 10 + int((i / total_steps) * 80)
            log(f"Adım {i+1}/{total_steps}: {task_type}", base_pct)

            if task_type not in STEP_MAP:
                err = f"Bilinmeyen adım tipi: {task_type}"
                logger.error(err)
                results.append({
                    "step": i + 1,
                    "task": task_type,
                    "status": "error",
                    "error": err,
                    "duration_s": 0,
                })
                if stop_on_error:
                    break
                continue

            try:
                module_path, class_name = STEP_MAP[task_type]
                import importlib
                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)

                # Her adım bir öncekinin çıktısını kullanır
                merged_config = {**step_config}
                if current_file_url:
                    merged_config.setdefault("input_file_url", current_file_url)

                runner = cls(config=merged_config, plan=self.plan)

                def step_progress(status: str, pct: int, msg: str = "") -> None:
                    adjusted = base_pct + int(pct * 0.8 / total_steps)
                    log(f"  [{task_type}] {msg}", adjusted)

                result = runner.run(
                    file_url=current_file_url,
                    user_id=user_id,
                    job_id=f"{job_id}_step{i}",
                    progress_callback=step_progress,
                )

                duration = round(time.time() - step_start, 2)
                results.append({
                    "step": i + 1,
                    "task": task_type,
                    "status": "ok",
                    "row_count": result.get("row_count"),
                    "output_url": result.get("output_url"),
                    "duration_s": duration,
                })

                # Bir sonraki adım bu çıktıyı kullanır
                if result.get("output_url"):
                    current_file_url = result["output_url"]

                log(f"Adım {i+1} OK ({duration}s): {result.get('row_count', '?')} satır", base_pct + 5)

            except Exception as e:
                duration = round(time.time() - step_start, 2)
                error_msg = str(e)
                logger.error(f"Pipeline adım hatası [{task_type}]: {error_msg}", exc_info=True)
                results.append({
                    "step": i + 1,
                    "task": task_type,
                    "status": "error",
                    "error": error_msg,
                    "duration_s": duration,
                })
                log(f"Adım {i+1} HATA (atlandı): {error_msg[:80]}", base_pct)

                if stop_on_error:
                    log("stop_on_error=true, pipeline durduruluyor.", base_pct)
                    break

        # AI özeti (Pro plan)
        summary = ""
        if self.plan == "pro":
            try:
                summary = self._ai.generate_pipeline_summary(results, plan="pro")
            except Exception as e:
                logger.warning(f"Pipeline özeti oluşturulamadı: {e}")

        ok_steps = sum(1 for r in results if r["status"] == "ok")
        total_rows = sum(r.get("row_count") or 0 for r in results)

        log(f"Pipeline tamamlandı: {ok_steps}/{len(results)} adım başarılı.", 99)

        return {
            "output_url": current_file_url,
            "row_count": total_rows,
            "steps_total": len(steps),
            "steps_ok": ok_steps,
            "steps_failed": len(results) - ok_steps,
            "step_results": results,
            "summary": summary,
        }
