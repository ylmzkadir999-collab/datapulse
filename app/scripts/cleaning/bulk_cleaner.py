"""DataPulse v5 — Toplu Veri Temizleyici (scripts/cleaning/bulk_cleaner.py)

Birden fazla dosyayı aynı konfigürasyonla temizler.
Config:
  file_urls      : ["path/file1.xlsx", "path/file2.csv", ...]
  merge_output   : true → tek Excel çıktısı
  cleaner_config : DataCleaner'a geçirilecek config
"""
from __future__ import annotations

import asyncio
import io
import logging
from typing import Callable, Optional

import pandas as pd

from scripts.cleaning.cleaner import DataCleaner

logger = logging.getLogger(__name__)


class BulkCleaner:
    """Çoklu dosya toplu temizleme."""

    def __init__(self, config: dict, plan: str = "starter") -> None:
        self.config = config
        self.plan = plan

    def run(
        self,
        file_url: Optional[str],
        user_id: str,
        job_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> dict:
        def log(msg: str, pct: int = 0) -> None:
            logger.info(f"[BulkCleaner] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        # Config'den dosya listesi al
        file_urls: list[str] = self.config.get("file_urls", [])
        if file_url:
            file_urls = [file_url] + file_urls

        if not file_urls:
            raise ValueError("Temizlenecek dosya bulunamadı.")

        merge_output: bool = self.config.get("merge_output", False)
        cleaner_config: dict = self.config.get("cleaner_config", {})

        log(f"Toplam {len(file_urls)} dosya temizlenecek.", 5)

        cleaned_frames: list[pd.DataFrame] = []
        results: list[dict] = []
        total = len(file_urls)

        for i, furl in enumerate(file_urls):
            pct = 10 + int((i / total) * 70)
            log(f"Dosya {i+1}/{total}: {furl.split('/')[-1]}", pct)

            try:
                # Her dosya için DataCleaner çalıştır
                sub_job_id = f"{job_id}_{i}"
                cleaner = DataCleaner(config=cleaner_config, plan=self.plan)

                # Direkt pandas üzerinden çalış (upload etme, merge için)
                raw = asyncio.run(self._download(furl))
                ext = furl.rsplit(".", 1)[-1].lower()
                df = self._load(raw, ext)

                original_len = len(df)

                # Pandas temizlik inline
                if cleaner_config.get("drop_empty_cols", True):
                    df.dropna(axis=1, how="all", inplace=True)
                if cleaner_config.get("drop_duplicates", True):
                    df.drop_duplicates(inplace=True)
                if cleaner_config.get("strip_strings", True):
                    for col in df.select_dtypes(include="object").columns:
                        df[col] = df[col].str.strip()

                df["_source_file"] = furl.split("/")[-1]
                cleaned_frames.append(df)
                results.append({
                    "file": furl.split("/")[-1],
                    "original_rows": original_len,
                    "cleaned_rows": len(df),
                    "status": "ok",
                })
                log(f"OK: {original_len} → {len(df)} satır", pct)

            except Exception as e:
                # stop_on_error: false — hata olsa devam
                logger.error(f"Dosya temizleme hatası {furl}: {e}", exc_info=True)
                results.append({
                    "file": furl.split("/")[-1],
                    "status": "error",
                    "error": str(e),
                })
                log(f"HATA (atlandı): {str(e)[:80]}", pct)

        if not cleaned_frames:
            raise ValueError("Hiçbir dosya temizlenemedi.")

        log("Çıktı oluşturuluyor...", 85)
        output_url = self._save_output(
            frames=cleaned_frames,
            merge=merge_output,
            user_id=user_id,
            job_id=job_id,
        )

        total_rows = sum(len(f) for f in cleaned_frames)
        log(f"Tamamlandı. Toplam {total_rows} satır.", 99)

        return {
            "output_url": output_url,
            "row_count": total_rows,
            "files_processed": len(cleaned_frames),
            "files_failed": total - len(cleaned_frames),
            "details": results,
        }

    @staticmethod
    def _load(content: bytes, ext: str) -> pd.DataFrame:
        buf = io.BytesIO(content)
        if ext in ("xlsx", "xls"):
            return pd.read_excel(buf)
        return pd.read_csv(buf)

    @staticmethod
    async def _download(file_url: str) -> bytes:
        from backend.services.supabase_service import SupabaseService
        svc = SupabaseService()
        return await svc.download_file(file_url)

    def _save_output(
        self,
        frames: list[pd.DataFrame],
        merge: bool,
        user_id: str,
        job_id: str,
    ) -> str:
        from backend.services.supabase_service import SupabaseService
        buf = io.BytesIO()

        if merge:
            combined = pd.concat(frames, ignore_index=True)
            combined.to_excel(buf, index=False)
        else:
            # Her dosya ayrı sheet
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                for i, df in enumerate(frames):
                    sheet_name = f"Dosya_{i+1}"[:31]
                    df.to_excel(writer, index=False, sheet_name=sheet_name)

        buf.seek(0)
        svc = SupabaseService()
        return asyncio.run(svc.upload_file(
            user_id=user_id,
            filename=f"bulk_cleaned_{job_id[:8]}.xlsx",
            content=buf.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
