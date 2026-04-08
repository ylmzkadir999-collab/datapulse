"""DataPulse v5 — Veri Temizleyici (scripts/cleaning/cleaner.py)

Pandas-first → gerekirse Claude AI
Config:
  drop_duplicates  : true
  drop_empty_cols  : true
  strip_strings    : true
  fix_dates        : ["tarih", "date"] sütun adları
  ai_instructions  : "Özel temizlik talimatları"
"""
from __future__ import annotations

import asyncio
import io
import logging
from typing import Callable, Optional

import pandas as pd

from backend.services.ai_service import AIService

logger = logging.getLogger(__name__)


class DataCleaner:
    """Tek dosya veri temizleme — pandas-first, AI destekli."""

    def __init__(self, config: dict, plan: str = "free") -> None:
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
            logger.info(f"[DataCleaner] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        log("Dosya indiriliyor...", 10)
        raw_bytes = self._download(file_url, user_id)

        log("Veri yükleniyor...", 20)
        df = self._load(raw_bytes, file_url or "data.csv")
        original_rows = len(df)
        log(f"{original_rows} satır yüklendi.", 25)

        # ── 1. Pandas temizlik ($0) ───────────────────────────
        if self.config.get("drop_empty_cols", True):
            before = df.shape[1]
            df.dropna(axis=1, how="all", inplace=True)
            log(f"Boş sütunlar silindi: {before - df.shape[1]} sütun", 35)

        if self.config.get("drop_duplicates", True):
            before = len(df)
            df.drop_duplicates(inplace=True)
            log(f"Yineleneler silindi: {before - len(df)} satır", 45)

        if self.config.get("strip_strings", True):
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].str.strip()
            log("String sütunlar temizlendi.", 50)

        date_cols = self.config.get("fix_dates", [])
        for col in date_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    log(f"Tarih sütunu düzeltildi: {col}", 55)
                except Exception as e:
                    logger.debug(f"Tarih dönüşüm hatası {col}: {e}")

        # ── 2. AI temizlik (özel talimat veya karmaşık veri) ──
        ai_instructions = self.config.get("ai_instructions", "")
        if ai_instructions or self.config.get("use_ai", False):
            log("AI temizliği uygulanıyor...", 60)
            try:
                df = self._ai.clean_data(df, plan=self.plan, instructions=ai_instructions)
                log("AI temizliği tamamlandı.", 75)
            except Exception as e:
                logger.warning(f"AI temizlik hatası, pandas sonucu kullanılıyor: {e}")

        log(f"Temizleme tamamlandı. {len(df)}/{original_rows} satır kaldı.", 85)

        output_url = self._upload(df, user_id, job_id)
        log("Tamamlandı.", 99)
        return {
            "output_url": output_url,
            "row_count": len(df),
            "original_row_count": original_rows,
            "removed_rows": original_rows - len(df),
        }

    @staticmethod
    def _load(content: bytes, path: str) -> pd.DataFrame:
        ext = path.rsplit(".", 1)[-1].lower()
        buf = io.BytesIO(content)
        if ext in ("xlsx", "xls"):
            return pd.read_excel(buf)
        return pd.read_csv(buf)

    def _download(self, file_url: Optional[str], user_id: str) -> bytes:
        if not file_url:
            raise ValueError("file_url zorunlu.")
        from backend.services.supabase_service import SupabaseService
        svc = SupabaseService()
        return asyncio.run(svc.download_file(file_url))

    def _upload(self, df: pd.DataFrame, user_id: str, job_id: str) -> str:
        from backend.services.supabase_service import SupabaseService
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)
        svc = SupabaseService()
        return asyncio.run(svc.upload_file(
            user_id=user_id,
            filename=f"cleaned_{job_id[:8]}.xlsx",
            content=buf.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
