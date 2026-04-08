"""DataPulse v5 — Excel Birleştirici (scripts/excel/merger.py)

Config:
  file_urls    : ["path/a.xlsx", "path/b.xlsx", ...]
  merge_mode   : "vertical" (satır ekle) | "horizontal" (sütun ekle) | "sheets" (ayrı sheet)
  key_column   : horizontal merge için birleştirme anahtarı
  output_name  : Çıktı dosya adı (opsiyonel)
"""
from __future__ import annotations

import asyncio
import io
import logging
from typing import Callable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class ExcelMerger:
    """Birden fazla Excel dosyasını birleştir."""

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
            logger.info(f"[ExcelMerger] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        file_urls: list[str] = self.config.get("file_urls", [])
        if file_url:
            file_urls = [file_url] + file_urls
        if len(file_urls) < 2:
            raise ValueError("Birleştirme için en az 2 dosya gerekli.")

        merge_mode: str = self.config.get("merge_mode", "vertical")
        key_column: str = self.config.get("key_column", "")

        log(f"{len(file_urls)} dosya {merge_mode} modda birleştirilecek.", 5)

        frames: list[pd.DataFrame] = []
        for i, furl in enumerate(file_urls):
            pct = 10 + int((i / len(file_urls)) * 50)
            log(f"Dosya yükleniyor: {furl.split('/')[-1]}", pct)
            try:
                content = asyncio.run(self._download(furl))
                df = self._load(content, furl)
                frames.append(df)
                log(f"OK: {len(df)} satır, {len(df.columns)} sütun", pct)
            except Exception as e:
                logger.error(f"Dosya yükleme hatası {furl}: {e}")
                log(f"HATA (atlandı): {str(e)[:80]}", pct)

        if not frames:
            raise ValueError("Hiçbir dosya yüklenemedi.")

        log("Birleştirme uygulanıyor...", 65)
        result_df = self._merge(frames, merge_mode, key_column)
        log(f"Birleştirme tamamlandı: {len(result_df)} satır, {len(result_df.columns)} sütun.", 80)

        output_url = self._upload(result_df, frames, merge_mode, user_id, job_id)
        log("Tamamlandı.", 99)

        return {
            "output_url": output_url,
            "row_count": len(result_df),
            "col_count": len(result_df.columns),
            "files_merged": len(frames),
        }

    @staticmethod
    def _merge(frames: list[pd.DataFrame], mode: str, key_col: str) -> pd.DataFrame:
        if mode == "vertical":
            return pd.concat(frames, ignore_index=True)

        elif mode == "horizontal":
            if key_col and all(key_col in df.columns for df in frames):
                result = frames[0]
                for df in frames[1:]:
                    result = result.merge(df, on=key_col, how="outer", suffixes=("", "_dup"))
                return result
            else:
                return pd.concat(frames, axis=1)

        elif mode == "sheets":
            # sheets modunda ana frame sadece ilk dosya
            return frames[0]

        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _load(content: bytes, path: str) -> pd.DataFrame:
        ext = path.rsplit(".", 1)[-1].lower()
        buf = io.BytesIO(content)
        if ext in ("xlsx", "xls"):
            return pd.read_excel(buf)
        return pd.read_csv(buf)

    @staticmethod
    async def _download(file_url: str) -> bytes:
        from backend.services.supabase_service import SupabaseService
        svc = SupabaseService()
        return await svc.download_file(file_url)

    def _upload(
        self,
        result_df: pd.DataFrame,
        all_frames: list[pd.DataFrame],
        mode: str,
        user_id: str,
        job_id: str,
    ) -> str:
        from backend.services.supabase_service import SupabaseService
        buf = io.BytesIO()

        if mode == "sheets":
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                for i, df in enumerate(all_frames):
                    df.to_excel(writer, index=False, sheet_name=f"Dosya_{i+1}"[:31])
        else:
            result_df.to_excel(buf, index=False)

        buf.seek(0)
        svc = SupabaseService()
        return asyncio.run(svc.upload_file(
            user_id=user_id,
            filename=f"merged_{job_id[:8]}.xlsx",
            content=buf.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
