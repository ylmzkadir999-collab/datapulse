"""DataPulse v5 — PDF → Excel Çevirici (scripts/pdf/extractor.py)

Strateji:
  1. pdfplumber → metin tabanlı PDF'ler (ücretsiz, hızlı)
  2. Tesseract OCR → taranmış PDF'ler (görüntü sayfalar)
  3. Claude AI → karmaşık tablo yapıları (gerektiğinde)

Her scriptte zorunlu: class + run() metodu
Hata olsa bile pipeline devam eder (stop_on_error: false)
"""
from __future__ import annotations

import io
import logging
import tempfile
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import pdfplumber
from PIL import Image

from backend.services.ai_service import AIService

logger = logging.getLogger(__name__)


class PDFExtractor:
    """PDF dosyasından Excel/CSV veri çıkarma."""

    def __init__(self, config: dict, plan: str = "starter") -> None:
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
        """
        Ana çalıştırma metodu.

        Args:
            file_url: Supabase storage path
            user_id: Kullanıcı ID
            job_id: Job ID
            progress_callback: (status, pct, log) çağrılabilir

        Returns:
            {"output_url": str, "row_count": int, "page_count": int}
        """
        def log(msg: str, pct: int = 0) -> None:
            logger.info(f"[PDFExtractor] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        log("PDF dosyası indiriliyor...", 10)
        pdf_bytes = self._download(file_url, user_id)

        log("PDF sayfaları analiz ediliyor...", 20)
        all_frames = []
        page_count = 0

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page_count = len(pdf.pages)
            log(f"Toplam sayfa: {page_count}", 25)

            for i, page in enumerate(pdf.pages):
                pct = 25 + int((i / page_count) * 50)
                log(f"Sayfa {i+1}/{page_count} işleniyor...", pct)

                # 1. pdfplumber tablo çıkarma (ücretsiz)
                frames = self._extract_with_pdfplumber(page)

                if not frames:
                    # 2. OCR dene
                    log(f"Sayfa {i+1}: OCR moduna geçiliyor...", pct)
                    frames = self._extract_with_ocr(page)

                if not frames:
                    # 3. Claude AI (son çare)
                    log(f"Sayfa {i+1}: AI analizi...", pct)
                    frames = self._extract_with_ai(page)

                all_frames.extend(frames)

        if not all_frames:
            raise ValueError("PDF'ten hiç veri çıkarılamadı.")

        log("DataFrame birleştiriliyor...", 80)
        df = self._merge_frames(all_frames)
        log(f"Toplam {len(df)} satır çıkarıldı.", 85)

        log("Excel dosyası oluşturuluyor...", 90)
        output_url = self._save_and_upload(df, user_id, job_id)

        log("Tamamlandı.", 99)
        return {
            "output_url": output_url,
            "row_count": len(df),
            "page_count": page_count,
        }

    # ── Extraction Methods ────────────────────────────────────
    def _extract_with_pdfplumber(self, page) -> list[pd.DataFrame]:
        """pdfplumber ile tablo çıkar."""
        try:
            tables = page.extract_tables()
            if not tables:
                return []
            frames = []
            for table in tables:
                if not table or len(table) < 2:
                    continue
                header = [str(c) if c else f"col_{i}" for i, c in enumerate(table[0])]
                rows = table[1:]
                df = pd.DataFrame(rows, columns=header)
                frames.append(df)
            return frames
        except Exception as e:
            logger.debug(f"pdfplumber extract hatası: {e}")
            return []

    def _extract_with_ocr(self, page) -> list[pd.DataFrame]:
        """Tesseract OCR ile tablo çıkar."""
        try:
            import pytesseract

            img = page.to_image(resolution=200).original
            lang = self.config.get("ocr_lang", "tur+eng")
            text = pytesseract.image_to_string(img, lang=lang)

            if not text.strip():
                return []

            # Basit tab/boşluk bazlı parse
            rows = []
            for line in text.splitlines():
                line = line.strip()
                if line:
                    cells = line.split("\t") if "\t" in line else line.split("  ")
                    cells = [c.strip() for c in cells if c.strip()]
                    if cells:
                        rows.append(cells)

            if len(rows) < 2:
                return []

            max_cols = max(len(r) for r in rows)
            header = rows[0] + [f"col_{i}" for i in range(len(rows[0]), max_cols)]
            data = [r + [""] * (max_cols - len(r)) for r in rows[1:]]
            return [pd.DataFrame(data, columns=header[:max_cols])]

        except Exception as e:
            logger.debug(f"OCR extract hatası: {e}")
            return []

    def _extract_with_ai(self, page) -> list[pd.DataFrame]:
        """Claude AI ile karmaşık tablo çıkar."""
        try:
            text = page.extract_text() or ""
            if not text.strip():
                return []

            rows = self._ai.analyze_pdf_table(text_block=text, plan=self.plan)
            if len(rows) < 2:
                return []

            header = rows[0]
            data = rows[1:]
            return [pd.DataFrame(data, columns=header)]
        except Exception as e:
            logger.warning(f"AI extract hatası: {e}")
            return []

    # ── Helpers ───────────────────────────────────────────────
    @staticmethod
    def _merge_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
        """Birden fazla DataFrame'i birleştir."""
        if not frames:
            return pd.DataFrame()
        if len(frames) == 1:
            return frames[0]
        # Aynı sütunlara sahip olanları concat et
        try:
            return pd.concat(frames, ignore_index=True)
        except Exception:
            return frames[0]

    def _download(self, file_url: Optional[str], user_id: str) -> bytes:
        """Storage'dan dosya indir."""
        if not file_url:
            raise ValueError("file_url zorunlu.")
        from backend.services.supabase_service import SupabaseService
        import asyncio
        svc = SupabaseService()
        return asyncio.run(svc.download_file(file_url))

    def _save_and_upload(
        self, df: pd.DataFrame, user_id: str, job_id: str
    ) -> str:
        """DataFrame'i Excel olarak kaydet ve Storage'a yükle."""
        from backend.services.supabase_service import SupabaseService
        import asyncio
        import uuid

        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        buf.seek(0)

        svc = SupabaseService()
        filename = f"pdf2excel_{job_id[:8]}.xlsx"
        return asyncio.run(svc.upload_file(
            user_id=user_id,
            filename=filename,
            content=buf.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
