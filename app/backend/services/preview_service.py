"""DataPulse v5 — Filigran & Unlock Token Sistemi (services/preview_service.py)

"Kazık yeme koruması":
  1. İlk 10 satır: gerçek veri (ücretsiz preview)
  2. Kalan satırlar: ████████ ile maskelenir
  3. Ödeme → create_unlock_token() → tek kullanımlık UUID
  4. validate_and_consume_token() → replay koruması (kullanıldı mı?)
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import pandas as pd
from supabase import Client, create_client

from backend.core.config import settings

logger = logging.getLogger(__name__)

WATERMARK = "████████"


class PreviewService:
    """Filigran üretimi ve unlock token yönetimi."""

    def __init__(self) -> None:
        if settings.SUPABASE_URL and settings.SUPABASE_SERVICE_KEY:
            self._db: Optional[Client] = create_client(
                settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY
            )
        else:
            self._db = None
            logger.warning("Supabase bağlantısı yok — PreviewService demo modda")

    # ── Preview ───────────────────────────────────────────────
    def build_preview(self, df: pd.DataFrame) -> dict:
        """
        DataFrame'den preview dict oluştur.

        Returns:
            {
                "columns": [...],
                "preview_rows": [...],   # İlk PREVIEW_ROWS gerçek
                "masked_rows": [...],    # Kalan satırlar filigranla
                "total_rows": int,
                "preview_count": int,
            }
        """
        n = settings.PREVIEW_ROWS
        total = len(df)
        columns = list(df.columns)

        # İlk n satır: gerçek veri
        preview_df = df.head(n)
        preview_rows = preview_df.to_dict(orient="records")

        # Geri kalan: her hücre maskelenmiş
        masked_count = max(0, total - n)
        masked_rows = [
            {col: WATERMARK for col in columns}
            for _ in range(masked_count)
        ]

        return {
            "columns": columns,
            "preview_rows": preview_rows,
            "masked_rows": masked_rows,
            "total_rows": total,
            "preview_count": len(preview_rows),
            "masked_count": masked_count,
        }

    async def get_preview(self, result_url: str) -> dict:
        """Storage'daki sonuç dosyasından preview oluştur."""
        if not self._db:
            return {"error": "Supabase bağlantısı yok"}
        try:
            file_bytes = self._db.storage.from_(settings.SUPABASE_BUCKET).download(
                result_url
            )
            df = self._load_df(result_url, file_bytes)
            return self.build_preview(df)
        except Exception as e:
            logger.error(f"Preview oluşturma hatası: {e}")
            return {"error": str(e)}

    @staticmethod
    def _load_df(path: str, content: bytes) -> pd.DataFrame:
        """Dosya uzantısına göre DataFrame yükle."""
        buf = BytesIO(content)
        ext = path.rsplit(".", 1)[-1].lower()
        if ext in ("xlsx", "xls"):
            return pd.read_excel(buf)
        elif ext == "csv":
            return pd.read_csv(buf)
        else:
            return pd.read_csv(buf)

    # ── Watermark helpers ─────────────────────────────────────
    @staticmethod
    def mask_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Tüm DataFrame'i maskele (indirme engeli)."""
        n = settings.PREVIEW_ROWS
        masked = df.copy()
        if len(masked) > n:
            masked.iloc[n:] = WATERMARK
        return masked

    @staticmethod
    def watermark_excel(df: pd.DataFrame, output: BytesIO) -> None:
        """Preview Excel dosyası yaz: ilk N gerçek, geri kalan filigranla."""
        n = settings.PREVIEW_ROWS
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            preview = df.head(n)
            preview.to_excel(writer, index=False, sheet_name="Preview")
            if len(df) > n:
                masked_data = {col: [WATERMARK] * (len(df) - n) for col in df.columns}
                masked = pd.DataFrame(masked_data)
                masked.to_excel(writer, index=False, sheet_name="Locked")

    # ── Unlock Token ──────────────────────────────────────────
    async def create_unlock_token(self, job_id: str, user_id: str) -> str:
        """
        Tek kullanımlık unlock token oluştur.
        Supabase 'unlock_tokens' tablosuna kaydet.
        """
        token = str(uuid.uuid4())
        # Token'ı hashleyerek sakla (raw token DB'de olmayacak)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        if self._db:
            self._db.table("unlock_tokens").insert({
                "token_hash": token_hash,
                "job_id": job_id,
                "user_id": user_id,
                "used": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }).execute()

        logger.info(f"Unlock token oluşturuldu: job={job_id}")
        return token  # Raw token kullanıcıya gönderilir

    async def validate_and_consume_token(
        self,
        token: str,
        job_id: str,
        user_id: str,
    ) -> bool:
        """
        Token'ı doğrula ve tek seferlik işaretle (replay koruması).

        Returns:
            True: Token geçerli ve ilk kullanım
            False: Token geçersiz, kullanılmış veya bulunamadı
        """
        if not self._db:
            # Demo mod: token doğrulaması atla
            logger.warning("Demo modda token doğrulaması atlandı")
            return True

        token_hash = hashlib.sha256(token.encode()).hexdigest()

        # Token kaydını bul
        result = (
            self._db.table("unlock_tokens")
            .select("*")
            .eq("token_hash", token_hash)
            .eq("job_id", job_id)
            .eq("user_id", user_id)
            .eq("used", False)
            .maybe_single()
            .execute()
        )

        if not result.data:
            logger.warning(
                f"Token doğrulama başarısız: job={job_id} user={user_id}"
            )
            return False

        # Tek kullanımlık: kullanıldı olarak işaretle
        self._db.table("unlock_tokens").update({
            "used": True,
            "used_at": datetime.now(timezone.utc).isoformat(),
        }).eq("token_hash", token_hash).execute()

        logger.info(f"Token kullanıldı: job={job_id}")
        return True
