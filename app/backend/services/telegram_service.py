"""DataPulse v5 — Telegram Bildirim Servisi (services/telegram_service.py)

Job tamamlandığında / hata verdiğinde / ödeme alındığında
bot aracılığıyla Telegram kanalına bildirim gönderir.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from backend.core.config import settings

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramService:
    """Telegram Bot API ile bildirim gönder."""

    def __init__(self) -> None:
        self._token = settings.TELEGRAM_BOT_TOKEN
        self._chat_id = settings.TELEGRAM_CHAT_ID
        self._enabled = bool(self._token and self._chat_id)
        if not self._enabled:
            logger.info("Telegram bildirim devre dışı (TOKEN/CHAT_ID eksik)")

    async def _send(self, text: str, parse_mode: str = "Markdown") -> bool:
        """Telegram'a mesaj gönder."""
        if not self._enabled:
            return False
        url = TELEGRAM_API.format(token=self._token)
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                return True
        except Exception as e:
            logger.warning(f"Telegram gönderme hatası: {e}")
            return False

    # ── Bildirim şablonları ───────────────────────────────────
    async def send_job_done(
        self,
        job_id: str,
        task_type: str,
        user_id: str,
        row_count: Optional[int] = None,
    ) -> bool:
        rows_info = f"\n📊 *Satır:* {row_count}" if row_count else ""
        text = (
            f"✅ *DataPulse v5 — Job Tamamlandı*\n\n"
            f"🆔 `{job_id[:8]}...`\n"
            f"🔧 *Görev:* `{task_type}`\n"
            f"👤 *User:* `{user_id[:8]}...`"
            f"{rows_info}"
        )
        return await self._send(text)

    async def send_job_failed(
        self,
        job_id: str,
        task_type: str,
        user_id: str,
        error: str,
    ) -> bool:
        text = (
            f"❌ *DataPulse v5 — Job Hata*\n\n"
            f"🆔 `{job_id[:8]}...`\n"
            f"🔧 *Görev:* `{task_type}`\n"
            f"👤 *User:* `{user_id[:8]}...`\n"
            f"⚠️ *Hata:* `{error[:200]}`"
        )
        return await self._send(text)

    async def send_payment_success(
        self,
        job_id: str,
        task_type: str,
        user_id: str,
    ) -> bool:
        text = (
            f"💰 *DataPulse v5 — Ödeme Alındı*\n\n"
            f"🆔 `{job_id[:8]}...`\n"
            f"🔧 *Görev:* `{task_type}`\n"
            f"👤 *User:* `{user_id[:8]}...`"
        )
        return await self._send(text)

    async def send_system_alert(self, message: str) -> bool:
        text = f"⚠️ *DataPulse v5 — Sistem Uyarısı*\n\n{message}"
        return await self._send(text)
