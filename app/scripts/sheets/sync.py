"""DataPulse v5 — Google Sheets Sync (scripts/sheets/sync.py)

Config:
  spreadsheet_id   : Google Sheets ID
  sheet_name       : Hedef sheet adı (varsayılan: "Sheet1")
  mode             : "append" | "overwrite"
  credentials_json : Service account JSON (string olarak)
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
from typing import Callable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SheetsSync:
    """Veriyi Google Sheets'e senkronize et."""

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
            logger.info(f"[SheetsSync] {msg}")
            if progress_callback:
                progress_callback("running", pct, msg)

        spreadsheet_id = self.config.get("spreadsheet_id")
        if not spreadsheet_id:
            raise ValueError("Config'de 'spreadsheet_id' zorunlu.")

        sheet_name = self.config.get("sheet_name", "Sheet1")
        mode = self.config.get("mode", "append")
        credentials_json = self.config.get("credentials_json", "")

        log("Veri dosyası indiriliyor...", 10)
        if not file_url:
            raise ValueError("file_url zorunlu.")

        content = asyncio.run(self._download(file_url))
        df = self._load(content, file_url)
        log(f"{len(df)} satır, {len(df.columns)} sütun yüklendi.", 25)

        log("Google Sheets bağlantısı kuruluyor...", 35)
        gc = self._get_client(credentials_json)

        log(f"Spreadsheet açılıyor: {spreadsheet_id[:20]}...", 45)
        try:
            sh = gc.open_by_key(spreadsheet_id)
            try:
                ws = sh.worksheet(sheet_name)
            except Exception:
                ws = sh.add_worksheet(title=sheet_name, rows=10000, cols=50)

            log(f"Sheet: '{sheet_name}', mode={mode}", 55)

            if mode == "overwrite":
                ws.clear()
                log("Sheet temizlendi.", 60)

            # DataFrame → list of lists
            df_str = df.fillna("").astype(str)
            if mode == "overwrite" or ws.row_count == 0:
                rows = [list(df_str.columns)] + df_str.values.tolist()
            else:
                rows = df_str.values.tolist()

            log(f"{len(rows)} satır yazılıyor...", 70)
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            log("Yazma tamamlandı.", 90)

        except Exception as e:
            raise RuntimeError(f"Google Sheets yazma hatası: {e}") from e

        # Sheets URL'si output olarak kaydet
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        log(f"Senkronizasyon tamamlandı: {sheet_url}", 99)

        return {
            "output_url": sheet_url,
            "row_count": len(df),
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
        }

    @staticmethod
    def _get_client(credentials_json: str):
        """Service account ile gspread client oluştur."""
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        if credentials_json:
            try:
                creds_dict = json.loads(credentials_json)
            except json.JSONDecodeError:
                raise ValueError("credentials_json geçerli JSON değil.")
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            # Application Default Credentials
            creds = Credentials.from_service_account_file(
                "service_account.json", scopes=scopes
            )
        return gspread.authorize(creds)

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
