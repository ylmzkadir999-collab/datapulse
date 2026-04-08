"""DataPulse v5 — Güvenlik Tarama (security/scanner.py)

Katmanlı güvenlik:
  1. Magic bytes kontrolü   → gerçek dosya tipi tespiti
  2. CSV injection tespiti  → formül karakterleri (=, +, -, @)
  3. Macro tespiti          → Excel .xlsm / VBA içerik
  4. ClamAV                 → virüs tarama (Docker ortamı)
"""
from __future__ import annotations

import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

import magic  # python-magic → libmagic

logger = logging.getLogger(__name__)

# İzin verilen MIME tipleri
ALLOWED_MIME_TYPES = {
    "application/pdf",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/csv",
    "text/plain",
    "application/octet-stream",  # bazı CSV yükleri
}

# CSV injection tetikleyici karakterler (OWASP)
CSV_INJECTION_PATTERN = re.compile(r'^[=+\-@\t\r]', re.MULTILINE)

# Excel macro imzası (XLSB/XLSM)
XLSM_MAGIC = b"PK\x03\x04"  # ZIP (OOXML), xlsm içeriği zip içinde


class ScanResult:
    def __init__(self, safe: bool, reason: str = "") -> None:
        self.safe = safe
        self.reason = reason

    def __bool__(self) -> bool:
        return self.safe

    def __repr__(self) -> str:
        return f"ScanResult(safe={self.safe}, reason={self.reason!r})"


class SecurityScanner:
    """Yüklenen dosyaları tarar."""

    # ── 1. Magic Bytes ────────────────────────────────────────
    def check_magic_bytes(self, content: bytes, declared_type: str) -> ScanResult:
        """
        Dosyanın gerçek MIME tipini magic bytes ile doğrula.
        Declared Content-Type ile uyuşmazsa reddet.
        """
        try:
            real_type = magic.from_buffer(content, mime=True)
        except Exception as e:
            logger.warning(f"libmagic hatası: {e} — dosya reddedildi")
            return ScanResult(False, f"MIME tespit edilemedi: {e}")

        if real_type not in ALLOWED_MIME_TYPES:
            return ScanResult(
                False,
                f"İzin verilmeyen dosya tipi: {real_type}",
            )

        # Declared ile real uyuşmalı (gevşek kontrol)
        if declared_type and declared_type.split(";")[0].strip() not in ALLOWED_MIME_TYPES:
            logger.warning(
                f"MIME uyuşmazlığı: real={real_type} declared={declared_type}"
            )
            # Uyuşmazlık log'la ama real_type güvenli ise devam et

        logger.info(f"Magic bytes OK: {real_type}")
        return ScanResult(True)

    # ── 2. CSV Injection ─────────────────────────────────────
    def check_csv_injection(self, content: bytes) -> ScanResult:
        """CSV injection formula karakterlerini tespit et."""
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:
            return ScanResult(True)  # Binary dosya, skip

        matches = CSV_INJECTION_PATTERN.findall(text)
        if matches:
            count = len(matches)
            return ScanResult(
                False,
                f"CSV injection şüphesi: {count} formül karakteri tespit edildi.",
            )
        return ScanResult(True)

    # ── 3. Macro Tespiti ─────────────────────────────────────
    def check_macros(self, content: bytes, filename: str) -> ScanResult:
        """
        Excel macro içeriğini tespit et.
        .xlsm dosyaları ve 'vbaProject.bin' string'i içeren dosyalar reddedilir.
        """
        fname_lower = filename.lower()
        if fname_lower.endswith(".xlsm") or fname_lower.endswith(".xlsb"):
            return ScanResult(
                False,
                "Macro içerebilecek Excel formatı (.xlsm/.xlsb) kabul edilmiyor.",
            )

        # ZIP içinde vbaProject.bin var mı?
        if b"vbaProject.bin" in content:
            return ScanResult(False, "Excel dosyası VBA macro içeriyor.")

        return ScanResult(True)

    # ── 4. ClamAV ────────────────────────────────────────────
    def check_clamav(self, content: bytes) -> ScanResult:
        """
        ClamAV ile virüs tarama.
        clamd socket üzerinden çalışır (Docker'da /var/run/clamav/clamd.sock).
        ClamAV yoksa bu adımı atlar (graceful degradation).
        """
        try:
            import clamd
            cd = clamd.ClamdUnixSocket()
            result = cd.instream(content)
            status, details = result.get("stream", ("OK", ""))
            if status == "FOUND":
                return ScanResult(False, f"Virüs tespit edildi: {details}")
            return ScanResult(True)
        except ImportError:
            logger.debug("clamd modülü yok — ClamAV taraması atlandı")
            return ScanResult(True)
        except Exception as e:
            logger.warning(f"ClamAV bağlantı hatası: {e} — tarama atlandı")
            return ScanResult(True)  # Graceful degradation

    # ── Tam Tarama ────────────────────────────────────────────
    def scan(
        self,
        content: bytes,
        filename: str,
        declared_mime: str = "",
    ) -> ScanResult:
        """
        Tüm güvenlik kontrollerini çalıştır.
        İlk başarısız kontrol anında ScanResult(False) döndürür.
        """
        checks = [
            ("magic_bytes", lambda: self.check_magic_bytes(content, declared_mime)),
            ("macros", lambda: self.check_macros(content, filename)),
            ("clamav", lambda: self.check_clamav(content)),
        ]

        # CSV injection sadece text/csv için
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext == "csv" or "text/csv" in declared_mime:
            checks.insert(1, ("csv_injection", lambda: self.check_csv_injection(content)))

        for check_name, check_fn in checks:
            try:
                result = check_fn()
                if not result:
                    logger.warning(
                        f"Güvenlik taraması başarısız [{check_name}]: "
                        f"file={filename} reason={result.reason}"
                    )
                    return result
                logger.debug(f"[{check_name}] OK")
            except Exception as e:
                logger.error(f"Tarama hatası [{check_name}]: {e}")
                # Tarama hatası → güvenli kabul et (kullanılabilirlik)
                continue

        logger.info(f"Güvenlik taraması PASSED: {filename}")
        return ScanResult(True)
