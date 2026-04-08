"""DataPulse v5 — Güvenlik Tarama Testleri"""
import pytest
from backend.security.scanner import SecurityScanner, ScanResult

scanner = SecurityScanner()

# Gerçek PDF magic bytes: %PDF-
VALID_PDF = b"%PDF-1.4 fake content"

# Gerçek Excel OOXML magic bytes: PK\x03\x04 (ZIP header)
VALID_XLSX = b"PK\x03\x04" + b"\x00" * 100

# CSV içeriği
VALID_CSV = b"isim,yas,sehir\nAli,25,Istanbul\nVeli,30,Ankara\n"

# CSV injection örneği
INJECTION_CSV = b"isim,komut\nAli,=CMD|' /C calc'!A0\n"


class TestMagicBytes:
    def test_valid_csv_passes(self):
        result = scanner.check_magic_bytes(VALID_CSV, "text/csv")
        assert result.safe, f"Beklenen: güvenli. Reason: {result.reason}"

    def test_executable_blocked(self):
        # ELF binary magic bytes
        elf_bytes = b"\x7fELF\x02\x01\x01" + b"\x00" * 100
        result = scanner.check_magic_bytes(elf_bytes, "application/octet-stream")
        assert not result.safe, "ELF dosyası reddedilmeli"

    def test_pdf_magic(self):
        result = scanner.check_magic_bytes(VALID_PDF, "application/pdf")
        assert result.safe


class TestCSVInjection:
    def test_clean_csv_passes(self):
        result = scanner.check_csv_injection(VALID_CSV)
        assert result.safe

    def test_formula_injection_blocked(self):
        result = scanner.check_csv_injection(INJECTION_CSV)
        assert not result.safe
        assert "injection" in result.reason.lower()

    def test_at_sign_injection(self):
        at_csv = b"name,cmd\nTest,@SUM(1+1)\n"
        result = scanner.check_csv_injection(at_csv)
        assert not result.safe

    def test_plus_injection(self):
        plus_csv = b"x,y\n1,+2+3+4\n"
        result = scanner.check_csv_injection(plus_csv)
        assert not result.safe


class TestMacroDetection:
    def test_clean_excel_passes(self):
        result = scanner.check_macros(VALID_XLSX, "data.xlsx")
        assert result.safe

    def test_xlsm_blocked(self):
        result = scanner.check_macros(VALID_XLSX, "report.xlsm")
        assert not result.safe

    def test_vba_content_blocked(self):
        vba_content = VALID_XLSX + b"vbaProject.bin" + b"\x00" * 50
        result = scanner.check_macros(vba_content, "data.xlsx")
        assert not result.safe


class TestFullScan:
    def test_valid_csv_full_scan(self):
        result = scanner.scan(VALID_CSV, "data.csv", "text/csv")
        assert result.safe

    def test_injection_csv_full_scan(self):
        result = scanner.scan(INJECTION_CSV, "bad.csv", "text/csv")
        assert not result.safe

    def test_xlsm_full_scan(self):
        result = scanner.scan(VALID_XLSX, "macro.xlsm", "application/vnd.ms-excel.sheet.macroEnabled.12")
        assert not result.safe

    def test_scan_result_bool(self):
        assert bool(ScanResult(True)) is True
        assert bool(ScanResult(False)) is False
