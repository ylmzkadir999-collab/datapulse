"""DataPulse v5 — Preview & Unlock Token Testleri"""
import pytest
import pandas as pd
from io import BytesIO

from backend.services.preview_service import PreviewService, WATERMARK

preview_svc = PreviewService()


def make_df(n: int = 25) -> pd.DataFrame:
    return pd.DataFrame({
        "isim": [f"Kullanici_{i}" for i in range(n)],
        "yas": list(range(20, 20 + n)),
        "sehir": ["Istanbul", "Ankara", "Izmir"] * (n // 3 + 1)[:n],
    })


class TestBuildPreview:
    def test_preview_row_count(self):
        df = make_df(25)
        result = preview_svc.build_preview(df)
        from backend.core.config import settings
        assert len(result["preview_rows"]) == settings.PREVIEW_ROWS

    def test_masked_count(self):
        df = make_df(25)
        result = preview_svc.build_preview(df)
        from backend.core.config import settings
        expected_masked = 25 - settings.PREVIEW_ROWS
        assert result["masked_count"] == expected_masked

    def test_masked_rows_contain_watermark(self):
        df = make_df(25)
        result = preview_svc.build_preview(df)
        for row in result["masked_rows"]:
            for val in row.values():
                assert val == WATERMARK, f"Filigran beklendi, {val!r} bulundu"

    def test_preview_rows_real_data(self):
        df = make_df(25)
        result = preview_svc.build_preview(df)
        # İlk preview satırındaki isim gerçek olmalı
        first = result["preview_rows"][0]
        assert first["isim"] == "Kullanici_0"

    def test_small_df_no_masked(self):
        """10'dan az satırda maskeleme olmamalı."""
        df = make_df(5)
        result = preview_svc.build_preview(df)
        assert result["masked_count"] == 0
        assert len(result["preview_rows"]) == 5

    def test_columns_correct(self):
        df = make_df(10)
        result = preview_svc.build_preview(df)
        assert result["columns"] == ["isim", "yas", "sehir"]

    def test_total_rows(self):
        df = make_df(30)
        result = preview_svc.build_preview(df)
        assert result["total_rows"] == 30


class TestMaskDataframe:
    def test_first_n_rows_unchanged(self):
        df = make_df(20)
        masked = preview_svc.mask_dataframe(df)
        from backend.core.config import settings
        n = settings.PREVIEW_ROWS
        pd.testing.assert_frame_equal(
            masked.head(n).reset_index(drop=True),
            df.head(n).reset_index(drop=True),
        )

    def test_remaining_rows_masked(self):
        df = make_df(20)
        masked = preview_svc.mask_dataframe(df)
        from backend.core.config import settings
        n = settings.PREVIEW_ROWS
        rest = masked.iloc[n:]
        for col in df.columns:
            assert all(rest[col] == WATERMARK)


class TestWatermarkExcel:
    def test_excel_has_two_sheets(self):
        df = make_df(25)
        buf = BytesIO()
        preview_svc.watermark_excel(df, buf)
        buf.seek(0)
        sheets = pd.read_excel(buf, sheet_name=None)
        assert "Preview" in sheets
        assert "Locked" in sheets

    def test_preview_sheet_row_count(self):
        df = make_df(25)
        buf = BytesIO()
        preview_svc.watermark_excel(df, buf)
        buf.seek(0)
        from backend.core.config import settings
        preview_df = pd.read_excel(buf, sheet_name="Preview")
        assert len(preview_df) == settings.PREVIEW_ROWS
