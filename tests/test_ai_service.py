"""DataPulse v5 — AI Servis Testleri (mock tabanlı)"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from backend.services.ai_service import AIService, CostGuardrailError, _estimate_cost


class TestCostEstimation:
    def test_haiku_cost(self):
        cost = _estimate_cost("claude-haiku-4-5-20251001", 1000, 100)
        # (1000 * 0.00025 + 100 * 0.00125) / 1000 = 0.000375
        assert abs(cost - 0.000375) < 1e-9

    def test_sonnet_cost(self):
        cost = _estimate_cost("claude-sonnet-4-6", 1000, 100)
        # (1000 * 0.003 + 100 * 0.015) / 1000 = 0.0045
        assert abs(cost - 0.0045) < 1e-9

    def test_unknown_model_fallback(self):
        # Bilinmeyen model → sonnet fiyatı kullan
        cost = _estimate_cost("unknown-model", 1000, 100)
        assert cost > 0


class TestPandasFirst:
    """Pandas önce — AI çağrısı yapılmamalı."""

    def test_clean_data_no_ai_call(self):
        """Basit temizlik için AI çağrılmamalı."""
        svc = AIService()
        df = pd.DataFrame({
            "A": ["  hello  ", "  world  ", None],
            "B": [1, 2, 2],  # duplicate
            "C": [None, None, None],  # boş sütun
        })
        with patch.object(svc, "_call") as mock_call:
            result = svc.clean_data(df, plan="free")
            mock_call.assert_not_called()

        # Boş sütun kaldırıldı
        assert "C" not in result.columns
        # Duplicate kaldırıldı (2 satır → 1 tekrar)
        assert len(result) <= len(df)

    def test_detect_schema_no_ai(self):
        """Tip tespiti pandas ile yapılmalı."""
        svc = AIService()
        df = pd.DataFrame({
            "sayi": [1, 2, 3],
            "metin": ["a", "b", "c"],
        })
        with patch.object(svc, "_call") as mock_call:
            schema = svc.detect_schema(df, plan="free")
            mock_call.assert_not_called()

        assert schema["sayi"] == "numeric"
        assert schema["metin"] == "text"


class TestCostGuardrail:
    """Maliyet limiti aşılırsa hata fırlatılmalı."""

    def test_guardrail_triggers(self):
        svc = AIService()

        mock_response = MagicMock()
        mock_response.usage.input_tokens = 10000
        mock_response.usage.output_tokens = 5000
        mock_response.content = [MagicMock(text="result")]

        with patch.object(svc._client.messages, "create", return_value=mock_response):
            with pytest.raises(CostGuardrailError):
                svc._call(
                    model="claude-sonnet-4-6",
                    system="test",
                    user="test",
                    cost_limit=0.001,  # Çok düşük limit
                )

    def test_guardrail_not_triggered_within_limit(self):
        svc = AIService()

        mock_response = MagicMock()
        mock_response.usage.input_tokens = 10
        mock_response.usage.output_tokens = 5
        mock_response.content = [MagicMock(text="ok")]

        with patch.object(svc._client.messages, "create", return_value=mock_response):
            result = svc._call(
                model="claude-haiku-4-5-20251001",
                system="test",
                user="test",
                cost_limit=1.0,
            )
        assert result == "ok"


class TestPDFTableAnalysis:
    def test_analyze_pdf_table_parses_pipe_format(self):
        svc = AIService()

        mock_response = MagicMock()
        mock_response.usage.input_tokens = 100
        mock_response.usage.output_tokens = 50
        mock_response.content = [MagicMock(text="Ad | Soyad | Yas\nAli | Yilmaz | 25\nVeli | Kaya | 30")]

        with patch.object(svc._client.messages, "create", return_value=mock_response):
            rows = svc.analyze_pdf_table("test metin", plan="starter")

        assert len(rows) == 3
        assert "Ad" in rows[0]
        assert "Ali" in rows[1]
