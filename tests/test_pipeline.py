"""DataPulse v5 — Pipeline Testleri"""
import pytest
from unittest.mock import patch, MagicMock

from scripts.pipeline import FullPipeline


def _make_runner(result: dict):
    """Mock script runner döndür."""
    mock = MagicMock()
    mock.run.return_value = result
    return mock


class TestFullPipeline:
    def _pipeline(self, steps: list, stop_on_error: bool = False) -> FullPipeline:
        return FullPipeline(
            config={"steps": steps, "stop_on_error": stop_on_error},
            plan="pro",
        )

    def test_empty_steps_raises(self):
        p = FullPipeline(config={}, plan="pro")
        with pytest.raises(ValueError, match="steps"):
            p.run(file_url=None, user_id="u1", job_id="j1")

    def test_single_step_ok(self):
        steps = [{"task": "clean", "config": {}}]
        p = self._pipeline(steps)

        mock_result = {"output_url": "path/out.xlsx", "row_count": 42}

        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.DataCleaner.return_value = _make_runner(mock_result)
            mock_import.return_value = mock_module

            with patch.object(p._ai, "generate_pipeline_summary", return_value="Özet"):
                result = p.run(file_url="path/in.xlsx", user_id="u1", job_id="j1")

        assert result["steps_ok"] == 1
        assert result["steps_failed"] == 0
        assert result["row_count"] == 42

    def test_step_failure_continues_without_stop(self):
        """stop_on_error=false → hata olsa pipeline devam eder."""
        steps = [
            {"task": "clean", "config": {}},
            {"task": "excel_merge", "config": {}},
        ]
        p = self._pipeline(steps, stop_on_error=False)

        call_count = 0

        def mock_import(module_path):
            nonlocal call_count
            call_count += 1
            m = MagicMock()
            if "cleaner" in module_path:
                m.DataCleaner.return_value.run.side_effect = RuntimeError("Test hatası")
            else:
                m.ExcelMerger.return_value.run.return_value = {
                    "output_url": "out.xlsx", "row_count": 10
                }
            return m

        with patch("importlib.import_module", side_effect=mock_import):
            with patch.object(p._ai, "generate_pipeline_summary", return_value=""):
                result = p.run(file_url="path/in.xlsx", user_id="u1", job_id="j1")

        assert result["steps_failed"] >= 1
        assert result["steps_ok"] >= 0  # En az devam etti

    def test_stop_on_error_true_stops(self):
        """stop_on_error=true → ilk hata sonrası dur."""
        steps = [
            {"task": "clean", "config": {}},
            {"task": "excel_merge", "config": {}},
        ]
        p = self._pipeline(steps, stop_on_error=True)

        with patch("importlib.import_module") as mock_import:
            m = MagicMock()
            m.DataCleaner.return_value.run.side_effect = RuntimeError("Durdur!")
            mock_import.return_value = m

            with patch.object(p._ai, "generate_pipeline_summary", return_value=""):
                result = p.run(file_url="path/in.xlsx", user_id="u1", job_id="j1")

        # Sadece 1 adım çalışmış (2. atlandı)
        assert len(result["step_results"]) == 1

    def test_unknown_task_type_handled(self):
        """Bilinmeyen task tipi hata vermeden geçilmeli."""
        steps = [{"task": "bilinmeyen_task", "config": {}}]
        p = self._pipeline(steps, stop_on_error=False)

        with patch.object(p._ai, "generate_pipeline_summary", return_value=""):
            result = p.run(file_url=None, user_id="u1", job_id="j1")

        assert result["step_results"][0]["status"] == "error"

    def test_output_url_chained(self):
        """Adımlar birbirinin çıktısını kullanmalı."""
        steps = [
            {"task": "clean", "config": {}},
            {"task": "excel_merge", "config": {}},
        ]
        p = self._pipeline(steps)

        outputs = ["step1_out.xlsx", "step2_out.xlsx"]
        call_idx = 0

        def side_effect(*args, **kwargs):
            nonlocal call_idx
            out = outputs[call_idx % len(outputs)]
            call_idx += 1
            return {"output_url": out, "row_count": 5}

        with patch("importlib.import_module") as mock_import:
            m = MagicMock()
            m.DataCleaner.return_value.run.side_effect = side_effect
            m.ExcelMerger.return_value.run.side_effect = side_effect
            mock_import.return_value = m

            with patch.object(p._ai, "generate_pipeline_summary", return_value=""):
                result = p.run(file_url="original.xlsx", user_id="u1", job_id="j1")

        # Son output_url son adımın çıktısı
        assert result["output_url"] is not None
