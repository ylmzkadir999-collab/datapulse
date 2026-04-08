"""DataPulse v5 — Claude AI Servisi (services/ai_service.py)

Maliyet optimizasyonu:
  1. Pandas önce ($0)  → pandas ile çözülüyorsa AI kullanma
  2. Haiku (basit)     → Free / Starter plan basit görevler
  3. Sonnet (pipeline) → Pro plan pipeline görevleri
  4. Prompt caching    → %90 token tasarrufu (beta header)
  5. Cost guardrail    → Plan başına maksimum USD limiti
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import anthropic
import pandas as pd

from backend.core.config import settings

logger = logging.getLogger(__name__)


class CostGuardrailError(Exception):
    """Harcama limiti aşıldığında fırlatılır."""


# ── Token maliyet tablosu (USD/1K token) ─────────────────────
_COST_TABLE = {
    # model: (input_cost, output_cost) per 1K tokens
    "claude-haiku-4-5-20251001": (0.00025, 0.00125),
    "claude-sonnet-4-6": (0.003, 0.015),
    "claude-opus-4-6": (0.015, 0.075),
}


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    inp, out = _COST_TABLE.get(model, (0.003, 0.015))
    return (input_tokens * inp + output_tokens * out) / 1000


class AIService:
    """Claude AI ile veri işleme servisi."""

    def __init__(self) -> None:
        self._client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    # ── Pandas-first helpers ──────────────────────────────────
    @staticmethod
    def _try_pandas_clean(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Temel temizlik işlemleri pandas ile dene ($0)."""
        try:
            df = df.copy()
            # Boş sütunları düşür
            df.dropna(axis=1, how="all", inplace=True)
            # Yinelenen satırları düşür
            df.drop_duplicates(inplace=True)
            # String sütunları strip et
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].str.strip()
            return df
        except Exception:
            return None

    @staticmethod
    def _try_pandas_detect_columns(df: pd.DataFrame) -> Optional[dict]:
        """Sütun tiplerini pandas ile tespit et ($0)."""
        try:
            result: dict[str, str] = {}
            for col in df.columns:
                dtype = str(df[col].dtype)
                if "int" in dtype or "float" in dtype:
                    result[col] = "numeric"
                elif "datetime" in dtype:
                    result[col] = "datetime"
                else:
                    # Tarih örüntüsü var mı?
                    sample = df[col].dropna().head(5).astype(str)
                    if sample.str.match(r"\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}").any():
                        result[col] = "datetime"
                    else:
                        result[col] = "text"
            return result
        except Exception:
            return None

    # ── Core AI call ──────────────────────────────────────────
    def _call(
        self,
        model: str,
        system: str,
        user: str,
        max_tokens: int = 1024,
        use_cache: bool = True,
        cost_limit: float = 1.0,
    ) -> str:
        """Claude API çağrısı — prompt caching + cost guardrail."""
        messages = [{"role": "user", "content": user}]

        kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": messages,
        }

        if use_cache:
            # Prompt caching: %90 ucuz tekrar eden system prompt'lar için
            kwargs["system"] = [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
            kwargs["extra_headers"] = {"anthropic-beta": "prompt-caching-2024-07-31"}
        else:
            kwargs["system"] = system

        response = self._client.messages.create(**kwargs)

        # Maliyet hesapla
        usage = response.usage
        cost = _estimate_cost(model, usage.input_tokens, usage.output_tokens)

        if cost > cost_limit:
            raise CostGuardrailError(
                f"Maliyet limiti aşıldı: ${cost:.4f} > ${cost_limit}"
            )

        logger.info(
            f"AI call: model={model} in={usage.input_tokens} "
            f"out={usage.output_tokens} cost=${cost:.5f}"
        )

        return response.content[0].text

    # ── Public API ────────────────────────────────────────────
    def clean_data(
        self,
        df: pd.DataFrame,
        plan: str = "free",
        instructions: str = "",
    ) -> pd.DataFrame:
        """Veri temizleme — pandas-first, gerekirse AI."""
        # 1. Pandas ile dene (ücretsiz)
        cleaned = self._try_pandas_clean(df)
        if cleaned is not None and not instructions:
            logger.info("Veri temizleme: pandas ile tamamlandı ($0)")
            return cleaned

        # 2. AI ile temizle
        model = settings.get_ai_model(plan)
        cost_limit = settings.get_cost_limit(plan)

        sample = df.head(20).to_csv(index=False)
        schema = df.dtypes.to_string()

        result_str = self._call(
            model=model,
            system=(
                "Sen bir veri temizleme uzmanısın. "
                "CSV formatında temizlenmiş veri döndür. "
                "Sadece CSV, açıklama yok."
            ),
            user=(
                f"Bu veriyi temizle:\n\nŞema:\n{schema}\n\n"
                f"Örnek (ilk 20 satır):\n{sample}\n\n"
                f"Özel talimat: {instructions or 'Standart temizlik yap.'}"
            ),
            max_tokens=2048,
            cost_limit=cost_limit,
        )

        # CSV'i parse et
        import io
        try:
            return pd.read_csv(io.StringIO(result_str))
        except Exception:
            logger.warning("AI çıktısı CSV parse edilemedi, pandas sonucu döndürülüyor")
            return cleaned or df

    def detect_schema(
        self,
        df: pd.DataFrame,
        plan: str = "free",
    ) -> dict:
        """Sütun tipi tespiti — pandas-first, gerekirse AI."""
        # 1. Pandas ile dene (ücretsiz)
        schema = self._try_pandas_detect_columns(df)
        if schema:
            logger.info("Şema tespiti: pandas ile tamamlandı ($0)")
            return schema

        # 2. AI ile tespit et
        model = settings.get_ai_model(plan)
        cost_limit = settings.get_cost_limit(plan)

        sample = df.head(10).to_csv(index=False)

        result_str = self._call(
            model=model,
            system=(
                "Sütun tiplerini JSON olarak döndür. "
                'Format: {"kolon_adı": "numeric|datetime|text|categorical"}'
            ),
            user=f"Bu CSV'nin sütun tiplerini belirle:\n{sample}",
            max_tokens=512,
            cost_limit=cost_limit,
        )

        import json
        try:
            return json.loads(result_str)
        except Exception:
            return {col: "text" for col in df.columns}

    def analyze_pdf_table(
        self,
        text_block: str,
        plan: str = "starter",
    ) -> list[list[str]]:
        """PDF'ten çıkarılan metin bloğundan tablo yapısı çıkar."""
        model = settings.get_ai_model(plan)
        cost_limit = settings.get_cost_limit(plan)

        result_str = self._call(
            model=model,
            system=(
                "PDF'ten tablo çıkar. "
                "Her satır için | ile ayrılmış değerler döndür. "
                "İlk satır başlık. Başka açıklama yok."
            ),
            user=f"Bu metinden tabloyu çıkar:\n\n{text_block}",
            max_tokens=2048,
            cost_limit=cost_limit,
        )

        rows = []
        for line in result_str.strip().splitlines():
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if cells:
                rows.append(cells)
        return rows

    def generate_pipeline_summary(
        self,
        results: list[dict],
        plan: str = "pro",
    ) -> str:
        """Pipeline sonuçlarını özetle (Sonnet)."""
        model = settings.get_ai_model(plan)
        cost_limit = settings.get_cost_limit(plan)

        import json
        results_str = json.dumps(results, ensure_ascii=False, indent=2)

        return self._call(
            model=model,
            system=(
                "DataPulse pipeline analisti olarak "
                "Türkçe kısa özet yaz. Bullet points kullan."
            ),
            user=f"Pipeline sonuçları:\n{results_str}",
            max_tokens=512,
            cost_limit=cost_limit,
        )
