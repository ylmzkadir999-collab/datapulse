"""DataPulse v5 — Pydantic Settings (core/config.py)"""
from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── App ──────────────────────────────────────────────────
    ENVIRONMENT: str = "production"
    SECRET_KEY: str = Field(default="change-me-in-dev-only-32chars!!")
    ALLOWED_ORIGINS: List[str] = ["*"]

    # ── Redis ────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"

    # ── Supabase ─────────────────────────────────────────────
    SUPABASE_URL: str = Field(default="")
    SUPABASE_KEY: str = Field(default="")
    SUPABASE_SERVICE_KEY: str = Field(default="")
    SUPABASE_BUCKET: str = "datapulse-files"

    # ── Claude AI ────────────────────────────────────────────
    ANTHROPIC_API_KEY: str = Field(default="")

    # Model per plan — Haiku (basit) / Sonnet (pipeline) / Opus (Pro)
    AI_MODEL_FREE: str = "claude-haiku-4-5-20251001"
    AI_MODEL_STARTER: str = "claude-haiku-4-5-20251001"
    AI_MODEL_PRO: str = "claude-sonnet-4-6"

    # ── İyzico ───────────────────────────────────────────────
    IYZICO_API_KEY: str = ""
    IYZICO_SECRET_KEY: str = ""
    IYZICO_BASE_URL: str = "https://api.iyzipay.com"

    # ── Telegram ─────────────────────────────────────────────
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # ── Cost Guardrails (USD per job) ────────────────────────
    COST_LIMIT_FREE: float = 0.005
    COST_LIMIT_STARTER: float = 0.05
    COST_LIMIT_PRO: float = 1.0

    # ── Preview System ───────────────────────────────────────
    PREVIEW_ROWS: int = 10

    # ── Plans ────────────────────────────────────────────────
    PLAN_FREE_JOBS_PER_MONTH: int = 5
    PLAN_STARTER_JOBS_PER_MONTH: int = 50
    PLAN_PRO_JOBS_PER_MONTH: int = -1  # unlimited

    def get_ai_model(self, plan: str) -> str:
        return {
            "free": self.AI_MODEL_FREE,
            "starter": self.AI_MODEL_STARTER,
            "pro": self.AI_MODEL_PRO,
        }.get(plan, self.AI_MODEL_FREE)

    def get_cost_limit(self, plan: str) -> float:
        return {
            "free": self.COST_LIMIT_FREE,
            "starter": self.COST_LIMIT_STARTER,
            "pro": self.COST_LIMIT_PRO,
        }.get(plan, self.COST_LIMIT_FREE)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
