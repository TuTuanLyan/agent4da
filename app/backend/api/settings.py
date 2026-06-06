from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import List


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_env: str = os.getenv("APP_ENV", "local")
    app_name: str = "Agent4DA Analytics Console"
    app_version: str = "0.1.0"
    cors_origins: str = os.getenv("APP_CORS_ORIGINS", "http://localhost:3000")

    db_url: str = os.getenv(
        "APP_DB_URL",
        "postgresql://bigdata:change_me@postgres-db:5432/agent4da",
    )
    jwt_secret: str = os.getenv("APP_JWT_SECRET", "change_me")
    jwt_alg: str = os.getenv("APP_JWT_ALG", "HS256")
    access_token_ttl_min: int = int(os.getenv("APP_ACCESS_TOKEN_TTL_MIN", "60"))
    refresh_token_ttl_days: int = int(os.getenv("APP_REFRESH_TOKEN_TTL_DAYS", "14"))

    bootstrap_admin_email: str = os.getenv("APP_BOOTSTRAP_ADMIN_EMAIL", "admin@example.com")
    bootstrap_admin_password: str = os.getenv("APP_BOOTSTRAP_ADMIN_PASSWORD", "")

    trino_host: str = os.getenv("APP_TRINO_HOST", "trino")
    trino_port: int = int(os.getenv("APP_TRINO_PORT", "8080"))
    trino_user: str = os.getenv("APP_TRINO_USER", "agent4da_app")

    airflow_base_url: str = os.getenv("APP_AIRFLOW_BASE_URL", "http://airflow:8080")
    airflow_user: str = os.getenv("APP_AIRFLOW_USER", "")
    airflow_password: str = os.getenv("APP_AIRFLOW_PASSWORD", "")

    spark_master_url: str = os.getenv("APP_SPARK_MASTER_URL", "http://spark-master:8080")

    minio_endpoint: str = os.getenv("APP_MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "")
    minio_bucket_bronze: str = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    minio_bucket_silver: str = os.getenv("MINIO_BUCKET_SILVER", "silver")
    minio_bucket_gold: str = os.getenv("MINIO_BUCKET_GOLD", "gold")

    groq_api_key: str = os.getenv("GROQ_API_KEY", "")
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    gemini_api_keys: str = os.getenv("GEMINI_API_KEYS", "")
    agent_llm_provider: str = os.getenv("AGENT_LLM_PROVIDER", "auto")
    groq_model_whitelist: str = os.getenv(
        "APP_GROQ_MODEL_WHITELIST",
        "gemini-2.5-flash,llama-3.3-70b-versatile,llama-3.1-8b-instant",
    )
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    groq_model: str = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    allow_temperature_override: bool = _bool_env("APP_ALLOW_TEMPERATURE_OVERRIDE", False)
    agent_engine: str = os.getenv("APP_AGENT_ENGINE", "legacy")

    # --- Redis cache + rate limiting (all optional / graceful) ----------------
    redis_url: str = os.getenv("APP_REDIS_URL", "redis://localhost:6379/0")
    cache_enabled: bool = _bool_env("APP_CACHE_ENABLED", True)
    # TTLs (seconds)
    cache_answer_ttl: int = int(os.getenv("APP_CACHE_ANSWER_TTL", "120"))
    cache_schema_ttl: int = int(os.getenv("APP_CACHE_SCHEMA_TTL", "600"))
    cache_session_ttl: int = int(os.getenv("APP_CACHE_SESSION_TTL", "900"))
    # Don't cache giant result sets (cap rows stored per answer).
    cache_answer_max_rows: int = int(os.getenv("APP_CACHE_ANSWER_MAX_ROWS", "2000"))
    # Rate limiting (fixed window; fail-open if Redis is down)
    rate_limit_enabled: bool = _bool_env("APP_RATE_LIMIT_ENABLED", True)
    rl_login_limit: int = int(os.getenv("APP_RL_LOGIN_LIMIT", "10"))
    rl_login_window_s: int = int(os.getenv("APP_RL_LOGIN_WINDOW_S", "60"))
    rl_ask_limit: int = int(os.getenv("APP_RL_ASK_LIMIT", "30"))
    rl_ask_window_s: int = int(os.getenv("APP_RL_ASK_WINDOW_S", "60"))

    @property
    def psycopg_dsn(self) -> str:
        return self.db_url.replace("postgresql+psycopg://", "postgresql://", 1)

    @property
    def cors_origins_list(self) -> List[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def model_whitelist_list(self) -> List[str]:
        models = [self.gemini_model, self.groq_model]
        models.extend(item.strip() for item in self.groq_model_whitelist.split(",") if item.strip())
        return list(dict.fromkeys(item for item in models if item))

    @property
    def normalized_agent_engine(self) -> str:
        return "v2" if self.agent_engine.strip().lower() == "v2" else "legacy"

    @property
    def system_status(self) -> dict:
        return {
            "trino": "configured" if self.trino_host else "missing",
            "airflow": "configured" if self.airflow_user else "missing",
            "minio": "configured" if self.minio_access_key else "missing",
            "gemini": "configured" if (self.gemini_api_key or self.gemini_api_keys) else "missing",
            "groq": "configured" if self.groq_api_key else "missing",
            "llm_provider": self.agent_llm_provider,
            "allow_temperature_override": self.allow_temperature_override,
            "model_whitelist": self.model_whitelist_list,
            "agent_engine": self.normalized_agent_engine,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def bridge_agent_env() -> None:
    settings = get_settings()
    os.environ.setdefault("TRINO_HOST", settings.trino_host)
    os.environ.setdefault("TRINO_PORT", str(settings.trino_port))
    os.environ.setdefault("TRINO_USER", settings.trino_user)
    if settings.groq_api_key:
        os.environ.setdefault("GROQ_API_KEY", settings.groq_api_key)
    if settings.gemini_api_key:
        os.environ.setdefault("GEMINI_API_KEY", settings.gemini_api_key)
    if settings.gemini_api_keys:
        os.environ.setdefault("GEMINI_API_KEYS", settings.gemini_api_keys)
    os.environ.setdefault("AGENT_LLM_PROVIDER", settings.agent_llm_provider)
