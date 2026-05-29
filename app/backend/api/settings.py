"""Runtime settings for the Agent4DA backend.

Reads from env files mounted at /envs inside the container (envs/app.env,
envs/groq.env). Secrets are never logged. See docs/ENV_SETUP.md.
"""

from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- App identity -------------------------------------------------------
    app_name: str = "Agent4DA Analytics Console"
    app_env: str = Field(default="local", alias="APP_ENV")
    app_version: str = "0.1.0"

    # --- CORS ---------------------------------------------------------------
    cors_origins: str = Field(default="http://localhost:3000", alias="APP_CORS_ORIGINS")

    # --- Auth (used from Phase 2) ------------------------------------------
    jwt_secret: str = Field(default="change_me", alias="APP_JWT_SECRET")
    jwt_alg: str = Field(default="HS256", alias="APP_JWT_ALG")
    access_token_ttl_min: int = Field(default=60, alias="APP_ACCESS_TOKEN_TTL_MIN")
    refresh_token_ttl_days: int = Field(default=14, alias="APP_REFRESH_TOKEN_TTL_DAYS")

    # --- Database (used from Phase 2) --------------------------------------
    db_url: str = Field(
        default="postgresql+psycopg://bigdata:change_me@postgres-db:5432/agent4da",
        alias="APP_DB_URL",
    )

    # --- Trino (used from Phase 3) -----------------------------------------
    trino_host: str = Field(default="trino", alias="APP_TRINO_HOST")
    trino_port: int = Field(default=8080, alias="APP_TRINO_PORT")
    trino_user: str = Field(default="agent4da_app", alias="APP_TRINO_USER")

    # --- Airflow (used from Phase 6) ---------------------------------------
    airflow_base_url: str = Field(
        default="http://airflow:8080",
        alias="APP_AIRFLOW_BASE_URL",
    )
    airflow_user: str = Field(default="", alias="APP_AIRFLOW_USER")
    airflow_password: str = Field(default="", alias="APP_AIRFLOW_PASSWORD")
    airflow_auth: str = Field(default="basic", alias="APP_AIRFLOW_AUTH")

    # --- Spark (used from Phase 7 health probe) ----------------------------
    spark_master_url: str = Field(
        default="http://spark-master:8080",
        alias="APP_SPARK_MASTER_URL",
    )

    # --- MinIO (used from Phase 4 result storage) --------------------------
    minio_endpoint: str = Field(default="http://minio:9000", alias="APP_MINIO_ENDPOINT")
    minio_access_key: str = Field(default="", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="", alias="MINIO_SECRET_KEY")
    minio_bucket_bronze: str = Field(default="bronze", alias="MINIO_BUCKET_BRONZE")
    minio_bucket_silver: str = Field(default="silver", alias="MINIO_BUCKET_SILVER")
    minio_bucket_gold: str = Field(default="gold", alias="MINIO_BUCKET_GOLD")

    # --- LLM / Groq (used from Phase 3) ------------------------------------
    groq_api_key: str = Field(default="", alias="GROQ_API_KEY")
    groq_model_whitelist: str = Field(
        default="llama-3.3-70b-versatile,llama-3.1-8b-instant",
        alias="APP_GROQ_MODEL_WHITELIST",
    )
    allow_temperature_override: bool = Field(
        default=False, alias="APP_ALLOW_TEMPERATURE_OVERRIDE"
    )

    # --- Background jobs ----------------------------------------------------
    enable_scheduler: bool = Field(default=True, alias="APP_ENABLE_SCHEDULER")

    model_config = SettingsConfigDict(
        env_file=("/envs/app.env", "/envs/groq.env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @property
    def cors_origins_list(self) -> List[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def model_whitelist_list(self) -> List[str]:
        return [m.strip() for m in self.groq_model_whitelist.split(",") if m.strip()]

    @property
    def system_status(self) -> dict:
        """Redacted view of which integrations are configured.

        Used by /settings/system in later phases. Never returns secret values.
        """
        return {
            "trino": "configured" if self.trino_host else "missing",
            "airflow": "configured" if self.airflow_user else "missing",
            "minio": "configured" if self.minio_access_key else "missing",
            "groq": "configured" if self.groq_api_key else "missing",
            "allow_temperature_override": self.allow_temperature_override,
            "model_whitelist": self.model_whitelist_list,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
