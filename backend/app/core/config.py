from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_HERE = Path(__file__).resolve()
_BACKEND_DIR = _HERE.parents[2]
_ROOT_DIR = _HERE.parents[3]
_ENV_CANDIDATES = (_BACKEND_DIR / ".env", _ROOT_DIR / ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=tuple(str(path) for path in _ENV_CANDIDATES),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = Field(default="Wematics Archive Dashboard", validation_alias="APP_NAME")
    api_prefix: str = Field(default="/api", validation_alias="API_PREFIX")
    cors_origins: str = Field(default="http://localhost:5173,http://localhost:3000", validation_alias="CORS_ORIGINS")

    database_url: str = Field(default="sqlite:///./data/wematics.db", validation_alias="DATABASE_URL")
    migrations_path: str = Field(default="./migrations", validation_alias="MIGRATIONS_PATH")

    wematics_api_key: str | None = Field(default=None, validation_alias="WEMATICS_API_KEY")
    default_timezone: str = Field(default="local", validation_alias="DEFAULT_TIMEZONE")
    remote_dates_ttl_seconds: int = Field(default=300, validation_alias="REMOTE_DATES_TTL_SECONDS")
    remote_retry_attempts: int = Field(default=4, validation_alias="REMOTE_RETRY_ATTEMPTS")
    remote_retry_base_delay_seconds: float = Field(default=1.0, validation_alias="REMOTE_RETRY_BASE_DELAY_SECONDS")

    archive_base_path: str = Field(default="./downloads", validation_alias="ARCHIVE_BASE_PATH")
    transfer_temp_base_path: str = Field(default="./data/transfer_tmp", validation_alias="TRANSFER_TEMP_BASE_PATH")
    local_scan_cache_seconds: int = Field(default=60, validation_alias="LOCAL_SCAN_CACHE_SECONDS")

    ftp_host: str | None = Field(default=None, validation_alias="FTP_HOST")
    ftp_port: int = Field(default=21, validation_alias="FTP_PORT")
    ftp_user: str | None = Field(default=None, validation_alias="FTP_USER")
    ftp_password: str | None = Field(default=None, validation_alias="FTP_PASSWORD")
    ftp_timeout_seconds: int = Field(default=30, validation_alias="FTP_TIMEOUT_SECONDS")
    ftp_passive_mode: bool = Field(default=True, validation_alias="FTP_PASSIVE_MODE")
    ftp_base_path: str = Field(default="/wematics", validation_alias="FTP_BASE_PATH")
    ftp_conflict_base_path: str = Field(default="/wematics_conflicts", validation_alias="FTP_CONFLICT_BASE_PATH")
    ftp_read_only_paths: str = Field(default="/images,/images safe", validation_alias="FTP_READ_ONLY_PATHS")
    ftp_max_retries: int = Field(default=4, validation_alias="FTP_MAX_RETRIES")
    ftp_retry_base_delay_seconds: float = Field(default=1.0, validation_alias="FTP_RETRY_BASE_DELAY_SECONDS")

    worker_poll_interval_seconds: float = Field(default=2.0, validation_alias="WORKER_POLL_INTERVAL_SECONDS")
    scheduler_poll_interval_seconds: float = Field(default=30.0, validation_alias="SCHEDULER_POLL_INTERVAL_SECONDS")
    upload_concurrency: int = Field(default=2, validation_alias="UPLOAD_CONCURRENCY")
    download_concurrency: int = Field(default=3, validation_alias="DOWNLOAD_CONCURRENCY")
    transfer_concurrency: int = Field(default=8, validation_alias="TRANSFER_CONCURRENCY")

    no_new_data_alert_minutes: int = Field(default=180, validation_alias="NO_NEW_DATA_ALERT_MINUTES")
    ftp_backlog_alert_threshold: int = Field(default=500, validation_alias="FTP_BACKLOG_ALERT_THRESHOLD")
    alert_webhook_url: str | None = Field(default=None, validation_alias="ALERT_WEBHOOK_URL")
    alert_webhook_kind: str = Field(default="generic", validation_alias="ALERT_WEBHOOK_KIND")
    alert_email_to: str | None = Field(default=None, validation_alias="ALERT_EMAIL_TO")
    smtp_host: str | None = Field(default=None, validation_alias="SMTP_HOST")
    smtp_port: int = Field(default=587, validation_alias="SMTP_PORT")
    smtp_user: str | None = Field(default=None, validation_alias="SMTP_USER")
    smtp_password: str | None = Field(default=None, validation_alias="SMTP_PASSWORD")
    smtp_from: str | None = Field(default=None, validation_alias="SMTP_FROM")
    alert_cooldown_minutes: int = Field(default=30, validation_alias="ALERT_COOLDOWN_MINUTES")
    csv_analysis_cache_enabled: bool = Field(default=True, validation_alias="CSV_ANALYSIS_CACHE_ENABLED")

    @property
    def archive_base_dir(self) -> Path:
        return Path(self.archive_base_path).resolve()

    @property
    def transfer_temp_base_dir(self) -> Path:
        return Path(self.transfer_temp_base_path).resolve()

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def env_files_checked(self) -> list[str]:
        return [str(path) for path in _ENV_CANDIDATES]

    @property
    def ftp_read_only_path_list(self) -> list[str]:
        return [item.strip() for item in self.ftp_read_only_paths.split(",") if item.strip()]

    @property
    def alert_email_recipients(self) -> list[str]:
        if not self.alert_email_to:
            return []
        return [item.strip() for item in self.alert_email_to.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.archive_base_dir.mkdir(parents=True, exist_ok=True)
    settings.transfer_temp_base_dir.mkdir(parents=True, exist_ok=True)
    return settings
