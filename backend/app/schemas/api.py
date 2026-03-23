from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.models import FileSource, JobKind, JobStatus, ScheduleCadence


class MessageResponse(BaseModel):
    message: str


class FileItem(BaseModel):
    source: FileSource | None = None
    camera: str
    variable: str
    date: str
    filename: str
    parsed_timestamp: datetime | None = None
    file_size: int | None = None
    checksum: str | None = None
    local_path: str | None = None
    ftp_path: str | None = None
    downloaded: bool | None = None
    uploaded: bool | None = None
    verified: bool | None = None
    seen_at: datetime | None = None


class FileListResponse(BaseModel):
    items: list[FileItem]
    total: int
    page: int
    page_size: int


class DiffQuery(BaseModel):
    source_a: FileSource
    source_b: FileSource
    camera: str
    variable: str
    date_from: str | None = None
    date_to: str | None = None
    cadence_seconds: int = 15


class DownloadJobRequest(BaseModel):
    camera: str
    variable: str
    timezone: Literal["local", "utc"] = "local"
    mode: Literal["single_date", "date_range", "rolling_days", "backfill_months", "latest_only"] = "date_range"
    date: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    rolling_days: int | None = None
    backfill_months: int | None = None
    file_selection: Literal["all", "newest_only", "newest_n"] = "all"
    newest_n: int | None = None
    start_time: str | None = None
    end_time: str | None = None
    verify_size: bool = False
    verify_checksum: bool = False
    csv_policy: Literal["always_refresh", "remote_newer", "scheduled_refresh", "never_refresh"] = "always_refresh"
    dry_run: bool = False
    idempotency_key: str | None = None


class UploadJobRequest(BaseModel):
    camera: str
    variable: str
    mode: Literal["single_date", "date_range", "rolling_days", "latest_only"] = "date_range"
    date: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    rolling_days: int | None = None
    dry_run: bool = False
    verify_checksum: bool = False
    run_isolated: bool = False
    idempotency_key: str | None = None


class TransferJobRequest(BaseModel):
    camera: str
    variable: str
    timezone: Literal["local", "utc"] = "local"
    mode: Literal["single_date", "date_range", "rolling_days", "backfill_months", "latest_only"] = "date_range"
    date: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    rolling_days: int | None = None
    backfill_months: int | None = None
    file_selection: Literal["all", "newest_only", "newest_n"] = "all"
    newest_n: int | None = None
    start_time: str | None = None
    end_time: str | None = None
    verify_size: bool = False
    verify_checksum: bool = False
    csv_policy: Literal["always_refresh", "remote_newer", "scheduled_refresh", "never_refresh"] = "always_refresh"
    dry_run: bool = False
    run_isolated: bool = False
    idempotency_key: str | None = None


class VerifyJobRequest(BaseModel):
    source_a: FileSource = FileSource.remote
    source_b: FileSource = FileSource.local
    camera: str
    variable: str
    date_from: str | None = None
    date_to: str | None = None
    cadence_seconds: int = 15
    idempotency_key: str | None = None


class JobResponse(BaseModel):
    id: str
    kind: JobKind
    status: JobStatus
    idempotency_key: str | None = None
    params: dict[str, Any]
    progress: dict[str, Any] | None = None
    error_summary: str | None = None
    retry_count: int
    max_retries: int
    created_at: datetime
    started_at: datetime | None = None
    ended_at: datetime | None = None


class JobEventResponse(BaseModel):
    created_at: datetime
    level: str
    message: str
    camera: str | None = None
    variable: str | None = None
    date: str | None = None
    filename: str | None = None
    reason: str | None = None
    details: dict[str, Any] | None = None


class ScheduleRequest(BaseModel):
    name: str
    enabled: bool = True
    job_kind: JobKind
    cadence: ScheduleCadence = ScheduleCadence.interval
    every_minutes: int | None = 60
    hour_of_day: int | None = None
    minute_of_hour: int | None = None
    day_of_week: int | None = None
    day_of_month: int | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class ScheduleResponse(BaseModel):
    id: str
    name: str
    enabled: bool
    job_kind: JobKind
    cadence: ScheduleCadence
    every_minutes: int | None = None
    hour_of_day: int | None = None
    minute_of_hour: int | None = None
    day_of_week: int | None = None
    day_of_month: int | None = None
    params: dict[str, Any]
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class GlossaryItemRequest(BaseModel):
    variable: str
    description: str | None = None
    expected_cadence_seconds: int | None = None
    is_image_like: bool = False


class GlossaryItemResponse(BaseModel):
    variable: str
    description: str | None = None
    expected_cadence_seconds: int | None = None
    is_image_like: bool
    updated_at: datetime

