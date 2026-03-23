from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.time import utc_now
from app.models.base import Base


class FileSource(str, enum.Enum):
    remote = "remote"
    local = "local"
    ftp = "ftp"


class JobKind(str, enum.Enum):
    download = "download"
    upload = "upload"
    transfer = "transfer"
    verify = "verify"
    inventory_scan = "inventory_scan"


class JobStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class ScheduleCadence(str, enum.Enum):
    interval = "interval"
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class Camera(Base):
    __tablename__ = "cameras"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now)


class VariableGlossary(Base):
    __tablename__ = "variable_glossary"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_cadence_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_image_like: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)


class RemoteDateCache(Base):
    __tablename__ = "remote_date_cache"
    __table_args__ = (
        UniqueConstraint("camera", "variable", "timezone", "date", name="uq_remote_date_cache"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    camera: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    timezone: Mapped[str] = mapped_column(String(8), nullable=False, default="local")
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    file_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    earliest_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    latest_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, index=True)


class FileRecord(Base):
    __tablename__ = "file_records"
    __table_args__ = (
        UniqueConstraint("source", "camera", "variable", "date", "filename", name="uq_file_record_identity"),
        Index("ix_file_records_lookup", "camera", "variable", "date", "source"),
        Index("ix_file_records_source_cam_var_date_ts", "source", "camera", "variable", "date", "parsed_timestamp"),
        Index("ix_file_records_source_date", "source", "date"),
        Index("ix_file_records_source_filename", "source", "filename"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[FileSource] = mapped_column(Enum(FileSource), nullable=False, index=True)
    camera: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    filename: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    parsed_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(128), nullable=True)
    local_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    ftp_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    downloaded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    uploaded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class LocalDateInventory(Base):
    __tablename__ = "local_date_inventory"
    __table_args__ = (UniqueConstraint("camera", "variable", "date", name="uq_local_date_inventory"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    camera: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_modified: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now)


class FTPDateInventory(Base):
    __tablename__ = "ftp_date_inventory"
    __table_args__ = (UniqueConstraint("camera", "variable", "date", name="uq_ftp_date_inventory"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    camera: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    kind: Mapped[JobKind] = mapped_column(Enum(JobKind), nullable=False, index=True)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), nullable=False, index=True, default=JobStatus.queued)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    params_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    progress_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    schedule_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("schedules.id"), nullable=True, index=True)

    events: Mapped[list["JobEvent"]] = relationship("JobEvent", back_populates="job", cascade="all, delete-orphan")


class JobEvent(Base):
    __tablename__ = "job_events"
    __table_args__ = (
        Index("ix_job_events_job_created", "job_id", "created_at"),
        Index("ix_job_events_created", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="INFO")
    message: Mapped[str] = mapped_column(Text, nullable=False)
    camera: Mapped[str | None] = mapped_column(String(128), nullable=True)
    variable: Mapped[str | None] = mapped_column(String(64), nullable=True)
    date: Mapped[str | None] = mapped_column(String(10), nullable=True)
    filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    details_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    job: Mapped[Job] = relationship("Job", back_populates="events")


class FileAuditEvent(Base):
    __tablename__ = "file_audit_events"
    __table_args__ = (
        Index("ix_file_audit_lookup", "camera", "variable", "date", "filename", "created_at"),
        Index("ix_file_audit_job", "job_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, index=True)
    camera: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    filename: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source: Mapped[FileSource | None] = mapped_column(Enum(FileSource), nullable=True, index=True)
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    details_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    job_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=True, index=True)


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    job_kind: Mapped[JobKind] = mapped_column(Enum(JobKind), nullable=False, index=True)
    cadence: Mapped[ScheduleCadence] = mapped_column(Enum(ScheduleCadence), nullable=False, default=ScheduleCadence.interval)
    every_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hour_of_day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    minute_of_hour: Mapped[int | None] = mapped_column(Integer, nullable=True)
    day_of_week: Mapped[int | None] = mapped_column(Integer, nullable=True)
    day_of_month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    params_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)

    __table_args__ = (Index("ix_schedules_enabled_next_run", "enabled", "next_run_at"),)


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)


class CsvAnalysisCache(Base):
    __tablename__ = "csv_analysis_cache"
    __table_args__ = (
        UniqueConstraint("cache_key", name="uq_csv_analysis_cache_key"),
        Index("ix_csv_analysis_cache_file_hash", "file_hash"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cache_key: Mapped[str] = mapped_column(String(256), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rows_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    time_column: Mapped[str | None] = mapped_column(String(128), nullable=True)
    value_column: Mapped[str | None] = mapped_column(String(128), nullable=True)
    result_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utc_now, onupdate=utc_now)
