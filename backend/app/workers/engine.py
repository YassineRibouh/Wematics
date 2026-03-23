from __future__ import annotations

import logging
import threading
import time
import calendar
import shutil
import traceback
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import and_, desc, func, select

from app.core.config import get_settings
from app.core.time import utc_now
from app.db.session import db_context
from app.models import (
    FileAuditEvent,
    FileRecord,
    FileSource,
    Job,
    JobEvent,
    JobKind,
    JobStatus,
    LocalDateInventory,
    Schedule,
    ScheduleCadence,
)
from app.services.diff_service import DiffService
from app.services.ftp_service import FTPService
from app.services.file_record_service import get_or_create_file_record
from app.services.job_utils import (
    apply_file_selection,
    filter_files_by_time_window,
    resolve_dates_from_mode,
    should_refresh_csv,
)
from app.services.local_inventory_service import LocalInventoryService
from app.services.notification_service import NotificationService
from app.services.paths import safe_join
from app.services.hash_utils import sha256_file
from app.services.timestamps import parse_filename_timestamp
from app.services.wematics_service import WematicsService

logger = logging.getLogger(__name__)
_OVERLAP_GUARD_KINDS = {
    JobKind.download,
    JobKind.upload,
    JobKind.transfer,
    JobKind.inventory_scan,
}


def _as_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _date_range(start: str, end: str) -> list[str]:
    current = _as_date(start)
    last = _as_date(end)
    out: list[str] = []
    while current <= last:
        out.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return out


def _requested_dates(mode: str, params: dict, available_dates: list[str]) -> list[str]:
    if mode == "single_date":
        return [params["date"]] if params.get("date") else []
    if mode == "date_range" and params.get("date_from") and params.get("date_to"):
        return _date_range(params["date_from"], params["date_to"])
    return resolve_dates_from_mode(mode, available_dates, params)


def _normalize_scope_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_date_window(params: dict | None) -> tuple[str | None, str | None]:
    payload = dict(params or {})
    mode = str(payload.get("mode") or "").strip()

    if mode == "single_date":
        single = _normalize_scope_value(payload.get("date"))
        if single:
            return single, single

    if mode == "date_range":
        start = _normalize_scope_value(payload.get("date_from"))
        end = _normalize_scope_value(payload.get("date_to"))
        if start and end and start > end:
            start, end = end, start
        return start, end

    start = _normalize_scope_value(payload.get("date_from"))
    end = _normalize_scope_value(payload.get("date_to"))
    if start and end and start > end:
        start, end = end, start
    return start, end


def _scope_overlaps(left: str | None, right: str | None) -> bool:
    return left is None or right is None or left == right


def _windows_overlap(
    left_start: str | None,
    left_end: str | None,
    right_start: str | None,
    right_end: str | None,
) -> bool:
    if left_end is not None and right_start is not None and left_end < right_start:
        return False
    if right_end is not None and left_start is not None and right_end < left_start:
        return False
    return True


def _job_windows_overlap(left_params: dict | None, right_params: dict | None) -> bool:
    left = dict(left_params or {})
    right = dict(right_params or {})
    left_camera = _normalize_scope_value(left.get("camera"))
    right_camera = _normalize_scope_value(right.get("camera"))
    left_variable = _normalize_scope_value(left.get("variable"))
    right_variable = _normalize_scope_value(right.get("variable"))
    if not _scope_overlaps(left_camera, right_camera):
        return False
    if not _scope_overlaps(left_variable, right_variable):
        return False

    left_start, left_end = _extract_date_window(left)
    right_start, right_end = _extract_date_window(right)
    return _windows_overlap(left_start, left_end, right_start, right_end)


def _compute_next_run(schedule: Schedule, now: datetime) -> datetime:
    if schedule.cadence == ScheduleCadence.interval:
        step = schedule.every_minutes or 60
        return now + timedelta(minutes=step)
    if schedule.cadence == ScheduleCadence.daily:
        hour = schedule.hour_of_day if schedule.hour_of_day is not None else 0
        minute = schedule.minute_of_hour if schedule.minute_of_hour is not None else 0
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target
    if schedule.cadence == ScheduleCadence.weekly:
        hour = schedule.hour_of_day if schedule.hour_of_day is not None else 0
        minute = schedule.minute_of_hour if schedule.minute_of_hour is not None else 0
        target_weekday = schedule.day_of_week if schedule.day_of_week is not None else 0
        days_ahead = (target_weekday - now.weekday()) % 7
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
        if target <= now:
            target += timedelta(days=7)
        return target
    day = schedule.day_of_month if schedule.day_of_month is not None else 1
    hour = schedule.hour_of_day if schedule.hour_of_day is not None else 0
    minute = schedule.minute_of_hour if schedule.minute_of_hour is not None else 0
    month = now.month
    year = now.year
    for _ in range(24):
        max_day = calendar.monthrange(year, month)[1]
        candidate_day = min(day, max_day)
        candidate = datetime(year=year, month=month, day=candidate_day, hour=hour, minute=minute)
        if candidate > now:
            return candidate
        month += 1
        if month > 12:
            month = 1
            year += 1
    return now + timedelta(days=30)


@dataclass(slots=True)
class JobCreateResult:
    id: str
    kind: JobKind
    status: JobStatus


@dataclass(slots=True)
class DownloadTask:
    camera: str
    variable: str
    date: str
    filename: str
    timezone: str
    target_dir: str
    local_path: Path


@dataclass(slots=True)
class DownloadTaskResult:
    task: DownloadTask
    error: str | None = None


@dataclass(slots=True)
class UploadTask:
    camera: str
    variable: str
    date: str
    filename: str
    local_path: str


class JobCancelledError(RuntimeError):
    pass


class JobProgressTracker:
    def __init__(self, **initial: object) -> None:
        self._lock = threading.Lock()
        state = dict(initial)
        state.setdefault("queued_files", 0)
        state.setdefault("processed_files", 0)
        state.setdefault("downloaded", 0)
        state.setdefault("uploaded", 0)
        state.setdefault("skipped", 0)
        state.setdefault("planned", 0)
        state.setdefault("errors", 0)
        state.setdefault("recent_failures", [])
        state.setdefault("cancel_requested", False)
        self._state = state
        self._failure_keys: set[str] = set()
        self._recompute_locked()

    def _recompute_locked(self) -> None:
        queued_files = int(self._state.get("queued_files") or 0)
        processed_files = int(self._state.get("processed_files") or 0)
        remaining_files = max(queued_files - processed_files, 0)
        progress_pct = 0.0
        if queued_files > 0:
            progress_pct = round((processed_files / queued_files) * 100.0, 1)
        elif str(self._state.get("stage") or "") == "completed":
            progress_pct = 100.0
        self._state["remaining_files"] = remaining_files
        self._state["progress_pct"] = progress_pct
        self._state["updated_at"] = utc_now().isoformat()

    def update(self, **updates: object) -> None:
        with self._lock:
            self._state.update(updates)
            self._recompute_locked()

    def mark_cancel_requested(self) -> None:
        with self._lock:
            self._state["cancel_requested"] = True
            self._state["cancel_requested_at"] = utc_now().isoformat()
            self._recompute_locked()

    def advance(
        self,
        *,
        processed: int = 0,
        downloaded: int = 0,
        uploaded: int = 0,
        skipped: int = 0,
        planned: int = 0,
        errors: int = 0,
        date: str | None = None,
        filename: str | None = None,
        activity: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            self._state["processed_files"] = int(self._state.get("processed_files") or 0) + int(processed or 0)
            self._state["downloaded"] = int(self._state.get("downloaded") or 0) + int(downloaded or 0)
            self._state["uploaded"] = int(self._state.get("uploaded") or 0) + int(uploaded or 0)
            self._state["skipped"] = int(self._state.get("skipped") or 0) + int(skipped or 0)
            self._state["planned"] = int(self._state.get("planned") or 0) + int(planned or 0)
            self._state["errors"] = int(self._state.get("errors") or 0) + int(errors or 0)
            if date is not None:
                self._state["current_date"] = date
            if filename is not None:
                self._state["current_file"] = filename
            if activity is not None:
                self._state["current_activity"] = activity
            if error:
                self._state["last_error"] = error
            self._recompute_locked()

    def record_failure(self, *, task: DownloadTask | UploadTask, phase: str, error: str) -> None:
        with self._lock:
            key = "|".join(
                [
                    getattr(task, "camera", "") or "",
                    getattr(task, "variable", "") or "",
                    getattr(task, "date", "") or "",
                    getattr(task, "filename", "") or "",
                    phase,
                ]
            )
            if key in self._failure_keys:
                return
            self._failure_keys.add(key)
            recent_failures = list(self._state.get("recent_failures") or [])
            recent_failures.insert(
                0,
                {
                    "camera": getattr(task, "camera", None),
                    "variable": getattr(task, "variable", None),
                    "date": getattr(task, "date", None),
                    "filename": getattr(task, "filename", None),
                    "phase": phase,
                    "error": error,
                },
            )
            self._state["recent_failures"] = recent_failures[:20]
            self._state["last_error"] = error
            self._recompute_locked()

    def snapshot(self) -> dict:
        with self._lock:
            snapshot = dict(self._state)
            snapshot["recent_failures"] = [dict(item) for item in list(self._state.get("recent_failures") or [])]
            return snapshot


def _split_work(items: list, workers: int) -> list[list]:
    if not items:
        return []
    count = max(1, min(int(workers or 1), len(items)))
    buckets = [[] for _ in range(count)]
    for idx, item in enumerate(items):
        buckets[idx % count].append(item)
    return [bucket for bucket in buckets if bucket]


def _resume_file_targets(params: dict | None) -> dict[str, set[str]]:
    targets: dict[str, set[str]] = {}
    for item in list((params or {}).get("resume_failure_files") or []):
        if not isinstance(item, dict):
            continue
        date = _normalize_scope_value(item.get("date"))
        filename = _normalize_scope_value(item.get("filename"))
        if not date or not filename:
            continue
        targets.setdefault(date, set()).add(filename)
    return targets


def _filter_resume_files(files: list[str], day: str, targets: dict[str, set[str]]) -> list[str]:
    if not targets:
        return files
    allowed = targets.get(day)
    if not allowed:
        return []
    return [name for name in files if name in allowed]


class JobEngine:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.notifier = NotificationService(self.settings)
        self._stop_event = threading.Event()
        self._worker_thread: threading.Thread | None = None
        self._scheduler_thread: threading.Thread | None = None
        self._alert_last_sent: dict[str, datetime] = {}
        self._runtime_lock = threading.Lock()
        self._job_cancel_flags: dict[str, threading.Event] = {}
        self._job_progress_trackers: dict[str, JobProgressTracker] = {}

    def start(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._recover_stale_jobs()
        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="job-worker")
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True, name="job-scheduler")
        self._worker_thread.start()
        self._scheduler_thread.start()
        logger.info("Job engine started.")

    def _recover_stale_jobs(self) -> None:
        with db_context() as db:
            rows = db.scalars(select(Job).where(Job.status == JobStatus.running)).all()
            for row in rows:
                row.status = JobStatus.queued
                row.started_at = None
                db.add(
                    JobEvent(
                        job_id=row.id,
                        level="WARNING",
                        message="Recovered running job after restart; re-queued.",
                    )
                )

    def stop(self) -> None:
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)

    def _register_runtime(self, job_id: str, tracker: JobProgressTracker) -> JobProgressTracker:
        with self._runtime_lock:
            cancel_event = self._job_cancel_flags.get(job_id)
            if cancel_event is None:
                cancel_event = threading.Event()
                self._job_cancel_flags[job_id] = cancel_event
            else:
                cancel_event.clear()
            self._job_progress_trackers[job_id] = tracker
        return tracker

    def _release_runtime(self, job_id: str) -> None:
        with self._runtime_lock:
            self._job_progress_trackers.pop(job_id, None)
            cancel_event = self._job_cancel_flags.pop(job_id, None)
        if cancel_event is not None:
            cancel_event.clear()

    def _get_progress_tracker(self, job_id: str) -> JobProgressTracker | None:
        with self._runtime_lock:
            return self._job_progress_trackers.get(job_id)

    def _get_cancel_event(self, job_id: str) -> threading.Event:
        with self._runtime_lock:
            cancel_event = self._job_cancel_flags.get(job_id)
            if cancel_event is None:
                cancel_event = threading.Event()
                self._job_cancel_flags[job_id] = cancel_event
            return cancel_event

    def _raise_if_job_cancelled(self, job_id: str) -> None:
        if self._get_cancel_event(job_id).is_set():
            raise JobCancelledError("Job cancelled by user.")

    def _create_progress_tracker(self, job_id: str, **initial: object) -> JobProgressTracker:
        tracker = self._register_runtime(job_id, JobProgressTracker(**initial))
        self._flush_runtime_progress(job_id)
        return tracker

    def _flush_runtime_progress(self, job_id: str) -> None:
        tracker = self._get_progress_tracker(job_id)
        if tracker is None:
            return
        snapshot = tracker.snapshot()
        with db_context() as db:
            row = db.get(Job, job_id)
            if row is None:
                return
            current = dict(row.progress_json or {})
            current.update(snapshot)
            row.progress_json = current

    def cancel_job(self, db, job_id: str) -> Job:
        row = db.get(Job, job_id)
        if row is None:
            raise RuntimeError("Job not found")
        if row.status in {JobStatus.completed, JobStatus.failed, JobStatus.cancelled}:
            raise RuntimeError("Only queued or running jobs can be stopped.")

        now = utc_now()
        current = dict(row.progress_json or {})
        current["cancel_requested"] = True
        current["cancel_requested_at"] = now.isoformat()
        row.progress_json = current

        if row.status == JobStatus.queued:
            row.status = JobStatus.cancelled
            row.ended_at = now
            row.progress_json = {**current, "stage": "cancelled"}
            db.add(JobEvent(job_id=row.id, level="WARNING", message="Queued job cancelled before start"))
            return row

        tracker = self._get_progress_tracker(row.id)
        if tracker is not None:
            tracker.mark_cancel_requested()
        self._get_cancel_event(row.id).set()
        db.add(JobEvent(job_id=row.id, level="WARNING", message="Stop requested for running job"))
        return row

    def _find_overlapping_active_job(self, db, kind: JobKind, params: dict) -> Job | None:
        if kind not in _OVERLAP_GUARD_KINDS:
            return None
        active_rows = db.scalars(
            select(Job)
            .where(
                and_(
                    Job.kind.in_(tuple(_OVERLAP_GUARD_KINDS)),
                    Job.status.in_([JobStatus.queued, JobStatus.running]),
                )
            )
            .order_by(Job.created_at.asc())
        ).all()
        requested_params = dict(params or {})
        for row in active_rows:
            if _job_windows_overlap(requested_params, dict(row.params_json or {})):
                return row
        return None

    def enqueue(
        self,
        kind: JobKind,
        params: dict,
        schedule_id: str | None = None,
        max_retries: int = 3,
        idempotency_key: str | None = None,
    ) -> JobCreateResult:
        with db_context() as db:
            if idempotency_key:
                existing = db.scalar(
                    select(Job).where(
                        and_(
                            Job.kind == kind,
                            Job.idempotency_key == idempotency_key,
                            Job.status.in_([JobStatus.queued, JobStatus.running]),
                        )
                    )
                )
                if existing is not None:
                    return JobCreateResult(id=existing.id, kind=existing.kind, status=existing.status)
            overlapping = self._find_overlapping_active_job(db, kind=kind, params=params)
            if overlapping is not None:
                db.add(
                    JobEvent(
                        job_id=overlapping.id,
                        level="INFO",
                        message="Skipped overlapping enqueue request; reusing active job.",
                        details_json={
                            "requested_kind": kind.value,
                            "requested_camera": params.get("camera"),
                            "requested_variable": params.get("variable"),
                            "requested_date": params.get("date"),
                            "requested_date_from": params.get("date_from"),
                            "requested_date_to": params.get("date_to"),
                        },
                    )
                )
                return JobCreateResult(id=overlapping.id, kind=overlapping.kind, status=overlapping.status)
            job = Job(
                kind=kind,
                params_json=params,
                schedule_id=schedule_id,
                max_retries=max_retries,
                idempotency_key=idempotency_key,
            )
            db.add(job)
            db.flush()
            db.add(
                JobEvent(
                    job_id=job.id,
                    level="INFO",
                    message=f"Job enqueued: {kind.value}",
                    details_json={"params": params, "idempotency_key": idempotency_key},
                )
            )
            return JobCreateResult(id=job.id, kind=job.kind, status=job.status)

    def _scheduler_loop(self) -> None:
        poll_interval = self.settings.scheduler_poll_interval_seconds
        while not self._stop_event.is_set():
            try:
                now = utc_now()
                with db_context() as db:
                    schedules = db.scalars(select(Schedule).where(Schedule.enabled.is_(True))).all()
                    for schedule in schedules:
                        if schedule.next_run_at is None:
                            schedule.next_run_at = _compute_next_run(schedule, now)
                            continue
                        if schedule.next_run_at <= now:
                            self.enqueue(kind=schedule.job_kind, params=schedule.params_json, schedule_id=schedule.id)
                            schedule.last_run_at = now
                            schedule.next_run_at = _compute_next_run(schedule, now + timedelta(seconds=1))
                    self._check_system_alerts(db, now)
            except Exception as exc:
                logger.exception("Scheduler loop failed: %s", exc)
            self._stop_event.wait(poll_interval)

    def _next_job(self) -> Job | None:
        with db_context() as db:
            job = db.scalar(
                select(Job)
                .where(Job.status == JobStatus.queued)
                .order_by(Job.created_at.asc())
                .limit(1)
            )
            if not job:
                return None
            job.status = JobStatus.running
            job.started_at = utc_now()
            db.add(JobEvent(job_id=job.id, level="INFO", message="Job started"))
            db.flush()
            db.expunge(job)
            return job

    def _worker_loop(self) -> None:
        poll_interval = self.settings.worker_poll_interval_seconds
        while not self._stop_event.is_set():
            job = self._next_job()
            if job is None:
                self._stop_event.wait(poll_interval)
                continue
            try:
                self._run_job(job)
            except Exception as exc:
                logger.exception("Unhandled job failure for %s: %s", job.id, exc)
                with db_context() as db:
                    row = db.get(Job, job.id)
                    if row:
                        row.status = JobStatus.failed
                        row.error_summary = str(exc)
                        row.ended_at = utc_now()
                        db.add(JobEvent(job_id=row.id, level="ERROR", message="Job failed", details_json={"error": str(exc)}))

    def _update_progress(self, db, job: Job, **updates) -> None:
        current = dict(job.progress_json or {})
        current.update(updates)
        job.progress_json = current
        db.add(job)

    def _download_chunk(self, tasks: list[DownloadTask], *, job_id: str) -> list[DownloadTaskResult]:
        remote = WematicsService(db=None)
        results: list[DownloadTaskResult] = []
        tracker = self._get_progress_tracker(job_id)
        for task in tasks:
            self._raise_if_job_cancelled(job_id)
            try:
                if tracker is not None:
                    tracker.update(current_activity="downloading", current_date=task.date, current_file=task.filename)
                remote.download_file(
                    camera=task.camera,
                    variable=task.variable,
                    filename=task.filename,
                    target_dir=task.target_dir,
                    timezone=task.timezone,
                )
                if tracker is not None:
                    tracker.advance(
                        processed=1,
                        downloaded=1,
                        date=task.date,
                        filename=task.filename,
                        activity="downloaded",
                    )
                results.append(DownloadTaskResult(task=task))
            except Exception as exc:
                if tracker is not None:
                    tracker.record_failure(task=task, phase="download", error=str(exc))
                    tracker.advance(
                        processed=1,
                        errors=1,
                        date=task.date,
                        filename=task.filename,
                        activity="error",
                        error=str(exc),
                    )
                results.append(DownloadTaskResult(task=task, error=str(exc)))
        return results

    def _build_upload_tasks(self, db, camera: str, variable: str, target_dates: list[str], *, params: dict | None = None) -> list[UploadTask]:
        tasks: list[UploadTask] = []
        resume_targets = _resume_file_targets(params)
        for day in target_dates:
            rows = db.scalars(
                select(FileRecord)
                .where(
                    and_(
                        FileRecord.source == FileSource.local,
                        FileRecord.camera == camera,
                        FileRecord.variable == variable,
                        FileRecord.date == day,
                    )
                )
                .order_by(FileRecord.date.asc(), FileRecord.filename.asc())
            ).all()
            allowed = resume_targets.get(day) if resume_targets else None
            for row in rows:
                if allowed is not None and row.filename not in allowed:
                    continue
                if not row.local_path:
                    continue
                local_path = Path(row.local_path)
                if not local_path.exists():
                    continue
                tasks.append(
                    UploadTask(
                        camera=camera,
                        variable=variable,
                        date=day,
                        filename=row.filename,
                        local_path=str(local_path),
                    )
                )
        return tasks

    def _upload_chunk(
        self,
        tasks: list[UploadTask],
        *,
        run_id: str,
        stable_mode: bool,
        verify_checksum: bool,
        dry_run: bool,
        job_id: str,
    ) -> dict:
        uploaded = 0
        skipped = 0
        errors = 0
        tracker = self._get_progress_tracker(job_id)
        with db_context() as worker_db:
            ftp_service = FTPService(worker_db)
            with ftp_service.connect() as ftp_client:
                for task in tasks:
                    self._raise_if_job_cancelled(job_id)
                    local_path = Path(task.local_path)
                    if not local_path.exists():
                        errors += 1
                        worker_db.add(
                            JobEvent(
                                job_id=job_id,
                                level="ERROR",
                                message=f"Upload skipped: local file missing ({task.filename})",
                                camera=task.camera,
                                variable=task.variable,
                                date=task.date,
                                filename=task.filename,
                                reason="missing_local_file",
                            )
                        )
                        worker_db.commit()
                        if tracker is not None:
                            tracker.record_failure(task=task, phase="upload", error="Local file missing")
                            tracker.advance(
                                processed=1,
                                errors=1,
                                date=task.date,
                                filename=task.filename,
                                activity="error",
                                error="Local file missing",
                            )
                        continue
                    try:
                        if tracker is not None:
                            tracker.update(current_activity="uploading", current_date=task.date, current_file=task.filename)
                        outcome = ftp_service.upload_additive(
                            client=ftp_client,
                            run_id=run_id,
                            camera=task.camera,
                            variable=task.variable,
                            date=task.date,
                            filename=task.filename,
                            local_path=local_path,
                            dry_run=dry_run,
                            stable_mode=stable_mode,
                            verify_checksum=verify_checksum,
                            job_id=job_id,
                        )
                        if outcome.action in {"uploaded", "plan_upload"}:
                            uploaded += 1
                            if tracker is not None:
                                tracker.advance(
                                    processed=1,
                                    uploaded=1,
                                    date=task.date,
                                    filename=task.filename,
                                    activity="uploaded",
                                )
                        else:
                            skipped += 1
                            if tracker is not None:
                                tracker.advance(
                                    processed=1,
                                    skipped=1,
                                    date=task.date,
                                    filename=task.filename,
                                    activity="skipped",
                                )
                    except Exception as exc:
                        errors += 1
                        worker_db.add(
                            JobEvent(
                                job_id=job_id,
                                level="ERROR",
                                message=f"Upload failed: {task.filename}",
                                camera=task.camera,
                                variable=task.variable,
                                date=task.date,
                                filename=task.filename,
                                details_json={"error": str(exc)},
                            )
                        )
                        worker_db.commit()
                        if tracker is not None:
                            tracker.record_failure(task=task, phase="upload", error=str(exc))
                            tracker.advance(
                                processed=1,
                                errors=1,
                                date=task.date,
                                filename=task.filename,
                                activity="error",
                                error=str(exc),
                            )
        return {"uploaded": uploaded, "skipped": skipped, "errors": errors}

    def _run_job(self, job_stub: Job) -> None:
        try:
            with db_context() as db:
                job = db.get(Job, job_stub.id)
                if not job:
                    return
                params = dict(job.params_json or {})
                try:
                    if job.kind == JobKind.download:
                        self._execute_download(db, job, params)
                    elif job.kind == JobKind.upload:
                        self._execute_upload(db, job, params)
                    elif job.kind == JobKind.transfer:
                        self._execute_transfer(db, job, params)
                    elif job.kind == JobKind.verify:
                        self._execute_verify(db, job, params)
                    elif job.kind == JobKind.inventory_scan:
                        self._execute_inventory_scan(db, job, params)

                    tracker = self._get_progress_tracker(job.id)
                    if tracker is not None:
                        tracker.update(stage="completed", current_activity="done")
                        self._flush_runtime_progress(job.id)
                    job.status = JobStatus.completed
                    job.ended_at = utc_now()
                    job.error_summary = None
                    db.add(JobEvent(job_id=job.id, level="INFO", message="Job completed"))
                except JobCancelledError:
                    db.rollback()
                    job = db.get(Job, job_stub.id)
                    if not job:
                        logger.error("Failed to reload cancelled job %s", job_stub.id)
                        return
                    tracker = self._get_progress_tracker(job.id)
                    if tracker is not None:
                        tracker.update(stage="cancelled", current_activity="cancelled")
                        tracker.mark_cancel_requested()
                        self._flush_runtime_progress(job.id)
                    job.status = JobStatus.cancelled
                    job.ended_at = utc_now()
                    job.error_summary = None
                    db.add(JobEvent(job_id=job.id, level="WARNING", message="Job cancelled"))
                except Exception as exc:
                    error_details = {
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    }
                    kind_value = job_stub.kind.value if getattr(job_stub, "kind", None) else "unknown"
                    logger.exception("Job %s (%s) failed during execution", job_stub.id, kind_value)
                    db.rollback()
                    job = db.get(Job, job_stub.id)
                    if not job:
                        logger.error("Failed to reload job %s after exception: %s", job_stub.id, exc)
                        return

                    job.retry_count += 1
                    job.error_summary = str(exc)
                    tracker = self._get_progress_tracker(job.id)
                    if job.retry_count <= job.max_retries:
                        if tracker is not None:
                            tracker.update(stage="retry_scheduled", current_activity="retrying", last_error=str(exc))
                            self._flush_runtime_progress(job.id)
                        job.status = JobStatus.queued
                        db.add(
                            JobEvent(
                                job_id=job.id,
                                level="WARNING",
                                message=f"Job retry scheduled ({job.retry_count}/{job.max_retries})",
                                details_json=error_details,
                            )
                        )
                    else:
                        if tracker is not None:
                            tracker.update(stage="failed", current_activity="failed", last_error=str(exc))
                            self._flush_runtime_progress(job.id)
                        job.status = JobStatus.failed
                        job.ended_at = utc_now()
                        db.add(JobEvent(job_id=job.id, level="ERROR", message="Job failed", details_json=error_details))
                        self._notify(
                            key=f"job_failed:{job.id}",
                            title=f"Wematics job failed ({job.kind.value})",
                            message=f"Job {job.id} failed after {job.retry_count} retries: {exc}",
                            details={"job_id": job.id, "kind": job.kind.value, "error": str(exc)},
                        )
        finally:
            self._release_runtime(job_stub.id)

    def _execute_inventory_scan(self, db, job: Job, params: dict) -> None:
        service = LocalInventoryService(db)
        summary = service.scan_incremental(camera=params.get("camera"), variable=params.get("variable"), force=True)
        self._update_progress(
            db,
            job,
            scanned_dates=summary.scanned_dates,
            scanned_files=summary.scanned_files,
            skipped_dates=summary.skipped_dates,
        )

    def _upsert_local_file(
        self,
        db,
        camera: str,
        variable: str,
        date: str,
        filename: str,
        local_path: Path,
        verify_checksum: bool = False,
    ) -> str | None:
        row, _ = get_or_create_file_record(
            db,
            source=FileSource.local,
            camera=camera,
            variable=variable,
            date=date,
            filename=filename,
        )
        row.file_size = local_path.stat().st_size if local_path.exists() else None
        row.local_path = str(local_path)
        checksum = None
        if verify_checksum and local_path.exists():
            checksum = sha256_file(local_path)
            row.checksum = checksum
        row.parsed_timestamp = parse_filename_timestamp(filename)
        row.downloaded = True
        row.downloaded_at = utc_now()
        row.seen_at = utc_now()
        return checksum

    def _cleanup_transfer_temp_file(self, local_path: Path) -> None:
        try:
            if local_path.exists():
                local_path.unlink()
        except Exception as exc:
            logger.warning("Failed to remove transfer temp file %s: %s", local_path, exc)

    def _cleanup_transfer_temp_root(self, job_temp_root: Path) -> None:
        try:
            shutil.rmtree(job_temp_root, ignore_errors=True)
        except Exception as exc:
            logger.warning("Failed to remove transfer temp workspace %s: %s", job_temp_root, exc)

    def _prepare_transfer_tasks(self, db, job: Job, params: dict) -> tuple[list[DownloadTask], list[str], list[str]]:
        camera = params["camera"]
        variable = params["variable"]
        timezone = params.get("timezone", self.settings.default_timezone)
        mode = params.get("mode", "date_range")
        resume_targets = _resume_file_targets(params)

        remote = WematicsService(db=db)
        available_dates = remote.list_dates(camera=camera, variable=variable, timezone=timezone)
        available_date_set = set(available_dates)
        requested_dates = _requested_dates(mode, params, available_dates)
        missing_dates: list[str] = []
        transfer_tasks: list[DownloadTask] = []
        job_temp_root = safe_join(self.settings.transfer_temp_base_dir, job.id)

        for day in requested_dates:
            if day not in available_date_set:
                missing_dates.append(day)
                db.add(
                    JobEvent(
                        job_id=job.id,
                        level="WARNING",
                        message=f"Date not available remotely: {day}",
                        camera=camera,
                        variable=variable,
                        date=day,
                        reason="missing_remote_date",
                    )
                )
                continue

            files = remote.list_files(camera=camera, variable=variable, date=day, timezone=timezone)
            files = filter_files_by_time_window(files, params.get("start_time"), params.get("end_time"))
            files = apply_file_selection(files, params.get("file_selection", "all"), params.get("newest_n"))
            files = _filter_resume_files(files, day, resume_targets)
            if not files:
                continue

            target_dir = safe_join(job_temp_root, camera, variable, day)
            for filename in files:
                transfer_tasks.append(
                    DownloadTask(
                        camera=camera,
                        variable=variable,
                        date=day,
                        filename=filename,
                        timezone=timezone,
                        target_dir=str(target_dir),
                        local_path=target_dir / filename,
                    )
                )

        return transfer_tasks, requested_dates, missing_dates

    def _transfer_chunk(
        self,
        tasks: list[DownloadTask],
        *,
        run_id: str,
        stable_mode: bool,
        verify_checksum: bool,
        dry_run: bool,
        job_id: str,
        job_temp_root: Path,
    ) -> dict:
        downloaded = 0
        uploaded = 0
        skipped = 0
        planned = 0
        errors = 0
        tracker = self._get_progress_tracker(job_id)

        if dry_run:
            with db_context() as worker_db:
                for task in tasks:
                    self._raise_if_job_cancelled(job_id)
                    planned += 1
                    worker_db.add(
                        JobEvent(
                            job_id=job_id,
                            level="INFO",
                            message=f"[dry-run] Transfer planned: {task.filename}",
                            camera=task.camera,
                            variable=task.variable,
                            date=task.date,
                            filename=task.filename,
                        )
                    )
                    worker_db.commit()
                    if tracker is not None:
                        tracker.advance(
                            processed=1,
                            planned=1,
                            date=task.date,
                            filename=task.filename,
                            activity="planned",
                        )
            return {"downloaded": 0, "uploaded": 0, "skipped": 0, "planned": planned, "errors": 0}

        remote = WematicsService(db=None)
        with db_context() as worker_db:
            ftp_service = FTPService(worker_db)
            with ftp_service.connect() as ftp_client:
                for task in tasks:
                    self._raise_if_job_cancelled(job_id)
                    local_path = task.local_path
                    try:
                        if tracker is not None:
                            tracker.update(current_activity="downloading", current_date=task.date, current_file=task.filename)
                        Path(task.target_dir).mkdir(parents=True, exist_ok=True)
                        remote.download_file(
                            camera=task.camera,
                            variable=task.variable,
                            filename=task.filename,
                            target_dir=task.target_dir,
                            timezone=task.timezone,
                        )
                        downloaded += 1
                        if tracker is not None:
                            tracker.advance(
                                downloaded=1,
                                date=task.date,
                                filename=task.filename,
                                activity="downloaded",
                            )

                        self._raise_if_job_cancelled(job_id)
                        if tracker is not None:
                            tracker.update(current_activity="uploading", current_date=task.date, current_file=task.filename)
                        outcome = ftp_service.upload_additive(
                            client=ftp_client,
                            run_id=run_id,
                            camera=task.camera,
                            variable=task.variable,
                            date=task.date,
                            filename=task.filename,
                            local_path=local_path,
                            dry_run=False,
                            stable_mode=stable_mode,
                            verify_checksum=verify_checksum,
                            job_id=job_id,
                        )
                        if outcome.action == "skip":
                            skipped += 1
                            if tracker is not None:
                                tracker.advance(
                                    processed=1,
                                    skipped=1,
                                    date=task.date,
                                    filename=task.filename,
                                    activity="skipped",
                                )
                        else:
                            uploaded += 1
                            if tracker is not None:
                                tracker.advance(
                                    processed=1,
                                    uploaded=1,
                                    date=task.date,
                                    filename=task.filename,
                                    activity="uploaded",
                                )
                    except Exception as exc:
                        errors += 1
                        worker_db.add(
                            JobEvent(
                                job_id=job_id,
                                level="ERROR",
                                message=f"Transfer failed: {task.filename}",
                                camera=task.camera,
                                variable=task.variable,
                                date=task.date,
                                filename=task.filename,
                                details_json={"error": str(exc)},
                            )
                        )
                        worker_db.commit()
                        if tracker is not None:
                            tracker.record_failure(task=task, phase="transfer", error=str(exc))
                            tracker.advance(
                                processed=1,
                                errors=1,
                                date=task.date,
                                filename=task.filename,
                                activity="error",
                                error=str(exc),
                            )
                    finally:
                        self._cleanup_transfer_temp_file(local_path)

        return {
            "downloaded": downloaded,
            "uploaded": uploaded,
            "skipped": skipped,
            "planned": planned,
            "errors": errors,
        }

    def _execute_download(self, db, job: Job, params: dict) -> None:
        camera = params["camera"]
        variable = params["variable"]
        timezone = params.get("timezone", self.settings.default_timezone)
        mode = params.get("mode", "date_range")
        csv_policy = params.get("csv_policy", "always_refresh")
        dry_run = bool(params.get("dry_run", False))
        verify_checksum = bool(params.get("verify_checksum", False))
        resume_targets = _resume_file_targets(params)

        remote = WematicsService(db=db)
        available_dates = remote.list_dates(camera=camera, variable=variable, timezone=timezone)
        available_date_set = set(available_dates)
        requested_dates = _requested_dates(mode, params, available_dates)

        missing_dates = [day for day in requested_dates if day not in available_date_set]
        downloaded_count = 0
        skipped_count = 0
        error_count = 0
        download_tasks: list[DownloadTask] = []

        for day in requested_dates:
            if day not in available_date_set:
                db.add(
                    JobEvent(
                        job_id=job.id,
                        level="WARNING",
                        message=f"Date not available remotely: {day}",
                        camera=camera,
                        variable=variable,
                        date=day,
                        reason="missing_remote_date",
                    )
                )
                continue

            files = remote.list_files(camera=camera, variable=variable, date=day, timezone=timezone)
            files = filter_files_by_time_window(files, params.get("start_time"), params.get("end_time"))
            files = apply_file_selection(files, params.get("file_selection", "all"), params.get("newest_n"))
            files = _filter_resume_files(files, day, resume_targets)
            if not files:
                continue

            target_dir = safe_join(self.settings.archive_base_dir, camera, variable, day)
            target_dir.mkdir(parents=True, exist_ok=True)

            for filename in files:
                local_path = target_dir / filename
                is_csv = filename.lower().endswith(".csv")
                if local_path.exists():
                    refresh_csv = is_csv and should_refresh_csv(
                        policy=csv_policy,
                        local_path=local_path,
                        remote_filename=filename,
                    )
                    if not refresh_csv:
                        skipped_count += 1
                        db.add(
                            FileAuditEvent(
                                camera=camera,
                                variable=variable,
                                date=day,
                                filename=filename,
                                source=FileSource.local,
                                action="download_skipped",
                                reason="already_exists",
                                job_id=job.id,
                            )
                        )
                        continue

                if dry_run:
                    skipped_count += 1
                    db.add(
                        JobEvent(
                            job_id=job.id,
                            level="INFO",
                            message=f"[dry-run] Download planned: {filename}",
                            camera=camera,
                            variable=variable,
                            date=day,
                            filename=filename,
                        )
                    )
                    continue

                download_tasks.append(
                    DownloadTask(
                        camera=camera,
                        variable=variable,
                        date=day,
                        filename=filename,
                        timezone=timezone,
                        target_dir=str(target_dir),
                        local_path=local_path,
                        )
                )

        tracker = self._create_progress_tracker(
            job.id,
            stage="download",
            requested_dates=len(requested_dates),
            missing_dates=missing_dates,
            queued_files=len(download_tasks),
            downloaded=downloaded_count,
            skipped=skipped_count,
            errors=error_count,
        )

        if download_tasks:
            chunks = _split_work(download_tasks, self.settings.download_concurrency)
            with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
                pending = {executor.submit(self._download_chunk, chunk, job_id=job.id) for chunk in chunks}
                while pending:
                    done, pending = wait(pending, timeout=1.0)
                    self._flush_runtime_progress(job.id)
                    for future in done:
                        self._raise_if_job_cancelled(job.id)
                        for result in future.result():
                            if result.error:
                                error_count += 1
                                db.add(
                                    JobEvent(
                                        job_id=job.id,
                                        level="ERROR",
                                        message=f"Download failed: {result.task.filename}",
                                        camera=result.task.camera,
                                        variable=result.task.variable,
                                        date=result.task.date,
                                        filename=result.task.filename,
                                        details_json={"error": result.error},
                                    )
                                )
                            else:
                                downloaded_count += 1
                                checksum = self._upsert_local_file(
                                    db,
                                    result.task.camera,
                                    result.task.variable,
                                    result.task.date,
                                    result.task.filename,
                                    result.task.local_path,
                                    verify_checksum=verify_checksum,
                                )
                                db.add(
                                    FileAuditEvent(
                                        camera=result.task.camera,
                                        variable=result.task.variable,
                                        date=result.task.date,
                                        filename=result.task.filename,
                                        source=FileSource.local,
                                        action="downloaded",
                                        job_id=job.id,
                                        details_json={"checksum": checksum} if checksum else None,
                                    )
                                )
                        db.commit()
                tracker.update(
                    downloaded=downloaded_count,
                    skipped=skipped_count,
                    errors=error_count,
                    stage="download",
                )
                self._flush_runtime_progress(job.id)

        if not dry_run and download_tasks:
            LocalInventoryService(db).scan_incremental(camera=camera, variable=variable, force=True)

    def _resolve_upload_dates(self, db, params: dict) -> list[str]:
        camera = params["camera"]
        variable = params["variable"]
        available_dates = db.scalars(
            select(LocalDateInventory.date)
            .where(and_(LocalDateInventory.camera == camera, LocalDateInventory.variable == variable))
            .distinct()
        ).all()
        mode = params.get("mode", "date_range")
        if mode == "date_range" and params.get("date_from") and params.get("date_to"):
            return _date_range(params["date_from"], params["date_to"])
        return _requested_dates(mode, params, available_dates)

    def _execute_upload(self, db, job: Job, params: dict, *, skip_inventory_scan: bool = False) -> None:
        camera = params["camera"]
        variable = params["variable"]
        dry_run = bool(params.get("dry_run", False))
        run_isolated = bool(params.get("run_isolated", False))
        verify_checksum = bool(params.get("verify_checksum", False))
        run_id = job.id

        local_inventory = LocalInventoryService(db)
        if not skip_inventory_scan:
            local_inventory.scan_incremental(camera=camera, variable=variable, force=True)
        target_dates = self._resolve_upload_dates(db, params)
        upload_tasks = self._build_upload_tasks(db, camera, variable, target_dates, params=params)
        uploaded = 0
        skipped = 0
        errors = 0

        tracker = self._create_progress_tracker(
            job.id,
            stage="upload",
            dates=len(target_dates),
            queued_files=len(upload_tasks),
            uploaded=uploaded,
            skipped=skipped,
            errors=errors,
        )

        if not upload_tasks:
            return

        chunks = _split_work(upload_tasks, self.settings.upload_concurrency)
        with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
            pending = {
                executor.submit(
                    self._upload_chunk,
                    chunk,
                    run_id=run_id,
                    stable_mode=not run_isolated,
                    verify_checksum=verify_checksum,
                    dry_run=dry_run,
                    job_id=job.id,
                )
                for chunk in chunks
            }
            while pending:
                done, pending = wait(pending, timeout=1.0)
                self._flush_runtime_progress(job.id)
                for future in done:
                    self._raise_if_job_cancelled(job.id)
                    batch = future.result()
                    uploaded += int(batch.get("uploaded", 0))
                    skipped += int(batch.get("skipped", 0))
                    errors += int(batch.get("errors", 0))
            tracker.update(uploaded=uploaded, skipped=skipped, errors=errors, stage="upload")
            self._flush_runtime_progress(job.id)

    def _execute_transfer(self, db, job: Job, params: dict) -> None:
        dry_run = bool(params.get("dry_run", False))
        run_isolated = bool(params.get("run_isolated", False))
        verify_checksum = bool(params.get("verify_checksum", False))
        transfer_tasks, requested_dates, missing_dates = self._prepare_transfer_tasks(db, job, params)
        downloaded = 0
        uploaded = 0
        skipped = 0
        planned = 0
        errors = 0
        job_temp_root = safe_join(self.settings.transfer_temp_base_dir, job.id)

        tracker = self._create_progress_tracker(
            job.id,
            stage="transfer",
            requested_dates=len(requested_dates),
            missing_dates=missing_dates,
            queued_files=len(transfer_tasks),
            downloaded=downloaded,
            uploaded=uploaded,
            skipped=skipped,
            planned=planned,
            errors=errors,
            keeps_local_copy=False,
        )

        if not transfer_tasks:
            tracker.update(stage="completed", keeps_local_copy=False)
            self._flush_runtime_progress(job.id)
            return

        chunks = _split_work(transfer_tasks, self.settings.transfer_concurrency)
        try:
            with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
                pending = {
                    executor.submit(
                        self._transfer_chunk,
                        chunk,
                        run_id=job.id,
                        stable_mode=not run_isolated,
                        verify_checksum=verify_checksum,
                        dry_run=dry_run,
                        job_id=job.id,
                        job_temp_root=job_temp_root,
                    )
                    for chunk in chunks
                }
                while pending:
                    done, pending = wait(pending, timeout=1.0)
                    self._flush_runtime_progress(job.id)
                    for future in done:
                        self._raise_if_job_cancelled(job.id)
                        batch = future.result()
                        downloaded += int(batch.get("downloaded", 0))
                        uploaded += int(batch.get("uploaded", 0))
                        skipped += int(batch.get("skipped", 0))
                        planned += int(batch.get("planned", 0))
                        errors += int(batch.get("errors", 0))
                tracker.update(
                    stage="transfer",
                    downloaded=downloaded,
                    uploaded=uploaded,
                    skipped=skipped,
                    planned=planned,
                    errors=errors,
                    keeps_local_copy=False,
                )
                self._flush_runtime_progress(job.id)
        finally:
            self._cleanup_transfer_temp_root(job_temp_root)

        tracker.update(
            stage="completed",
            keeps_local_copy=False,
            current_activity="done",
        )
        self._flush_runtime_progress(job.id)

    def _notify(self, key: str, title: str, message: str, details: dict | None = None) -> None:
        now = utc_now()
        cooldown = max(1, int(self.settings.alert_cooldown_minutes))
        last = self._alert_last_sent.get(key)
        if last and (now - last) < timedelta(minutes=cooldown):
            return
        self._alert_last_sent[key] = now
        try:
            self.notifier.notify(title=title, message=message, details=details)
        except Exception as exc:
            logger.warning("Failed to emit alert notification: %s", exc)

    def _check_system_alerts(self, db, now: datetime) -> None:
        last_remote_file = db.scalar(select(func.max(FileRecord.parsed_timestamp)).where(FileRecord.source == FileSource.remote))
        last_local_file = db.scalar(select(func.max(FileRecord.parsed_timestamp)).where(FileRecord.source == FileSource.local))
        local_count = db.scalar(select(func.count(FileRecord.id)).where(FileRecord.source == FileSource.local)) or 0
        ftp_count = db.scalar(select(func.count(FileRecord.id)).where(FileRecord.source == FileSource.ftp)) or 0
        backlog = int(local_count) - int(ftp_count)

        if backlog > self.settings.ftp_backlog_alert_threshold:
            self._notify(
                key="ftp_backlog",
                title="Wematics FTP backlog alert",
                message=f"FTP backlog is {backlog} files (threshold: {self.settings.ftp_backlog_alert_threshold}).",
                details={"backlog": backlog, "threshold": self.settings.ftp_backlog_alert_threshold},
            )

        newest = last_remote_file or last_local_file
        if newest:
            newest_naive = newest.replace(tzinfo=None) if newest.tzinfo else newest
            no_new_minutes = int((now - newest_naive).total_seconds() / 60)
            if no_new_minutes > self.settings.no_new_data_alert_minutes:
                self._notify(
                    key="no_new_data",
                    title="Wematics no-new-data alert",
                    message=f"No new remote/local file for {no_new_minutes} minutes.",
                    details={"minutes_since_latest": no_new_minutes, "threshold": self.settings.no_new_data_alert_minutes},
                )

    def _execute_verify(self, db, job: Job, params: dict) -> None:
        diff = DiffService(db).compare(
            source_a=FileSource(params.get("source_a", "remote")),
            source_b=FileSource(params.get("source_b", "local")),
            camera=params["camera"],
            variable=params["variable"],
            date_from=params.get("date_from"),
            date_to=params.get("date_to"),
            cadence_seconds=int(params.get("cadence_seconds", 15)),
        )
        self._update_progress(db, job, diff=diff)
        db.add(
            JobEvent(
                job_id=job.id,
                level="INFO",
                message="Verification completed",
                details_json={"summary": diff.get("summary", {})},
            )
        )


job_engine = JobEngine()




