from __future__ import annotations

import csv
import io
import posixpath
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.time import utc_now
from app.db.session import get_db
from app.models import (
    FTPDateInventory,
    FileAuditEvent,
    FileRecord,
    FileSource,
    Job,
    JobEvent,
    JobKind,
    JobStatus,
    RemoteDateCache,
    Schedule,
)
from app.schemas.api import (
    DiffQuery,
    DownloadJobRequest,
    FileItem,
    FileListResponse,
    GlossaryItemRequest,
    GlossaryItemResponse,
    JobEventResponse,
    JobResponse,
    MessageResponse,
    ScheduleRequest,
    ScheduleResponse,
    TransferJobRequest,
    UploadJobRequest,
    VerifyJobRequest,
)
from app.services.diff_service import DiffService
from app.services.csv_analysis import analyze_csv_for_time_plot, analyze_csv_for_time_plot_cached
from app.services.ftp_service import FTPService
from app.services.local_inventory_service import LocalInventoryService
from app.services.paths import safe_join
from app.services.settings_service import SettingsService
from app.services.timestamps import parse_filename_timestamp
from app.services.wematics_service import WematicsService
from app.workers.engine import _compute_next_run, job_engine

router = APIRouter()
_IMAGE_EXTENSIONS = (".webp", ".jpg", ".jpeg", ".png")


def _job_to_schema(job: Job) -> JobResponse:
    return JobResponse(
        id=job.id,
        kind=job.kind,
        status=job.status,
        idempotency_key=job.idempotency_key,
        params=job.params_json or {},
        progress=job.progress_json,
        error_summary=job.error_summary,
        retry_count=job.retry_count,
        max_retries=job.max_retries,
        created_at=job.created_at,
        started_at=job.started_at,
        ended_at=job.ended_at,
    )


def _file_to_schema(row: FileRecord) -> FileItem:
    return FileItem(
        source=row.source,
        camera=row.camera,
        variable=row.variable,
        date=row.date,
        filename=row.filename,
        parsed_timestamp=row.parsed_timestamp,
        file_size=row.file_size,
        checksum=row.checksum,
        local_path=row.local_path,
        ftp_path=row.ftp_path,
        downloaded=row.downloaded,
        uploaded=row.uploaded,
        verified=row.verified,
        seen_at=row.seen_at,
    )


def _job_event_to_dict(row: JobEvent) -> dict:
    return {
        "created_at": row.created_at.isoformat(),
        "job_id": row.job_id,
        "level": row.level,
        "message": row.message,
        "camera": row.camera,
        "variable": row.variable,
        "date": row.date,
        "filename": row.filename,
        "reason": row.reason,
        "details": row.details_json,
    }


def _job_failure_entries(db: Session, job_id: str) -> list[dict]:
    rows = db.scalars(
        select(JobEvent)
        .where(
            and_(
                JobEvent.job_id == job_id,
                JobEvent.level == "ERROR",
                JobEvent.filename.is_not(None),
            )
        )
        .order_by(JobEvent.created_at.desc())
        .limit(5000)
    ).all()

    grouped: dict[tuple[str | None, str | None, str | None, str], dict] = {}
    for row in rows:
        if not row.filename:
            continue
        key = (row.camera, row.variable, row.date, row.filename)
        error_text = None
        if isinstance(row.details_json, dict) and row.details_json.get("error"):
            error_text = str(row.details_json.get("error"))
        item = grouped.get(key)
        if item is None:
            item = {
                "camera": row.camera,
                "variable": row.variable,
                "date": row.date,
                "filename": row.filename,
                "message": row.message,
                "reason": row.reason,
                "error": error_text,
                "attempts": 0,
                "first_seen_at": row.created_at,
                "last_seen_at": row.created_at,
            }
            grouped[key] = item
        item["attempts"] += 1
        if row.created_at < item["first_seen_at"]:
            item["first_seen_at"] = row.created_at
        if row.created_at > item["last_seen_at"]:
            item["last_seen_at"] = row.created_at
        if not item.get("error") and error_text:
            item["error"] = error_text

    ordered = sorted(grouped.values(), key=lambda item: item["last_seen_at"], reverse=True)
    return [
        {
            **item,
            "first_seen_at": item["first_seen_at"].isoformat() if item.get("first_seen_at") else None,
            "last_seen_at": item["last_seen_at"].isoformat() if item.get("last_seen_at") else None,
        }
        for item in ordered
    ]


def _audit_event_to_dict(row: FileAuditEvent) -> dict:
    return {
        "created_at": row.created_at.isoformat(),
        "camera": row.camera,
        "variable": row.variable,
        "date": row.date,
        "filename": row.filename,
        "action": row.action,
        "source": row.source.value if row.source else None,
        "reason": row.reason,
        "job_id": row.job_id,
        "details": row.details_json,
    }


def _normalized_ftp_path(raw_path: str | None) -> str:
    value = (raw_path or "/").strip().replace("\\", "/")
    if not value.startswith("/"):
        value = f"/{value}"
    parts: list[str] = []
    for part in value.split("/"):
        if not part or part == ".":
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/" + "/".join(parts) if parts else "/"


def _parent_ftp_path(path: str) -> str:
    if path == "/":
        return "/"
    parts = [item for item in path.split("/") if item]
    if not parts:
        return "/"
    return "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"


@router.get("/health", response_model=MessageResponse)
def health() -> MessageResponse:
    return MessageResponse(message="ok")


@router.get("/remote/cameras")
def remote_cameras(db: Session = Depends(get_db)) -> dict:
    service = WematicsService(db)
    cameras = service.list_cameras()
    return {"cameras": cameras}


@router.get("/remote/variables/{camera}")
def remote_variables(camera: str, db: Session = Depends(get_db)) -> dict:
    service = WematicsService(db)
    values = service.list_variables(camera=camera)
    return {"variables": values}


@router.get("/remote/dates")
def remote_dates(
    camera: str,
    variable: str,
    timezone: str = Query("local", pattern="^(local|utc)$"),
    force_refresh: bool = False,
    db: Session = Depends(get_db),
) -> dict:
    service = WematicsService(db)
    dates = service.list_dates(camera=camera, variable=variable, timezone=timezone, force_refresh=force_refresh)
    return {"dates": dates}


@router.get("/remote/files")
def remote_files(
    camera: str,
    variable: str,
    date: str,
    timezone: str = Query("local", pattern="^(local|utc)$"),
    db: Session = Depends(get_db),
) -> dict:
    service = WematicsService(db)
    files = sorted(service.list_files(camera=camera, variable=variable, date=date, timezone=timezone))
    parsed_map = {name: parse_filename_timestamp(name) for name in files}
    parsed_non_null = [item for item in parsed_map.values() if item is not None]
    bins: dict[str, int] = {}
    for item in parsed_non_null:
        label = item.strftime("%H:00")
        bins[label] = bins.get(label, 0) + 1

    image_files = [name for name in files if name.lower().endswith(_IMAGE_EXTENSIONS)]
    csv_files = [name for name in files if name.lower().endswith(".csv")]
    other_files = [name for name in files if name not in image_files and name not in csv_files]

    hourly_images: list[str] = []
    seen_hours: set[str] = set()
    for name in image_files:
        stamp = parsed_map.get(name)
        if stamp is None:
            continue
        hour_key = stamp.strftime("%Y-%m-%d-%H")
        if hour_key in seen_hours:
            continue
        seen_hours.add(hour_key)
        hourly_images.append(name)

    return {
        "files": files,
        "count": len(files),
        "earliest": min(parsed_non_null).isoformat() if parsed_non_null else None,
        "latest": max(parsed_non_null).isoformat() if parsed_non_null else None,
        "time_distribution": [{"label": key, "count": bins[key]} for key in sorted(bins.keys())],
        "images_hourly": hourly_images,
        "image_preview_mode": bool(image_files and not csv_files),
        "file_breakdown": {
            "images": len(image_files),
            "csv": len(csv_files),
            "other": len(other_files),
        },
    }


@router.get("/remote/preview")
def remote_preview(
    camera: str,
    variable: str,
    date: str,
    filename: str,
    timezone: str = Query("local", pattern="^(local|utc)$"),
    db: Session = Depends(get_db),
) -> FileResponse:
    try:
        target = safe_join(Path("data").resolve(), "remote_preview", camera, variable, date, filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        service = WematicsService(db)
        service.download_file(
            camera=camera,
            variable=variable,
            filename=filename,
            target_dir=str(target.parent),
            timezone=timezone,
        )
    if not target.exists():
        raise HTTPException(status_code=404, detail="Remote preview file unavailable")
    return FileResponse(path=str(target))


@router.get("/remote/file-sample")
def remote_file_sample(
    camera: str,
    variable: str,
    date: str,
    filename: str,
    timezone: str = Query("local", pattern="^(local|utc)$"),
    rows: int = 10,
    db: Session = Depends(get_db),
) -> dict:
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Sampling is supported for CSV files only.")
    try:
        target = safe_join(Path("data").resolve(), "remote_preview", camera, variable, date, filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        service = WematicsService(db)
        service.download_file(
            camera=camera,
            variable=variable,
            filename=filename,
            target_dir=str(target.parent),
            timezone=timezone,
        )
    if not target.exists():
        raise HTTPException(status_code=404, detail="CSV file not available for sampling.")

    sample_rows: list[dict] = []
    headers: list[str] = []
    with target.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        for idx, row in enumerate(reader):
            if idx >= rows:
                break
            sample_rows.append(row)
    return {"filename": filename, "headers": headers, "rows": sample_rows}


@router.get("/remote/file-analysis")
def remote_file_analysis(
    camera: str,
    variable: str,
    date: str,
    filename: str,
    timezone: str = Query("local", pattern="^(local|utc)$"),
    rows: int = Query(3000, ge=100, le=20000),
    time_column: str | None = None,
    value_column: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Analysis is supported for CSV files only.")
    try:
        target = safe_join(Path("data").resolve(), "remote_preview", camera, variable, date, filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        service = WematicsService(db)
        service.download_file(
            camera=camera,
            variable=variable,
            filename=filename,
            target_dir=str(target.parent),
            timezone=timezone,
        )
    if not target.exists():
        raise HTTPException(status_code=404, detail="CSV file not available for analysis.")
    try:
        settings = get_settings()
        if settings.csv_analysis_cache_enabled:
            analysis = analyze_csv_for_time_plot_cached(
                db=db,
                path=target,
                max_rows=rows,
                requested_time_column=time_column,
                requested_value_column=value_column,
            )
        else:
            analysis = analyze_csv_for_time_plot(
                path=target,
                max_rows=rows,
                requested_time_column=time_column,
                requested_value_column=value_column,
            )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"CSV analysis failed: {exc}") from exc
    return {"filename": filename, **analysis}


@router.post("/local/scan")
def local_scan(
    camera: str | None = None,
    variable: str | None = None,
) -> dict:
    result = job_engine.enqueue(kind=JobKind.inventory_scan, params={"camera": camera, "variable": variable})
    return {"job_id": result.id, "status": result.status}


@router.get("/local/dates")
def local_dates(camera: str | None = None, variable: str | None = None, db: Session = Depends(get_db)) -> dict:
    service = LocalInventoryService(db)
    service.scan_incremental(camera=camera, variable=variable)
    rows = service.list_date_inventory(camera=camera, variable=variable)
    return {
        "dates": [
            {
                "camera": row.camera,
                "variable": row.variable,
                "date": row.date,
                "file_count": row.file_count,
                "total_size": row.total_size,
                "last_modified": row.last_modified.isoformat() if row.last_modified else None,
                "scanned_at": row.scanned_at.isoformat() if row.scanned_at else None,
            }
            for row in rows
        ]
    }


@router.get("/local/files", response_model=FileListResponse)
def local_files(
    camera: str,
    variable: str,
    date: str,
    page: int = 1,
    page_size: int = 200,
    search: str | None = None,
    db: Session = Depends(get_db),
) -> FileListResponse:
    service = LocalInventoryService(db)
    rows, total = service.list_files(camera=camera, variable=variable, date=date, page=page, page_size=page_size, search=search)
    return FileListResponse(items=[_file_to_schema(row) for row in rows], total=total, page=page, page_size=page_size)


@router.get("/local/storage-summary")
def local_storage_summary(camera: str | None = None, variable: str | None = None, db: Session = Depends(get_db)) -> dict:
    summary = LocalInventoryService(db).summarize_storage(camera=camera, variable=variable)
    return summary


@router.get("/local/preview")
def local_preview(camera: str, variable: str, date: str, filename: str) -> FileResponse:
    settings = get_settings()
    try:
        path = safe_join(settings.archive_base_dir, camera, variable, date, filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(path))


@router.get("/ftp/dates")
def ftp_dates(camera: str | None = None, variable: str | None = None, db: Session = Depends(get_db)) -> dict:
    stmt = select(FTPDateInventory).order_by(desc(FTPDateInventory.date))
    if camera:
        stmt = stmt.where(FTPDateInventory.camera == camera)
    if variable:
        stmt = stmt.where(FTPDateInventory.variable == variable)
    rows = db.scalars(stmt).all()
    return {
        "dates": [
            {
                "camera": row.camera,
                "variable": row.variable,
                "date": row.date,
                "file_count": row.file_count,
                "total_size": row.total_size,
                "scanned_at": row.scanned_at.isoformat() if row.scanned_at else None,
            }
            for row in rows
        ]
    }


@router.get("/ftp/files", response_model=FileListResponse)
def ftp_files(
    camera: str,
    variable: str,
    date: str,
    page: int = 1,
    page_size: int = 200,
    search: str | None = None,
    db: Session = Depends(get_db),
) -> FileListResponse:
    stmt = select(FileRecord).where(
        and_(
            FileRecord.source == FileSource.ftp,
            FileRecord.camera == camera,
            FileRecord.variable == variable,
            FileRecord.date == date,
        )
    )
    count_stmt = select(func.count(FileRecord.id)).where(
        and_(
            FileRecord.source == FileSource.ftp,
            FileRecord.camera == camera,
            FileRecord.variable == variable,
            FileRecord.date == date,
        )
    )
    if search:
        stmt = stmt.where(FileRecord.filename.ilike(f"%{search}%"))
        count_stmt = count_stmt.where(FileRecord.filename.ilike(f"%{search}%"))
    total = int(db.scalar(count_stmt) or 0)
    offset = max(0, page - 1) * page_size
    rows = db.scalars(stmt.order_by(FileRecord.filename.asc()).offset(offset).limit(page_size)).all()
    return FileListResponse(items=[_file_to_schema(row) for row in rows], total=total, page=page, page_size=page_size)


@router.get("/ftp/server/list")
def ftp_server_list(
    path: str = "/",
    limit: int = Query(2000, ge=50, le=10000),
    db: Session = Depends(get_db),
) -> dict:
    normalized = _normalized_ftp_path(path)
    service = FTPService(db)
    try:
        with service.connect() as client:
            entries = client.list_directory(normalized)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"FTP server listing failed: {exc}") from exc
    ordered = sorted(
        entries,
        key=lambda item: (item.get("type") != "dir", str(item.get("name", "")).lower()),
    )
    truncated = len(ordered) > limit
    return {
        "path": normalized,
        "parent": _parent_ftp_path(normalized),
        "entries": ordered[:limit],
        "truncated": truncated,
    }


@router.get("/ftp/server/download")
def ftp_server_download(
    path: str,
    db: Session = Depends(get_db),
) -> StreamingResponse:
    normalized = _normalized_ftp_path(path)
    service = FTPService(db)
    try:
        with service.connect() as client:
            content = client.read_binary(normalized)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"FTP file download failed: {exc}") from exc
    filename = Path(normalized).name or "ftp_file"
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/files/search", response_model=FileListResponse)
def search_files(
    q: str | None = None,
    source: FileSource | None = None,
    camera: str | None = None,
    variable: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    page: int = 1,
    page_size: int = 200,
    db: Session = Depends(get_db),
) -> FileListResponse:
    stmt = select(FileRecord)
    count_stmt = select(func.count(FileRecord.id))
    filters = []
    if q:
        filters.append(FileRecord.filename.ilike(f"%{q}%"))
    if source:
        filters.append(FileRecord.source == source)
    if camera:
        filters.append(FileRecord.camera == camera)
    if variable:
        filters.append(FileRecord.variable == variable)
    if date_from:
        filters.append(FileRecord.date >= date_from)
    if date_to:
        filters.append(FileRecord.date <= date_to)
    if filters:
        stmt = stmt.where(and_(*filters))
        count_stmt = count_stmt.where(and_(*filters))
    total = int(db.scalar(count_stmt) or 0)
    offset = max(0, page - 1) * page_size
    rows = db.scalars(
        stmt.order_by(FileRecord.date.desc(), FileRecord.filename.asc()).offset(offset).limit(page_size)
    ).all()
    return FileListResponse(items=[_file_to_schema(row) for row in rows], total=total, page=page, page_size=page_size)


@router.get("/files/lineage")
def file_lineage(camera: str, variable: str, date: str, filename: str, db: Session = Depends(get_db)) -> dict:
    rows = db.scalars(
        select(FileRecord)
        .where(
            and_(
                FileRecord.camera == camera,
                FileRecord.variable == variable,
                FileRecord.date == date,
                FileRecord.filename == filename,
            )
        )
        .order_by(FileRecord.source.asc())
    ).all()
    if not rows:
        raise HTTPException(status_code=404, detail="File lineage not found")

    audit_rows = db.scalars(
        select(FileAuditEvent)
        .where(
            and_(
                FileAuditEvent.camera == camera,
                FileAuditEvent.variable == variable,
                FileAuditEvent.date == date,
                FileAuditEvent.filename == filename,
            )
        )
        .order_by(FileAuditEvent.created_at.desc())
        .limit(250)
    ).all()
    job_event_rows = db.scalars(
        select(JobEvent)
        .where(
            and_(
                JobEvent.camera == camera,
                JobEvent.variable == variable,
                JobEvent.date == date,
                JobEvent.filename == filename,
            )
        )
        .order_by(JobEvent.created_at.desc())
        .limit(250)
    ).all()

    related_job_ids = {row.job_id for row in audit_rows if row.job_id}
    related_job_ids.update(row.job_id for row in job_event_rows if row.job_id)
    related_jobs = []
    if related_job_ids:
        related_jobs = [_job_to_schema(row).model_dump(mode="json") for row in db.scalars(select(Job).where(Job.id.in_(related_job_ids))).all()]
        related_jobs.sort(key=lambda item: item.get("created_at") or "", reverse=True)

    presence = {source.value: False for source in FileSource}
    sources = {source.value: None for source in FileSource}
    records = []
    for row in rows:
        payload = _file_to_schema(row).model_dump(mode="json")
        presence[row.source.value] = True
        sources[row.source.value] = payload
        records.append(payload)

    return {
        "camera": camera,
        "variable": variable,
        "date": date,
        "filename": filename,
        "presence": presence,
        "sources": sources,
        "records": records,
        "audit_events": [_audit_event_to_dict(row) for row in audit_rows],
        "job_events": [_job_event_to_dict(row) for row in job_event_rows],
        "related_jobs": related_jobs,
    }

@router.post("/diff/compute")
def diff_compute(payload: DiffQuery, db: Session = Depends(get_db)) -> dict:
    service = DiffService(db)
    result = service.compare(
        source_a=payload.source_a,
        source_b=payload.source_b,
        camera=payload.camera,
        variable=payload.variable,
        date_from=payload.date_from,
        date_to=payload.date_to,
        cadence_seconds=payload.cadence_seconds,
    )
    return result


@router.get("/diff/export-csv")
def diff_export_csv(
    source_a: FileSource,
    source_b: FileSource,
    camera: str,
    variable: str,
    date_from: str | None = None,
    date_to: str | None = None,
    cadence_seconds: int = 15,
    db: Session = Depends(get_db),
) -> StreamingResponse:
    data = DiffService(db).compare(
        source_a=source_a,
        source_b=source_b,
        camera=camera,
        variable=variable,
        date_from=date_from,
        date_to=date_to,
        cadence_seconds=cadence_seconds,
    )
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "date",
            "missing_start",
            "missing_end",
            "expected_count",
            "observed_count",
            "completeness_pct",
            "missing_count",
        ],
    )
    writer.writeheader()
    for row in data["gap_rows"]:
        writer.writerow(row)
    content = io.BytesIO(output.getvalue().encode("utf-8"))
    filename = f"gap_report_{camera}_{variable}_{source_a.value}_vs_{source_b.value}.csv"
    return StreamingResponse(
        content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/jobs/download", response_model=JobResponse)
def jobs_download(payload: DownloadJobRequest, db: Session = Depends(get_db)) -> JobResponse:
    result = job_engine.enqueue(
        kind=JobKind.download,
        params=payload.model_dump(),
        idempotency_key=payload.idempotency_key,
    )
    job = db.get(Job, result.id)
    if job is None:
        raise HTTPException(status_code=500, detail="Job creation failed")
    return _job_to_schema(job)


@router.post("/jobs/upload", response_model=JobResponse)
def jobs_upload(payload: UploadJobRequest, db: Session = Depends(get_db)) -> JobResponse:
    result = job_engine.enqueue(
        kind=JobKind.upload,
        params=payload.model_dump(),
        idempotency_key=payload.idempotency_key,
    )
    job = db.get(Job, result.id)
    if job is None:
        raise HTTPException(status_code=500, detail="Job creation failed")
    return _job_to_schema(job)


@router.post("/jobs/transfer", response_model=JobResponse)
def jobs_transfer(payload: TransferJobRequest, db: Session = Depends(get_db)) -> JobResponse:
    result = job_engine.enqueue(
        kind=JobKind.transfer,
        params=payload.model_dump(),
        idempotency_key=payload.idempotency_key,
    )
    job = db.get(Job, result.id)
    if job is None:
        raise HTTPException(status_code=500, detail="Job creation failed")
    return _job_to_schema(job)


@router.post("/jobs/verify", response_model=JobResponse)
def jobs_verify(payload: VerifyJobRequest, db: Session = Depends(get_db)) -> JobResponse:
    result = job_engine.enqueue(
        kind=JobKind.verify,
        params=payload.model_dump(),
        idempotency_key=payload.idempotency_key,
    )
    job = db.get(Job, result.id)
    if job is None:
        raise HTTPException(status_code=500, detail="Job creation failed")
    return _job_to_schema(job)


@router.post("/jobs/inventory", response_model=JobResponse)
def jobs_inventory(camera: str | None = None, variable: str | None = None, db: Session = Depends(get_db)) -> JobResponse:
    result = job_engine.enqueue(kind=JobKind.inventory_scan, params={"camera": camera, "variable": variable})
    job = db.get(Job, result.id)
    if job is None:
        raise HTTPException(status_code=500, detail="Job creation failed")
    return _job_to_schema(job)


@router.post("/jobs/{job_id}/resume", response_model=JobResponse)
def resume_job(job_id: str, failed_only: bool = True, db: Session = Depends(get_db)) -> JobResponse:
    original = db.get(Job, job_id)
    if original is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if original.kind not in {JobKind.download, JobKind.upload, JobKind.transfer}:
        raise HTTPException(status_code=400, detail="Resume is supported for download/upload/transfer jobs only.")
    if original.status in {JobStatus.queued, JobStatus.running}:
        raise HTTPException(status_code=400, detail="Stop or wait for the active job before creating a resume run.")
    new_params = dict(original.params_json or {})
    new_params.pop("resume_failure_files", None)
    new_params["resume_from_job_id"] = original.id
    resume_mode = "scope"
    if failed_only:
        failure_items = [
            {"date": item["date"], "filename": item["filename"]}
            for item in _job_failure_entries(db, original.id)
            if item.get("date") and item.get("filename")
        ]
        if not failure_items:
            raise HTTPException(status_code=400, detail="No failed files were recorded for this job.")
        new_params["resume_failure_files"] = failure_items
        resume_mode = "failed_only"
    resume_key = f"resume-{original.id}-{int(utc_now().timestamp())}"
    result = job_engine.enqueue(kind=original.kind, params=new_params, idempotency_key=resume_key)
    row = db.get(Job, result.id)
    if row is None:
        raise HTTPException(status_code=500, detail="Resume job creation failed")
    db.add(
        JobEvent(
            job_id=row.id,
            level="INFO",
            message=f"Resumed from job {original.id}",
            details_json={"source_job_id": original.id, "mode": resume_mode},
        )
    )
    db.commit()
    db.refresh(row)
    return _job_to_schema(row)


@router.post("/jobs/{job_id}/cancel", response_model=JobResponse)
def cancel_job(job_id: str, db: Session = Depends(get_db)) -> JobResponse:
    try:
        row = job_engine.cancel_job(db, job_id)
    except RuntimeError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(status_code=404, detail=message) from exc
        raise HTTPException(status_code=400, detail=message) from exc
    db.commit()
    db.refresh(row)
    return _job_to_schema(row)


@router.get("/jobs", response_model=list[JobResponse])
def list_jobs(
    status: JobStatus | None = None,
    kind: JobKind | None = None,
    limit: int = 50,
    db: Session = Depends(get_db),
) -> list[JobResponse]:
    stmt = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if status:
        stmt = stmt.where(Job.status == status)
    if kind:
        stmt = stmt.where(Job.kind == kind)
    return [_job_to_schema(row) for row in db.scalars(stmt).all()]


@router.get("/jobs/ftp-availability")
def jobs_ftp_availability(
    camera: str,
    variable: str,
    timezone: str = Query("local", pattern="^(local|utc)$"),
    date_from: str | None = None,
    date_to: str | None = None,
    max_days: int = Query(31, ge=1, le=120),
    db: Session = Depends(get_db),
) -> dict:
    remote_service = WematicsService(db)
    ftp_service = FTPService(db)

    remote_dates = sorted(remote_service.list_dates(camera=camera, variable=variable, timezone=timezone))
    selected_dates = [d for d in remote_dates if (not date_from or d >= date_from) and (not date_to or d <= date_to)]
    if len(selected_dates) > max_days:
        raise HTTPException(
            status_code=400,
            detail=f"Selected range contains {len(selected_dates)} dates, which exceeds max_days={max_days}.",
        )

    rows: list[dict] = []
    with ftp_service.connect() as ftp_client:
        for day in selected_dates:
            remote_files = sorted(remote_service.list_files(camera=camera, variable=variable, date=day, timezone=timezone))
            ftp_dir = ftp_service._normalize_remote_path(
                posixpath.join(ftp_service.settings.ftp_base_path, camera, variable, day)
            )
            entries = ftp_client.list_directory(ftp_dir)
            ftp_files = sorted(
                {
                    str(item.get("name"))
                    for item in entries
                    if item.get("type") == "file" and item.get("name")
                }
            )

            remote_set = set(remote_files)
            ftp_set = set(ftp_files)
            missing_files = sorted(remote_set - ftp_set)
            extra_files = sorted(ftp_set - remote_set)
            completeness_pct = round(
                100.0 if not remote_set else ((len(remote_set) - len(missing_files)) / len(remote_set)) * 100.0,
                2,
            )
            status = "complete" if not missing_files else ("empty" if not ftp_set else "partial")

            rows.append(
                {
                    "date": day,
                    "status": status,
                    "remote_file_count": len(remote_set),
                    "ftp_file_count": len(ftp_set),
                    "missing_count": len(missing_files),
                    "extra_count": len(extra_files),
                    "completeness_pct": completeness_pct,
                    "missing_examples": missing_files[:10],
                    "extra_examples": extra_files[:10],
                    "ftp_path": ftp_dir,
                }
            )

    complete_dates = sum(1 for item in rows if item["status"] == "complete")
    partial_dates = sum(1 for item in rows if item["status"] == "partial")
    empty_dates = sum(1 for item in rows if item["status"] == "empty")
    total_missing_files = sum(int(item["missing_count"]) for item in rows)

    return {
        "camera": camera,
        "variable": variable,
        "timezone": timezone,
        "date_from": date_from,
        "date_to": date_to,
        "dates": rows,
        "summary": {
            "total_dates": len(rows),
            "complete_dates": complete_dates,
            "partial_dates": partial_dates,
            "empty_dates": empty_dates,
            "total_missing_files": total_missing_files,
        },
    }


@router.get("/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: str, db: Session = Depends(get_db)) -> JobResponse:
    row = db.get(Job, job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_to_schema(row)


@router.get("/jobs/{job_id}/events", response_model=list[JobEventResponse])
def get_job_events(job_id: str, db: Session = Depends(get_db)) -> list[JobEventResponse]:
    rows = db.scalars(
        select(JobEvent).where(JobEvent.job_id == job_id).order_by(JobEvent.created_at.asc()).limit(1000)
    ).all()
    return [
        JobEventResponse(
            created_at=row.created_at,
            level=row.level,
            message=row.message,
            camera=row.camera,
            variable=row.variable,
            date=row.date,
            filename=row.filename,
            reason=row.reason,
            details=row.details_json,
        )
        for row in rows
    ]


@router.get("/jobs/{job_id}/failures")
def get_job_failures(
    job_id: str,
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> dict:
    row = db.get(Job, job_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Job not found")
    items = _job_failure_entries(db, job_id)
    return {
        "job_id": job_id,
        "job_status": row.status.value,
        "job_kind": row.kind.value,
        "total_unique_failures": len(items),
        "total_failure_events": sum(int(item.get("attempts") or 0) for item in items),
        "items": items[:limit],
    }


@router.get("/schedules", response_model=list[ScheduleResponse])
def list_schedules(db: Session = Depends(get_db)) -> list[ScheduleResponse]:
    rows = db.scalars(select(Schedule).order_by(Schedule.created_at.desc())).all()
    return [
        ScheduleResponse(
            id=row.id,
            name=row.name,
            enabled=row.enabled,
            job_kind=row.job_kind,
            cadence=row.cadence,
            every_minutes=row.every_minutes,
            hour_of_day=row.hour_of_day,
            minute_of_hour=row.minute_of_hour,
            day_of_week=row.day_of_week,
            day_of_month=row.day_of_month,
            params=row.params_json,
            next_run_at=row.next_run_at,
            last_run_at=row.last_run_at,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
        for row in rows
    ]


@router.post("/schedules", response_model=ScheduleResponse)
def create_schedule(payload: ScheduleRequest, db: Session = Depends(get_db)) -> ScheduleResponse:
    row = Schedule(
        name=payload.name,
        enabled=payload.enabled,
        job_kind=payload.job_kind,
        cadence=payload.cadence,
        every_minutes=payload.every_minutes,
        hour_of_day=payload.hour_of_day,
        minute_of_hour=payload.minute_of_hour,
        day_of_week=payload.day_of_week,
        day_of_month=payload.day_of_month,
        params_json=payload.params,
    )
    row.next_run_at = _compute_next_run(row, utc_now())
    db.add(row)
    db.commit()
    db.refresh(row)
    return ScheduleResponse(
        id=row.id,
        name=row.name,
        enabled=row.enabled,
        job_kind=row.job_kind,
        cadence=row.cadence,
        every_minutes=row.every_minutes,
        hour_of_day=row.hour_of_day,
        minute_of_hour=row.minute_of_hour,
        day_of_week=row.day_of_week,
        day_of_month=row.day_of_month,
        params=row.params_json,
        next_run_at=row.next_run_at,
        last_run_at=row.last_run_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.put("/schedules/{schedule_id}", response_model=ScheduleResponse)
def update_schedule(schedule_id: str, payload: ScheduleRequest, db: Session = Depends(get_db)) -> ScheduleResponse:
    row = db.get(Schedule, schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    row.name = payload.name
    row.enabled = payload.enabled
    row.job_kind = payload.job_kind
    row.cadence = payload.cadence
    row.every_minutes = payload.every_minutes
    row.hour_of_day = payload.hour_of_day
    row.minute_of_hour = payload.minute_of_hour
    row.day_of_week = payload.day_of_week
    row.day_of_month = payload.day_of_month
    row.params_json = payload.params
    row.next_run_at = _compute_next_run(row, utc_now()) if row.enabled else None
    db.commit()
    db.refresh(row)
    return ScheduleResponse(
        id=row.id,
        name=row.name,
        enabled=row.enabled,
        job_kind=row.job_kind,
        cadence=row.cadence,
        every_minutes=row.every_minutes,
        hour_of_day=row.hour_of_day,
        minute_of_hour=row.minute_of_hour,
        day_of_week=row.day_of_week,
        day_of_month=row.day_of_month,
        params=row.params_json,
        next_run_at=row.next_run_at,
        last_run_at=row.last_run_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.delete("/schedules/{schedule_id}", response_model=MessageResponse)
def delete_schedule(schedule_id: str, db: Session = Depends(get_db)) -> MessageResponse:
    row = db.get(Schedule, schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    db.delete(row)
    db.commit()
    return MessageResponse(message="Deleted")


@router.get("/settings/{key}")
def get_setting(key: str, db: Session = Depends(get_db)) -> dict:
    service = SettingsService(db)
    return {"key": key, "value": service.get_setting(key, default={})}


@router.put("/settings/{key}")
def put_setting(key: str, value: dict, db: Session = Depends(get_db)) -> dict:
    service = SettingsService(db)
    return {"key": key, "value": service.set_setting(key, value)}


@router.get("/glossary", response_model=list[GlossaryItemResponse])
def list_glossary(db: Session = Depends(get_db)) -> list[GlossaryItemResponse]:
    rows = SettingsService(db).list_glossary()
    return [
        GlossaryItemResponse(
            variable=row.variable,
            description=row.description,
            expected_cadence_seconds=row.expected_cadence_seconds,
            is_image_like=row.is_image_like,
            updated_at=row.updated_at,
        )
        for row in rows
    ]


@router.post("/glossary", response_model=GlossaryItemResponse)
def upsert_glossary(payload: GlossaryItemRequest, db: Session = Depends(get_db)) -> GlossaryItemResponse:
    row = SettingsService(db).upsert_glossary(
        variable=payload.variable,
        description=payload.description,
        expected_cadence_seconds=payload.expected_cadence_seconds,
        is_image_like=payload.is_image_like,
    )
    return GlossaryItemResponse(
        variable=row.variable,
        description=row.description,
        expected_cadence_seconds=row.expected_cadence_seconds,
        is_image_like=row.is_image_like,
        updated_at=row.updated_at,
    )


@router.get("/logs")
def logs(
    job_id: str | None = None,
    camera: str | None = None,
    variable: str | None = None,
    date: str | None = None,
    filename: str | None = None,
    level: str | None = None,
    q: str | None = None,
    limit: int = 500,
    db: Session = Depends(get_db),
) -> dict:
    stmt = select(JobEvent)
    if job_id:
        stmt = stmt.where(JobEvent.job_id == job_id)
    if camera:
        stmt = stmt.where(JobEvent.camera == camera)
    if variable:
        stmt = stmt.where(JobEvent.variable == variable)
    if date:
        stmt = stmt.where(JobEvent.date == date)
    if filename:
        stmt = stmt.where(JobEvent.filename.ilike(f"%{filename}%"))
    if level:
        stmt = stmt.where(func.lower(JobEvent.level) == level.lower())
    if q:
        pattern = f"%{q}%"
        stmt = stmt.where(
            or_(
                JobEvent.message.ilike(pattern),
                JobEvent.reason.ilike(pattern),
                JobEvent.filename.ilike(pattern),
            )
        )
    rows = db.scalars(stmt.order_by(JobEvent.created_at.desc()).limit(limit)).all()
    return {"events": [_job_event_to_dict(row) for row in rows]}


@router.get("/audit")
def audit(
    camera: str | None = None,
    variable: str | None = None,
    date: str | None = None,
    filename: str | None = None,
    source: FileSource | None = None,
    action: str | None = None,
    job_id: str | None = None,
    q: str | None = None,
    limit: int = 1000,
    db: Session = Depends(get_db),
) -> dict:
    stmt = select(FileAuditEvent)
    if camera:
        stmt = stmt.where(FileAuditEvent.camera == camera)
    if variable:
        stmt = stmt.where(FileAuditEvent.variable == variable)
    if date:
        stmt = stmt.where(FileAuditEvent.date == date)
    if filename:
        stmt = stmt.where(FileAuditEvent.filename.ilike(f"%{filename}%"))
    if source:
        stmt = stmt.where(FileAuditEvent.source == source)
    if action:
        stmt = stmt.where(FileAuditEvent.action.ilike(f"%{action}%"))
    if job_id:
        stmt = stmt.where(FileAuditEvent.job_id == job_id)
    if q:
        pattern = f"%{q}%"
        stmt = stmt.where(
            or_(
                FileAuditEvent.filename.ilike(pattern),
                FileAuditEvent.action.ilike(pattern),
                FileAuditEvent.reason.ilike(pattern),
            )
        )
    rows = db.scalars(stmt.order_by(FileAuditEvent.created_at.desc()).limit(limit)).all()
    return {"events": [_audit_event_to_dict(row) for row in rows]}


@router.get("/overview")
def overview(db: Session = Depends(get_db)) -> dict:
    settings = get_settings()
    last_remote_check = db.scalar(select(func.max(RemoteDateCache.fetched_at)))
    last_remote_file = db.scalar(select(func.max(FileRecord.parsed_timestamp)).where(FileRecord.source == FileSource.remote))
    last_local_file = db.scalar(select(func.max(FileRecord.parsed_timestamp)).where(FileRecord.source == FileSource.local))
    last_ftp_file = db.scalar(select(func.max(FileRecord.parsed_timestamp)).where(FileRecord.source == FileSource.ftp))

    local_count = db.scalar(select(func.count(FileRecord.id)).where(FileRecord.source == FileSource.local)) or 0
    ftp_count = db.scalar(select(func.count(FileRecord.id)).where(FileRecord.source == FileSource.ftp)) or 0
    backlog = int(local_count) - int(ftp_count)

    newest = last_remote_file or last_local_file
    no_new_minutes = None
    no_new_alert = False
    if newest:
        newest_naive = newest.replace(tzinfo=None) if newest.tzinfo else newest
        no_new_minutes = int((utc_now() - newest_naive).total_seconds() / 60)
        no_new_alert = no_new_minutes > settings.no_new_data_alert_minutes

    active_jobs = db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.running)) or 0
    queued_jobs = db.scalar(select(func.count(Job.id)).where(Job.status == JobStatus.queued)) or 0

    return {
        "last_remote_check_time": last_remote_check.isoformat() if last_remote_check else None,
        "last_new_file_time_remote": last_remote_file.isoformat() if last_remote_file else None,
        "last_new_file_time_local": last_local_file.isoformat() if last_local_file else None,
        "last_new_file_time_ftp": last_ftp_file.isoformat() if last_ftp_file else None,
        "job_counts": {"running": int(active_jobs), "queued": int(queued_jobs)},
        "counts": {"local_files": int(local_count), "ftp_files": int(ftp_count), "ftp_backlog": backlog},
        "alerts": {
            "no_new_data": {"status": "alert" if no_new_alert else "ok", "minutes_since_latest": no_new_minutes},
            "ftp_backlog": {
                "status": "alert" if backlog > settings.ftp_backlog_alert_threshold else "ok",
                "threshold": settings.ftp_backlog_alert_threshold,
                "count": backlog,
            },
        },
    }



