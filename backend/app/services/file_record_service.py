from __future__ import annotations

from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import FileRecord, FileSource


def _identity_predicate(
    *,
    source: FileSource,
    camera: str,
    variable: str,
    date: str,
    filename: str,
):
    return and_(
        FileRecord.source == source,
        FileRecord.camera == camera,
        FileRecord.variable == variable,
        FileRecord.date == date,
        FileRecord.filename == filename,
    )


def get_or_create_file_record(
    session: Session,
    *,
    source: FileSource,
    camera: str,
    variable: str,
    date: str,
    filename: str,
) -> tuple[FileRecord, bool]:
    """Return a unique file record and whether this call created it."""
    predicate = _identity_predicate(
        source=source,
        camera=camera,
        variable=variable,
        date=date,
        filename=filename,
    )
    row = session.scalar(select(FileRecord).where(predicate))
    if row is not None:
        return row, False

    payload = {
        "source": source,
        "camera": camera,
        "variable": variable,
        "date": date,
        "filename": filename,
    }
    index_elements = ["source", "camera", "variable", "date", "filename"]

    bind = session.get_bind()
    dialect_name = bind.dialect.name if bind is not None else ""
    insert_stmt = None
    if dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        insert_stmt = sqlite_insert(FileRecord).values(**payload).on_conflict_do_nothing(index_elements=index_elements)
    elif dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as postgresql_insert

        insert_stmt = postgresql_insert(FileRecord).values(**payload).on_conflict_do_nothing(index_elements=index_elements)

    if insert_stmt is not None:
        result = session.execute(insert_stmt)
        row = session.scalar(select(FileRecord).where(predicate))
        if row is None:
            raise RuntimeError("FileRecord insert conflict handling did not return a row.")
        return row, bool(result.rowcount)

    candidate = FileRecord(**payload)
    try:
        with session.begin_nested():
            session.add(candidate)
            session.flush([candidate])
        return candidate, True
    except IntegrityError:
        row = session.scalar(select(FileRecord).where(predicate))
        if row is None:
            raise
        return row, False
