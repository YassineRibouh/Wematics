from __future__ import annotations

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session


def run_sql_migrations(db: Session, migrations_path: Path) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.commit()

    applied = {row[0] for row in db.execute(text("SELECT version FROM schema_migrations")).all()}
    migration_files = sorted(migrations_path.glob("*.sql"))

    for migration in migration_files:
        version = migration.name
        if version in applied:
            continue
        sql = migration.read_text(encoding="utf-8")
        if sql.strip():
            bind = db.get_bind()
            if bind and bind.dialect.name == "sqlite":
                db.connection().connection.executescript(sql)
            else:
                for statement in [segment.strip() for segment in sql.split(";") if segment.strip()]:
                    db.execute(text(statement))
            db.execute(text("INSERT INTO schema_migrations(version) VALUES (:version)"), {"version": version})
            db.commit()
