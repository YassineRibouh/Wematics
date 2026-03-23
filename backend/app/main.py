from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.migrations import run_sql_migrations
from app.db.session import SessionLocal
from app.workers.engine import job_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging()
    Path("data").mkdir(parents=True, exist_ok=True)
    with SessionLocal() as db:
        run_sql_migrations(db=db, migrations_path=Path(settings.migrations_path))
    job_engine.start()
    try:
        yield
    finally:
        job_engine.stop()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix=settings.api_prefix)
    return app


app = create_app()

