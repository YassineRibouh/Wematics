from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Setting, VariableGlossary


class SettingsService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_setting(self, key: str, default: dict | None = None) -> dict:
        row = self.db.scalar(select(Setting).where(Setting.key == key))
        if row is None:
            return default or {}
        return row.value_json

    def set_setting(self, key: str, value: dict) -> dict:
        row = self.db.scalar(select(Setting).where(Setting.key == key))
        if row is None:
            row = Setting(key=key, value_json=value)
            self.db.add(row)
        else:
            row.value_json = value
        self.db.commit()
        return row.value_json

    def list_glossary(self) -> list[VariableGlossary]:
        return self.db.scalars(select(VariableGlossary).order_by(VariableGlossary.variable.asc())).all()

    def upsert_glossary(
        self,
        variable: str,
        description: str | None,
        expected_cadence_seconds: int | None,
        is_image_like: bool,
    ) -> VariableGlossary:
        row = self.db.scalar(select(VariableGlossary).where(VariableGlossary.variable == variable))
        if row is None:
            row = VariableGlossary(variable=variable)
            self.db.add(row)
        row.description = description
        row.expected_cadence_seconds = expected_cadence_seconds
        row.is_image_like = is_image_like
        self.db.commit()
        return row

