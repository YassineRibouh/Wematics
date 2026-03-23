from __future__ import annotations

import re
from pathlib import Path


_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._+-]+$")


def sanitize_segment(value: str) -> str:
    if not value or not _SAFE_SEGMENT_RE.fullmatch(value):
        raise ValueError(f"Invalid path segment: {value}")
    return value


def safe_join(base: Path, *segments: str) -> Path:
    current = base
    for segment in segments:
        current = current / sanitize_segment(segment)
    resolved = current.resolve()
    base_resolved = base.resolve()
    if not str(resolved).startswith(str(base_resolved)):
        raise ValueError("Resolved path escaped base directory.")
    return resolved
