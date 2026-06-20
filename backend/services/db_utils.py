"""
GRIP — Shared database utility helpers for Phase 3 services.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def row_to_dict(row: tuple, columns: list[str]) -> dict[str, Any]:
    """Convert a database row tuple to a dictionary with ISO timestamps."""
    record: dict[str, Any] = {}
    for idx, col_name in enumerate(columns):
        value = row[idx]
        if isinstance(value, datetime):
            value = value.isoformat()
        record[col_name] = value
    return record


def rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict[str, Any]]:
    """Convert multiple database rows to dictionaries."""
    return [row_to_dict(row, columns) for row in rows]
