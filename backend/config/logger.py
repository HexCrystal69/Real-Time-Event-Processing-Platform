"""
GRIP — Structured JSON logging.

Provides a consistent, machine-readable log format across all services.
Every log line includes timestamp, service name, level, and structured context.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from backend.config.settings import settings


class JSONFormatter(logging.Formatter):
    """Emits each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", record.name),
            "message": record.getMessage(),
        }

        # Attach structured extras if provided via `extra={"context": {...}}`
        context = getattr(record, "context", None)
        if context and isinstance(context, dict):
            log_entry["context"] = context

        # Attach exception info
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def get_logger(service_name: str) -> logging.Logger:
    """
    Return a named logger pre-configured with JSON output to stdout.

    Parameters
    ----------
    service_name : str
        Identifier that appears in every log line (e.g. "earthquake_producer").
    """
    logger = logging.getLogger(f"grip.{service_name}")

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    return logger
