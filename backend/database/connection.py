"""
GRIP — PostgreSQL connection management.

Provides a thread-safe connection pool and an idempotent database
initializer that creates all tables on first startup.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool as pg_pool

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.models import ALL_TABLES

logger = get_logger("database")

_connection_pool: pg_pool.ThreadedConnectionPool | None = None

MAX_RETRIES = 10
RETRY_DELAY_SECONDS = 3


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    """
    Lazily initialise and return the connection pool.

    Retries up to MAX_RETRIES times in case PostgreSQL is still starting
    (common in Docker Compose where depends_on does not wait for readiness).
    """
    global _connection_pool

    if _connection_pool is not None and not _connection_pool.closed:
        return _connection_pool

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _connection_pool = pg_pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                host=settings.postgres_host,
                port=settings.postgres_port,
                dbname=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
            )
            logger.info(
                "PostgreSQL connection pool established",
                extra={"context": {"host": settings.postgres_host, "db": settings.postgres_db}},
            )
            return _connection_pool
        except psycopg2.OperationalError as exc:
            last_error = exc
            logger.warning(
                "PostgreSQL not ready, retrying",
                extra={"context": {"attempt": attempt, "max": MAX_RETRIES, "error": str(exc)}},
            )
            time.sleep(RETRY_DELAY_SECONDS)

    raise ConnectionError(
        f"Failed to connect to PostgreSQL after {MAX_RETRIES} attempts"
    ) from last_error


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager that checks out a connection from the pool, yields it,
    and returns it when done. Auto-commits on clean exit, rolls back on error.
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def init_database() -> None:
    """
    Create all tables if they do not already exist.
    Safe to call multiple times (idempotent).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for ddl in ALL_TABLES:
                cur.execute(ddl)
    logger.info("Database initialisation complete — all tables verified")


def log_ingestion(
    source: str,
    status: str,
    records_count: int,
    latency_ms: float,
    error_message: str | None = None,
) -> None:
    """Insert a row into ingestion_logs to track pipeline health."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_logs (source, status, records_count, latency_ms, error_message)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (source, status, records_count, latency_ms, error_message),
            )
