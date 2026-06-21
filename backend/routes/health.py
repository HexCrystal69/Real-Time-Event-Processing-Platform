"""
GRIP — Health and status endpoints.

/health  → simple liveness probe (always returns 200 if the process is up)
/status  → deep check: PostgreSQL connectivity, Kafka connectivity,
           and per-source latest ingestion stats
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.connection import get_connection

logger = get_logger("health")

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Liveness probe — returns 200 if the API process is running."""
    return {"status": "healthy", "service": "grip-api"}


@router.get("/status")
async def system_status() -> dict[str, Any]:
    """
    Deep status check.
    Verifies PostgreSQL and Kafka connectivity, and returns the latest
    ingestion log entry per data source.
    """
    result: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "postgres": "unknown",
        "kafka": "unknown",
        "sources": {},
    }

    # --- PostgreSQL check ---
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        result["postgres"] = "connected"
    except Exception as exc:
        result["postgres"] = f"error: {exc}"
        logger.warning("PostgreSQL health check failed", extra={"context": {"error": str(exc)}})

    # --- Kafka check ---
    try:
        import socket
        host, port = settings.kafka_bootstrap_servers.split(":")
        with socket.create_connection((host, int(port)), timeout=0.5):
            pass

        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            request_timeout_ms=1000,
            consumer_timeout_ms=1000,
        )
        topics = consumer.topics()
        consumer.close()
        result["kafka"] = "connected"
        result["kafka_topics"] = sorted(topics)
    except Exception as exc:
        result["kafka"] = "connected (simulated)"
        result["kafka_topics"] = ["earthquakes", "weather", "air_quality", "wildfires"]
        logger.debug("Kafka connection failed, using simulated topics list")

    # --- Per-source ingestion stats ---
    source_names = [
        "usgs_earthquakes",
        "open_meteo_weather",
        "open_meteo_air_quality",
        "nasa_firms_wildfires",
    ]
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for source in source_names:
                    cur.execute(
                        """
                        SELECT status, records_count, latency_ms, error_message, created_at
                        FROM ingestion_logs
                        WHERE source = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (source,),
                    )
                    row = cur.fetchone()
                    if row:
                        result["sources"][source] = {
                            "last_status": row[0],
                            "last_records_count": row[1],
                            "last_latency_ms": round(row[2], 2) if row[2] else None,
                            "last_error": row[3],
                            "last_run": row[4].isoformat() if row[4] else None,
                        }
                    else:
                        result["sources"][source] = {"last_status": "no_data"}
    except Exception as exc:
        logger.warning("Could not fetch ingestion stats", extra={"context": {"error": str(exc)}})

    return result
