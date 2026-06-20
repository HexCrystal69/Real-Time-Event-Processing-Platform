"""
GRIP — System monitoring service.

Checks health of all platform components using live status probes.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.connection import get_connection

logger = get_logger("monitoring")


def _check_postgres() -> dict[str, Any]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM earthquakes_processed) +
                        (SELECT COUNT(*) FROM wildfires_processed) +
                        (SELECT COUNT(*) FROM weather_processed) +
                        (SELECT COUNT(*) FROM air_quality_processed)
                    """
                )
                total_records = cur.fetchone()[0]
        return {"status": "healthy", "total_records": total_records}
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


def _check_kafka() -> dict[str, Any]:
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            request_timeout_ms=5000,
            consumer_timeout_ms=5000,
        )
        topics = sorted(consumer.topics())
        consumer.close()
        return {"status": "healthy", "topics": topics}
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


def _check_spark() -> dict[str, Any]:
    try:
        response = httpx.get(
            f"{settings.spark_master_url}/json/",
            timeout=5.0,
        )
        if response.status_code == 200:
            data = response.json()
            workers = data.get("workers", [])
            active_apps = data.get("activeapps", [])
            return {
                "status": "healthy",
                "workers_alive": len(workers),
                "active_applications": len(active_apps),
                "master_url": settings.spark_master_url,
            }
        return {"status": "unhealthy", "http_status": response.status_code}
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


def _get_pipeline_health() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source,
                       MAX(created_at) AS last_batch,
                       AVG(processing_time_ms) AS avg_processing_ms,
                       AVG(records_per_minute) AS avg_throughput,
                       SUM(records_count) AS total_processed
                FROM pipeline_metrics
                WHERE created_at >= NOW() - INTERVAL '1 hour'
                GROUP BY source
                """
            )
            pipeline = [
                {
                    "source": row[0],
                    "last_batch": row[1].isoformat() if row[1] else None,
                    "avg_processing_ms": round(row[2] or 0, 2),
                    "avg_throughput": round(row[3] or 0, 2),
                    "total_processed": row[4],
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT source, SUM(dropped_events + validation_errors + malformed_records)
                FROM data_quality_metrics
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY source
                """
            )
            errors = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT source, AVG(latency_ms)
                FROM ingestion_logs
                WHERE created_at >= NOW() - INTERVAL '1 hour'
                  AND status = 'success'
                GROUP BY source
                """
            )
            ingestion_rates = {
                row[0]: round(row[1] or 0, 2) for row in cur.fetchall()
            }

    return {
        "pipeline": pipeline,
        "error_counts": errors,
        "ingestion_latency_ms": ingestion_rates,
    }


def get_system_monitoring() -> dict[str, Any]:
    """Return comprehensive system monitoring data."""
    pipeline = _get_pipeline_health()

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api": {"status": "healthy", "version": "3.0.0"},
        "postgres": _check_postgres(),
        "kafka": _check_kafka(),
        "spark": _check_spark(),
        "pipeline_health": pipeline["pipeline"],
        "ingestion_rate": pipeline["ingestion_latency_ms"],
        "processing_latency": {
            src["source"]: src["avg_processing_ms"]
            for src in pipeline["pipeline"]
        },
        "error_counts": pipeline["error_counts"],
    }
