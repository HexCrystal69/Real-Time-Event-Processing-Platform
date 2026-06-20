"""
GRIP — Alert engine.

Generates and stores alerts from live processed pipeline data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.config.logger import get_logger
from backend.database.connection import get_connection

logger = get_logger("alert_engine")

ALERT_RULES = {
    "major_earthquake": {
        "source": "earthquakes",
        "query": """
            SELECT event_id, magnitude, place, latitude, longitude, event_time, risk_category
            FROM earthquakes_processed
            WHERE magnitude >= 6.0
              AND event_time >= NOW() - INTERVAL '1 hour'
              AND event_id NOT IN (
                  SELECT metadata->>'event_id' FROM alerts
                  WHERE alert_type = 'major_earthquake'
                    AND created_at >= NOW() - INTERVAL '24 hours'
                    AND metadata->>'event_id' IS NOT NULL
              )
        """,
        "severity_fn": lambda row: "Critical" if row[1] >= 7.0 else "High",
        "title_fn": lambda row: f"M{row[1]:.1f} Earthquake — {row[2]}",
        "desc_fn": lambda row: (
            f"Magnitude {row[1]} earthquake detected at {row[2]}. "
            f"Risk category: {row[6]}."
        ),
    },
    "extreme_aqi": {
        "source": "air_quality",
        "query": """
            SELECT location_name, us_aqi, latitude, longitude, observed_at, aqi_category, id
            FROM air_quality_processed
            WHERE us_aqi > 300
              AND observed_at >= NOW() - INTERVAL '2 hours'
              AND id NOT IN (
                  SELECT (metadata->>'record_id')::int FROM alerts
                  WHERE alert_type = 'extreme_aqi'
                    AND created_at >= NOW() - INTERVAL '6 hours'
                    AND metadata->>'record_id' IS NOT NULL
              )
        """,
        "severity_fn": lambda row: "Critical" if row[1] > 400 else "High",
        "title_fn": lambda row: f"Extreme AQI — {row[0]} ({row[1]})",
        "desc_fn": lambda row: (
            f"Air quality index of {row[1]} detected at {row[0]}. "
            f"Category: {row[5]}."
        ),
    },
    "severe_weather": {
        "source": "weather",
        "query": """
            SELECT location_name, storm_severity, wind_speed_kmh, precipitation_mm,
                   latitude, longitude, observed_at, id
            FROM weather_processed
            WHERE storm_severity IN ('Severe', 'Extreme')
              AND observed_at >= NOW() - INTERVAL '2 hours'
              AND id NOT IN (
                  SELECT (metadata->>'record_id')::int FROM alerts
                  WHERE alert_type = 'severe_weather'
                    AND created_at >= NOW() - INTERVAL '6 hours'
                    AND metadata->>'record_id' IS NOT NULL
              )
        """,
        "severity_fn": lambda row: "Critical" if row[1] == "Extreme" else "High",
        "title_fn": lambda row: f"Severe Weather — {row[0]} ({row[1]})",
        "desc_fn": lambda row: (
            f"{row[1]} weather at {row[0]}. "
            f"Wind: {row[2]} km/h, Precipitation: {row[3]} mm."
        ),
    },
    "major_wildfire": {
        "source": "wildfires",
        "query": """
            SELECT latitude, longitude, frp, fire_severity, acq_date, id, detection_confidence
            FROM wildfires_processed
            WHERE (frp >= 200 OR fire_severity = 'Extreme')
              AND acq_date >= CURRENT_DATE - INTERVAL '1 day'
              AND id NOT IN (
                  SELECT (metadata->>'record_id')::int FROM alerts
                  WHERE alert_type = 'major_wildfire'
                    AND created_at >= NOW() - INTERVAL '12 hours'
                    AND metadata->>'record_id' IS NOT NULL
              )
        """,
        "severity_fn": lambda row: "Critical" if (row[2] or 0) >= 500 else "High",
        "title_fn": lambda row: f"Major Wildfire — FRP {row[2]:.0f} MW",
        "desc_fn": lambda row: (
            f"Major wildfire detected at ({row[0]:.2f}, {row[1]:.2f}). "
            f"FRP: {row[2]} MW, Severity: {row[3]}, Confidence: {row[6]}."
        ),
    },
}


def _build_metadata(alert_type: str, row: tuple) -> dict[str, Any]:
    if alert_type == "major_earthquake":
        return {"event_id": row[0], "magnitude": row[1]}
    if alert_type == "extreme_aqi":
        return {"record_id": row[6], "location": row[0], "us_aqi": row[1]}
    if alert_type == "severe_weather":
        return {"record_id": row[7], "storm_severity": row[1]}
    if alert_type == "major_wildfire":
        return {"record_id": row[5], "frp": row[2]}
    return {}


def _coordinates(row: tuple, alert_type: str) -> tuple[float | None, float | None]:
    if alert_type == "major_earthquake":
        return row[3], row[4]
    if alert_type == "extreme_aqi":
        return row[2], row[3]
    if alert_type == "severe_weather":
        return row[4], row[5]
    if alert_type == "major_wildfire":
        return row[0], row[1]
    return None, None


def _region_name(row: tuple, alert_type: str) -> str | None:
    if alert_type in ("extreme_aqi", "severe_weather"):
        return row[0]
    return None


def check_and_generate_alerts() -> list[dict[str, Any]]:
    """Scan processed data and create new alerts."""
    new_alerts: list[dict[str, Any]] = []

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for alert_type, rule in ALERT_RULES.items():
                    cur.execute(rule["query"])
                    rows = cur.fetchall()

                    for row in rows:
                        severity = rule["severity_fn"](row)
                        title = rule["title_fn"](row)
                        description = rule["desc_fn"](row)
                        metadata = _build_metadata(alert_type, row)
                        region = _region_name(row, alert_type)
                        lat, lon = _coordinates(row, alert_type)

                        cur.execute(
                            """
                            INSERT INTO alerts (
                                alert_type, source, severity, title, description,
                                latitude, longitude, region_name, metadata
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                            RETURNING id, created_at
                            """,
                            (
                                alert_type,
                                rule["source"],
                                severity,
                                title,
                                description,
                                lat,
                                lon,
                                region,
                                __import__("json").dumps(metadata),
                            ),
                        )
                        alert_id, created_at = cur.fetchone()
                        new_alerts.append({
                            "id": alert_id,
                            "alert_type": alert_type,
                            "source": rule["source"],
                            "severity": severity,
                            "title": title,
                            "description": description,
                            "latitude": lat,
                            "longitude": lon,
                            "region_name": region,
                            "created_at": created_at.isoformat(),
                        })

        if new_alerts:
            logger.info(
                "New alerts generated",
                extra={"context": {"count": len(new_alerts)}},
            )
    except Exception as exc:
        logger.error(
            "Alert generation failed",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )

    return new_alerts


def get_active_alerts(limit: int = 50) -> list[dict[str, Any]]:
    """Return active alerts ordered by creation time."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, alert_type, source, severity, title, description,
                       latitude, longitude, region_name, metadata, created_at
                FROM alerts
                WHERE is_active = TRUE
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            return [
                {
                    cols[i]: (row[i].isoformat() if isinstance(row[i], datetime) else row[i])
                    for i in range(len(cols))
                }
                for row in cur.fetchall()
            ]


def get_alert_history(
    limit: int = 100,
    offset: int = 0,
    alert_type: str | None = None,
    severity: str | None = None,
) -> dict[str, Any]:
    """Return paginated alert history."""
    where: list[str] = []
    params: list[Any] = []

    if alert_type:
        where.append("alert_type = %s")
        params.append(alert_type)
    if severity:
        where.append("severity = %s")
        params.append(severity)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM alerts {where_sql}", params)
            total = cur.fetchone()[0]

            cur.execute(
                f"""
                SELECT id, alert_type, source, severity, title, description,
                       latitude, longitude, region_name, is_active, created_at, resolved_at
                FROM alerts {where_sql}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            cols = [d[0] for d in cur.description]
            data = [
                {
                    cols[i]: (row[i].isoformat() if isinstance(row[i], datetime) else row[i])
                    for i in range(len(cols))
                }
                for row in cur.fetchall()
            ]

    return {"total_count": total, "limit": limit, "offset": offset, "data": data}
