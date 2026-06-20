"""
GRIP — Analytics aggregation service.

Computes dashboard analytics from live processed pipeline data.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from backend.config.logger import get_logger
from backend.database.connection import get_connection
from backend.services.risk_scoring import get_risk_distribution

logger = get_logger("analytics")


def _count_table(cur, table: str, time_col: str, interval: str) -> int:
    cur.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {time_col} >= NOW() - INTERVAL %s",
        (interval,),
    )
    return cur.fetchone()[0]


def get_dashboard_summary() -> dict[str, Any]:
    """Return high-level dashboard metrics."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM earthquakes_processed")
            total_earthquakes = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM wildfires_processed")
            total_wildfires = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM weather_processed")
            total_weather = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM air_quality_processed")
            total_air_quality = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM anomaly_events")
            total_anomalies = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM alerts WHERE is_active = TRUE")
            active_alerts = cur.fetchone()[0]

            events_last_hour = (
                _count_table(cur, "earthquakes_processed", "event_time", "1 hour")
                + _count_table(cur, "wildfires_processed", "processed_at", "1 hour")
                + _count_table(cur, "weather_processed", "observed_at", "1 hour")
                + _count_table(cur, "air_quality_processed", "observed_at", "1 hour")
            )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_events": total_earthquakes + total_wildfires + total_weather + total_air_quality,
        "total_earthquakes": total_earthquakes,
        "total_wildfires": total_wildfires,
        "total_weather": total_weather,
        "total_air_quality": total_air_quality,
        "total_anomalies": total_anomalies,
        "active_alerts": active_alerts,
        "events_last_hour": events_last_hour,
        "risk_distribution": get_risk_distribution(),
    }


def get_events_per_hour(hours: int = 24) -> dict[str, list[dict[str, Any]]]:
    """Return hourly event counts per source."""
    result: dict[str, list[dict[str, Any]]] = {}

    queries = {
        "earthquakes": """
            SELECT date_trunc('hour', event_time) AS hour, COUNT(*) AS count
            FROM earthquakes_processed
            WHERE event_time >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY hour ORDER BY hour
        """,
        "wildfires": """
            SELECT date_trunc('hour', processed_at) AS hour, COUNT(*) AS count
            FROM wildfires_processed
            WHERE processed_at >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY hour ORDER BY hour
        """,
        "weather": """
            SELECT date_trunc('hour', observed_at) AS hour, COUNT(*) AS count
            FROM weather_processed
            WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY hour ORDER BY hour
        """,
        "air_quality": """
            SELECT date_trunc('hour', observed_at) AS hour, COUNT(*) AS count
            FROM air_quality_processed
            WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY hour ORDER BY hour
        """,
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            for source, query in queries.items():
                cur.execute(query, (hours,))
                result[source] = [
                    {"hour": row[0].isoformat(), "count": row[1]}
                    for row in cur.fetchall()
                ]

    return result


def get_risk_distribution_analytics() -> dict[str, Any]:
    """Return risk distribution across all sources."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT risk_category, COUNT(*)
                FROM earthquakes_processed
                WHERE event_time >= NOW() - INTERVAL '7 days'
                GROUP BY risk_category
                """
            )
            earthquake_risk = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT fire_severity, COUNT(*)
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY fire_severity
                """
            )
            wildfire_risk = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT storm_severity, COUNT(*)
                FROM weather_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                GROUP BY storm_severity
                """
            )
            weather_risk = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT aqi_category, COUNT(*)
                FROM air_quality_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                GROUP BY aqi_category
                """
            )
            air_quality_risk = {row[0]: row[1] for row in cur.fetchall()}

    return {
        "earthquakes": earthquake_risk,
        "wildfires": wildfire_risk,
        "weather": weather_risk,
        "air_quality": air_quality_risk,
        "unified": get_risk_distribution(),
    }


def get_source_activity() -> list[dict[str, Any]]:
    """Return latest ingestion activity per source."""
    sources = [
        "usgs_earthquakes",
        "open_meteo_weather",
        "open_meteo_air_quality",
        "nasa_firms_wildfires",
    ]
    activity: list[dict[str, Any]] = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            for source in sources:
                cur.execute(
                    """
                    SELECT status, records_count, latency_ms, created_at
                    FROM ingestion_logs
                    WHERE source = %s
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (source,),
                )
                row = cur.fetchone()
                if row:
                    activity.append({
                        "source": source,
                        "status": row[0],
                        "records_count": row[1],
                        "latency_ms": round(row[2], 2) if row[2] else None,
                        "last_run": row[3].isoformat(),
                    })
                else:
                    activity.append({"source": source, "status": "no_data"})

    return activity


def get_regional_rankings() -> list[dict[str, Any]]:
    """Return regions ranked by unified risk score."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (region_name)
                    region_name, unified_score, risk_level,
                    earthquake_score, wildfire_score, weather_score, air_quality_score
                FROM risk_scores
                ORDER BY region_name, computed_at DESC
                """
            )
            regions = [
                {
                    "region_name": row[0],
                    "unified_score": row[1],
                    "risk_level": row[2],
                    "earthquake_score": row[3],
                    "wildfire_score": row[4],
                    "weather_score": row[5],
                    "air_quality_score": row[6],
                }
                for row in cur.fetchall()
            ]

    return sorted(regions, key=lambda r: r["unified_score"], reverse=True)


def get_map_markers(limit: int = 500) -> dict[str, list[dict[str, Any]]]:
    """Return geo markers for the global risk map."""
    markers: dict[str, list[dict[str, Any]]] = {
        "earthquakes": [],
        "wildfires": [],
        "weather": [],
        "air_quality": [],
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, magnitude, place, latitude, longitude,
                       risk_category, event_time
                FROM earthquakes_processed
                WHERE event_time >= NOW() - INTERVAL '7 days'
                ORDER BY event_time DESC LIMIT %s
                """,
                (limit,),
            )
            for row in cur.fetchall():
                markers["earthquakes"].append({
                    "id": row[0], "magnitude": row[1], "place": row[2],
                    "lat": row[3], "lng": row[4], "severity": row[5],
                    "time": row[6].isoformat(),
                })

            cur.execute(
                """
                SELECT latitude, longitude, frp, fire_severity,
                       detection_confidence, acq_date
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '3 days'
                ORDER BY frp DESC NULLS LAST LIMIT %s
                """,
                (limit,),
            )
            for row in cur.fetchall():
                markers["wildfires"].append({
                    "lat": row[0], "lng": row[1], "frp": row[2],
                    "severity": row[3], "confidence": row[4],
                    "date": row[5].isoformat() if row[5] else None,
                })

            cur.execute(
                """
                SELECT location_name, latitude, longitude, storm_severity,
                       wind_speed_kmh, temperature_c, observed_at
                FROM weather_processed
                WHERE observed_at >= (
                    SELECT MAX(observed_at) - INTERVAL '6 hours' FROM weather_processed
                )
                ORDER BY observed_at DESC
                """
            )
            for row in cur.fetchall():
                markers["weather"].append({
                    "location": row[0], "lat": row[1], "lng": row[2],
                    "severity": row[3], "wind_kmh": row[4],
                    "temp_c": row[5], "time": row[6].isoformat(),
                })

            cur.execute(
                """
                SELECT location_name, latitude, longitude, us_aqi,
                       aqi_category, observed_at
                FROM air_quality_processed
                WHERE observed_at >= (
                    SELECT MAX(observed_at) - INTERVAL '6 hours' FROM air_quality_processed
                )
                ORDER BY us_aqi DESC NULLS LAST
                """
            )
            for row in cur.fetchall():
                markers["air_quality"].append({
                    "location": row[0], "lat": row[1], "lng": row[2],
                    "aqi": row[3], "category": row[4],
                    "time": row[5].isoformat(),
                })

    return markers


def save_analytics_snapshot() -> None:
    """Persist current analytics state for historical tracking."""
    snapshot = {
        "summary": get_dashboard_summary(),
        "events_per_hour": get_events_per_hour(24),
        "risk_distribution": get_risk_distribution_analytics(),
        "regional_rankings": get_regional_rankings(),
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics_snapshots (snapshot_type, payload)
                VALUES (%s, %s::jsonb)
                """,
                ("dashboard", json.dumps(snapshot, default=str)),
            )


def get_earthquake_analytics() -> dict[str, Any]:
    """Detailed earthquake intelligence analytics."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT magnitude, depth_km, risk_category, latitude, longitude, event_time
                FROM earthquakes_processed
                WHERE event_time >= NOW() - INTERVAL '30 days'
                ORDER BY event_time DESC
                """
            )
            events = cur.fetchall()

            cur.execute(
                """
                SELECT risk_category, COUNT(*)
                FROM earthquakes_processed
                WHERE event_time >= NOW() - INTERVAL '30 days'
                GROUP BY risk_category
                """
            )
            risk_cats = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT
                    CASE
                        WHEN depth_km IS NULL THEN 'Unknown'
                        WHEN depth_km < 70 THEN 'Shallow'
                        WHEN depth_km < 300 THEN 'Intermediate'
                        ELSE 'Deep'
                    END AS depth_group,
                    COUNT(*)
                FROM earthquakes_processed
                WHERE event_time >= NOW() - INTERVAL '30 days'
                GROUP BY depth_group
                """
            )
            depth_dist = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT date_trunc('day', event_time) AS day, COUNT(*)
                FROM earthquakes_processed
                WHERE event_time >= NOW() - INTERVAL '30 days'
                GROUP BY day ORDER BY day
                """
            )
            trends = [{"day": row[0].isoformat(), "count": row[1]} for row in cur.fetchall()]

    magnitudes = [e[0] for e in events if e[0] is not None]
    mag_bins = {"0-2": 0, "2-4": 0, "4-6": 0, "6-8": 0, "8+": 0}
    for m in magnitudes:
        if m < 2:
            mag_bins["0-2"] += 1
        elif m < 4:
            mag_bins["2-4"] += 1
        elif m < 6:
            mag_bins["4-6"] += 1
        elif m < 8:
            mag_bins["6-8"] += 1
        else:
            mag_bins["8+"] += 1

    return {
        "recent_count": len(events),
        "magnitude_distribution": mag_bins,
        "depth_analysis": depth_dist,
        "risk_categories": risk_cats,
        "historical_trends": trends,
        "recent_events": [
            {
                "magnitude": e[0], "depth_km": e[1], "risk_category": e[2],
                "latitude": e[3], "longitude": e[4],
                "event_time": e[5].isoformat(),
            }
            for e in events[:50]
        ],
    }


def get_wildfire_analytics() -> dict[str, Any]:
    """Detailed wildfire intelligence analytics."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fire_severity, COUNT(*)
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY fire_severity
                """
            )
            severity_dist = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT detection_confidence, COUNT(*)
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY detection_confidence
                """
            )
            confidence_dist = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT acq_date, COUNT(*), AVG(frp)
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY acq_date ORDER BY acq_date
                """
            )
            growth_trends = [
                {"date": row[0].isoformat(), "count": row[1], "avg_frp": round(row[2] or 0, 2)}
                for row in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT latitude, longitude, frp, fire_severity, detection_confidence, acq_date
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '3 days'
                ORDER BY frp DESC NULLS LAST LIMIT 100
                """
            )
            active_fires = [
                {
                    "latitude": row[0], "longitude": row[1], "frp": row[2],
                    "severity": row[3], "confidence": row[4],
                    "date": row[5].isoformat(),
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT
                    ROUND(latitude::numeric, 0) AS lat_band,
                    ROUND(longitude::numeric, 0) AS lon_band,
                    COUNT(*) AS fire_count,
                    AVG(frp) AS avg_frp
                FROM wildfires_processed
                WHERE acq_date >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY lat_band, lon_band
                HAVING COUNT(*) >= 5
                ORDER BY fire_count DESC
                LIMIT 10
                """
            )
            high_risk_regions = [
                {
                    "latitude": float(row[0]), "longitude": float(row[1]),
                    "fire_count": row[2], "avg_frp": round(row[3] or 0, 2),
                }
                for row in cur.fetchall()
            ]

    return {
        "severity_distribution": severity_dist,
        "confidence_distribution": confidence_dist,
        "growth_trends": growth_trends,
        "active_fires": active_fires,
        "high_risk_regions": high_risk_regions,
    }


def get_weather_analytics() -> dict[str, Any]:
    """Detailed weather intelligence analytics."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT location_name, observed_at, temperature_c, precipitation_mm,
                       wind_speed_kmh, storm_severity, wind_severity
                FROM weather_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                ORDER BY observed_at DESC
                """
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT storm_severity, COUNT(*)
                FROM weather_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                GROUP BY storm_severity
                """
            )
            storm_risks = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT wind_severity, COUNT(*)
                FROM weather_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                GROUP BY wind_severity
                """
            )
            wind_severity = {row[0]: row[1] for row in cur.fetchall()}

    by_location: dict[str, list] = {}
    for row in rows:
        loc = row[0]
        if loc not in by_location:
            by_location[loc] = []
        by_location[loc].append({
            "time": row[1].isoformat(),
            "temperature_c": row[2],
            "precipitation_mm": row[3],
            "wind_speed_kmh": row[4],
            "storm_severity": row[5],
            "wind_severity": row[6],
        })

    alerts = [
        {
            "location": row[0], "storm_severity": row[5],
            "wind_kmh": row[4], "time": row[1].isoformat(),
        }
        for row in rows
        if row[5] in ("Severe", "Extreme")
    ][:20]

    return {
        "temperature_trends": by_location,
        "storm_risks": storm_risks,
        "wind_severity": wind_severity,
        "weather_alerts": alerts,
    }


def get_air_quality_analytics() -> dict[str, Any]:
    """Detailed air quality intelligence analytics."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT aqi_category, COUNT(*)
                FROM air_quality_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                GROUP BY aqi_category
                """
            )
            aqi_distribution = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT location_name, observed_at, us_aqi, pm2_5, pm10, aqi_category
                FROM air_quality_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                ORDER BY observed_at DESC
                """
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT location_name, AVG(us_aqi) AS avg_aqi, MAX(us_aqi) AS max_aqi
                FROM air_quality_processed
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                GROUP BY location_name
                ORDER BY avg_aqi DESC
                """
            )
            regional = [
                {"location": row[0], "avg_aqi": round(row[1] or 0, 1), "max_aqi": row[2]}
                for row in cur.fetchall()
            ]

    by_location: dict[str, list] = {}
    hotspots: list[dict[str, Any]] = []
    for row in rows:
        loc = row[0]
        if loc not in by_location:
            by_location[loc] = []
        by_location[loc].append({
            "time": row[1].isoformat(), "us_aqi": row[2],
            "pm2_5": row[3], "pm10": row[4], "category": row[5],
        })
        if row[2] and row[2] > 150:
            hotspots.append({
                "location": loc, "us_aqi": row[2],
                "category": row[5], "time": row[1].isoformat(),
            })

    return {
        "aqi_distribution": aqi_distribution,
        "aqi_trends": by_location,
        "pollution_hotspots": hotspots[:20],
        "regional_comparisons": regional,
    }
