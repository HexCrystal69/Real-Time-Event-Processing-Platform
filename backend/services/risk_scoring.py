"""
GRIP — Unified risk scoring engine.

Computes regional risk scores from live processed pipeline data.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.connection import get_connection

logger = get_logger("risk_scoring")

RISK_LEVELS = ("Low", "Moderate", "High", "Critical")

SEVERITY_WEIGHTS = {
    "earthquake": {"Critical": 100, "High": 75, "Medium": 45, "Low": 15},
    "wildfire": {"Extreme": 100, "High": 75, "Medium": 45, "Low": 15},
    "weather": {"Extreme": 100, "Severe": 75, "Moderate": 45, "Minor": 20, "None": 5},
    "air_quality": {"Hazardous": 100, "Very Poor": 80, "Poor": 55, "Moderate": 30, "Good": 10, "Unknown": 5},
}


def _score_to_level(score: float) -> str:
    if score >= 75:
        return "Critical"
    if score >= 50:
        return "High"
    if score >= 25:
        return "Moderate"
    return "Low"


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _earthquake_score_for_region(cur, lat: float, lon: float, radius_km: float) -> float:
    cur.execute(
        """
        SELECT risk_category, magnitude, latitude, longitude
        FROM earthquakes_processed
        WHERE event_time >= NOW() - (%s * INTERVAL '1 hour')
        """,
        (settings.risk_lookback_hours,),
    )
    max_score = 0.0
    for risk_category, magnitude, eq_lat, eq_lon in cur.fetchall():
        if eq_lat is None or eq_lon is None:
            continue
        if _haversine_km(lat, lon, eq_lat, eq_lon) <= radius_km:
            cat_score = SEVERITY_WEIGHTS["earthquake"].get(risk_category, 10)
            mag_boost = min((magnitude or 0) * 5, 25)
            max_score = max(max_score, cat_score + mag_boost)
    return min(max_score, 100.0)


def _wildfire_score_for_region(cur, lat: float, lon: float, radius_km: float) -> float:
    cur.execute(
        """
        SELECT fire_severity, frp, latitude, longitude
        FROM wildfires_processed
        WHERE acq_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
        """,
        (max(settings.risk_lookback_hours // 24, 1),),
    )
    max_score = 0.0
    count = 0
    for severity, frp, wf_lat, wf_lon in cur.fetchall():
        if wf_lat is None or wf_lon is None:
            continue
        if _haversine_km(lat, lon, wf_lat, wf_lon) <= radius_km:
            count += 1
            base = SEVERITY_WEIGHTS["wildfire"].get(severity, 10)
            frp_boost = min((frp or 0) / 5, 20)
            max_score = max(max_score, base + frp_boost)
    if count > 3:
        max_score = min(max_score + count * 2, 100.0)
    return min(max_score, 100.0)


def _weather_score_for_location(cur, location_name: str) -> float:
    cur.execute(
        """
        SELECT storm_severity, wind_severity, precipitation_mm
        FROM weather_processed
        WHERE location_name = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (location_name,),
    )
    row = cur.fetchone()
    if not row:
        return 0.0
    storm_severity, wind_severity, precipitation = row
    score = SEVERITY_WEIGHTS["weather"].get(storm_severity, 5)
    if wind_severity == "Hurricane":
        score = max(score, 90)
    elif wind_severity == "Gale":
        score = max(score, 60)
    if precipitation and precipitation >= 50:
        score = max(score, 85)
    return min(score, 100.0)


def _air_quality_score_for_location(cur, location_name: str) -> float:
    cur.execute(
        """
        SELECT aqi_category, us_aqi
        FROM air_quality_processed
        WHERE location_name = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (location_name,),
    )
    row = cur.fetchone()
    if not row:
        return 0.0
    aqi_category, us_aqi = row
    score = SEVERITY_WEIGHTS["air_quality"].get(aqi_category, 5)
    if us_aqi and us_aqi > 300:
        score = max(score, 95)
    return min(score, 100.0)


def compute_risk_scores() -> list[dict[str, Any]]:
    """Compute and persist unified risk scores for all monitored regions."""
    results: list[dict[str, Any]] = []
    radius_km = 500.0

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for location in settings.monitored_locations:
                    eq_score = _earthquake_score_for_region(
                        cur, location.latitude, location.longitude, radius_km,
                    )
                    wf_score = _wildfire_score_for_region(
                        cur, location.latitude, location.longitude, radius_km,
                    )
                    weather_score = _weather_score_for_location(cur, location.name)
                    aq_score = _air_quality_score_for_location(cur, location.name)

                    unified = (
                        eq_score * 0.30
                        + wf_score * 0.25
                        + weather_score * 0.25
                        + aq_score * 0.20
                    )
                    risk_level = _score_to_level(unified)

                    cur.execute(
                        """
                        INSERT INTO risk_scores (
                            region_name, latitude, longitude,
                            earthquake_score, wildfire_score, weather_score,
                            air_quality_score, unified_score, risk_level
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            location.name,
                            location.latitude,
                            location.longitude,
                            round(eq_score, 2),
                            round(wf_score, 2),
                            round(weather_score, 2),
                            round(aq_score, 2),
                            round(unified, 2),
                            risk_level,
                        ),
                    )

                    results.append({
                        "region_name": location.name,
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                        "earthquake_score": round(eq_score, 2),
                        "wildfire_score": round(wf_score, 2),
                        "weather_score": round(weather_score, 2),
                        "air_quality_score": round(aq_score, 2),
                        "unified_score": round(unified, 2),
                        "risk_level": risk_level,
                    })

        logger.info(
            "Risk scores computed",
            extra={"context": {"regions": len(results)}},
        )
    except Exception as exc:
        logger.error(
            "Risk score computation failed",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )

    return results


def get_latest_risk_scores() -> list[dict[str, Any]]:
    """Return the most recent risk score per region."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (region_name)
                    region_name, latitude, longitude,
                    earthquake_score, wildfire_score, weather_score,
                    air_quality_score, unified_score, risk_level, computed_at
                FROM risk_scores
                ORDER BY region_name, computed_at DESC
                """
            )
            cols = [d[0] for d in cur.description]
            return [
                {
                    cols[i]: (row[i].isoformat() if isinstance(row[i], datetime) else row[i])
                    for i in range(len(cols))
                }
                for row in cur.fetchall()
            ]


def get_risk_distribution() -> dict[str, int]:
    """Count regions by current risk level."""
    scores = get_latest_risk_scores()
    distribution = {level: 0 for level in RISK_LEVELS}
    for score in scores:
        level = score.get("risk_level", "Low")
        if level in distribution:
            distribution[level] += 1
    return distribution
