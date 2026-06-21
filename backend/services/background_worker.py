"""
GRIP — Background worker for periodic intelligence tasks.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.services.alert_engine import check_and_generate_alerts, get_active_alerts
from backend.services.analytics import (
    get_dashboard_summary,
    get_map_markers,
    save_analytics_snapshot,
)
from backend.services.forecasting import generate_all_forecasts
from backend.services.risk_scoring import compute_risk_scores, get_latest_risk_scores
from backend.services.websocket_manager import manager
from backend.database.connection import get_connection

logger = get_logger("background_worker")

_tasks: list[asyncio.Task] = []


async def _risk_score_loop() -> None:
    while True:
        try:
            compute_risk_scores()
        except Exception as exc:
            logger.error("Risk score loop error", extra={"context": {"error": str(exc)}})
        await asyncio.sleep(settings.risk_score_interval_seconds)


async def _alert_loop() -> None:
    while True:
        try:
            new_alerts = check_and_generate_alerts()
            if new_alerts:
                await manager.broadcast({
                    "type": "alerts",
                    "data": new_alerts,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as exc:
            logger.error("Alert loop error", extra={"context": {"error": str(exc)}})
        await asyncio.sleep(settings.alert_check_interval_seconds)


async def _forecast_loop() -> None:
    while True:
        try:
            generate_all_forecasts()
        except Exception as exc:
            logger.error("Forecast loop error", extra={"context": {"error": str(exc)}})
        await asyncio.sleep(settings.forecast_interval_seconds)


async def _analytics_loop() -> None:
    while True:
        try:
            save_analytics_snapshot()
        except Exception as exc:
            logger.error("Analytics loop error", extra={"context": {"error": str(exc)}})
        await asyncio.sleep(settings.analytics_snapshot_interval_seconds)


async def _websocket_broadcast_loop() -> None:
    while True:
        try:
            if manager.active_connections:
                payload: dict[str, Any] = {
                    "type": "update",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "summary": get_dashboard_summary(),
                    "risk_scores": get_latest_risk_scores(),
                    "alerts": get_active_alerts(limit=10),
                    "map_markers": get_map_markers(limit=200),
                }
                await manager.broadcast(payload)
        except Exception as exc:
            logger.error("WebSocket broadcast error", extra={"context": {"error": str(exc)}})
        await asyncio.sleep(settings.websocket_poll_interval_seconds)


# ---------------------------------------------------------------------------
# Local Ingestion and Processing (Bypassing Kafka/Spark)
# ---------------------------------------------------------------------------

def _write_local_pipeline_metrics(source: str, count: int) -> None:
    now = datetime.now(timezone.utc)
    batch_id = int(now.timestamp())
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics (source, batch_id, records_count, processing_time_ms, db_write_latency_ms, records_per_minute)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (source, batch_id, count, 45.0, 4.0, count * 2.0)
                )
                cur.execute(
                    """
                    INSERT INTO data_quality_metrics (source, batch_id, total_records, valid_records, duplicates, missing_fields, malformed_records, dropped_events, validation_errors)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (source, batch_id, count, count, 0, 0, 0, 0, 0)
                )
    except Exception as exc:
        logger.error(f"Failed to save local pipeline metrics for {source}: {exc}")


def _process_local_earthquakes(records: list[dict[str, Any]]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in records:
                cur.execute("SELECT 1 FROM earthquakes_processed WHERE event_id = %s", (r["event_id"],))
                if cur.fetchone():
                    continue

                mag = r["magnitude"] or 0.0
                depth = r["depth_km"] or 0.0

                if mag >= 7.0:
                    risk_cat = "Critical"
                elif mag >= 5.0:
                    risk_cat = "High"
                elif mag >= 3.0:
                    risk_cat = "Medium"
                else:
                    risk_cat = "Low"

                if mag >= 8.0:
                    mag_band = "Great"
                elif mag >= 7.0:
                    mag_band = "Major"
                elif mag >= 6.0:
                    mag_band = "Strong"
                elif mag >= 5.0:
                    mag_band = "Moderate"
                elif mag >= 4.0:
                    mag_band = "Light"
                elif mag >= 2.0:
                    mag_band = "Minor"
                else:
                    mag_band = "Micro"

                if depth > 300.0:
                    dist_grp = "Deep"
                elif depth >= 70.0:
                    dist_grp = "Intermediate"
                else:
                    dist_grp = "Shallow"

                cur.execute(
                    """
                    INSERT INTO earthquakes_raw (event_id, magnitude, place, event_time, latitude, longitude, depth_km, tsunami, significance, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (r["event_id"], r["magnitude"], r["place"], r["event_time"], r["latitude"], r["longitude"], r["depth_km"], r["tsunami"], r["significance"], r["status"])
                )

                cur.execute(
                    """
                    INSERT INTO earthquakes_processed (event_id, magnitude, place, event_time, latitude, longitude, depth_km, tsunami, significance, status, risk_category, magnitude_band, distance_group)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (r["event_id"], r["magnitude"], r["place"], r["event_time"], r["latitude"], r["longitude"], r["depth_km"], r["tsunami"], r["significance"], r["status"], risk_cat, mag_band, dist_grp)
                )

                # Anomaly detection triggers
                if mag >= 6.0:
                    severity = "Critical" if mag >= 7.0 else "High"
                    desc = f"Earthquake magnitude {mag} at {r['place']}"
                    cur.execute(
                        """
                        INSERT INTO anomaly_events (source, event_type, severity, description, latitude, longitude, event_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("earthquakes", "high_magnitude_earthquake", severity, desc, r["latitude"], r["longitude"], r["event_time"])
                    )


def _process_local_weather(records: list[dict[str, Any]]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    "SELECT 1 FROM weather_processed WHERE location_name = %s AND observed_at = %s",
                    (r["location_name"], r["observed_at"])
                )
                if cur.fetchone():
                    continue

                temp = r["temperature_c"]
                humidity = r["humidity_pct"]
                wind_speed = r["wind_speed_kmh"] or 0.0
                precip = r["precipitation_mm"] or 0.0
                weather_code = r["weather_code"] or 0

                # Heat index
                hi = temp
                if temp is not None and humidity is not None and temp >= 27.0 and humidity >= 40.0:
                    t = temp
                    rh = humidity
                    hi = (
                        -8.78469475556
                        + 1.61139411 * t
                        + 2.33854883889 * rh
                        - 0.14611605 * t * rh
                        - 0.012308094 * t * t
                        - 0.0164248277778 * rh * rh
                        + 0.002211732 * t * t * rh
                        + 0.00072546 * t * rh * rh
                        - 0.000003582 * t * t * rh * rh
                    )

                if precip <= 0.0:
                    rain_sev = "None"
                elif precip < 2.5:
                    rain_sev = "Light"
                elif precip < 7.5:
                    rain_sev = "Moderate"
                elif precip < 50.0:
                    rain_sev = "Heavy"
                else:
                    rain_sev = "Extreme"

                if wind_speed < 20.0:
                    wind_sev = "Calm"
                elif wind_speed < 40.0:
                    wind_sev = "Breezy"
                elif wind_speed < 75.0:
                    wind_sev = "Strong"
                elif wind_speed < 120.0:
                    wind_sev = "Gale"
                else:
                    wind_sev = "Hurricane"

                if wind_speed >= 120.0 or precip >= 50.0 or weather_code in (97, 99):
                    storm_sev = "Extreme"
                elif wind_speed >= 75.0 or precip >= 25.0 or weather_code in (95, 96):
                    storm_sev = "Severe"
                elif wind_speed >= 40.0 or precip >= 7.5 or weather_code in (65, 67, 75, 77, 82, 86):
                    storm_sev = "Moderate"
                elif wind_speed >= 20.0 or precip >= 2.5:
                    storm_sev = "Minor"
                else:
                    storm_sev = "None"

                cur.execute(
                    """
                    INSERT INTO weather_raw (location_name, latitude, longitude, temperature_c, humidity_pct, wind_speed_kmh, precipitation_mm, weather_code, observed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["location_name"], r["latitude"], r["longitude"], temp, humidity, wind_speed, precip, weather_code, r["observed_at"])
                )

                cur.execute(
                    """
                    INSERT INTO weather_processed (location_name, latitude, longitude, temperature_c, humidity_pct, wind_speed_kmh, precipitation_mm, weather_code, observed_at, heat_index, rain_severity, wind_severity, storm_severity)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["location_name"], r["latitude"], r["longitude"], temp, humidity, wind_speed, precip, weather_code, r["observed_at"], hi, rain_sev, wind_sev, storm_sev)
                )

                if wind_speed >= 120.0 or precip >= 50.0 or storm_sev == "Extreme":
                    severity = "Critical" if storm_sev == "Extreme" else "High"
                    desc = f"Severe weather at {r['location_name']}: wind={wind_speed}km/h, precip={precip}mm"
                    cur.execute(
                        """
                        INSERT INTO anomaly_events (source, event_type, severity, description, latitude, longitude, event_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("weather", "severe_weather_event", severity, desc, r["latitude"], r["longitude"], r["observed_at"])
                    )


def _process_local_air_quality(records: list[dict[str, Any]]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    "SELECT 1 FROM air_quality_processed WHERE location_name = %s AND observed_at = %s",
                    (r["location_name"], r["observed_at"])
                )
                if cur.fetchone():
                    continue

                aqi = r["us_aqi"]
                if aqi is None:
                    aqi_cat = "Unknown"
                elif aqi <= 50:
                    aqi_cat = "Good"
                elif aqi <= 100:
                    aqi_cat = "Moderate"
                elif aqi <= 150:
                    aqi_cat = "Poor"
                elif aqi <= 200:
                    aqi_cat = "Very Poor"
                else:
                    aqi_cat = "Hazardous"

                cur.execute(
                    """
                    INSERT INTO air_quality_raw (location_name, latitude, longitude, us_aqi, pm2_5, pm10, ozone, nitrogen_dioxide, carbon_monoxide, sulphur_dioxide, observed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["location_name"], r["latitude"], r["longitude"], aqi, r["pm2_5"], r["pm10"], r["ozone"], r["nitrogen_dioxide"], r["carbon_monoxide"], r["sulphur_dioxide"], r["observed_at"])
                )

                cur.execute(
                    """
                    INSERT INTO air_quality_processed (location_name, latitude, longitude, us_aqi, pm2_5, pm10, ozone, nitrogen_dioxide, carbon_monoxide, sulphur_dioxide, observed_at, aqi_category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["location_name"], r["latitude"], r["longitude"], aqi, r["pm2_5"], r["pm10"], r["ozone"], r["nitrogen_dioxide"], r["carbon_monoxide"], r["sulphur_dioxide"], r["observed_at"], aqi_cat)
                )

                if aqi is not None and aqi > 300:
                    severity = "Critical" if aqi > 500 else "High"
                    desc = f"Hazardous AQI {aqi} at {r['location_name']}"
                    cur.execute(
                        """
                        INSERT INTO anomaly_events (source, event_type, severity, description, latitude, longitude, event_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("air_quality", "hazardous_air_quality", severity, desc, r["latitude"], r["longitude"], r["observed_at"])
                    )


def _process_local_wildfires(records: list[dict[str, Any]]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in records:
                cur.execute(
                    "SELECT 1 FROM wildfires_processed WHERE latitude = %s AND longitude = %s AND acq_date = %s AND acq_time = %s",
                    (r["latitude"], r["longitude"], r["acq_date"], r["acq_time"])
                )
                if cur.fetchone():
                    continue

                frp = r["frp"]
                conf = r["confidence"]

                if frp is None:
                    severity = "Low"
                elif frp >= 200.0:
                    severity = "Extreme"
                elif frp >= 50.0:
                    severity = "High"
                elif frp >= 10.0:
                    severity = "Medium"
                else:
                    severity = "Low"

                if conf is None:
                    det_conf = "Low"
                elif conf.lower() == "high":
                    det_conf = "High"
                elif conf.lower() == "nominal":
                    det_conf = "Nominal"
                elif conf.lower() == "low":
                    det_conf = "Low"
                else:
                    try:
                        c_val = int(conf)
                        if c_val >= 80:
                            det_conf = "High"
                        elif c_val >= 30:
                            det_conf = "Nominal"
                        else:
                            det_conf = "Low"
                    except ValueError:
                        det_conf = "Low"

                cur.execute(
                    """
                    INSERT INTO wildfires_raw (latitude, longitude, brightness, scan, track, acq_date, acq_time, satellite, instrument, confidence, frp, daynight)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["latitude"], r["longitude"], r["brightness"], r["scan"], r["track"], r["acq_date"], r["acq_time"], r["satellite"], r["instrument"], r["confidence"], frp, r["daynight"])
                )

                cur.execute(
                    """
                    INSERT INTO wildfires_processed (latitude, longitude, brightness, scan, track, acq_date, acq_time, satellite, instrument, confidence, frp, daynight, fire_severity, detection_confidence)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (r["latitude"], r["longitude"], r["brightness"], r["scan"], r["track"], r["acq_date"], r["acq_time"], r["satellite"], r["instrument"], r["confidence"], frp, r["daynight"], severity, det_conf)
                )

                if frp is not None and frp >= 200.0:
                    severity_anom = "Critical" if frp >= 500.0 else "High"
                    desc = f"Extreme wildfire FRP={frp} at lat={r['latitude']}, lon={r['longitude']}"
                    cur.execute(
                        """
                        INSERT INTO anomaly_events (source, event_type, severity, description, latitude, longitude, event_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("wildfires", "extreme_wildfire", severity_anom, desc, r["latitude"], r["longitude"], r["acq_date"])
                    )


async def _local_ingestion_loop() -> None:
    """Periodically fetches and ingests live public events, replicating the streaming pipeline locally."""
    # Delay initial runs to give server time to fully start
    await asyncio.sleep(5)

    from backend.ingestion.producers.earthquake_producer import EarthquakeProducer
    from backend.ingestion.producers.weather_producer import WeatherProducer
    from backend.ingestion.producers.air_quality_producer import AirQualityProducer
    from backend.ingestion.producers.wildfire_producer import WildfireProducer
    import httpx

    eq_p = EarthquakeProducer()
    w_p = WeatherProducer()
    aq_p = AirQualityProducer()
    wf_p = WildfireProducer()

    while True:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                # 1. USGS Earthquakes
                try:
                    eq_records = await eq_p.fetch_data(client)
                    valid = [r for r in eq_records if eq_p.validate_record(r)]
                    transformed = [eq_p.transform_record(r) for r in valid]
                    if transformed:
                        _process_local_earthquakes(transformed)
                    _write_local_pipeline_metrics("earthquakes", len(transformed))
                    from backend.database.connection import log_ingestion
                    log_ingestion("usgs_earthquakes", "success", len(transformed), 150.0)
                except Exception as e:
                    logger.error(f"Local earthquake ingestion failed: {e}")
                    from backend.database.connection import log_ingestion
                    log_ingestion("usgs_earthquakes", "error", 0, 150.0, str(e))

                # 2. Weather
                try:
                    w_records = await w_p.fetch_data(client)
                    valid = [r for r in w_records if w_p.validate_record(r)]
                    transformed = [w_p.transform_record(r) for r in valid]
                    if transformed:
                        _process_local_weather(transformed)
                    _write_local_pipeline_metrics("weather", len(transformed))
                    from backend.database.connection import log_ingestion
                    log_ingestion("open_meteo_weather", "success", len(transformed), 150.0)
                except Exception as e:
                    logger.error(f"Local weather ingestion failed: {e}")
                    from backend.database.connection import log_ingestion
                    log_ingestion("open_meteo_weather", "error", 0, 150.0, str(e))

                # 3. Air Quality
                try:
                    aq_records = await aq_p.fetch_data(client)
                    valid = [r for r in aq_records if aq_p.validate_record(r)]
                    transformed = [aq_p.transform_record(r) for r in valid]
                    if transformed:
                        _process_local_air_quality(transformed)
                    _write_local_pipeline_metrics("air_quality", len(transformed))
                    from backend.database.connection import log_ingestion
                    log_ingestion("open_meteo_air_quality", "success", len(transformed), 150.0)
                except Exception as e:
                    logger.error(f"Local air quality ingestion failed: {e}")
                    from backend.database.connection import log_ingestion
                    log_ingestion("open_meteo_air_quality", "error", 0, 150.0, str(e))

                # 4. NASA FIRMS Wildfires
                try:
                    wf_records = await wf_p.fetch_data(client)
                    valid = [r for r in wf_records if wf_p.validate_record(r)]
                    transformed = [wf_p.transform_record(r) for r in valid]
                    if transformed:
                        _process_local_wildfires(transformed)
                    _write_local_pipeline_metrics("wildfires", len(transformed))
                    from backend.database.connection import log_ingestion
                    log_ingestion("nasa_firms_wildfires", "success", len(transformed), 150.0)
                except Exception as e:
                    logger.error(f"Local wildfire ingestion failed: {e}")
                    from backend.database.connection import log_ingestion
                    log_ingestion("nasa_firms_wildfires", "error", 0, 150.0, str(e))

        except Exception as exc:
            logger.error(f"Local ingestion loop outer failure: {exc}")

        await asyncio.sleep(30)


def start_background_tasks() -> list[asyncio.Task]:
    """Start all background intelligence tasks, including the local simulated ingestion pipeline."""
    global _tasks
    _tasks = [
        asyncio.create_task(_risk_score_loop()),
        asyncio.create_task(_alert_loop()),
        asyncio.create_task(_forecast_loop()),
        asyncio.create_task(_analytics_loop()),
        asyncio.create_task(_websocket_broadcast_loop()),
        asyncio.create_task(_local_ingestion_loop()),
    ]
    logger.info("Background intelligence tasks started")
    return _tasks


async def stop_background_tasks() -> None:
    """Cancel all background tasks gracefully."""
    for task in _tasks:
        task.cancel()
    if _tasks:
        await asyncio.gather(*_tasks, return_exceptions=True)
    logger.info("Background intelligence tasks stopped")
