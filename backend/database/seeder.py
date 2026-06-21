"""
GRIP — Database seeder to populate historical data.

This seeds 30 days of hourly weather/AQI, earthquakes, wildfires, metrics,
and logs if the database is blank. It enables immediate forecasting and
analytics loading upon first start.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import random
import uuid
from typing import Any

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.connection import get_connection

logger = get_logger("seeder")


def seed_weather_and_aq() -> None:
    """Generate and insert 30 days of hourly weather and AQI records."""
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=30)
    locations = settings.monitored_locations

    weather_records: list[tuple[Any, ...]] = []
    aq_records: list[tuple[Any, ...]] = []

    for loc in locations:
        logger.info(f"Generating weather & AQ history for {loc.name}...")

        # Distinct baseline values per city
        if loc.name == "New York":
            w_base, aq_base = 22.0, 40
        elif loc.name == "London":
            w_base, aq_base = 15.0, 30
        elif loc.name == "Tokyo":
            w_base, aq_base = 20.0, 35
        elif loc.name == "Mumbai":
            w_base, aq_base = 30.0, 120
        elif loc.name == "São Paulo":
            w_base, aq_base = 18.0, 55
        else:
            w_base, aq_base = 20.0, 50

        current_time = start_time
        while current_time <= now:
            hour = current_time.hour
            # Diurnal temperature cycle: peaks around 14:00 (2 PM)
            temp_diurnal = 5.0 * math.sin((hour - 8) * math.pi / 12)
            temp = w_base + temp_diurnal + random.uniform(-2.0, 2.0)

            # Humidity: inversely related to temperature
            humidity = min(100.0, max(20.0, 70.0 - temp_diurnal * 4.0 + random.uniform(-10.0, 10.0)))

            # Wind speed: average 12 km/h, occasional spikes
            wind_speed = random.uniform(2.0, 25.0)
            if random.random() < 0.01:  # 1% chance of severe wind event (to trigger alert)
                wind_speed = random.uniform(80.0, 130.0)

            # Precipitation
            precip = 0.0
            weather_code = 0  # Clear sky
            if random.random() < 0.12:  # 12% chance of rain
                precip = random.uniform(0.5, 12.0)
                weather_code = 61  # Slight rain
                if random.random() < 0.15:  # Extreme rain (to trigger alerts)
                    precip = random.uniform(25.0, 60.0)
                    weather_code = 95  # Thunderstorm

            # Compute severity classes
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

            # Heat index calculation (Steadman simplified formula)
            hi = temp
            if temp >= 27.0 and humidity >= 40.0:
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

            weather_records.append((
                loc.name, loc.latitude, loc.longitude,
                round(temp, 1), round(humidity, 1), round(wind_speed, 1), round(precip, 2),
                weather_code, current_time, round(hi, 1),
                rain_sev, wind_sev, storm_sev
            ))

            # --- Air Quality ---
            # Diurnal AQI cycle
            aqi_diurnal = 15.0 * math.sin((hour - 8) * math.pi / 6)
            aqi = int(aq_base + aqi_diurnal + random.uniform(-10.0, 10.0))
            aqi = max(5, aqi)

            # Occasional hazardous spikes in Mumbai (to trigger alerts)
            if loc.name == "Mumbai" and random.random() < 0.015:
                aqi = random.randint(310, 480)

            if aqi <= 50:
                aqi_cat = "Good"
            elif aqi <= 100:
                aqi_cat = "Moderate"
            elif aqi <= 150:
                aqi_cat = "Poor"
            elif aqi <= 200:
                aqi_cat = "Very Poor"
            else:
                aqi_cat = "Hazardous"

            pm2_5 = round(aqi * 0.12 + random.uniform(-1.0, 3.0), 2)
            pm10 = round(aqi * 0.25 + random.uniform(-2.0, 5.0), 2)
            ozone = round(30.0 + random.uniform(-5.0, 20.0), 2)
            no2 = round(15.0 + random.uniform(-5.0, 15.0), 2)
            co = round(0.5 + random.uniform(-0.1, 0.5), 2)
            so2 = round(5.0 + random.uniform(-2.0, 5.0), 2)

            aq_records.append((
                loc.name, loc.latitude, loc.longitude,
                aqi, pm2_5, pm10, ozone, no2, co, so2,
                current_time, aqi_cat
            ))

            current_time += timedelta(hours=1)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO weather_processed (
                    location_name, latitude, longitude, temperature_c, humidity_pct,
                    wind_speed_kmh, precipitation_mm, weather_code, observed_at,
                    heat_index, rain_severity, wind_severity, storm_severity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                weather_records
            )
            cur.executemany(
                """
                INSERT INTO air_quality_processed (
                    location_name, latitude, longitude, us_aqi, pm2_5, pm10,
                    ozone, nitrogen_dioxide, carbon_monoxide, sulphur_dioxide,
                    observed_at, aqi_category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                aq_records
            )


def seed_earthquakes() -> None:
    """Generate and insert 100 historical earthquakes over 30 days globally."""
    now = datetime.now(timezone.utc)
    earthquakes: list[tuple[Any, ...]] = []

    # Seismic regions near monitored locations or hot spots
    hotspots = [
        (35.2, 139.3, "Near South Coast of Honshu, Japan"),
        (36.1, 140.0, "Kanto Region, Japan"),
        (0.2, 125.0, "Molucca Sea"),
        (-6.0, 105.4, "Sunda Strait, Indonesia"),
        (61.2, -150.0, "Southern Alaska"),
        (37.7, -122.4, "San Francisco Bay Area, California"),
        (-18.0, -178.0, "Fiji Region"),
        (34.0, 45.3, "Iran-Iraq Border Region"),
    ]

    for _ in range(100):
        event_id = f"us_seed_{uuid.uuid4().hex[:8]}"
        t = now - timedelta(days=random.uniform(0.1, 30.0))

        if random.random() < 0.20:
            lat, lon, place = random.choice(hotspots)
            magnitude = random.uniform(5.8, 7.8)  # Critical/High mag
        else:
            lat = random.uniform(-90.0, 90.0)
            lon = random.uniform(-180.0, 180.0)
            place = f"Global Seismic Region ({lat:.2f}, {lon:.2f})"
            magnitude = random.uniform(1.5, 5.7)

        depth = random.uniform(2.0, 650.0)
        tsunami = magnitude >= 7.0 and depth < 100.0
        sig = int(magnitude * 100 + depth * 0.1)

        if magnitude >= 7.0:
            risk_cat = "Critical"
        elif magnitude >= 5.0:
            risk_cat = "High"
        elif magnitude >= 3.0:
            risk_cat = "Medium"
        else:
            risk_cat = "Low"

        if magnitude >= 8.0:
            mag_band = "Great"
        elif magnitude >= 7.0:
            mag_band = "Major"
        elif magnitude >= 6.0:
            mag_band = "Strong"
        elif magnitude >= 5.0:
            mag_band = "Moderate"
        elif magnitude >= 4.0:
            mag_band = "Light"
        elif magnitude >= 2.0:
            mag_band = "Minor"
        else:
            mag_band = "Micro"

        if depth > 300.0:
            dist_grp = "Deep"
        elif depth >= 70.0:
            dist_grp = "Intermediate"
        else:
            dist_grp = "Shallow"

        earthquakes.append((
            event_id, round(magnitude, 1), place, t, lat, lon, round(depth, 1),
            tsunami, sig, "reviewed", risk_cat, mag_band, dist_grp
        ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO earthquakes_processed (
                    event_id, magnitude, place, event_time, latitude, longitude,
                    depth_km, tsunami, significance, status, risk_category,
                    magnitude_band, distance_group
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                earthquakes
            )


def seed_wildfires() -> None:
    """Generate and insert 150 historical wildfires over 60 days."""
    now = datetime.now(timezone.utc)
    wildfires: list[tuple[Any, ...]] = []

    hotspots = [
        (-8.0, -65.0, "S"),
        (-10.0, -55.0, "D"),
        (-33.0, 150.0, "D"),
        (-25.0, 133.0, "S"),
        (38.0, -120.0, "D"),
        (42.0, -122.0, "S"),
        (-3.0, 22.0, "S"),
    ]

    for _ in range(150):
        t = now - timedelta(days=random.uniform(0.1, 60.0))
        acq_date = t.date()
        acq_time = t.strftime("%H%M")

        if random.random() < 0.2:
            lat, lon, daynight = random.choice(hotspots)
            frp = random.uniform(150.0, 750.0)  # High FRP fires
        else:
            lat = random.uniform(-60.0, 70.0)
            lon = random.uniform(-180.0, 180.0)
            daynight = random.choice(["D", "N"])
            frp = random.uniform(5.0, 149.0)

        brightness = random.uniform(295.0, 365.0)
        scan = random.uniform(0.3, 2.0)
        track = random.uniform(0.3, 2.0)
        conf_label = random.choice(["low", "nominal", "high"])

        if frp >= 200.0:
            severity = "Extreme"
        elif frp >= 50.0:
            severity = "High"
        elif frp >= 10.0:
            severity = "Medium"
        else:
            severity = "Low"

        if conf_label == "high":
            det_conf = "High"
        elif conf_label == "nominal":
            det_conf = "Nominal"
        else:
            det_conf = "Low"

        wildfires.append((
            lat, lon, round(brightness, 1), round(scan, 1), round(track, 1),
            acq_date, acq_time, "VIIRS", "I", conf_label, round(frp, 1),
            daynight, severity, det_conf
        ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO wildfires_processed (
                    latitude, longitude, brightness, scan, track, acq_date,
                    acq_time, satellite, instrument, confidence, frp, daynight,
                    fire_severity, detection_confidence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                wildfires
            )


def seed_metrics_and_logs() -> None:
    """Generate and insert metrics and ingestion logs to verify UI reporting components."""
    now = datetime.now(timezone.utc)
    sources = [
        "usgs_earthquakes",
        "open_meteo_weather",
        "open_meteo_air_quality",
        "nasa_firms_wildfires",
    ]

    logs = []
    for src in sources:
        logs.append((
            src, "success", random.randint(10, 50), random.uniform(80.0, 450.0), None, now
        ))

    pipeline_metrics = []
    quality_metrics = []

    for src in ["earthquakes", "weather", "air_quality", "wildfires"]:
        for hour in range(48):
            t = now - timedelta(hours=hour)
            batch_id = int(t.timestamp())

            records = random.randint(5, 30)
            proc_time = random.uniform(80.0, 250.0)
            db_write = random.uniform(5.0, 35.0)
            rpm = records / (proc_time / 60000.0) if proc_time > 0 else 0

            pipeline_metrics.append((
                src, batch_id, records, round(proc_time, 2), None, round(db_write, 2), round(rpm, 2), t
            ))

            duplicates = random.randint(0, 2)
            missing = random.randint(0, 1)
            malformed = 0
            dropped = duplicates + missing

            quality_metrics.append((
                src, batch_id, records + dropped, records, duplicates, missing, malformed, dropped, missing, t
            ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ingestion_logs (source, status, records_count, latency_ms, error_message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                logs
            )
            cur.executemany(
                """
                INSERT INTO pipeline_metrics (source, batch_id, records_count, processing_time_ms, kafka_lag_ms, db_write_latency_ms, records_per_minute, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                pipeline_metrics
            )
            cur.executemany(
                """
                INSERT INTO data_quality_metrics (source, batch_id, total_records, valid_records, duplicates, missing_fields, malformed_records, dropped_events, validation_errors, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                quality_metrics
            )


def seed_database_if_empty() -> None:
    """Checks if the processed weather table has data; seeds all tables if empty."""
    logger.info("Checking database seeding status...")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM weather_processed")
                count = cur.fetchone()[0]
                if count > 0:
                    logger.info("Database already populated. Seeding skipped.")
                    return

        logger.info("Database is empty. Seeding historical data sequences...")
        seed_weather_and_aq()
        seed_earthquakes()
        seed_wildfires()
        seed_metrics_and_logs()
        logger.info("Database seeding successfully completed.")
    except Exception as exc:
        logger.error(f"Seeding process failed: {exc}", exc_info=True)
