"""
GRIP — Open-Meteo Weather producer.

Polls the Open-Meteo forecast API for each monitored location every
300 seconds and pushes current weather observations into the 'weather'
Kafka topic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from backend.config.settings import settings
from backend.ingestion.producers.base_producer import BaseProducer

OPEN_METEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"


class WeatherProducer(BaseProducer):
    """Ingests current weather data from Open-Meteo for monitored locations."""

    def __init__(self) -> None:
        super().__init__(
            topic=settings.kafka_topic_weather,
            source_name="open_meteo_weather",
            poll_interval=settings.weather_poll_interval,
        )

    async def fetch_data(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []

        for loc in settings.monitored_locations:
            params = {
                "latitude": loc.latitude,
                "longitude": loc.longitude,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code",
                "timezone": "auto",
            }
            response = await client.get(OPEN_METEO_WEATHER_URL, params=params)
            response.raise_for_status()
            data = response.json()

            records.append(
                {
                    "location_name": loc.name,
                    "latitude": data.get("latitude", loc.latitude),
                    "longitude": data.get("longitude", loc.longitude),
                    "current": data.get("current", {}),
                    "current_units": data.get("current_units", {}),
                }
            )

        return records

    def validate_record(self, record: dict[str, Any]) -> bool:
        current = record.get("current", {})
        if not current:
            return False
        if "temperature_2m" not in current:
            return False
        if not record.get("location_name"):
            return False
        return True

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        current = record["current"]
        return {
            "location_name": record["location_name"],
            "latitude": record["latitude"],
            "longitude": record["longitude"],
            "temperature_c": current.get("temperature_2m"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "precipitation_mm": current.get("precipitation"),
            "weather_code": current.get("weather_code"),
            "observed_at": current.get(
                "time", datetime.now(timezone.utc).isoformat()
            ),
        }
