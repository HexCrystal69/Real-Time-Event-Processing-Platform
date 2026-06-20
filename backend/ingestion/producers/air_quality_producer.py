"""
GRIP — Open-Meteo Air Quality producer.

Polls the Open-Meteo Air Quality API for each monitored location every
300 seconds and pushes current AQI readings into the 'air_quality'
Kafka topic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from backend.config.settings import settings
from backend.ingestion.producers.base_producer import BaseProducer

OPEN_METEO_AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


class AirQualityProducer(BaseProducer):
    """Ingests current air quality data from Open-Meteo for monitored locations."""

    def __init__(self) -> None:
        super().__init__(
            topic=settings.kafka_topic_air_quality,
            source_name="open_meteo_air_quality",
            poll_interval=settings.air_quality_poll_interval,
        )

    async def fetch_data(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []

        for loc in settings.monitored_locations:
            params = {
                "latitude": loc.latitude,
                "longitude": loc.longitude,
                "current": "us_aqi,pm2_5,pm10,ozone,nitrogen_dioxide,carbon_monoxide,sulphur_dioxide",
                "timezone": "auto",
            }
            response = await client.get(OPEN_METEO_AQ_URL, params=params)
            response.raise_for_status()
            data = response.json()

            records.append(
                {
                    "location_name": loc.name,
                    "latitude": data.get("latitude", loc.latitude),
                    "longitude": data.get("longitude", loc.longitude),
                    "current": data.get("current", {}),
                }
            )

        return records

    def validate_record(self, record: dict[str, Any]) -> bool:
        current = record.get("current", {})
        if not current:
            return False
        if not record.get("location_name"):
            return False
        # At minimum we need AQI or PM2.5
        if current.get("us_aqi") is None and current.get("pm2_5") is None:
            return False
        return True

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        current = record["current"]
        return {
            "location_name": record["location_name"],
            "latitude": record["latitude"],
            "longitude": record["longitude"],
            "us_aqi": current.get("us_aqi"),
            "pm2_5": current.get("pm2_5"),
            "pm10": current.get("pm10"),
            "ozone": current.get("ozone"),
            "nitrogen_dioxide": current.get("nitrogen_dioxide"),
            "carbon_monoxide": current.get("carbon_monoxide"),
            "sulphur_dioxide": current.get("sulphur_dioxide"),
            "observed_at": current.get(
                "time", datetime.now(timezone.utc).isoformat()
            ),
        }
