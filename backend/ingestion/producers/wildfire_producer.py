"""
GRIP — NASA FIRMS Wildfire producer.

Polls the NASA FIRMS area/CSV endpoint every 600 seconds for global
active fire detections and pushes records into the 'wildfires' Kafka
topic.

Requires a free MAP_KEY from:
  https://firms.modaps.eosdis.nasa.gov/api/map_key

If NASA_FIRMS_MAP_KEY is not set, the producer logs a warning each
cycle and produces zero records (it does not crash).
"""

from __future__ import annotations

import csv
import io
from typing import Any

import httpx

from backend.config.settings import settings
from backend.ingestion.producers.base_producer import BaseProducer

FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
FIRMS_SOURCE = "VIIRS_SNPP_NRT"
FIRMS_AREA = "world"
FIRMS_DAY_RANGE = "1"


class WildfireProducer(BaseProducer):
    """Ingests active fire detections from NASA FIRMS VIIRS data."""

    def __init__(self) -> None:
        super().__init__(
            topic=settings.kafka_topic_wildfires,
            source_name="nasa_firms_wildfires",
            poll_interval=settings.wildfire_poll_interval,
        )

    async def fetch_data(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        map_key = settings.nasa_firms_map_key
        if not map_key:
            self.logger.warning(
                "NASA_FIRMS_MAP_KEY not set — skipping wildfire ingestion. "
                "Register free at https://firms.modaps.eosdis.nasa.gov/api/map_key"
            )
            return []

        url = f"{FIRMS_BASE_URL}/{map_key}/{FIRMS_SOURCE}/{FIRMS_AREA}/{FIRMS_DAY_RANGE}"

        response = await client.get(url, timeout=60.0)
        response.raise_for_status()

        reader = csv.DictReader(io.StringIO(response.text))
        records: list[dict[str, Any]] = list(reader)

        # FIRMS can return 50k+ records for world/1-day.
        # Limit to most recent 2 000 by brightness (proxy for significance).
        records.sort(key=lambda r: float(r.get("bright_ti4", 0) or 0), reverse=True)
        return records[:2000]

    def validate_record(self, record: dict[str, Any]) -> bool:
        try:
            lat = float(record.get("latitude", ""))
            lon = float(record.get("longitude", ""))
        except (ValueError, TypeError):
            return False

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return False

        if not record.get("acq_date"):
            return False

        return True

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        def safe_float(val: Any) -> float | None:
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        return {
            "latitude": float(record["latitude"]),
            "longitude": float(record["longitude"]),
            "brightness": safe_float(record.get("bright_ti4")),
            "scan": safe_float(record.get("scan")),
            "track": safe_float(record.get("track")),
            "acq_date": record.get("acq_date", ""),
            "acq_time": record.get("acq_time", ""),
            "satellite": record.get("satellite", ""),
            "instrument": record.get("instrument", ""),
            "confidence": record.get("confidence", ""),
            "frp": safe_float(record.get("frp")),
            "daynight": record.get("daynight", ""),
        }
