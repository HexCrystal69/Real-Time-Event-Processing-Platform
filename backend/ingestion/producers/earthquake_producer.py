"""
GRIP — USGS Earthquake producer.

Polls the USGS GeoJSON summary feed every 60 seconds and pushes
de-duplicated earthquake events into the 'earthquakes' Kafka topic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from backend.config.settings import settings
from backend.ingestion.producers.base_producer import BaseProducer

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


class EarthquakeProducer(BaseProducer):
    """Ingests real-time earthquake data from the USGS GeoJSON feed."""

    def __init__(self) -> None:
        super().__init__(
            topic=settings.kafka_topic_earthquakes,
            source_name="usgs_earthquakes",
            poll_interval=settings.usgs_poll_interval,
        )
        # Bounded set of recently seen event IDs for deduplication.
        # Keeps the last 5 000 IDs (the hourly feed rarely exceeds ~500).
        self._seen_ids: set[str] = set()
        self._max_seen = 5000

    async def fetch_data(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        response = await client.get(USGS_FEED_URL)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        new_features: list[dict[str, Any]] = []

        for feature in features:
            event_id = feature.get("id", "")
            if event_id and event_id not in self._seen_ids:
                new_features.append(feature)
                self._seen_ids.add(event_id)

        # Prune if the set grows too large
        if len(self._seen_ids) > self._max_seen:
            to_remove = len(self._seen_ids) - self._max_seen
            for _ in range(to_remove):
                self._seen_ids.pop()

        return new_features

    def validate_record(self, record: dict[str, Any]) -> bool:
        props = record.get("properties", {})
        geometry = record.get("geometry", {})
        coords = geometry.get("coordinates", [])

        if not record.get("id"):
            return False
        if not props.get("time"):
            return False
        if len(coords) < 2:
            return False

        return True

    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        props = record["properties"]
        coords = record["geometry"]["coordinates"]

        return {
            "event_id": record["id"],
            "magnitude": props.get("mag"),
            "place": props.get("place", ""),
            "event_time": datetime.fromtimestamp(
                props["time"] / 1000, tz=timezone.utc
            ).isoformat(),
            "latitude": coords[1],
            "longitude": coords[0],
            "depth_km": coords[2] if len(coords) > 2 else None,
            "tsunami": bool(props.get("tsunami", 0)),
            "significance": props.get("sig"),
            "status": props.get("status", ""),
        }
