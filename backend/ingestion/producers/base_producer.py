"""
GRIP — Abstract base class for all Kafka producers.

Every data-source producer inherits from BaseProducer and implements:
  - fetch_data()       → pull raw data from the external API
  - validate_record()  → check a single record is well-formed
  - transform_record() → normalise into the schema expected downstream

The base class handles the polling loop, Kafka delivery, retry logic,
ingestion logging, and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import json
import signal
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.connection import log_ingestion


class BaseProducer(ABC):
    """
    Long-running async producer that polls an external API and pushes
    validated, transformed records into a Kafka topic.
    """

    def __init__(self, topic: str, source_name: str, poll_interval: int) -> None:
        self.topic = topic
        self.source_name = source_name
        self.poll_interval = poll_interval
        self.logger = get_logger(source_name)
        self._running = True
        self._kafka_producer: KafkaProducer | None = None

    # ------------------------------------------------------------------
    # Abstract interface — subclasses must implement
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_data(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        """Fetch raw records from the external API."""

    @abstractmethod
    def validate_record(self, record: dict[str, Any]) -> bool:
        """Return True if the record has all required fields."""

    @abstractmethod
    def transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalise a raw record into the downstream schema."""

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _ensure_kafka(self) -> KafkaProducer:
        """Lazily create the KafkaProducer, retrying on broker unavailability."""
        if self._kafka_producer is not None:
            return self._kafka_producer

        max_attempts = 15
        for attempt in range(1, max_attempts + 1):
            try:
                self._kafka_producer = KafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                self.logger.info(
                    "Kafka producer connected",
                    extra={"context": {"broker": settings.kafka_bootstrap_servers}},
                )
                return self._kafka_producer
            except KafkaError as exc:
                self.logger.warning(
                    "Kafka broker not ready, retrying",
                    extra={"context": {"attempt": attempt, "error": str(exc)}},
                )
                time.sleep(3)

        raise ConnectionError("Could not connect to Kafka after multiple attempts")

    def _send_to_kafka(self, records: list[dict[str, Any]]) -> int:
        """
        Send a batch of records to the Kafka topic.
        Returns the count of successfully sent records.
        """
        producer = self._ensure_kafka()
        sent = 0
        for record in records:
            try:
                producer.send(self.topic, value=record, key=record.get("event_id"))
                sent += 1
            except KafkaError as exc:
                self.logger.error(
                    "Failed to send record to Kafka",
                    extra={"context": {"topic": self.topic, "error": str(exc)}},
                )
        producer.flush()
        return sent

    # ------------------------------------------------------------------
    # Ingestion cycle (with retry)
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def _fetch_with_retry(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        return await self.fetch_data(client)

    async def _run_cycle(self, client: httpx.AsyncClient) -> None:
        """Execute one full poll→validate→transform→produce cycle."""
        start = time.monotonic()
        error_msg: str | None = None
        record_count = 0

        try:
            raw_records = await self._fetch_with_retry(client)

            valid_records = [r for r in raw_records if self.validate_record(r)]
            if len(valid_records) < len(raw_records):
                self.logger.warning(
                    "Some records failed validation",
                    extra={
                        "context": {
                            "total": len(raw_records),
                            "valid": len(valid_records),
                            "dropped": len(raw_records) - len(valid_records),
                        }
                    },
                )

            transformed = [self.transform_record(r) for r in valid_records]
            record_count = self._send_to_kafka(transformed)

            latency_ms = (time.monotonic() - start) * 1000
            self.logger.info(
                "Ingestion cycle complete",
                extra={
                    "context": {
                        "source": self.source_name,
                        "records": record_count,
                        "latency_ms": round(latency_ms, 2),
                    }
                },
            )
            log_ingestion(self.source_name, "success", record_count, latency_ms)

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            error_msg = str(exc)
            self.logger.error(
                "Ingestion cycle failed",
                extra={"context": {"source": self.source_name, "error": error_msg}},
                exc_info=True,
            )
            log_ingestion(self.source_name, "error", record_count, latency_ms, error_msg)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Start the infinite polling loop.
        Handles SIGTERM / SIGINT for graceful shutdown inside Docker.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._shutdown)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: self._shutdown())

        self.logger.info(
            "Producer starting",
            extra={
                "context": {
                    "source": self.source_name,
                    "topic": self.topic,
                    "interval_s": self.poll_interval,
                }
            },
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            while self._running:
                await self._run_cycle(client)
                await asyncio.sleep(self.poll_interval)

        self.logger.info("Producer stopped gracefully")
        if self._kafka_producer:
            self._kafka_producer.close(timeout=5)

    def _shutdown(self) -> None:
        self.logger.info("Shutdown signal received")
        self._running = False
