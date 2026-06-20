"""
GRIP — Producer orchestrator.

Starts all four ingestion producers concurrently using asyncio.
This is the main entrypoint for the ingestion layer and is invoked
by the Docker backend service.
"""

from __future__ import annotations

import asyncio
import sys
import time

from backend.config.logger import get_logger
from backend.database.connection import init_database
from backend.ingestion.producers.air_quality_producer import AirQualityProducer
from backend.ingestion.producers.earthquake_producer import EarthquakeProducer
from backend.ingestion.producers.weather_producer import WeatherProducer
from backend.ingestion.producers.wildfire_producer import WildfireProducer

logger = get_logger("producer_orchestrator")


async def run_all_producers() -> None:
    """Initialize the database and launch all producers concurrently."""
    logger.info("Initialising database schema before starting producers")

    # Wait a moment for PostgreSQL to be fully available
    max_db_retries = 20
    for attempt in range(1, max_db_retries + 1):
        try:
            init_database()
            break
        except Exception as exc:
            logger.warning(
                "Database not ready, retrying",
                extra={"context": {"attempt": attempt, "error": str(exc)}},
            )
            time.sleep(3)
    else:
        logger.error("Could not initialise database — aborting")
        sys.exit(1)

    producers = [
        EarthquakeProducer(),
        WeatherProducer(),
        AirQualityProducer(),
        WildfireProducer(),
    ]

    logger.info(
        "Starting all producers",
        extra={"context": {"count": len(producers)}},
    )

    await asyncio.gather(*(p.run() for p in producers))


def main() -> None:
    asyncio.run(run_all_producers())


if __name__ == "__main__":
    main()
