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


def start_background_tasks() -> list[asyncio.Task]:
    """Start all background intelligence tasks."""
    global _tasks
    _tasks = [
        asyncio.create_task(_risk_score_loop()),
        asyncio.create_task(_alert_loop()),
        asyncio.create_task(_forecast_loop()),
        asyncio.create_task(_analytics_loop()),
        asyncio.create_task(_websocket_broadcast_loop()),
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
