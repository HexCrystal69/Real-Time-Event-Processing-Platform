"""
GRIP — WebSocket connection manager for real-time dashboard updates.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

from backend.config.logger import get_logger

logger = get_logger("websocket")


class ConnectionManager:
    """Manages active WebSocket connections and broadcasts updates."""

    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
        logger.info(
            "WebSocket client connected",
            extra={"context": {"total": len(self.active_connections)}},
        )

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.info(
            "WebSocket client disconnected",
            extra={"context": {"total": len(self.active_connections)}},
        )

    async def broadcast(self, message: dict[str, Any]) -> None:
        if not self.active_connections:
            return

        payload = json.dumps(message, default=str)
        dead: list[WebSocket] = []

        async with self._lock:
            connections = list(self.active_connections)

        for connection in connections:
            try:
                await connection.send_text(payload)
            except Exception:
                dead.append(connection)

        if dead:
            async with self._lock:
                for conn in dead:
                    if conn in self.active_connections:
                        self.active_connections.remove(conn)

    async def send_personal(self, websocket: WebSocket, message: dict[str, Any]) -> None:
        await websocket.send_text(json.dumps(message, default=str))


manager = ConnectionManager()
