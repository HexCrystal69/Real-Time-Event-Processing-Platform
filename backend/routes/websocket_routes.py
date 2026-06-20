"""
GRIP — WebSocket endpoint for real-time dashboard updates.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from backend.services.alert_engine import get_active_alerts
from backend.services.analytics import get_dashboard_summary, get_map_markers
from backend.services.risk_scoring import get_latest_risk_scores
from backend.services.websocket_manager import manager

router = APIRouter(tags=["WebSocket"])


@router.websocket("/ws/live")
async def websocket_live(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        await manager.send_personal(websocket, {
            "type": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": get_dashboard_summary(),
            "risk_scores": get_latest_risk_scores(),
            "alerts": get_active_alerts(limit=10),
            "map_markers": get_map_markers(limit=200),
        })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception:
        await manager.disconnect(websocket)
