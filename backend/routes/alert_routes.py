"""
GRIP — Alert API endpoints (Phase 3).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from backend.middleware.rate_limit import limiter
from backend.services.alert_engine import get_active_alerts, get_alert_history

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])


@router.get("/active")
@limiter.limit("60/minute")
async def active_alerts(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    alerts = get_active_alerts(limit)
    return {"count": len(alerts), "alerts": alerts}


@router.get("/history")
@limiter.limit("60/minute")
async def alert_history(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    alert_type: str | None = Query(default=None),
    severity: str | None = Query(default=None),
) -> dict[str, Any]:
    return get_alert_history(limit, offset, alert_type, severity)
