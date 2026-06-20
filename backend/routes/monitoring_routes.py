"""
GRIP — System monitoring API endpoints (Phase 3).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from backend.middleware.rate_limit import limiter
from backend.services.monitoring import get_system_monitoring

router = APIRouter(prefix="/api/monitoring", tags=["Monitoring"])


@router.get("")
@limiter.limit("60/minute")
async def system_monitoring(request: Request) -> dict[str, Any]:
    return get_system_monitoring()
