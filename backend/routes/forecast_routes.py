"""
GRIP — Forecasting API endpoints (Phase 3).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from backend.middleware.rate_limit import limiter
from backend.services.forecasting import generate_all_forecasts, get_forecasts

router = APIRouter(prefix="/api/forecasts", tags=["Forecasts"])

VALID_HORIZONS = ("24h", "7d", "30d")


@router.get("")
@limiter.limit("30/minute")
async def list_forecasts(
    request: Request,
    metric: str | None = Query(default=None),
    source: str | None = Query(default=None),
    location: str | None = Query(default=None),
    horizon: str = Query(default="24h"),
) -> dict[str, Any]:
    if horizon not in VALID_HORIZONS:
        raise HTTPException(status_code=422, detail=f"horizon must be one of {VALID_HORIZONS}")
    data = get_forecasts(metric, source, location, horizon)
    return {"horizon": horizon, "count": len(data), "forecasts": data}


@router.post("/generate")
@limiter.limit("5/minute")
async def trigger_forecast_generation(request: Request) -> dict[str, Any]:
    return generate_all_forecasts()
