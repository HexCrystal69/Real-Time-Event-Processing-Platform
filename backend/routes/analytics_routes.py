"""
GRIP — Analytics API endpoints (Phase 3).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from backend.middleware.rate_limit import limiter
from backend.services.analytics import (
    get_air_quality_analytics,
    get_dashboard_summary,
    get_earthquake_analytics,
    get_events_per_hour,
    get_map_markers,
    get_regional_rankings,
    get_risk_distribution_analytics,
    get_source_activity,
    get_weather_analytics,
    get_wildfire_analytics,
)
from backend.services.risk_scoring import get_latest_risk_scores

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])


@router.get("/summary")
@limiter.limit("60/minute")
async def analytics_summary(request: Request) -> dict[str, Any]:
    return get_dashboard_summary()


@router.get("/events-per-hour")
@limiter.limit("60/minute")
async def events_per_hour(
    request: Request,
    hours: int = Query(default=24, ge=1, le=168),
) -> dict[str, Any]:
    return get_events_per_hour(hours)


@router.get("/risk-distribution")
@limiter.limit("60/minute")
async def risk_distribution(request: Request) -> dict[str, Any]:
    return get_risk_distribution_analytics()


@router.get("/source-activity")
@limiter.limit("60/minute")
async def source_activity(request: Request) -> list[dict[str, Any]]:
    return get_source_activity()


@router.get("/regional-rankings")
@limiter.limit("60/minute")
async def regional_rankings(request: Request) -> list[dict[str, Any]]:
    return get_regional_rankings()


@router.get("/risk-scores")
@limiter.limit("60/minute")
async def risk_scores(request: Request) -> list[dict[str, Any]]:
    return get_latest_risk_scores()


@router.get("/map-markers")
@limiter.limit("30/minute")
async def map_markers(
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000),
) -> dict[str, list[dict[str, Any]]]:
    return get_map_markers(limit)


@router.get("/earthquakes")
@limiter.limit("60/minute")
async def earthquake_analytics(request: Request) -> dict[str, Any]:
    return get_earthquake_analytics()


@router.get("/wildfires")
@limiter.limit("60/minute")
async def wildfire_analytics(request: Request) -> dict[str, Any]:
    return get_wildfire_analytics()


@router.get("/weather")
@limiter.limit("60/minute")
async def weather_analytics(request: Request) -> dict[str, Any]:
    return get_weather_analytics()


@router.get("/air-quality")
@limiter.limit("60/minute")
async def air_quality_analytics(request: Request) -> dict[str, Any]:
    return get_air_quality_analytics()
