"""
GRIP — Data export API endpoints (Phase 3).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from backend.middleware.rate_limit import limiter
from backend.services.export import EXPORT_TABLES, export_csv, export_json, export_pdf_report

router = APIRouter(prefix="/api/export", tags=["Export"])


@router.get("/csv/{data_type}")
@limiter.limit("10/minute")
async def export_data_csv(
    request: Request,
    data_type: str,
    limit: int = Query(default=1000, ge=1, le=5000),
) -> Response:
    if data_type not in EXPORT_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown data type: {data_type}")
    content = export_csv(data_type, limit)
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=grip_{data_type}.csv"},
    )


@router.get("/json/{data_type}")
@limiter.limit("10/minute")
async def export_data_json(
    request: Request,
    data_type: str,
    limit: int = Query(default=1000, ge=1, le=5000),
) -> Response:
    if data_type not in EXPORT_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown data type: {data_type}")
    content = export_json(data_type, limit)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=grip_{data_type}.json"},
    )


@router.get("/pdf/report")
@limiter.limit("5/minute")
async def export_pdf(request: Request) -> Response:
    content = export_pdf_report()
    return Response(
        content=content,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=grip_intelligence_report.pdf"},
    )
