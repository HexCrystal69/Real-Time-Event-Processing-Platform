"""
GRIP — Data and metrics API endpoints (Phase 2).

Provides paginated access to processed data, anomalies, and pipeline metrics.
All endpoints query PostgreSQL directly using the existing connection pool.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.config.logger import get_logger
from backend.database.connection import get_connection

logger = get_logger("data_routes")

router = APIRouter(tags=["Data"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _paginated_query(
    table: str,
    order_col: str,
    filters: list[tuple[str, str, Any]],
    limit: int,
    offset: int,
    columns: str = "*",
) -> dict[str, Any]:
    """
    Execute a paginated SELECT with optional WHERE filters.

    Parameters
    ----------
    table : str
        Table name to query.
    order_col : str
        Column to ORDER BY DESC.
    filters : list of (column, operator, value)
        WHERE clauses applied only when value is not None.
    limit : int
        Max rows to return.
    offset : int
        Number of rows to skip.
    columns : str
        Comma-separated column list (default "*").
    """
    where_clauses: list[str] = []
    params: list[Any] = []

    for col, op, val in filters:
        if val is not None:
            where_clauses.append(f"{col} {op} %s")
            params.append(val)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Count total matching rows
                count_sql = f"SELECT COUNT(*) FROM {table} {where_sql}"
                cur.execute(count_sql, params)
                total_count = cur.fetchone()[0]

                # Fetch page
                data_sql = (
                    f"SELECT {columns} FROM {table} {where_sql} "
                    f"ORDER BY {order_col} DESC LIMIT %s OFFSET %s"
                )
                cur.execute(data_sql, params + [limit, offset])

                column_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

        data = []
        for row in rows:
            record = {}
            for idx, col_name in enumerate(column_names):
                value = row[idx]
                if isinstance(value, datetime):
                    value = value.isoformat()
                record[col_name] = value
            data.append(record)

        return {
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "data": data,
        }
    except Exception as exc:
        logger.error(
            f"Database query failed for {table}",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=f"Database query failed: {exc}")


# ---------------------------------------------------------------------------
# Earthquake endpoints
# ---------------------------------------------------------------------------
@router.get("/earthquakes")
async def get_earthquakes(
    min_magnitude: float | None = Query(default=None, description="Minimum magnitude filter"),
    risk_category: str | None = Query(default=None, description="Filter by risk category (Low/Medium/High/Critical)"),
    limit: int = Query(default=50, ge=1, le=500, description="Number of records to return"),
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
) -> dict[str, Any]:
    """Return processed earthquake data with enrichment fields."""
    filters = [
        ("magnitude", ">=", min_magnitude),
        ("risk_category", "=", risk_category),
    ]
    return _paginated_query(
        "earthquakes_processed", "event_time", filters, limit, offset,
    )


# ---------------------------------------------------------------------------
# Weather endpoints
# ---------------------------------------------------------------------------
@router.get("/weather")
async def get_weather(
    location: str | None = Query(default=None, description="Filter by location name"),
    min_wind: float | None = Query(default=None, description="Minimum wind speed (km/h)"),
    storm_severity: str | None = Query(default=None, description="Filter by storm severity"),
    limit: int = Query(default=50, ge=1, le=500, description="Number of records to return"),
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
) -> dict[str, Any]:
    """Return processed weather data with enrichment fields."""
    filters = [
        ("location_name", "=", location),
        ("wind_speed_kmh", ">=", min_wind),
        ("storm_severity", "=", storm_severity),
    ]
    return _paginated_query(
        "weather_processed", "observed_at", filters, limit, offset,
    )


# ---------------------------------------------------------------------------
# Air quality endpoints
# ---------------------------------------------------------------------------
@router.get("/air-quality")
async def get_air_quality(
    location: str | None = Query(default=None, description="Filter by location name"),
    min_aqi: int | None = Query(default=None, description="Minimum AQI value"),
    aqi_category: str | None = Query(default=None, description="Filter by AQI category"),
    limit: int = Query(default=50, ge=1, le=500, description="Number of records to return"),
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
) -> dict[str, Any]:
    """Return processed air quality data with enrichment fields."""
    filters = [
        ("location_name", "=", location),
        ("us_aqi", ">=", min_aqi),
        ("aqi_category", "=", aqi_category),
    ]
    return _paginated_query(
        "air_quality_processed", "observed_at", filters, limit, offset,
    )


# ---------------------------------------------------------------------------
# Wildfire endpoints
# ---------------------------------------------------------------------------
@router.get("/wildfires")
async def get_wildfires(
    min_severity: str | None = Query(default=None, description="Minimum fire severity (Low/Medium/High/Extreme)"),
    min_frp: float | None = Query(default=None, description="Minimum fire radiative power"),
    limit: int = Query(default=50, ge=1, le=500, description="Number of records to return"),
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
) -> dict[str, Any]:
    """Return processed wildfire data with enrichment fields."""
    filters = [
        ("fire_severity", "=", min_severity),
        ("frp", ">=", min_frp),
    ]
    return _paginated_query(
        "wildfires_processed", "acq_date", filters, limit, offset,
    )


# ---------------------------------------------------------------------------
# Anomaly endpoints
# ---------------------------------------------------------------------------
@router.get("/anomalies")
async def get_anomalies(
    source: str | None = Query(default=None, description="Filter by source (earthquakes/weather/air_quality/wildfires)"),
    severity: str | None = Query(default=None, description="Filter by severity (High/Critical)"),
    event_type: str | None = Query(default=None, description="Filter by event type"),
    limit: int = Query(default=50, ge=1, le=500, description="Number of records to return"),
    offset: int = Query(default=0, ge=0, description="Number of records to skip"),
) -> dict[str, Any]:
    """Return detected anomaly events across all data sources."""
    filters = [
        ("source", "=", source),
        ("severity", "=", severity),
        ("event_type", "=", event_type),
    ]
    return _paginated_query(
        "anomaly_events", "detected_at", filters, limit, offset,
    )


# ---------------------------------------------------------------------------
# Metrics endpoints
# ---------------------------------------------------------------------------
@router.get("/metrics")
async def get_metrics(
    source: str | None = Query(default=None, description="Filter by data source"),
) -> dict[str, Any]:
    """
    Return pipeline and data quality metrics.
    Provides aggregated statistics and recent per-batch details.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # --- Pipeline metrics summary ---
                pipeline_where = ""
                pipeline_params: list[Any] = []
                if source:
                    pipeline_where = "WHERE source = %s"
                    pipeline_params = [source]

                cur.execute(
                    f"""
                    SELECT
                        source,
                        COUNT(*) as total_batches,
                        SUM(records_count) as total_records,
                        ROUND(AVG(processing_time_ms)::numeric, 2) as avg_processing_time_ms,
                        ROUND(AVG(records_per_minute)::numeric, 2) as avg_records_per_minute,
                        ROUND(AVG(db_write_latency_ms)::numeric, 2) as avg_db_write_latency_ms,
                        MAX(created_at) as last_batch_at
                    FROM pipeline_metrics
                    {pipeline_where}
                    GROUP BY source
                    ORDER BY source
                    """,
                    pipeline_params,
                )
                pipeline_cols = [desc[0] for desc in cur.description]
                pipeline_rows = cur.fetchall()

                pipeline_summary = []
                for row in pipeline_rows:
                    record = {}
                    for idx, col_name in enumerate(pipeline_cols):
                        value = row[idx]
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        record[col_name] = value
                    pipeline_summary.append(record)

                # --- Data quality summary ---
                dq_where = ""
                dq_params: list[Any] = []
                if source:
                    dq_where = "WHERE source = %s"
                    dq_params = [source]

                cur.execute(
                    f"""
                    SELECT
                        source,
                        COUNT(*) as total_batches,
                        SUM(total_records) as total_ingested,
                        SUM(valid_records) as total_valid,
                        SUM(duplicates) as total_duplicates,
                        SUM(missing_fields) as total_missing_fields,
                        SUM(malformed_records) as total_malformed,
                        SUM(dropped_events) as total_dropped,
                        SUM(validation_errors) as total_validation_errors
                    FROM data_quality_metrics
                    {dq_where}
                    GROUP BY source
                    ORDER BY source
                    """,
                    dq_params,
                )
                dq_cols = [desc[0] for desc in cur.description]
                dq_rows = cur.fetchall()

                quality_summary = []
                for row in dq_rows:
                    record = {}
                    for idx, col_name in enumerate(dq_cols):
                        value = row[idx]
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        record[col_name] = value
                    quality_summary.append(record)

                # --- Recent batches (last 10 per source) ---
                recent_where = ""
                recent_params: list[Any] = []
                if source:
                    recent_where = "WHERE source = %s"
                    recent_params = [source]

                cur.execute(
                    f"""
                    SELECT source, batch_id, records_count, processing_time_ms,
                           records_per_minute, db_write_latency_ms, created_at
                    FROM pipeline_metrics
                    {recent_where}
                    ORDER BY created_at DESC
                    LIMIT 20
                    """,
                    recent_params,
                )
                recent_cols = [desc[0] for desc in cur.description]
                recent_rows = cur.fetchall()

                recent_batches = []
                for row in recent_rows:
                    record = {}
                    for idx, col_name in enumerate(recent_cols):
                        value = row[idx]
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        record[col_name] = value
                    recent_batches.append(record)

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_summary": pipeline_summary,
            "data_quality_summary": quality_summary,
            "recent_batches": recent_batches,
        }

    except Exception as exc:
        logger.error(
            "Failed to fetch metrics",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=f"Failed to fetch metrics: {exc}")
