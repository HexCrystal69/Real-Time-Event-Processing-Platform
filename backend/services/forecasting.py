"""
GRIP — Prophet-based operational forecasting service.

Generates forecasts from historical processed pipeline data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from prophet import Prophet

from backend.config.logger import get_logger
from backend.config.settings import settings
from backend.database.connection import get_connection

logger = get_logger("forecasting")

HORIZON_PERIODS = {
    "24h": 24,
    "7d": 168,
    "30d": 720,
}

HORIZON_FREQ = {
    "24h": "H",
    "7d": "H",
    "30d": "D",
}


def _run_prophet(
    df: pd.DataFrame,
    horizon: str,
) -> pd.DataFrame | None:
    if len(df) < settings.forecast_min_data_points:
        return None

    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=len(df) > 48,
        yearly_seasonality=False,
        changepoint_prior_scale=0.05,
    )
    model.fit(df)

    periods = HORIZON_PERIODS[horizon]
    freq = HORIZON_FREQ[horizon]
    future = model.make_future_dataframe(periods=periods, freq=freq, include_history=False)
    forecast = model.predict(future)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]


def _fetch_time_series(
    query: str,
    params: tuple[Any, ...] = (),
) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=["ds", "y"])

    df = pd.DataFrame(rows, columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"], utc=True).dt.tz_localize(None)
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna().sort_values("ds")
    return df


def _store_forecasts(
    metric: str,
    source: str,
    location_name: str | None,
    horizon: str,
    forecast_df: pd.DataFrame,
) -> int:
    count = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM forecasts
                WHERE metric = %s AND source = %s
                  AND COALESCE(location_name, '') = COALESCE(%s, '')
                  AND horizon = %s
                """,
                (metric, source, location_name, horizon),
            )

            for _, row in forecast_df.iterrows():
                cur.execute(
                    """
                    INSERT INTO forecasts (
                        metric, source, location_name, horizon,
                        forecast_time, predicted_value, lower_bound, upper_bound
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        metric,
                        source,
                        location_name,
                        horizon,
                        row["ds"].to_pydatetime(),
                        round(float(row["yhat"]), 4),
                        round(float(row["yhat_lower"]), 4),
                        round(float(row["yhat_upper"]), 4),
                    ),
                )
                count += 1
    return count


def generate_aqi_forecasts() -> dict[str, int]:
    """Forecast AQI for each monitored location."""
    results: dict[str, int] = {}
    query = """
        SELECT date_trunc('hour', observed_at) AS ts, AVG(us_aqi) AS val
        FROM air_quality_processed
        WHERE location_name = %s
          AND observed_at >= NOW() - INTERVAL '30 days'
        GROUP BY date_trunc('hour', observed_at)
        ORDER BY ts
    """

    for location in settings.monitored_locations:
        df = _fetch_time_series(query, (location.name,))
        total = 0
        for horizon in HORIZON_PERIODS:
            forecast = _run_prophet(df, horizon)
            if forecast is not None:
                total += _store_forecasts(
                    "us_aqi", "air_quality", location.name, horizon, forecast,
                )
        results[location.name] = total

    return results


def generate_weather_forecasts() -> dict[str, int]:
    """Forecast temperature trends for monitored locations."""
    results: dict[str, int] = {}
    query = """
        SELECT date_trunc('hour', observed_at) AS ts, AVG(temperature_c) AS val
        FROM weather_processed
        WHERE location_name = %s
          AND observed_at >= NOW() - INTERVAL '30 days'
        GROUP BY date_trunc('hour', observed_at)
        ORDER BY ts
    """

    for location in settings.monitored_locations:
        df = _fetch_time_series(query, (location.name,))
        total = 0
        for horizon in HORIZON_PERIODS:
            forecast = _run_prophet(df, horizon)
            if forecast is not None:
                total += _store_forecasts(
                    "temperature_c", "weather", location.name, horizon, forecast,
                )
        results[location.name] = total

    return results


def generate_wildfire_forecasts() -> dict[str, int]:
    """Forecast daily wildfire detection counts."""
    query = """
        SELECT acq_date AS ts, COUNT(*) AS val
        FROM wildfires_processed
        WHERE acq_date >= CURRENT_DATE - INTERVAL '60 days'
        GROUP BY acq_date
        ORDER BY ts
    """
    df = _fetch_time_series(query)
    results: dict[str, int] = {}
    total = 0
    for horizon in HORIZON_PERIODS:
        forecast = _run_prophet(df, horizon)
        if forecast is not None:
            total += _store_forecasts(
                "fire_count", "wildfires", None, horizon, forecast,
            )
    results["global"] = total
    return results


def generate_earthquake_forecasts() -> dict[str, int]:
    """Forecast hourly earthquake event frequency."""
    query = """
        SELECT date_trunc('hour', event_time) AS ts, COUNT(*) AS val
        FROM earthquakes_processed
        WHERE event_time >= NOW() - INTERVAL '30 days'
        GROUP BY date_trunc('hour', event_time)
        ORDER BY ts
    """
    df = _fetch_time_series(query)
    results: dict[str, int] = {}
    total = 0
    for horizon in HORIZON_PERIODS:
        forecast = _run_prophet(df, horizon)
        if forecast is not None:
            total += _store_forecasts(
                "event_count", "earthquakes", None, horizon, forecast,
            )
    results["global"] = total
    return results


def generate_all_forecasts() -> dict[str, Any]:
    """Run all forecasting models and return summary."""
    summary: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "aqi": {},
        "weather": {},
        "wildfires": {},
        "earthquakes": {},
    }

    try:
        summary["aqi"] = generate_aqi_forecasts()
        summary["weather"] = generate_weather_forecasts()
        summary["wildfires"] = generate_wildfire_forecasts()
        summary["earthquakes"] = generate_earthquake_forecasts()
        logger.info("Forecasts generated", extra={"context": summary})
    except Exception as exc:
        logger.error(
            "Forecast generation failed",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )
        summary["error"] = str(exc)

    return summary


def get_forecasts(
    metric: str | None = None,
    source: str | None = None,
    location: str | None = None,
    horizon: str = "24h",
) -> list[dict[str, Any]]:
    """Retrieve stored forecasts."""
    where: list[str] = ["horizon = %s"]
    params: list[Any] = [horizon]

    if metric:
        where.append("metric = %s")
        params.append(metric)
    if source:
        where.append("source = %s")
        params.append(source)
    if location:
        where.append("location_name = %s")
        params.append(location)

    where_sql = "WHERE " + " AND ".join(where)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT metric, source, location_name, horizon,
                       forecast_time, predicted_value, lower_bound, upper_bound, generated_at
                FROM forecasts {where_sql}
                ORDER BY forecast_time ASC
                """,
                params,
            )
            cols = [d[0] for d in cur.description]
            return [
                {
                    cols[i]: (row[i].isoformat() if isinstance(row[i], datetime) else row[i])
                    for i in range(len(cols))
                }
                for row in cur.fetchall()
            ]
