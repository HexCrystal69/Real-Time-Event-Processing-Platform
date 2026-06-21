"""
GRIP — Phase 3 FastAPI application entry point.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.responses import JSONResponse

from backend.config.logger import get_logger
from backend.database.connection import init_database
from backend.middleware.errors import register_exception_handlers
from backend.middleware.rate_limit import limiter
from backend.routes.health import router as health_router
from backend.routes.data_routes import router as data_router
from backend.routes.analytics_routes import router as analytics_router
from backend.routes.forecast_routes import router as forecast_router
from backend.routes.alert_routes import router as alert_router
from backend.routes.monitoring_routes import router as monitoring_router
from backend.routes.export_routes import router as export_router
from backend.routes.websocket_routes import router as websocket_router
from backend.services.background_worker import start_background_tasks, stop_background_tasks
from backend.services.forecasting import generate_all_forecasts
from backend.services.risk_scoring import compute_risk_scores
from backend.services.alert_engine import check_and_generate_alerts

logger = get_logger("fastapi")

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle hooks."""
    logger.info("GRIP API starting up — initialising database")
    try:
        init_database()
        logger.info("Database initialisation successful")
        
        # Seed the database if it is empty to ensure forecasting and analytics load immediately
        from backend.database.seeder import seed_database_if_empty
        seed_database_if_empty()
        
        compute_risk_scores()
        check_and_generate_alerts()
        generate_all_forecasts()
    except Exception as exc:
        logger.error(
            "Startup initialisation failed",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )

    start_background_tasks()
    yield
    await stop_background_tasks()
    logger.info("GRIP API shutting down")


app = FastAPI(
    title="GRIP — Global Risk Intelligence Platform",
    description=(
        "Real-time global risk monitoring and intelligence system. "
        "Earthquakes, wildfires, air quality, and weather risk analytics."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(_request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "rate_limit_exceeded", "detail": str(exc.detail)},
    )


app.include_router(health_router)
app.include_router(data_router)
app.include_router(analytics_router)
app.include_router(forecast_router)
app.include_router(alert_router)
app.include_router(monitoring_router)
app.include_router(export_router)
app.include_router(websocket_router)

if FRONTEND_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
