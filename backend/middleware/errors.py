"""
GRIP — Global exception handlers for secure API responses.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from backend.config.logger import get_logger

logger = get_logger("errors")


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ValueError)
    async def value_error_handler(_request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_request", "detail": str(exc)},
        )

    @app.exception_handler(Exception)
    async def generic_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "Unhandled API error",
            extra={"context": {"error": str(exc)}},
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "detail": "An internal error occurred"},
        )
