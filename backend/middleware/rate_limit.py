"""
GRIP — Rate limiting middleware using slowapi.
"""

from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.config.settings import settings

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{settings.rate_limit_per_minute}/minute"],
)
