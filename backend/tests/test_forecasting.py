"""
GRIP — Forecasting service tests (Phase 3).
"""

from __future__ import annotations

import pandas as pd
import pytest

from backend.services.forecasting import HORIZON_PERIODS, _run_prophet


class TestForecasting:
    def test_horizon_periods_defined(self):
        assert "24h" in HORIZON_PERIODS
        assert "7d" in HORIZON_PERIODS
        assert "30d" in HORIZON_PERIODS

    def test_prophet_insufficient_data(self):
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="h"),
            "y": [10, 12, 11, 13, 12],
        })
        result = _run_prophet(df, "24h")
        assert result is None

    @pytest.mark.slow
    def test_prophet_with_sufficient_data(self):
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=48, freq="h"),
            "y": [50 + (i % 10) for i in range(48)],
        })
        result = _run_prophet(df, "24h")
        if result is not None:
            assert len(result) == HORIZON_PERIODS["24h"]
            assert "yhat" in result.columns
