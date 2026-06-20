"""
GRIP — Risk scoring engine tests (Phase 3).
"""

from __future__ import annotations

from backend.services.risk_scoring import _score_to_level, _haversine_km


class TestScoreToLevel:
    def test_critical(self):
        assert _score_to_level(80) == "Critical"
        assert _score_to_level(75) == "Critical"

    def test_high(self):
        assert _score_to_level(60) == "High"
        assert _score_to_level(50) == "High"

    def test_moderate(self):
        assert _score_to_level(30) == "Moderate"
        assert _score_to_level(25) == "Moderate"

    def test_low(self):
        assert _score_to_level(10) == "Low"
        assert _score_to_level(0) == "Low"


class TestHaversine:
    def test_same_point(self):
        assert _haversine_km(0, 0, 0, 0) == 0.0

    def test_known_distance(self):
        dist = _haversine_km(40.7128, -74.0060, 51.5074, -0.1278)
        assert 5500 < dist < 5600
