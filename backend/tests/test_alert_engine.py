"""
GRIP — Alert engine tests (Phase 3).
"""

from __future__ import annotations

from backend.services.alert_engine import (
    ALERT_RULES,
    _build_metadata,
    _coordinates,
    _region_name,
)


class TestAlertRules:
    def test_all_rules_defined(self):
        assert "major_earthquake" in ALERT_RULES
        assert "extreme_aqi" in ALERT_RULES
        assert "severe_weather" in ALERT_RULES
        assert "major_wildfire" in ALERT_RULES

    def test_earthquake_severity(self):
        rule = ALERT_RULES["major_earthquake"]
        row = ("eq1", 7.5, "Test Place", 35.0, 140.0, None, "Critical")
        assert rule["severity_fn"](row) == "Critical"
        row_low = ("eq2", 6.2, "Test", 35.0, 140.0, None, "High")
        assert rule["severity_fn"](row_low) == "High"


class TestAlertHelpers:
    def test_build_metadata_earthquake(self):
        row = ("eq1", 6.5, "Place", 35.0, 140.0, None, "High")
        meta = _build_metadata("major_earthquake", row)
        assert meta["event_id"] == "eq1"
        assert meta["magnitude"] == 6.5

    def test_coordinates_wildfire(self):
        row = (35.0, 140.0, 300.0, "Extreme", None, 1, "High")
        lat, lon = _coordinates(row, "major_wildfire")
        assert lat == 35.0
        assert lon == 140.0

    def test_region_name_weather(self):
        row = ("Tokyo", "Severe", 80.0, 10.0, 35.0, 140.0, None, 1)
        assert _region_name(row, "severe_weather") == "Tokyo"
