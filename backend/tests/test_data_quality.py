"""
GRIP — Data quality and DDL validation tests (Phase 2).

Tests the new DDL definitions for Phase 2 tables and verifies
that the data quality and anomaly detection table schemas are correct.
"""

from __future__ import annotations

import pytest

from backend.database.models import (
    ALL_TABLES,
    EARTHQUAKES_RAW_TABLE,
    WEATHER_RAW_TABLE,
    AIR_QUALITY_RAW_TABLE,
    WILDFIRES_RAW_TABLE,
    EARTHQUAKES_PROCESSED_TABLE,
    WEATHER_PROCESSED_TABLE,
    AIR_QUALITY_PROCESSED_TABLE,
    WILDFIRES_PROCESSED_TABLE,
    ANOMALY_EVENTS_TABLE,
    DATA_QUALITY_METRICS_TABLE,
    PIPELINE_METRICS_TABLE,
    RISK_SCORES_TABLE,
    ALERTS_TABLE,
    FORECASTS_TABLE,
    ANALYTICS_SNAPSHOTS_TABLE,
)


# ===================================================================
# DDL Structure Tests
# ===================================================================
class TestPhase2DDL:
    """Tests for Phase 2 DDL definitions."""

    def test_all_tables_has_20_entries(self):
        """5 Phase 1 + 11 Phase 2 + 4 Phase 3 = 20 total."""
        assert len(ALL_TABLES) == 20

    def test_each_ddl_contains_create_table(self):
        for ddl in ALL_TABLES:
            assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_all_phase2_tables_have_indexes(self):
        phase2_tables = [
            EARTHQUAKES_RAW_TABLE,
            WEATHER_RAW_TABLE,
            AIR_QUALITY_RAW_TABLE,
            WILDFIRES_RAW_TABLE,
            EARTHQUAKES_PROCESSED_TABLE,
            WEATHER_PROCESSED_TABLE,
            AIR_QUALITY_PROCESSED_TABLE,
            WILDFIRES_PROCESSED_TABLE,
            ANOMALY_EVENTS_TABLE,
            DATA_QUALITY_METRICS_TABLE,
            PIPELINE_METRICS_TABLE,
        ]
        for ddl in phase2_tables:
            assert "CREATE INDEX IF NOT EXISTS" in ddl


# ===================================================================
# Raw Tables
# ===================================================================
class TestRawTables:
    """Tests for raw table DDL definitions."""

    def test_earthquakes_raw_has_event_id(self):
        assert "event_id" in EARTHQUAKES_RAW_TABLE

    def test_earthquakes_raw_has_ingested_at(self):
        assert "ingested_at" in EARTHQUAKES_RAW_TABLE

    def test_weather_raw_has_location_name(self):
        assert "location_name" in WEATHER_RAW_TABLE

    def test_weather_raw_has_temperature(self):
        assert "temperature_c" in WEATHER_RAW_TABLE

    def test_air_quality_raw_has_us_aqi(self):
        assert "us_aqi" in AIR_QUALITY_RAW_TABLE

    def test_air_quality_raw_has_pm2_5(self):
        assert "pm2_5" in AIR_QUALITY_RAW_TABLE

    def test_wildfires_raw_has_brightness(self):
        assert "brightness" in WILDFIRES_RAW_TABLE

    def test_wildfires_raw_has_frp(self):
        assert "frp" in WILDFIRES_RAW_TABLE


# ===================================================================
# Processed Tables
# ===================================================================
class TestProcessedTables:
    """Tests for processed table DDL definitions."""

    def test_earthquakes_processed_has_risk_category(self):
        assert "risk_category" in EARTHQUAKES_PROCESSED_TABLE

    def test_earthquakes_processed_has_magnitude_band(self):
        assert "magnitude_band" in EARTHQUAKES_PROCESSED_TABLE

    def test_earthquakes_processed_has_distance_group(self):
        assert "distance_group" in EARTHQUAKES_PROCESSED_TABLE

    def test_earthquakes_processed_has_processed_at(self):
        assert "processed_at" in EARTHQUAKES_PROCESSED_TABLE

    def test_weather_processed_has_heat_index(self):
        assert "heat_index" in WEATHER_PROCESSED_TABLE

    def test_weather_processed_has_rain_severity(self):
        assert "rain_severity" in WEATHER_PROCESSED_TABLE

    def test_weather_processed_has_wind_severity(self):
        assert "wind_severity" in WEATHER_PROCESSED_TABLE

    def test_weather_processed_has_storm_severity(self):
        assert "storm_severity" in WEATHER_PROCESSED_TABLE

    def test_air_quality_processed_has_aqi_category(self):
        assert "aqi_category" in AIR_QUALITY_PROCESSED_TABLE

    def test_wildfires_processed_has_fire_severity(self):
        assert "fire_severity" in WILDFIRES_PROCESSED_TABLE

    def test_wildfires_processed_has_detection_confidence(self):
        assert "detection_confidence" in WILDFIRES_PROCESSED_TABLE


# ===================================================================
# Anomaly Events Table
# ===================================================================
class TestAnomalyEventsTable:
    """Tests for anomaly_events DDL."""

    def test_has_source_column(self):
        assert "source" in ANOMALY_EVENTS_TABLE

    def test_has_event_type_column(self):
        assert "event_type" in ANOMALY_EVENTS_TABLE

    def test_has_severity_column(self):
        assert "severity" in ANOMALY_EVENTS_TABLE

    def test_has_description_column(self):
        assert "description" in ANOMALY_EVENTS_TABLE

    def test_has_raw_data_jsonb(self):
        assert "JSONB" in ANOMALY_EVENTS_TABLE

    def test_has_detected_at(self):
        assert "detected_at" in ANOMALY_EVENTS_TABLE

    def test_has_coordinates(self):
        assert "latitude" in ANOMALY_EVENTS_TABLE
        assert "longitude" in ANOMALY_EVENTS_TABLE


# ===================================================================
# Data Quality Metrics Table
# ===================================================================
class TestDataQualityMetricsTable:
    """Tests for data_quality_metrics DDL."""

    def test_has_source_column(self):
        assert "source" in DATA_QUALITY_METRICS_TABLE

    def test_has_batch_id(self):
        assert "batch_id" in DATA_QUALITY_METRICS_TABLE

    def test_has_total_records(self):
        assert "total_records" in DATA_QUALITY_METRICS_TABLE

    def test_has_valid_records(self):
        assert "valid_records" in DATA_QUALITY_METRICS_TABLE

    def test_has_duplicates(self):
        assert "duplicates" in DATA_QUALITY_METRICS_TABLE

    def test_has_missing_fields(self):
        assert "missing_fields" in DATA_QUALITY_METRICS_TABLE

    def test_has_malformed_records(self):
        assert "malformed_records" in DATA_QUALITY_METRICS_TABLE

    def test_has_dropped_events(self):
        assert "dropped_events" in DATA_QUALITY_METRICS_TABLE

    def test_has_validation_errors(self):
        assert "validation_errors" in DATA_QUALITY_METRICS_TABLE


# ===================================================================
# Pipeline Metrics Table
# ===================================================================
class TestPipelineMetricsTable:
    """Tests for pipeline_metrics DDL."""

    def test_has_source_column(self):
        assert "source" in PIPELINE_METRICS_TABLE

    def test_has_records_count(self):
        assert "records_count" in PIPELINE_METRICS_TABLE

    def test_has_processing_time(self):
        assert "processing_time_ms" in PIPELINE_METRICS_TABLE

    def test_has_kafka_lag(self):
        assert "kafka_lag_ms" in PIPELINE_METRICS_TABLE

    def test_has_db_write_latency(self):
        assert "db_write_latency_ms" in PIPELINE_METRICS_TABLE

    def test_has_records_per_minute(self):
        assert "records_per_minute" in PIPELINE_METRICS_TABLE


# ===================================================================
# Phase 3 Tables
# ===================================================================
class TestPhase3DDL:
    """Tests for Phase 3 DDL definitions."""

    def test_risk_scores_has_unified_score(self):
        assert "unified_score" in RISK_SCORES_TABLE
        assert "risk_level" in RISK_SCORES_TABLE

    def test_alerts_has_alert_type(self):
        assert "alert_type" in ALERTS_TABLE
        assert "is_active" in ALERTS_TABLE

    def test_forecasts_has_horizon(self):
        assert "horizon" in FORECASTS_TABLE
        assert "predicted_value" in FORECASTS_TABLE

    def test_analytics_snapshots_has_payload(self):
        assert "JSONB" in ANALYTICS_SNAPSHOTS_TABLE
