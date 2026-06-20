"""
GRIP — Data routes endpoint tests (Phase 2).

Tests all six FastAPI data endpoints with mocked database connections.
Verifies response structure, pagination, filtering, and error handling.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a FastAPI test client with database init mocked out."""
    with patch("backend.main.init_database"), \
         patch("backend.main.start_background_tasks", return_value=[]), \
         patch("backend.main.stop_background_tasks"), \
         patch("backend.main.compute_risk_scores"), \
         patch("backend.main.check_and_generate_alerts"), \
         patch("backend.main.generate_all_forecasts"):
        from backend.main import app
        with TestClient(app) as c:
            yield c


def _mock_cursor_with_data(rows, columns):
    """Create a mock cursor that returns specified rows and column names."""
    mock_cursor = MagicMock()
    mock_cursor.description = [(col,) for col in columns]
    mock_cursor.fetchone.return_value = (len(rows),)
    mock_cursor.fetchall.return_value = rows
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    return mock_cursor


def _mock_connection(cursor):
    """Create a mock connection that returns the given cursor."""
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


def _mock_pool(conn):
    """Create a mock pool that returns the given connection."""
    mock_pool = MagicMock()
    mock_pool.getconn.return_value = conn
    mock_pool.closed = False
    return mock_pool


# ===================================================================
# Earthquake Endpoint
# ===================================================================
class TestEarthquakeEndpoint:
    """Tests for GET /earthquakes."""

    @patch("backend.routes.data_routes.get_connection")
    def test_earthquakes_returns_200(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "event_id", "magnitude"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/earthquakes")
        assert response.status_code == 200
        data = response.json()
        assert "total_count" in data
        assert "data" in data
        assert "limit" in data
        assert "offset" in data

    @patch("backend.routes.data_routes.get_connection")
    def test_earthquakes_with_filters(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "event_id", "magnitude"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/earthquakes?min_magnitude=5.0&risk_category=High&limit=10&offset=0")
        assert response.status_code == 200

    @patch("backend.routes.data_routes.get_connection")
    def test_earthquakes_pagination(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "event_id", "magnitude"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/earthquakes?limit=10&offset=20")
        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 10
        assert data["offset"] == 20


# ===================================================================
# Weather Endpoint
# ===================================================================
class TestWeatherEndpoint:
    """Tests for GET /weather."""

    @patch("backend.routes.data_routes.get_connection")
    def test_weather_returns_200(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "location_name", "temperature_c"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/weather")
        assert response.status_code == 200
        data = response.json()
        assert "total_count" in data
        assert "data" in data

    @patch("backend.routes.data_routes.get_connection")
    def test_weather_with_location_filter(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "location_name"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/weather?location=Tokyo")
        assert response.status_code == 200


# ===================================================================
# Air Quality Endpoint
# ===================================================================
class TestAirQualityEndpoint:
    """Tests for GET /air-quality."""

    @patch("backend.routes.data_routes.get_connection")
    def test_air_quality_returns_200(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "location_name", "us_aqi"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/air-quality")
        assert response.status_code == 200
        data = response.json()
        assert "total_count" in data

    @patch("backend.routes.data_routes.get_connection")
    def test_air_quality_with_aqi_filter(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "us_aqi"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/air-quality?min_aqi=100")
        assert response.status_code == 200


# ===================================================================
# Wildfire Endpoint
# ===================================================================
class TestWildfireEndpoint:
    """Tests for GET /wildfires."""

    @patch("backend.routes.data_routes.get_connection")
    def test_wildfires_returns_200(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "latitude", "longitude"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/wildfires")
        assert response.status_code == 200
        data = response.json()
        assert "total_count" in data

    @patch("backend.routes.data_routes.get_connection")
    def test_wildfires_with_severity_filter(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "fire_severity"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/wildfires?min_severity=High")
        assert response.status_code == 200


# ===================================================================
# Anomaly Endpoint
# ===================================================================
class TestAnomalyEndpoint:
    """Tests for GET /anomalies."""

    @patch("backend.routes.data_routes.get_connection")
    def test_anomalies_returns_200(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "source", "severity"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/anomalies")
        assert response.status_code == 200
        data = response.json()
        assert "total_count" in data
        assert "data" in data

    @patch("backend.routes.data_routes.get_connection")
    def test_anomalies_with_source_filter(self, mock_get_conn, client):
        cursor = _mock_cursor_with_data([], ["id", "source"])
        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/anomalies?source=earthquakes&severity=Critical")
        assert response.status_code == 200


# ===================================================================
# Metrics Endpoint
# ===================================================================
class TestMetricsEndpoint:
    """Tests for GET /metrics."""

    @patch("backend.routes.data_routes.get_connection")
    def test_metrics_returns_200(self, mock_get_conn, client):
        cursor = MagicMock()
        cursor.description = [("source",), ("total_batches",)]
        cursor.fetchall.return_value = []
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)

        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "timestamp" in data
        assert "pipeline_summary" in data
        assert "data_quality_summary" in data
        assert "recent_batches" in data

    @patch("backend.routes.data_routes.get_connection")
    def test_metrics_with_source_filter(self, mock_get_conn, client):
        cursor = MagicMock()
        cursor.description = [("source",), ("total_batches",)]
        cursor.fetchall.return_value = []
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)

        conn = _mock_connection(cursor)
        mock_get_conn.return_value = conn

        response = client.get("/metrics?source=earthquakes")
        assert response.status_code == 200


# ===================================================================
# Error Handling
# ===================================================================
class TestErrorHandling:
    """Tests for error scenarios."""

    @patch("backend.routes.data_routes.get_connection")
    def test_database_error_returns_500(self, mock_get_conn, client):
        mock_get_conn.side_effect = Exception("Connection refused")

        response = client.get("/earthquakes")
        assert response.status_code == 500

    def test_invalid_limit_returns_422(self, client):
        response = client.get("/earthquakes?limit=0")
        assert response.status_code == 422

    def test_negative_offset_returns_422(self, client):
        response = client.get("/earthquakes?offset=-1")
        assert response.status_code == 422

    def test_limit_exceeds_max_returns_422(self, client):
        response = client.get("/earthquakes?limit=1000")
        assert response.status_code == 422
