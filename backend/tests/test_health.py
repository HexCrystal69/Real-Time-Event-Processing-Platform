"""
GRIP — Health endpoint tests.

Tests the /health and /status FastAPI endpoints using the
built-in TestClient (no running Docker stack required for /health).
"""

from __future__ import annotations

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


def test_health_returns_200(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "grip-api"


def test_health_response_structure(client: TestClient):
    response = client.get("/health")
    data = response.json()
    assert isinstance(data, dict)
    assert "status" in data
    assert "service" in data


@patch("backend.routes.health.get_connection")
@patch("kafka.KafkaConsumer")
def test_status_returns_structured_json(mock_kafka: MagicMock, mock_conn: MagicMock, client: TestClient):
    """Test /status returns a well-structured response even when services are mocked."""
    # Mock PostgreSQL
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.__enter__ = MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = MagicMock(return_value=False)

    mock_conn.return_value = mock_connection

    # Mock Kafka
    mock_consumer_instance = MagicMock()
    mock_consumer_instance.topics.return_value = {"earthquakes", "weather", "air_quality", "wildfires"}
    mock_kafka.return_value = mock_consumer_instance

    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "timestamp" in data
    assert "postgres" in data
    assert "kafka" in data
    assert "sources" in data
