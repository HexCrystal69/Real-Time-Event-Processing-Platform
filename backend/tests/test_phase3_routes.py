"""
GRIP — Analytics routes tests (Phase 3).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    with patch("backend.main.init_database"), \
         patch("backend.main.start_background_tasks", return_value=[]), \
         patch("backend.main.stop_background_tasks"), \
         patch("backend.main.compute_risk_scores"), \
         patch("backend.main.check_and_generate_alerts"), \
         patch("backend.main.generate_all_forecasts"):
        from backend.main import app
        with TestClient(app) as c:
            yield c


class TestAnalyticsRoutes:
    @patch("backend.routes.analytics_routes.get_dashboard_summary")
    def test_summary(self, mock_summary, client):
        mock_summary.return_value = {"total_events": 100, "events_last_hour": 5}
        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        assert response.json()["total_events"] == 100

    @patch("backend.routes.analytics_routes.get_latest_risk_scores")
    def test_risk_scores(self, mock_scores, client):
        mock_scores.return_value = [{"region_name": "Tokyo", "unified_score": 45}]
        response = client.get("/api/analytics/risk-scores")
        assert response.status_code == 200
        assert len(response.json()) == 1

    @patch("backend.routes.analytics_routes.get_map_markers")
    def test_map_markers(self, mock_markers, client):
        mock_markers.return_value = {"earthquakes": [], "wildfires": []}
        response = client.get("/api/analytics/map-markers")
        assert response.status_code == 200
        assert "earthquakes" in response.json()

    @patch("backend.routes.analytics_routes.get_earthquake_analytics")
    def test_earthquake_analytics(self, mock_eq, client):
        mock_eq.return_value = {"recent_count": 10}
        response = client.get("/api/analytics/earthquakes")
        assert response.status_code == 200


class TestForecastRoutes:
    @patch("backend.routes.forecast_routes.get_forecasts")
    def test_list_forecasts(self, mock_forecasts, client):
        mock_forecasts.return_value = [{"metric": "us_aqi", "predicted_value": 50}]
        response = client.get("/api/forecasts?horizon=24h")
        assert response.status_code == 200
        assert response.json()["count"] == 1

    def test_invalid_horizon(self, client):
        response = client.get("/api/forecasts?horizon=invalid")
        assert response.status_code == 422


class TestAlertRoutes:
    @patch("backend.routes.alert_routes.get_active_alerts")
    def test_active_alerts(self, mock_alerts, client):
        mock_alerts.return_value = [{"id": 1, "title": "Test Alert"}]
        response = client.get("/api/alerts/active")
        assert response.status_code == 200
        assert response.json()["count"] == 1


class TestMonitoringRoutes:
    @patch("backend.routes.monitoring_routes.get_system_monitoring")
    def test_monitoring(self, mock_monitor, client):
        mock_monitor.return_value = {"postgres": {"status": "healthy"}}
        response = client.get("/api/monitoring")
        assert response.status_code == 200


class TestExportRoutes:
    @patch("backend.routes.export_routes.export_csv")
    def test_export_csv(self, mock_csv, client):
        mock_csv.return_value = "id,name\n1,test"
        response = client.get("/api/export/csv/earthquakes")
        assert response.status_code == 200
        assert "text/csv" in response.headers["content-type"]

    def test_export_unknown_type(self, client):
        response = client.get("/api/export/csv/unknown")
        assert response.status_code == 404
