"""
GRIP — WebSocket endpoint tests (Phase 3).
"""

from __future__ import annotations

from unittest.mock import patch

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


class TestWebSocket:
    @patch("backend.routes.websocket_routes.get_dashboard_summary")
    @patch("backend.routes.websocket_routes.get_latest_risk_scores")
    @patch("backend.routes.websocket_routes.get_active_alerts")
    @patch("backend.routes.websocket_routes.get_map_markers")
    def test_websocket_connect(
        self, mock_map, mock_alerts, mock_scores, mock_summary, client,
    ):
        mock_summary.return_value = {"total_events": 0}
        mock_scores.return_value = []
        mock_alerts.return_value = []
        mock_map.return_value = {"earthquakes": []}

        with client.websocket_connect("/ws/live") as ws:
            data = ws.receive_json()
            assert data["type"] == "connected"
            assert "summary" in data
            assert "risk_scores" in data
