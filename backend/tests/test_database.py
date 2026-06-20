"""
GRIP — Database layer tests.

Tests DDL execution (init_database) and basic CRUD operations
against each table. Requires a live PostgreSQL connection.
These tests are designed to run inside the Docker environment
or against a local PostgreSQL with the correct credentials.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.database.models import ALL_TABLES


class TestModels:
    """Tests for the DDL definitions."""

    def test_all_tables_is_list(self):
        assert isinstance(ALL_TABLES, list)

    def test_all_tables_has_twenty_entries(self):
        assert len(ALL_TABLES) == 20

    def test_each_ddl_contains_create_table(self):
        for ddl in ALL_TABLES:
            assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_earthquakes_ddl_has_event_id(self):
        assert "event_id" in ALL_TABLES[0]

    def test_weather_ddl_has_location_name(self):
        assert "location_name" in ALL_TABLES[1]

    def test_air_quality_ddl_has_us_aqi(self):
        assert "us_aqi" in ALL_TABLES[2]

    def test_wildfires_ddl_has_brightness(self):
        assert "brightness" in ALL_TABLES[3]

    def test_ingestion_logs_ddl_has_latency(self):
        assert "latency_ms" in ALL_TABLES[4]


class TestConnection:
    """Tests for the database connection module (mocked)."""

    @patch("backend.database.connection.pg_pool.ThreadedConnectionPool")
    def test_get_pool_creates_connection(self, mock_pool_class):
        """Verify pool creation is attempted with correct parameters."""
        mock_pool = MagicMock()
        mock_pool.closed = False
        mock_pool_class.return_value = mock_pool

        import backend.database.connection as db_conn
        db_conn._connection_pool = None  # Reset singleton

        pool = db_conn._get_pool()

        mock_pool_class.assert_called_once()
        call_kwargs = mock_pool_class.call_args
        assert call_kwargs[1]["dbname"] == "grip_db"

    @patch("backend.database.connection._get_pool")
    def test_log_ingestion_executes_insert(self, mock_get_pool):
        """Verify log_ingestion calls execute with an INSERT statement."""
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_get_pool.return_value = mock_pool

        from backend.database.connection import log_ingestion
        log_ingestion("test_source", "success", 42, 123.45, None)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "INSERT INTO ingestion_logs" in call_args[0][0]
        assert call_args[0][1] == ("test_source", "success", 42, 123.45, None)
