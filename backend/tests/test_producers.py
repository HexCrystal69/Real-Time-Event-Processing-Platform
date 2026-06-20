"""
GRIP — Producer unit tests.

Tests validate_record() and transform_record() for each producer.
Uses real-world-shaped sample data to ensure correctness.
"""

from __future__ import annotations

import pytest

from backend.ingestion.producers.earthquake_producer import EarthquakeProducer
from backend.ingestion.producers.weather_producer import WeatherProducer
from backend.ingestion.producers.air_quality_producer import AirQualityProducer
from backend.ingestion.producers.wildfire_producer import WildfireProducer


# ===================================================================
# Earthquake Producer
# ===================================================================
class TestEarthquakeProducer:
    """Tests for the USGS earthquake producer."""

    @pytest.fixture
    def producer(self):
        return EarthquakeProducer()

    @pytest.fixture
    def valid_feature(self):
        return {
            "id": "us7000abc1",
            "properties": {
                "mag": 5.2,
                "place": "10 km NE of Ridgecrest, CA",
                "time": 1718000000000,
                "tsunami": 0,
                "sig": 416,
                "status": "reviewed",
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-117.6049, 35.7695, 8.04],
            },
        }

    def test_validate_valid_record(self, producer, valid_feature):
        assert producer.validate_record(valid_feature) is True

    def test_validate_missing_id(self, producer, valid_feature):
        valid_feature["id"] = ""
        assert producer.validate_record(valid_feature) is False

    def test_validate_missing_time(self, producer, valid_feature):
        del valid_feature["properties"]["time"]
        assert producer.validate_record(valid_feature) is False

    def test_validate_missing_coordinates(self, producer):
        record = {"id": "test", "properties": {"time": 123}, "geometry": {"coordinates": []}}
        assert producer.validate_record(record) is False

    def test_transform_record(self, producer, valid_feature):
        result = producer.transform_record(valid_feature)
        assert result["event_id"] == "us7000abc1"
        assert result["magnitude"] == 5.2
        assert result["latitude"] == 35.7695
        assert result["longitude"] == -117.6049
        assert result["depth_km"] == 8.04
        assert result["tsunami"] is False
        assert "event_time" in result


# ===================================================================
# Weather Producer
# ===================================================================
class TestWeatherProducer:
    """Tests for the Open-Meteo weather producer."""

    @pytest.fixture
    def producer(self):
        return WeatherProducer()

    @pytest.fixture
    def valid_record(self):
        return {
            "location_name": "Tokyo",
            "latitude": 35.6762,
            "longitude": 139.6503,
            "current": {
                "time": "2026-06-17T12:00",
                "temperature_2m": 28.5,
                "relative_humidity_2m": 65,
                "wind_speed_10m": 12.3,
                "precipitation": 0.0,
                "weather_code": 1,
            },
            "current_units": {},
        }

    def test_validate_valid_record(self, producer, valid_record):
        assert producer.validate_record(valid_record) is True

    def test_validate_missing_current(self, producer):
        record = {"location_name": "Test", "current": {}}
        assert producer.validate_record(record) is False

    def test_validate_missing_location(self, producer, valid_record):
        valid_record["location_name"] = ""
        assert producer.validate_record(valid_record) is False

    def test_transform_record(self, producer, valid_record):
        result = producer.transform_record(valid_record)
        assert result["location_name"] == "Tokyo"
        assert result["temperature_c"] == 28.5
        assert result["humidity_pct"] == 65
        assert result["wind_speed_kmh"] == 12.3
        assert result["precipitation_mm"] == 0.0
        assert result["weather_code"] == 1


# ===================================================================
# Air Quality Producer
# ===================================================================
class TestAirQualityProducer:
    """Tests for the Open-Meteo air quality producer."""

    @pytest.fixture
    def producer(self):
        return AirQualityProducer()

    @pytest.fixture
    def valid_record(self):
        return {
            "location_name": "Mumbai",
            "latitude": 19.076,
            "longitude": 72.8777,
            "current": {
                "time": "2026-06-17T12:00",
                "us_aqi": 85,
                "pm2_5": 22.1,
                "pm10": 45.3,
                "ozone": 68.0,
                "nitrogen_dioxide": 15.2,
                "carbon_monoxide": 450.0,
                "sulphur_dioxide": 8.5,
            },
        }

    def test_validate_valid_record(self, producer, valid_record):
        assert producer.validate_record(valid_record) is True

    def test_validate_missing_aqi_and_pm(self, producer):
        record = {"location_name": "Test", "current": {"ozone": 50}}
        assert producer.validate_record(record) is False

    def test_validate_aqi_only(self, producer):
        record = {"location_name": "Test", "current": {"us_aqi": 50}}
        assert producer.validate_record(record) is True

    def test_transform_record(self, producer, valid_record):
        result = producer.transform_record(valid_record)
        assert result["location_name"] == "Mumbai"
        assert result["us_aqi"] == 85
        assert result["pm2_5"] == 22.1
        assert result["pm10"] == 45.3


# ===================================================================
# Wildfire Producer
# ===================================================================
class TestWildfireProducer:
    """Tests for the NASA FIRMS wildfire producer."""

    @pytest.fixture
    def producer(self):
        return WildfireProducer()

    @pytest.fixture
    def valid_record(self):
        return {
            "latitude": "-33.8688",
            "longitude": "151.2093",
            "bright_ti4": "325.6",
            "scan": "0.39",
            "track": "0.36",
            "acq_date": "2026-06-17",
            "acq_time": "0430",
            "satellite": "N",
            "instrument": "VIIRS",
            "confidence": "nominal",
            "frp": "12.5",
            "daynight": "N",
        }

    def test_validate_valid_record(self, producer, valid_record):
        assert producer.validate_record(valid_record) is True

    def test_validate_invalid_latitude(self, producer, valid_record):
        valid_record["latitude"] = "999"
        assert producer.validate_record(valid_record) is False

    def test_validate_missing_acq_date(self, producer, valid_record):
        valid_record["acq_date"] = ""
        assert producer.validate_record(valid_record) is False

    def test_validate_non_numeric_coords(self, producer, valid_record):
        valid_record["latitude"] = "not_a_number"
        assert producer.validate_record(valid_record) is False

    def test_transform_record(self, producer, valid_record):
        result = producer.transform_record(valid_record)
        assert result["latitude"] == -33.8688
        assert result["longitude"] == 151.2093
        assert result["brightness"] == 325.6
        assert result["acq_date"] == "2026-06-17"
        assert result["satellite"] == "N"
        assert result["frp"] == 12.5
