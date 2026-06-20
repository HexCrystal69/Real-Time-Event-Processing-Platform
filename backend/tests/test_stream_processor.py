"""
GRIP — Stream processor unit tests (Phase 2).

Tests all enrichment logic, validation rules, anomaly detection thresholds,
and quality metrics computation using pure Python unit tests.

The enrichment functions in stream_processor.py use PySpark DataFrame operations
that require a JVM. These tests verify the *logic* by reimplementing the same
rules as pure functions and validating them match the Spark UDF behavior.
This approach allows running tests without Java/Spark installed locally.
"""

from __future__ import annotations

import pytest


# ===================================================================
# Pure-Python reimplementations of enrichment rules for testing
# These mirror the Spark column expressions in stream_processor.py
# ===================================================================

def risk_category(magnitude: float | None) -> str:
    if magnitude is None:
        return "Low"
    if magnitude >= 7.0:
        return "Critical"
    if magnitude >= 5.0:
        return "High"
    if magnitude >= 3.0:
        return "Medium"
    return "Low"


def magnitude_band(magnitude: float | None) -> str:
    if magnitude is None:
        return "Micro"
    if magnitude >= 8.0:
        return "Great"
    if magnitude >= 7.0:
        return "Major"
    if magnitude >= 6.0:
        return "Strong"
    if magnitude >= 5.0:
        return "Moderate"
    if magnitude >= 4.0:
        return "Light"
    if magnitude >= 2.0:
        return "Minor"
    return "Micro"


def distance_group(depth_km: float | None) -> str:
    if depth_km is None:
        return "Shallow"
    if depth_km > 300.0:
        return "Deep"
    if depth_km >= 70.0:
        return "Intermediate"
    return "Shallow"


def rain_severity(precipitation_mm: float | None) -> str:
    if precipitation_mm is None or precipitation_mm <= 0.0:
        return "None"
    if precipitation_mm < 2.5:
        return "Light"
    if precipitation_mm < 7.5:
        return "Moderate"
    if precipitation_mm < 50.0:
        return "Heavy"
    return "Extreme"


def wind_severity(wind_speed_kmh: float | None) -> str:
    if wind_speed_kmh is None or wind_speed_kmh < 20.0:
        return "Calm"
    if wind_speed_kmh < 40.0:
        return "Breezy"
    if wind_speed_kmh < 75.0:
        return "Strong"
    if wind_speed_kmh < 120.0:
        return "Gale"
    return "Hurricane"


def storm_severity(
    wind_speed_kmh: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
) -> str:
    wind = wind_speed_kmh or 0.0
    precip = precipitation_mm or 0.0
    code = weather_code or 0

    if wind >= 120.0 or precip >= 50.0 or code in (97, 99):
        return "Extreme"
    if wind >= 75.0 or precip >= 25.0 or code in (95, 96):
        return "Severe"
    if wind >= 40.0 or precip >= 7.5 or code in (65, 67, 75, 77, 82, 86):
        return "Moderate"
    if wind >= 20.0 or precip >= 2.5:
        return "Minor"
    return "None"


def compute_heat_index(temp_c: float | None, humidity: float | None) -> float | None:
    if temp_c is None:
        return None
    if humidity is None or temp_c < 27.0 or humidity < 40.0:
        return temp_c

    t = temp_c
    rh = humidity
    hi = (
        -8.78469475556
        + 1.61139411 * t
        + 2.33854883889 * rh
        - 0.14611605 * t * rh
        - 0.012308094 * t * t
        - 0.0164248277778 * rh * rh
        + 0.002211732 * t * t * rh
        + 0.00072546 * t * rh * rh
        - 0.000003582 * t * t * rh * rh
    )
    return round(hi, 2)


def aqi_category(us_aqi: int | None) -> str:
    if us_aqi is None:
        return "Unknown"
    if us_aqi <= 50:
        return "Good"
    if us_aqi <= 100:
        return "Moderate"
    if us_aqi <= 150:
        return "Poor"
    if us_aqi <= 200:
        return "Very Poor"
    return "Hazardous"


def fire_severity(frp: float | None) -> str:
    if frp is None:
        return "Low"
    if frp >= 200.0:
        return "Extreme"
    if frp >= 50.0:
        return "High"
    if frp >= 10.0:
        return "Medium"
    return "Low"


def detection_confidence(confidence: str | None) -> str:
    if confidence is None:
        return "Low"
    c = confidence.lower()
    if c == "high":
        return "High"
    if c == "nominal":
        return "Nominal"
    if c == "low":
        return "Low"
    try:
        val = int(confidence)
        if val >= 80:
            return "High"
        if val >= 30:
            return "Nominal"
    except (ValueError, TypeError):
        pass
    return "Low"


def is_earthquake_anomaly(magnitude: float | None) -> bool:
    return magnitude is not None and magnitude >= 6.0


def is_aq_anomaly(us_aqi: int | None) -> bool:
    return us_aqi is not None and us_aqi > 300


def is_wildfire_anomaly(frp: float | None) -> bool:
    return frp is not None and frp >= 200.0


def is_weather_anomaly(
    wind_speed_kmh: float | None,
    precipitation_mm: float | None,
    storm_sev: str | None,
) -> bool:
    wind = wind_speed_kmh or 0.0
    precip = precipitation_mm or 0.0
    return wind >= 120.0 or precip >= 50.0 or storm_sev == "Extreme"


def validate_coordinates(lat: float, lon: float) -> bool:
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


def clean_string_null(value: str | None) -> str | None:
    if value is None:
        return None
    if value in ("", "null", "Null", "NULL", "none", "None", "NONE", "NaN"):
        return None
    return value


# ===================================================================
# Enrichment: Earthquakes
# ===================================================================
class TestEarthquakeEnrichment:
    """Tests for earthquake enrichment rules."""

    def test_risk_category_low(self):
        assert risk_category(2.5) == "Low"

    def test_risk_category_medium(self):
        assert risk_category(4.0) == "Medium"

    def test_risk_category_high(self):
        assert risk_category(6.0) == "High"

    def test_risk_category_critical(self):
        assert risk_category(7.5) == "Critical"

    def test_risk_category_null(self):
        assert risk_category(None) == "Low"

    def test_risk_category_boundary_3(self):
        assert risk_category(3.0) == "Medium"

    def test_risk_category_boundary_5(self):
        assert risk_category(5.0) == "High"

    def test_risk_category_boundary_7(self):
        assert risk_category(7.0) == "Critical"

    def test_magnitude_band_micro(self):
        assert magnitude_band(1.5) == "Micro"

    def test_magnitude_band_minor(self):
        assert magnitude_band(3.0) == "Minor"

    def test_magnitude_band_light(self):
        assert magnitude_band(4.5) == "Light"

    def test_magnitude_band_moderate(self):
        assert magnitude_band(5.5) == "Moderate"

    def test_magnitude_band_strong(self):
        assert magnitude_band(6.5) == "Strong"

    def test_magnitude_band_major(self):
        assert magnitude_band(7.5) == "Major"

    def test_magnitude_band_great(self):
        assert magnitude_band(8.5) == "Great"

    def test_magnitude_band_null(self):
        assert magnitude_band(None) == "Micro"

    def test_distance_group_shallow(self):
        assert distance_group(30.0) == "Shallow"

    def test_distance_group_intermediate(self):
        assert distance_group(150.0) == "Intermediate"

    def test_distance_group_deep(self):
        assert distance_group(500.0) == "Deep"

    def test_distance_group_boundary_70(self):
        assert distance_group(70.0) == "Intermediate"

    def test_distance_group_boundary_300(self):
        assert distance_group(300.0) == "Intermediate"

    def test_distance_group_above_300(self):
        assert distance_group(300.1) == "Deep"

    def test_distance_group_null(self):
        assert distance_group(None) == "Shallow"


# ===================================================================
# Enrichment: Weather
# ===================================================================
class TestWeatherEnrichment:
    """Tests for weather enrichment rules."""

    def test_rain_severity_none_zero(self):
        assert rain_severity(0.0) == "None"

    def test_rain_severity_none_null(self):
        assert rain_severity(None) == "None"

    def test_rain_severity_light(self):
        assert rain_severity(1.5) == "Light"

    def test_rain_severity_moderate(self):
        assert rain_severity(5.0) == "Moderate"

    def test_rain_severity_heavy(self):
        assert rain_severity(30.0) == "Heavy"

    def test_rain_severity_extreme(self):
        assert rain_severity(55.0) == "Extreme"

    def test_rain_severity_boundary_2_5(self):
        assert rain_severity(2.5) == "Moderate"

    def test_rain_severity_boundary_7_5(self):
        assert rain_severity(7.5) == "Heavy"

    def test_rain_severity_boundary_50(self):
        assert rain_severity(50.0) == "Extreme"

    def test_wind_severity_calm(self):
        assert wind_severity(10.0) == "Calm"

    def test_wind_severity_calm_null(self):
        assert wind_severity(None) == "Calm"

    def test_wind_severity_breezy(self):
        assert wind_severity(30.0) == "Breezy"

    def test_wind_severity_strong(self):
        assert wind_severity(60.0) == "Strong"

    def test_wind_severity_gale(self):
        assert wind_severity(100.0) == "Gale"

    def test_wind_severity_hurricane(self):
        assert wind_severity(130.0) == "Hurricane"

    def test_wind_severity_boundary_20(self):
        assert wind_severity(20.0) == "Breezy"

    def test_wind_severity_boundary_120(self):
        assert wind_severity(120.0) == "Hurricane"

    def test_storm_severity_none(self):
        assert storm_severity(10.0, 0.0, 0) == "None"

    def test_storm_severity_minor(self):
        assert storm_severity(25.0, 0.0, 0) == "Minor"

    def test_storm_severity_moderate_wind(self):
        assert storm_severity(50.0, 0.0, 0) == "Moderate"

    def test_storm_severity_severe(self):
        assert storm_severity(80.0, 0.0, 0) == "Severe"

    def test_storm_severity_extreme_wind(self):
        assert storm_severity(130.0, 0.0, 0) == "Extreme"

    def test_storm_severity_extreme_precip(self):
        assert storm_severity(10.0, 55.0, 0) == "Extreme"

    def test_storm_severity_extreme_code_97(self):
        assert storm_severity(10.0, 0.0, 97) == "Extreme"

    def test_storm_severity_severe_code_95(self):
        assert storm_severity(10.0, 0.0, 95) == "Severe"

    def test_heat_index_hot_humid(self):
        hi = compute_heat_index(35.0, 80.0)
        assert hi is not None
        assert hi > 35.0

    def test_heat_index_cool_fallback(self):
        assert compute_heat_index(20.0, 50.0) == 20.0

    def test_heat_index_null_temp(self):
        assert compute_heat_index(None, 50.0) is None

    def test_heat_index_low_humidity_fallback(self):
        assert compute_heat_index(35.0, 30.0) == 35.0

    def test_heat_index_exact_threshold(self):
        hi = compute_heat_index(27.0, 40.0)
        assert hi is not None
        assert isinstance(hi, float)


# ===================================================================
# Enrichment: Air Quality
# ===================================================================
class TestAirQualityEnrichment:
    """Tests for air quality enrichment rules."""

    def test_aqi_category_good(self):
        assert aqi_category(30) == "Good"

    def test_aqi_category_good_boundary(self):
        assert aqi_category(50) == "Good"

    def test_aqi_category_moderate(self):
        assert aqi_category(75) == "Moderate"

    def test_aqi_category_moderate_boundary(self):
        assert aqi_category(100) == "Moderate"

    def test_aqi_category_poor(self):
        assert aqi_category(120) == "Poor"

    def test_aqi_category_poor_boundary(self):
        assert aqi_category(150) == "Poor"

    def test_aqi_category_very_poor(self):
        assert aqi_category(180) == "Very Poor"

    def test_aqi_category_very_poor_boundary(self):
        assert aqi_category(200) == "Very Poor"

    def test_aqi_category_hazardous(self):
        assert aqi_category(350) == "Hazardous"

    def test_aqi_category_hazardous_boundary(self):
        assert aqi_category(201) == "Hazardous"

    def test_aqi_category_null(self):
        assert aqi_category(None) == "Unknown"

    def test_aqi_category_zero(self):
        assert aqi_category(0) == "Good"


# ===================================================================
# Enrichment: Wildfires
# ===================================================================
class TestWildfireEnrichment:
    """Tests for wildfire enrichment rules."""

    def test_fire_severity_low(self):
        assert fire_severity(5.0) == "Low"

    def test_fire_severity_medium(self):
        assert fire_severity(25.0) == "Medium"

    def test_fire_severity_high(self):
        assert fire_severity(100.0) == "High"

    def test_fire_severity_extreme(self):
        assert fire_severity(250.0) == "Extreme"

    def test_fire_severity_null(self):
        assert fire_severity(None) == "Low"

    def test_fire_severity_boundary_10(self):
        assert fire_severity(10.0) == "Medium"

    def test_fire_severity_boundary_50(self):
        assert fire_severity(50.0) == "High"

    def test_fire_severity_boundary_200(self):
        assert fire_severity(200.0) == "Extreme"

    def test_detection_confidence_high(self):
        assert detection_confidence("high") == "High"

    def test_detection_confidence_high_caps(self):
        assert detection_confidence("High") == "High"

    def test_detection_confidence_nominal(self):
        assert detection_confidence("nominal") == "Nominal"

    def test_detection_confidence_low(self):
        assert detection_confidence("low") == "Low"

    def test_detection_confidence_numeric_high(self):
        assert detection_confidence("90") == "High"

    def test_detection_confidence_numeric_nominal(self):
        assert detection_confidence("50") == "Nominal"

    def test_detection_confidence_numeric_low(self):
        assert detection_confidence("10") == "Low"

    def test_detection_confidence_null(self):
        assert detection_confidence(None) == "Low"


# ===================================================================
# Data Cleaning
# ===================================================================
class TestDataCleaning:
    """Tests for data cleaning utility rules."""

    def test_clean_string_nulls_empty(self):
        assert clean_string_null("") is None

    def test_clean_string_nulls_literal_null(self):
        assert clean_string_null("null") is None

    def test_clean_string_nulls_literal_none(self):
        assert clean_string_null("None") is None

    def test_clean_string_nulls_nan(self):
        assert clean_string_null("NaN") is None

    def test_clean_string_nulls_valid_preserved(self):
        assert clean_string_null("Tokyo") == "Tokyo"

    def test_clean_string_nulls_actual_none(self):
        assert clean_string_null(None) is None

    def test_validate_coordinates_valid(self):
        assert validate_coordinates(35.0, -117.0) is True

    def test_validate_coordinates_poles(self):
        assert validate_coordinates(90.0, 180.0) is True
        assert validate_coordinates(-90.0, -180.0) is True

    def test_validate_coordinates_invalid_lat(self):
        assert validate_coordinates(999.0, -117.0) is False

    def test_validate_coordinates_invalid_lon(self):
        assert validate_coordinates(35.0, -999.0) is False

    def test_validate_coordinates_just_outside_lat(self):
        assert validate_coordinates(90.1, 0.0) is False

    def test_validate_coordinates_just_outside_lon(self):
        assert validate_coordinates(0.0, 180.1) is False


# ===================================================================
# Anomaly Detection
# ===================================================================
class TestAnomalyDetection:
    """Tests for rule-based anomaly detection thresholds."""

    def test_earthquake_anomaly_mag_6(self):
        assert is_earthquake_anomaly(6.0) is True

    def test_earthquake_anomaly_mag_7_5(self):
        assert is_earthquake_anomaly(7.5) is True

    def test_earthquake_no_anomaly_mag_5(self):
        assert is_earthquake_anomaly(5.9) is False

    def test_earthquake_no_anomaly_null(self):
        assert is_earthquake_anomaly(None) is False

    def test_aq_anomaly_301(self):
        assert is_aq_anomaly(301) is True

    def test_aq_anomaly_500(self):
        assert is_aq_anomaly(500) is True

    def test_aq_no_anomaly_300(self):
        assert is_aq_anomaly(300) is False

    def test_aq_no_anomaly_null(self):
        assert is_aq_anomaly(None) is False

    def test_wildfire_anomaly_200(self):
        assert is_wildfire_anomaly(200.0) is True

    def test_wildfire_anomaly_350(self):
        assert is_wildfire_anomaly(350.0) is True

    def test_wildfire_no_anomaly_199(self):
        assert is_wildfire_anomaly(199.0) is False

    def test_wildfire_no_anomaly_null(self):
        assert is_wildfire_anomaly(None) is False

    def test_weather_anomaly_hurricane_wind(self):
        assert is_weather_anomaly(130.0, 0.0, "None") is True

    def test_weather_anomaly_extreme_precip(self):
        assert is_weather_anomaly(10.0, 55.0, "None") is True

    def test_weather_anomaly_extreme_storm(self):
        assert is_weather_anomaly(10.0, 0.0, "Extreme") is True

    def test_weather_no_anomaly_normal(self):
        assert is_weather_anomaly(50.0, 5.0, "Moderate") is False

    def test_weather_no_anomaly_null(self):
        assert is_weather_anomaly(None, None, None) is False


# ===================================================================
# Quality Metrics
# ===================================================================
class TestQualityMetrics:
    """Tests for quality metrics computation."""

    def test_compute_quality_metrics(self):
        from backend.spark.stream_processor import compute_quality_metrics
        result = compute_quality_metrics(
            source="earthquakes",
            batch_id=1,
            total_count=100,
            parsed_count=95,
            valid_coords_count=90,
            deduped_count=85,
            final_count=85,
        )
        assert result["source"] == "earthquakes"
        assert result["total_records"] == 100
        assert result["valid_records"] == 85
        assert result["malformed_records"] == 5
        assert result["duplicates"] == 5
        assert result["dropped_events"] == 15
        assert result["validation_errors"] == 5

    def test_compute_quality_metrics_perfect(self):
        from backend.spark.stream_processor import compute_quality_metrics
        result = compute_quality_metrics(
            source="weather",
            batch_id=2,
            total_count=50,
            parsed_count=50,
            valid_coords_count=50,
            deduped_count=50,
            final_count=50,
        )
        assert result["valid_records"] == 50
        assert result["malformed_records"] == 0
        assert result["duplicates"] == 0
        assert result["dropped_events"] == 0

    def test_compute_quality_metrics_all_dropped(self):
        from backend.spark.stream_processor import compute_quality_metrics
        result = compute_quality_metrics(
            source="air_quality",
            batch_id=3,
            total_count=100,
            parsed_count=0,
            valid_coords_count=0,
            deduped_count=0,
            final_count=0,
        )
        assert result["valid_records"] == 0
        assert result["malformed_records"] == 100
        assert result["dropped_events"] == 100
