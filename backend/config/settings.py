"""
GRIP — Central configuration via environment variables.

Uses pydantic-settings for type-safe, validated configuration.
All values have sensible defaults for the Docker Compose dev environment.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MonitoredLocation:
    """A geographic point to poll weather / air-quality data for."""

    def __init__(self, name: str, latitude: float, longitude: float) -> None:
        self.name = name
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self) -> str:
        return f"MonitoredLocation({self.name}, {self.latitude}, {self.longitude})"


# Pre-configured cities that span major global risk regions.
DEFAULT_MONITORED_LOCATIONS: list[MonitoredLocation] = [
    MonitoredLocation("New York", 40.7128, -74.0060),
    MonitoredLocation("London", 51.5074, -0.1278),
    MonitoredLocation("Tokyo", 35.6762, 139.6503),
    MonitoredLocation("Mumbai", 19.0760, 72.8777),
    MonitoredLocation("São Paulo", -23.5505, -46.6333),
]


class Settings(BaseSettings):
    """Application-wide settings loaded from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- PostgreSQL ---
    postgres_host: str = Field(default="postgres")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="grip_db")
    postgres_user: str = Field(default="grip")
    postgres_password: str = Field(default="grip_password")

    # --- Kafka ---
    kafka_bootstrap_servers: str = Field(default="kafka:9092")

    # --- NASA FIRMS ---
    nasa_firms_map_key: str = Field(default="")

    # --- Polling intervals (seconds) ---
    usgs_poll_interval: int = Field(default=60)
    weather_poll_interval: int = Field(default=300)
    air_quality_poll_interval: int = Field(default=300)
    wildfire_poll_interval: int = Field(default=600)

    # --- Kafka topic names ---
    kafka_topic_earthquakes: str = Field(default="earthquakes")
    kafka_topic_weather: str = Field(default="weather")
    kafka_topic_air_quality: str = Field(default="air_quality")
    kafka_topic_wildfires: str = Field(default="wildfires")

    # --- Logging ---
    log_level: str = Field(default="INFO")

    # --- Phase 3: Analytics & Intelligence ---
    risk_score_interval_seconds: int = Field(default=60)
    alert_check_interval_seconds: int = Field(default=30)
    forecast_interval_seconds: int = Field(default=300)
    analytics_snapshot_interval_seconds: int = Field(default=120)
    websocket_poll_interval_seconds: int = Field(default=10)
    rate_limit_per_minute: int = Field(default=120)
    spark_master_url: str = Field(default="http://spark-master:8080")
    risk_lookback_hours: int = Field(default=24)
    forecast_min_data_points: int = Field(default=10)

    # --- Computed helpers ---
    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}"
            f"/{self.postgres_db}"
        )

    @property
    def monitored_locations(self) -> list[MonitoredLocation]:
        return DEFAULT_MONITORED_LOCATIONS


# Singleton – import this everywhere.
settings = Settings()
