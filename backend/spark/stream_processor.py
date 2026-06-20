"""
GRIP — Spark Structured Streaming processor (Phase 2).

Full ETL pipeline for all four Kafka topics:
  1. Parse JSON from Kafka
  2. Write raw records to *_raw tables
  3. Clean: null handling, type validation, timestamp normalization,
     coordinate validation
  4. Deduplicate within each micro-batch
  5. Validate against schema constraints
  6. Enrich with computed columns (risk categories, severity, etc.)
  7. Detect anomalies via rule-based thresholds
  8. Write processed records to *_processed tables
  9. Write anomaly events to anomaly_events table
  10. Collect data quality and pipeline metrics

This script is submitted to Spark via spark-submit inside the
spark-job Docker container.
"""

from __future__ import annotations

import json
import math
import os
import time
import traceback

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration (read from environment inside the Spark container)
# ---------------------------------------------------------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "grip_db")
PG_USER = os.getenv("POSTGRES_USER", "grip")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "grip_password")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

JDBC_BATCH_SIZE = 1000
JDBC_MAX_RETRIES = 3
JDBC_RETRY_DELAY = 2

# ---------------------------------------------------------------------------
# Schemas matching the Kafka JSON payloads
# ---------------------------------------------------------------------------
EARTHQUAKE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("magnitude", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("event_time", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("depth_km", DoubleType(), True),
        StructField("tsunami", BooleanType(), True),
        StructField("significance", IntegerType(), True),
        StructField("status", StringType(), True),
    ]
)

WEATHER_SCHEMA = StructType(
    [
        StructField("location_name", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("observed_at", StringType(), False),
    ]
)

AIR_QUALITY_SCHEMA = StructType(
    [
        StructField("location_name", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("us_aqi", IntegerType(), True),
        StructField("pm2_5", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("ozone", DoubleType(), True),
        StructField("nitrogen_dioxide", DoubleType(), True),
        StructField("carbon_monoxide", DoubleType(), True),
        StructField("sulphur_dioxide", DoubleType(), True),
        StructField("observed_at", StringType(), False),
    ]
)

WILDFIRE_SCHEMA = StructType(
    [
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("brightness", DoubleType(), True),
        StructField("scan", DoubleType(), True),
        StructField("track", DoubleType(), True),
        StructField("acq_date", StringType(), False),
        StructField("acq_time", StringType(), True),
        StructField("satellite", StringType(), True),
        StructField("instrument", StringType(), True),
        StructField("confidence", StringType(), True),
        StructField("frp", DoubleType(), True),
        StructField("daynight", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# JDBC writer with retry logic
# ---------------------------------------------------------------------------
def write_jdbc_with_retry(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame to PostgreSQL with exponential backoff retries."""
    for attempt in range(1, JDBC_MAX_RETRIES + 1):
        try:
            df.write.jdbc(
                url=JDBC_URL,
                table=table_name,
                mode="append",
                properties={
                    **JDBC_PROPERTIES,
                    "batchsize": str(JDBC_BATCH_SIZE),
                    "reWriteBatchedInserts": "true",
                },
            )
            return
        except Exception as exc:
            if attempt == JDBC_MAX_RETRIES:
                raise
            print(
                f"[{table_name}] JDBC write attempt {attempt}/{JDBC_MAX_RETRIES} "
                f"failed: {exc}. Retrying in {JDBC_RETRY_DELAY * attempt}s..."
            )
            time.sleep(JDBC_RETRY_DELAY * attempt)


# ---------------------------------------------------------------------------
# Data cleaning utilities
# ---------------------------------------------------------------------------
def clean_string_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    """Replace empty strings and literal 'null'/'None' with actual NULL."""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(
                    (F.col(col_name) == "") |
                    (F.lower(F.col(col_name)) == "null") |
                    (F.lower(F.col(col_name)) == "none") |
                    (F.col(col_name) == "NaN"),
                    F.lit(None),
                ).otherwise(F.col(col_name)),
            )
    return df


def validate_coordinates(df: DataFrame) -> DataFrame:
    """Filter out rows with coordinates outside valid ranges."""
    return df.filter(
        (F.col("latitude") >= -90.0) & (F.col("latitude") <= 90.0) &
        (F.col("longitude") >= -180.0) & (F.col("longitude") <= 180.0)
    )


def normalize_timestamp_column(df: DataFrame, col_name: str) -> DataFrame:
    """Parse various timestamp formats into TimestampType."""
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm"),
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col(col_name)),
        ),
    )


# ---------------------------------------------------------------------------
# Enrichment: Earthquakes
# ---------------------------------------------------------------------------
def enrich_earthquakes(df: DataFrame) -> DataFrame:
    """Add risk_category, magnitude_band, and distance_group columns."""
    df = df.withColumn(
        "risk_category",
        F.when(F.col("magnitude") >= 7.0, F.lit("Critical"))
        .when(F.col("magnitude") >= 5.0, F.lit("High"))
        .when(F.col("magnitude") >= 3.0, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )

    df = df.withColumn(
        "magnitude_band",
        F.when(F.col("magnitude") >= 8.0, F.lit("Great"))
        .when(F.col("magnitude") >= 7.0, F.lit("Major"))
        .when(F.col("magnitude") >= 6.0, F.lit("Strong"))
        .when(F.col("magnitude") >= 5.0, F.lit("Moderate"))
        .when(F.col("magnitude") >= 4.0, F.lit("Light"))
        .when(F.col("magnitude") >= 2.0, F.lit("Minor"))
        .otherwise(F.lit("Micro")),
    )

    df = df.withColumn(
        "distance_group",
        F.when(F.col("depth_km") > 300.0, F.lit("Deep"))
        .when(F.col("depth_km") >= 70.0, F.lit("Intermediate"))
        .otherwise(F.lit("Shallow")),
    )

    return df


# ---------------------------------------------------------------------------
# Enrichment: Weather
# ---------------------------------------------------------------------------
def compute_heat_index(df: DataFrame) -> DataFrame:
    """
    Compute heat index using the simplified Steadman formula.
    HI = -8.785 + 1.611*T + 2.339*RH - 0.1461*T*RH
         - 0.01231*T^2 - 0.01642*RH^2 + 0.002212*T^2*RH
         + 0.0007255*T*RH^2 - 0.000003582*T^2*RH^2
    Only valid when temp >= 27°C and humidity >= 40%.
    Falls back to temperature when conditions are not met.
    """
    t = F.col("temperature_c")
    rh = F.col("humidity_pct")

    heat_index_expr = (
        F.lit(-8.78469475556)
        + F.lit(1.61139411) * t
        + F.lit(2.33854883889) * rh
        - F.lit(0.14611605) * t * rh
        - F.lit(0.012308094) * t * t
        - F.lit(0.0164248277778) * rh * rh
        + F.lit(0.002211732) * t * t * rh
        + F.lit(0.00072546) * t * rh * rh
        - F.lit(0.000003582) * t * t * rh * rh
    )

    return df.withColumn(
        "heat_index",
        F.when(
            (t >= 27.0) & (rh >= 40.0) &
            t.isNotNull() & rh.isNotNull(),
            F.round(heat_index_expr, 2),
        ).otherwise(t),
    )


def enrich_weather(df: DataFrame) -> DataFrame:
    """Add heat_index, rain_severity, wind_severity, storm_severity."""
    df = compute_heat_index(df)

    df = df.withColumn(
        "rain_severity",
        F.when(F.col("precipitation_mm").isNull(), F.lit("None"))
        .when(F.col("precipitation_mm") <= 0.0, F.lit("None"))
        .when(F.col("precipitation_mm") < 2.5, F.lit("Light"))
        .when(F.col("precipitation_mm") < 7.5, F.lit("Moderate"))
        .when(F.col("precipitation_mm") < 50.0, F.lit("Heavy"))
        .otherwise(F.lit("Extreme")),
    )

    df = df.withColumn(
        "wind_severity",
        F.when(F.col("wind_speed_kmh").isNull(), F.lit("Calm"))
        .when(F.col("wind_speed_kmh") < 20.0, F.lit("Calm"))
        .when(F.col("wind_speed_kmh") < 40.0, F.lit("Breezy"))
        .when(F.col("wind_speed_kmh") < 75.0, F.lit("Strong"))
        .when(F.col("wind_speed_kmh") < 120.0, F.lit("Gale"))
        .otherwise(F.lit("Hurricane")),
    )

    # Storm severity: combined metric from wind, precipitation, and weather code
    # Weather codes 95-99 indicate thunderstorms in WMO standard
    df = df.withColumn(
        "storm_severity",
        F.when(
            (F.col("wind_speed_kmh") >= 120.0) |
            (F.col("precipitation_mm") >= 50.0) |
            (F.col("weather_code").isin(97, 99)),
            F.lit("Extreme"),
        )
        .when(
            (F.col("wind_speed_kmh") >= 75.0) |
            (F.col("precipitation_mm") >= 25.0) |
            (F.col("weather_code").isin(95, 96)),
            F.lit("Severe"),
        )
        .when(
            (F.col("wind_speed_kmh") >= 40.0) |
            (F.col("precipitation_mm") >= 7.5) |
            (F.col("weather_code").isin(65, 67, 75, 77, 82, 86)),
            F.lit("Moderate"),
        )
        .when(
            (F.col("wind_speed_kmh") >= 20.0) |
            (F.col("precipitation_mm") >= 2.5),
            F.lit("Minor"),
        )
        .otherwise(F.lit("None")),
    )

    return df


# ---------------------------------------------------------------------------
# Enrichment: Air Quality
# ---------------------------------------------------------------------------
def enrich_air_quality(df: DataFrame) -> DataFrame:
    """Add aqi_category based on US AQI breakpoints."""
    return df.withColumn(
        "aqi_category",
        F.when(F.col("us_aqi").isNull(), F.lit("Unknown"))
        .when(F.col("us_aqi") <= 50, F.lit("Good"))
        .when(F.col("us_aqi") <= 100, F.lit("Moderate"))
        .when(F.col("us_aqi") <= 150, F.lit("Poor"))
        .when(F.col("us_aqi") <= 200, F.lit("Very Poor"))
        .otherwise(F.lit("Hazardous")),
    )


# ---------------------------------------------------------------------------
# Enrichment: Wildfires
# ---------------------------------------------------------------------------
def enrich_wildfires(df: DataFrame) -> DataFrame:
    """Add fire_severity and detection_confidence columns."""
    df = df.withColumn(
        "fire_severity",
        F.when(F.col("frp").isNull(), F.lit("Low"))
        .when(F.col("frp") >= 200.0, F.lit("Extreme"))
        .when(F.col("frp") >= 50.0, F.lit("High"))
        .when(F.col("frp") >= 10.0, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )

    df = df.withColumn(
        "detection_confidence",
        F.when(F.lower(F.col("confidence")) == "high", F.lit("High"))
        .when(F.lower(F.col("confidence")) == "nominal", F.lit("Nominal"))
        .when(F.lower(F.col("confidence")) == "low", F.lit("Low"))
        .when(F.col("confidence").cast("int") >= 80, F.lit("High"))
        .when(F.col("confidence").cast("int") >= 30, F.lit("Nominal"))
        .otherwise(F.lit("Low")),
    )

    return df


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------
ANOMALY_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("description", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("raw_data", StringType(), True),
    ]
)


def detect_earthquake_anomalies(df: DataFrame) -> DataFrame:
    """Detect anomalous earthquakes: magnitude >= 6.0."""
    anomalies = df.filter(F.col("magnitude") >= 6.0)
    if anomalies.rdd.isEmpty():
        return None

    return anomalies.select(
        F.lit("earthquakes").alias("source"),
        F.lit("high_magnitude_earthquake").alias("event_type"),
        F.when(F.col("magnitude") >= 8.0, F.lit("Critical"))
        .when(F.col("magnitude") >= 7.0, F.lit("Critical"))
        .otherwise(F.lit("High"))
        .alias("severity"),
        F.concat(
            F.lit("Earthquake magnitude "),
            F.col("magnitude").cast("string"),
            F.lit(" at "),
            F.coalesce(F.col("place"), F.lit("unknown location")),
        ).alias("description"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("event_time"),
        F.to_json(F.struct("*")).alias("raw_data"),
    )


def detect_weather_anomalies(df: DataFrame) -> DataFrame:
    """Detect anomalous weather: hurricane winds, extreme precip, extreme storms."""
    anomalies = df.filter(
        (F.col("wind_speed_kmh") >= 120.0) |
        (F.col("precipitation_mm") >= 50.0) |
        (F.col("storm_severity") == "Extreme")
    )
    if anomalies.rdd.isEmpty():
        return None

    return anomalies.select(
        F.lit("weather").alias("source"),
        F.lit("severe_weather_event").alias("event_type"),
        F.when(F.col("storm_severity") == "Extreme", F.lit("Critical"))
        .otherwise(F.lit("High"))
        .alias("severity"),
        F.concat(
            F.lit("Severe weather at "),
            F.coalesce(F.col("location_name"), F.lit("unknown")),
            F.lit(": wind="),
            F.coalesce(F.col("wind_speed_kmh").cast("string"), F.lit("N/A")),
            F.lit("km/h, precip="),
            F.coalesce(F.col("precipitation_mm").cast("string"), F.lit("N/A")),
            F.lit("mm"),
        ).alias("description"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("observed_at").alias("event_time"),
        F.to_json(F.struct("*")).alias("raw_data"),
    )


def detect_air_quality_anomalies(df: DataFrame) -> DataFrame:
    """Detect anomalous air quality: AQI > 300."""
    anomalies = df.filter(F.col("us_aqi") > 300)
    if anomalies.rdd.isEmpty():
        return None

    return anomalies.select(
        F.lit("air_quality").alias("source"),
        F.lit("hazardous_air_quality").alias("event_type"),
        F.when(F.col("us_aqi") > 500, F.lit("Critical"))
        .otherwise(F.lit("High"))
        .alias("severity"),
        F.concat(
            F.lit("Hazardous AQI "),
            F.col("us_aqi").cast("string"),
            F.lit(" at "),
            F.coalesce(F.col("location_name"), F.lit("unknown")),
        ).alias("description"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("observed_at").alias("event_time"),
        F.to_json(F.struct("*")).alias("raw_data"),
    )


def detect_wildfire_anomalies(df: DataFrame) -> DataFrame:
    """Detect anomalous wildfires: FRP >= 200 (Extreme severity)."""
    anomalies = df.filter(F.col("frp") >= 200.0)
    if anomalies.rdd.isEmpty():
        return None

    return anomalies.select(
        F.lit("wildfires").alias("source"),
        F.lit("extreme_wildfire").alias("event_type"),
        F.when(F.col("frp") >= 500.0, F.lit("Critical"))
        .otherwise(F.lit("High"))
        .alias("severity"),
        F.concat(
            F.lit("Extreme wildfire FRP="),
            F.coalesce(F.col("frp").cast("string"), F.lit("N/A")),
            F.lit(" at lat="),
            F.col("latitude").cast("string"),
            F.lit(", lon="),
            F.col("longitude").cast("string"),
        ).alias("description"),
        F.col("latitude"),
        F.col("longitude"),
        F.to_timestamp(F.col("acq_date").cast("string")).alias("event_time"),
        F.to_json(F.struct("*")).alias("raw_data"),
    )


# ---------------------------------------------------------------------------
# Data quality metrics helpers
# ---------------------------------------------------------------------------
def compute_quality_metrics(
    source: str,
    batch_id: int,
    total_count: int,
    parsed_count: int,
    valid_coords_count: int,
    deduped_count: int,
    final_count: int,
) -> dict:
    """Compute data quality metrics for a batch."""
    malformed = total_count - parsed_count
    coord_invalid = parsed_count - valid_coords_count
    duplicates = valid_coords_count - deduped_count
    dropped = total_count - final_count
    validation_errors = coord_invalid
    missing_fields = malformed

    return {
        "source": source,
        "batch_id": batch_id,
        "total_records": total_count,
        "valid_records": final_count,
        "duplicates": duplicates,
        "missing_fields": missing_fields,
        "malformed_records": malformed,
        "dropped_events": dropped,
        "validation_errors": validation_errors,
    }


def write_quality_metrics(spark: SparkSession, metrics: dict) -> None:
    """Write quality metrics to PostgreSQL."""
    try:
        metrics_df = spark.createDataFrame([metrics])
        write_jdbc_with_retry(metrics_df, "data_quality_metrics")
    except Exception as exc:
        print(f"[data_quality_metrics] Failed to write: {exc}")


def write_pipeline_metrics(
    spark: SparkSession,
    source: str,
    batch_id: int,
    records_count: int,
    processing_time_ms: float,
    db_write_latency_ms: float,
) -> None:
    """Write pipeline performance metrics to PostgreSQL."""
    try:
        records_per_minute = (records_count / (processing_time_ms / 60000.0)) if processing_time_ms > 0 else 0.0
        metrics = {
            "source": source,
            "batch_id": batch_id,
            "records_count": records_count,
            "processing_time_ms": round(processing_time_ms, 2),
            "kafka_lag_ms": None,
            "db_write_latency_ms": round(db_write_latency_ms, 2),
            "records_per_minute": round(records_per_minute, 2),
        }
        metrics_df = spark.createDataFrame([metrics])
        write_jdbc_with_retry(metrics_df, "pipeline_metrics")
    except Exception as exc:
        print(f"[pipeline_metrics] Failed to write: {exc}")


# ---------------------------------------------------------------------------
# Per-topic foreachBatch processors
# ---------------------------------------------------------------------------
def make_earthquake_processor(spark: SparkSession):
    """Create the foreachBatch handler for earthquakes."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_start = time.time()
        source = "earthquakes"

        try:
            total_count = batch_df.count()

            # --- Raw write ---
            raw_df = batch_df.withColumn(
                "event_time", F.to_timestamp(F.col("event_time"))
            )
            raw_write_start = time.time()
            write_jdbc_with_retry(raw_df, "earthquakes_raw")
            raw_write_time = (time.time() - raw_write_start) * 1000

            # --- Clean ---
            cleaned = batch_df
            cleaned = clean_string_nulls(cleaned, ["place", "status", "event_id"])
            cleaned = normalize_timestamp_column(cleaned, "event_time")

            # Filter out rows where parsing failed
            cleaned = cleaned.filter(F.col("event_time").isNotNull())
            cleaned = cleaned.filter(F.col("event_id").isNotNull())
            parsed_count = cleaned.count()

            # Validate coordinates
            cleaned = validate_coordinates(cleaned)
            valid_coords_count = cleaned.count()

            # Deduplicate on event_id
            cleaned = cleaned.dropDuplicates(["event_id"])
            deduped_count = cleaned.count()

            # Replace null magnitude with 0.0 for enrichment safety
            cleaned = cleaned.withColumn(
                "magnitude",
                F.coalesce(F.col("magnitude"), F.lit(0.0)),
            )
            cleaned = cleaned.withColumn(
                "depth_km",
                F.coalesce(F.col("depth_km"), F.lit(0.0)),
            )

            # --- Enrich ---
            enriched = enrich_earthquakes(cleaned)
            final_count = enriched.count()

            # --- Anomaly detection ---
            anomalies = detect_earthquake_anomalies(enriched)
            if anomalies is not None:
                write_jdbc_with_retry(anomalies, "anomaly_events")

            # --- Processed write ---
            proc_write_start = time.time()
            proc_columns = [
                "event_id", "magnitude", "place", "event_time",
                "latitude", "longitude", "depth_km", "tsunami",
                "significance", "status", "risk_category",
                "magnitude_band", "distance_group",
            ]
            write_jdbc_with_retry(enriched.select(*proc_columns), "earthquakes_processed")
            proc_write_time = (time.time() - proc_write_start) * 1000

            processing_time = (time.time() - batch_start) * 1000

            # --- Metrics ---
            quality = compute_quality_metrics(
                source, batch_id, total_count, parsed_count,
                valid_coords_count, deduped_count, final_count,
            )
            write_quality_metrics(spark, quality)
            write_pipeline_metrics(
                spark, source, batch_id, final_count,
                processing_time, raw_write_time + proc_write_time,
            )

            print(
                f"[{source}] Batch {batch_id}: {total_count} raw → "
                f"{final_count} processed in {processing_time:.0f}ms"
            )

        except Exception as exc:
            print(f"[{source}] Batch {batch_id} FAILED: {exc}")
            traceback.print_exc()

    return process_batch


def make_weather_processor(spark: SparkSession):
    """Create the foreachBatch handler for weather."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_start = time.time()
        source = "weather"

        try:
            total_count = batch_df.count()

            # --- Raw write ---
            raw_df = batch_df.withColumn(
                "observed_at", F.to_timestamp(F.col("observed_at"))
            )
            raw_write_start = time.time()
            write_jdbc_with_retry(raw_df, "weather_raw")
            raw_write_time = (time.time() - raw_write_start) * 1000

            # --- Clean ---
            cleaned = batch_df
            cleaned = clean_string_nulls(cleaned, ["location_name"])
            cleaned = normalize_timestamp_column(cleaned, "observed_at")

            cleaned = cleaned.filter(F.col("observed_at").isNotNull())
            cleaned = cleaned.filter(F.col("location_name").isNotNull())
            parsed_count = cleaned.count()

            # Validate coordinates
            cleaned = validate_coordinates(cleaned)
            valid_coords_count = cleaned.count()

            # Deduplicate on location + timestamp
            cleaned = cleaned.dropDuplicates(["location_name", "observed_at"])
            deduped_count = cleaned.count()

            # Default null numerics for enrichment
            cleaned = cleaned.withColumn(
                "temperature_c", F.coalesce(F.col("temperature_c"), F.lit(None).cast(DoubleType()))
            )
            cleaned = cleaned.withColumn(
                "humidity_pct", F.coalesce(F.col("humidity_pct"), F.lit(None).cast(DoubleType()))
            )
            cleaned = cleaned.withColumn(
                "wind_speed_kmh", F.coalesce(F.col("wind_speed_kmh"), F.lit(0.0))
            )
            cleaned = cleaned.withColumn(
                "precipitation_mm", F.coalesce(F.col("precipitation_mm"), F.lit(0.0))
            )

            # --- Enrich ---
            enriched = enrich_weather(cleaned)
            final_count = enriched.count()

            # --- Anomaly detection ---
            anomalies = detect_weather_anomalies(enriched)
            if anomalies is not None:
                write_jdbc_with_retry(anomalies, "anomaly_events")

            # --- Processed write ---
            proc_write_start = time.time()
            proc_columns = [
                "location_name", "latitude", "longitude", "temperature_c",
                "humidity_pct", "wind_speed_kmh", "precipitation_mm",
                "weather_code", "observed_at", "heat_index",
                "rain_severity", "wind_severity", "storm_severity",
            ]
            write_jdbc_with_retry(enriched.select(*proc_columns), "weather_processed")
            proc_write_time = (time.time() - proc_write_start) * 1000

            processing_time = (time.time() - batch_start) * 1000

            # --- Metrics ---
            quality = compute_quality_metrics(
                source, batch_id, total_count, parsed_count,
                valid_coords_count, deduped_count, final_count,
            )
            write_quality_metrics(spark, quality)
            write_pipeline_metrics(
                spark, source, batch_id, final_count,
                processing_time, raw_write_time + proc_write_time,
            )

            print(
                f"[{source}] Batch {batch_id}: {total_count} raw → "
                f"{final_count} processed in {processing_time:.0f}ms"
            )

        except Exception as exc:
            print(f"[{source}] Batch {batch_id} FAILED: {exc}")
            traceback.print_exc()

    return process_batch


def make_air_quality_processor(spark: SparkSession):
    """Create the foreachBatch handler for air quality."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_start = time.time()
        source = "air_quality"

        try:
            total_count = batch_df.count()

            # --- Raw write ---
            raw_df = batch_df.withColumn(
                "observed_at", F.to_timestamp(F.col("observed_at"))
            )
            raw_write_start = time.time()
            write_jdbc_with_retry(raw_df, "air_quality_raw")
            raw_write_time = (time.time() - raw_write_start) * 1000

            # --- Clean ---
            cleaned = batch_df
            cleaned = clean_string_nulls(cleaned, ["location_name"])
            cleaned = normalize_timestamp_column(cleaned, "observed_at")

            cleaned = cleaned.filter(F.col("observed_at").isNotNull())
            cleaned = cleaned.filter(F.col("location_name").isNotNull())
            parsed_count = cleaned.count()

            # Validate coordinates
            cleaned = validate_coordinates(cleaned)
            valid_coords_count = cleaned.count()

            # Deduplicate on location + timestamp
            cleaned = cleaned.dropDuplicates(["location_name", "observed_at"])
            deduped_count = cleaned.count()

            # --- Enrich ---
            enriched = enrich_air_quality(cleaned)
            final_count = enriched.count()

            # --- Anomaly detection ---
            anomalies = detect_air_quality_anomalies(enriched)
            if anomalies is not None:
                write_jdbc_with_retry(anomalies, "anomaly_events")

            # --- Processed write ---
            proc_write_start = time.time()
            proc_columns = [
                "location_name", "latitude", "longitude", "us_aqi",
                "pm2_5", "pm10", "ozone", "nitrogen_dioxide",
                "carbon_monoxide", "sulphur_dioxide", "observed_at",
                "aqi_category",
            ]
            write_jdbc_with_retry(enriched.select(*proc_columns), "air_quality_processed")
            proc_write_time = (time.time() - proc_write_start) * 1000

            processing_time = (time.time() - batch_start) * 1000

            # --- Metrics ---
            quality = compute_quality_metrics(
                source, batch_id, total_count, parsed_count,
                valid_coords_count, deduped_count, final_count,
            )
            write_quality_metrics(spark, quality)
            write_pipeline_metrics(
                spark, source, batch_id, final_count,
                processing_time, raw_write_time + proc_write_time,
            )

            print(
                f"[{source}] Batch {batch_id}: {total_count} raw → "
                f"{final_count} processed in {processing_time:.0f}ms"
            )

        except Exception as exc:
            print(f"[{source}] Batch {batch_id} FAILED: {exc}")
            traceback.print_exc()

    return process_batch


def make_wildfire_processor(spark: SparkSession):
    """Create the foreachBatch handler for wildfires."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_start = time.time()
        source = "wildfires"

        try:
            total_count = batch_df.count()

            # --- Raw write ---
            raw_df = batch_df.withColumn(
                "acq_date", F.to_date(F.col("acq_date"), "yyyy-MM-dd")
            )
            raw_write_start = time.time()
            write_jdbc_with_retry(raw_df, "wildfires_raw")
            raw_write_time = (time.time() - raw_write_start) * 1000

            # --- Clean ---
            cleaned = batch_df
            cleaned = clean_string_nulls(
                cleaned,
                ["satellite", "instrument", "confidence", "daynight", "acq_time"],
            )

            # Parse acq_date
            cleaned = cleaned.withColumn(
                "acq_date", F.to_date(F.col("acq_date"), "yyyy-MM-dd")
            )
            cleaned = cleaned.filter(F.col("acq_date").isNotNull())
            parsed_count = cleaned.count()

            # Validate coordinates
            cleaned = validate_coordinates(cleaned)
            valid_coords_count = cleaned.count()

            # Deduplicate on coordinates + date + time
            cleaned = cleaned.dropDuplicates(
                ["latitude", "longitude", "acq_date", "acq_time"]
            )
            deduped_count = cleaned.count()

            # Default FRP to 0 for enrichment
            cleaned = cleaned.withColumn(
                "frp", F.coalesce(F.col("frp"), F.lit(0.0))
            )

            # --- Enrich ---
            enriched = enrich_wildfires(cleaned)
            final_count = enriched.count()

            # --- Anomaly detection ---
            anomalies = detect_wildfire_anomalies(enriched)
            if anomalies is not None:
                write_jdbc_with_retry(anomalies, "anomaly_events")

            # --- Processed write ---
            proc_write_start = time.time()
            proc_columns = [
                "latitude", "longitude", "brightness", "scan", "track",
                "acq_date", "acq_time", "satellite", "instrument",
                "confidence", "frp", "daynight",
                "fire_severity", "detection_confidence",
            ]
            write_jdbc_with_retry(enriched.select(*proc_columns), "wildfires_processed")
            proc_write_time = (time.time() - proc_write_start) * 1000

            processing_time = (time.time() - batch_start) * 1000

            # --- Metrics ---
            quality = compute_quality_metrics(
                source, batch_id, total_count, parsed_count,
                valid_coords_count, deduped_count, final_count,
            )
            write_quality_metrics(spark, quality)
            write_pipeline_metrics(
                spark, source, batch_id, final_count,
                processing_time, raw_write_time + proc_write_time,
            )

            print(
                f"[{source}] Batch {batch_id}: {total_count} raw → "
                f"{final_count} processed in {processing_time:.0f}ms"
            )

        except Exception as exc:
            print(f"[{source}] Batch {batch_id} FAILED: {exc}")
            traceback.print_exc()

    return process_batch


# ---------------------------------------------------------------------------
# Stream builder
# ---------------------------------------------------------------------------
def build_stream(
    spark: SparkSession,
    topic: str,
    schema: StructType,
    processor_fn,
    checkpoint_dir: str,
):
    """
    Subscribe to a Kafka topic, parse JSON values using the given schema,
    and process each micro-batch with the provided processor function.
    Malformed JSON (records that fail schema parsing) produce null structs
    and are filtered out before processing.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).filter(
        F.col("data").isNotNull()
    ).select("data.*")

    query = (
        parsed.writeStream
        .foreachBatch(processor_fn)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    spark = (
        SparkSession.builder.appName("GRIP-StreamProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.1",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    checkpoint_base = "/tmp/grip_checkpoints"

    streams = [
        build_stream(
            spark, "earthquakes", EARTHQUAKE_SCHEMA,
            make_earthquake_processor(spark),
            f"{checkpoint_base}/earthquakes",
        ),
        build_stream(
            spark, "weather", WEATHER_SCHEMA,
            make_weather_processor(spark),
            f"{checkpoint_base}/weather",
        ),
        build_stream(
            spark, "air_quality", AIR_QUALITY_SCHEMA,
            make_air_quality_processor(spark),
            f"{checkpoint_base}/air_quality",
        ),
        build_stream(
            spark, "wildfires", WILDFIRE_SCHEMA,
            make_wildfire_processor(spark),
            f"{checkpoint_base}/wildfires",
        ),
    ]

    print(f"Started {len(streams)} streaming queries — awaiting termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
