"""
GRIP — Database DDL definitions.

Contains raw SQL CREATE TABLE statements for all persistent tables.
Each table uses IF NOT EXISTS so init_database() is idempotent.
"""

EARTHQUAKES_TABLE = """
CREATE TABLE IF NOT EXISTS earthquakes (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(64) UNIQUE NOT NULL,
    magnitude       DOUBLE PRECISION,
    place           TEXT,
    event_time      TIMESTAMPTZ NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    depth_km        DOUBLE PRECISION,
    tsunami         BOOLEAN DEFAULT FALSE,
    significance    INTEGER,
    status          VARCHAR(32),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_earthquakes_event_time
    ON earthquakes (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude
    ON earthquakes (magnitude DESC);
"""

WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS weather (
    id                  SERIAL PRIMARY KEY,
    location_name       VARCHAR(128) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    temperature_c       DOUBLE PRECISION,
    humidity_pct        DOUBLE PRECISION,
    wind_speed_kmh      DOUBLE PRECISION,
    precipitation_mm    DOUBLE PRECISION,
    weather_code        INTEGER,
    observed_at         TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_observed_at
    ON weather (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_location
    ON weather (location_name);
"""

AIR_QUALITY_TABLE = """
CREATE TABLE IF NOT EXISTS air_quality (
    id                  SERIAL PRIMARY KEY,
    location_name       VARCHAR(128) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    us_aqi              INTEGER,
    pm2_5               DOUBLE PRECISION,
    pm10                DOUBLE PRECISION,
    ozone               DOUBLE PRECISION,
    nitrogen_dioxide    DOUBLE PRECISION,
    carbon_monoxide     DOUBLE PRECISION,
    sulphur_dioxide     DOUBLE PRECISION,
    observed_at         TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_air_quality_observed_at
    ON air_quality (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_air_quality_location
    ON air_quality (location_name);
"""

WILDFIRES_TABLE = """
CREATE TABLE IF NOT EXISTS wildfires (
    id              SERIAL PRIMARY KEY,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    brightness      DOUBLE PRECISION,
    scan            DOUBLE PRECISION,
    track           DOUBLE PRECISION,
    acq_date        DATE NOT NULL,
    acq_time        VARCHAR(8),
    satellite       VARCHAR(16),
    instrument      VARCHAR(16),
    confidence      VARCHAR(16),
    frp             DOUBLE PRECISION,
    daynight        VARCHAR(2),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wildfires_acq_date
    ON wildfires (acq_date DESC);

CREATE INDEX IF NOT EXISTS idx_wildfires_coords
    ON wildfires (latitude, longitude);
"""

INGESTION_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS ingestion_logs (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(32) NOT NULL,
    status          VARCHAR(16) NOT NULL,
    records_count   INTEGER NOT NULL DEFAULT 0,
    latency_ms      DOUBLE PRECISION,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_logs_source
    ON ingestion_logs (source, created_at DESC);
"""

# -----------------------------------------------------------------------
# Phase 2 — Raw Tables (verbatim Kafka payloads)
# -----------------------------------------------------------------------

EARTHQUAKES_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS earthquakes_raw (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(64) NOT NULL,
    magnitude       DOUBLE PRECISION,
    place           TEXT,
    event_time      TIMESTAMPTZ,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    depth_km        DOUBLE PRECISION,
    tsunami         BOOLEAN DEFAULT FALSE,
    significance    INTEGER,
    status          VARCHAR(32),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_earthquakes_raw_event_time
    ON earthquakes_raw (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_earthquakes_raw_event_id
    ON earthquakes_raw (event_id);
"""

WEATHER_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS weather_raw (
    id                  SERIAL PRIMARY KEY,
    location_name       VARCHAR(128),
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    temperature_c       DOUBLE PRECISION,
    humidity_pct        DOUBLE PRECISION,
    wind_speed_kmh      DOUBLE PRECISION,
    precipitation_mm    DOUBLE PRECISION,
    weather_code        INTEGER,
    observed_at         TIMESTAMPTZ,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_raw_observed_at
    ON weather_raw (observed_at DESC);
"""

AIR_QUALITY_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS air_quality_raw (
    id                  SERIAL PRIMARY KEY,
    location_name       VARCHAR(128),
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    us_aqi              INTEGER,
    pm2_5               DOUBLE PRECISION,
    pm10                DOUBLE PRECISION,
    ozone               DOUBLE PRECISION,
    nitrogen_dioxide    DOUBLE PRECISION,
    carbon_monoxide     DOUBLE PRECISION,
    sulphur_dioxide     DOUBLE PRECISION,
    observed_at         TIMESTAMPTZ,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_air_quality_raw_observed_at
    ON air_quality_raw (observed_at DESC);
"""

WILDFIRES_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS wildfires_raw (
    id              SERIAL PRIMARY KEY,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    brightness      DOUBLE PRECISION,
    scan            DOUBLE PRECISION,
    track           DOUBLE PRECISION,
    acq_date        DATE,
    acq_time        VARCHAR(8),
    satellite       VARCHAR(16),
    instrument      VARCHAR(16),
    confidence      VARCHAR(16),
    frp             DOUBLE PRECISION,
    daynight        VARCHAR(2),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wildfires_raw_acq_date
    ON wildfires_raw (acq_date DESC);
"""

# -----------------------------------------------------------------------
# Phase 2 — Processed Tables (cleaned + enriched)
# -----------------------------------------------------------------------

EARTHQUAKES_PROCESSED_TABLE = """
CREATE TABLE IF NOT EXISTS earthquakes_processed (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(64) UNIQUE NOT NULL,
    magnitude       DOUBLE PRECISION,
    place           TEXT,
    event_time      TIMESTAMPTZ NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    depth_km        DOUBLE PRECISION,
    tsunami         BOOLEAN DEFAULT FALSE,
    significance    INTEGER,
    status          VARCHAR(32),
    risk_category   VARCHAR(16) NOT NULL,
    magnitude_band  VARCHAR(16) NOT NULL,
    distance_group  VARCHAR(16) NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eq_proc_event_time
    ON earthquakes_processed (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_eq_proc_risk
    ON earthquakes_processed (risk_category);

CREATE INDEX IF NOT EXISTS idx_eq_proc_magnitude
    ON earthquakes_processed (magnitude DESC);
"""

WEATHER_PROCESSED_TABLE = """
CREATE TABLE IF NOT EXISTS weather_processed (
    id                  SERIAL PRIMARY KEY,
    location_name       VARCHAR(128) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    temperature_c       DOUBLE PRECISION,
    humidity_pct        DOUBLE PRECISION,
    wind_speed_kmh      DOUBLE PRECISION,
    precipitation_mm    DOUBLE PRECISION,
    weather_code        INTEGER,
    observed_at         TIMESTAMPTZ NOT NULL,
    heat_index          DOUBLE PRECISION,
    rain_severity       VARCHAR(16) NOT NULL,
    wind_severity       VARCHAR(16) NOT NULL,
    storm_severity      VARCHAR(16) NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_proc_observed_at
    ON weather_processed (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_proc_location
    ON weather_processed (location_name);

CREATE INDEX IF NOT EXISTS idx_weather_proc_storm
    ON weather_processed (storm_severity);
"""

AIR_QUALITY_PROCESSED_TABLE = """
CREATE TABLE IF NOT EXISTS air_quality_processed (
    id                  SERIAL PRIMARY KEY,
    location_name       VARCHAR(128) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    us_aqi              INTEGER,
    pm2_5               DOUBLE PRECISION,
    pm10                DOUBLE PRECISION,
    ozone               DOUBLE PRECISION,
    nitrogen_dioxide    DOUBLE PRECISION,
    carbon_monoxide     DOUBLE PRECISION,
    sulphur_dioxide     DOUBLE PRECISION,
    observed_at         TIMESTAMPTZ NOT NULL,
    aqi_category        VARCHAR(16) NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aq_proc_observed_at
    ON air_quality_processed (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_aq_proc_location
    ON air_quality_processed (location_name);

CREATE INDEX IF NOT EXISTS idx_aq_proc_category
    ON air_quality_processed (aqi_category);
"""

WILDFIRES_PROCESSED_TABLE = """
CREATE TABLE IF NOT EXISTS wildfires_processed (
    id                      SERIAL PRIMARY KEY,
    latitude                DOUBLE PRECISION NOT NULL,
    longitude               DOUBLE PRECISION NOT NULL,
    brightness              DOUBLE PRECISION,
    scan                    DOUBLE PRECISION,
    track                   DOUBLE PRECISION,
    acq_date                DATE NOT NULL,
    acq_time                VARCHAR(8),
    satellite               VARCHAR(16),
    instrument              VARCHAR(16),
    confidence              VARCHAR(16),
    frp                     DOUBLE PRECISION,
    daynight                VARCHAR(2),
    fire_severity           VARCHAR(16) NOT NULL,
    detection_confidence    VARCHAR(16) NOT NULL,
    processed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wf_proc_acq_date
    ON wildfires_processed (acq_date DESC);

CREATE INDEX IF NOT EXISTS idx_wf_proc_severity
    ON wildfires_processed (fire_severity);

CREATE INDEX IF NOT EXISTS idx_wf_proc_coords
    ON wildfires_processed (latitude, longitude);
"""

# -----------------------------------------------------------------------
# Phase 2 — Anomaly Events
# -----------------------------------------------------------------------

ANOMALY_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS anomaly_events (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(32) NOT NULL,
    event_type      VARCHAR(64) NOT NULL,
    severity        VARCHAR(16) NOT NULL,
    description     TEXT NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    event_time      TIMESTAMPTZ,
    raw_data        JSONB,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_detected_at
    ON anomaly_events (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_source
    ON anomaly_events (source);

CREATE INDEX IF NOT EXISTS idx_anomaly_severity
    ON anomaly_events (severity);
"""

# -----------------------------------------------------------------------
# Phase 2 — Data Quality Metrics
# -----------------------------------------------------------------------

DATA_QUALITY_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id                  SERIAL PRIMARY KEY,
    source              VARCHAR(32) NOT NULL,
    batch_id            BIGINT NOT NULL,
    total_records       INTEGER NOT NULL DEFAULT 0,
    valid_records       INTEGER NOT NULL DEFAULT 0,
    duplicates          INTEGER NOT NULL DEFAULT 0,
    missing_fields      INTEGER NOT NULL DEFAULT 0,
    malformed_records   INTEGER NOT NULL DEFAULT 0,
    dropped_events      INTEGER NOT NULL DEFAULT 0,
    validation_errors   INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_source
    ON data_quality_metrics (source, created_at DESC);
"""

# -----------------------------------------------------------------------
# Phase 2 — Pipeline Metrics
# -----------------------------------------------------------------------

PIPELINE_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                  SERIAL PRIMARY KEY,
    source              VARCHAR(32) NOT NULL,
    batch_id            BIGINT NOT NULL,
    records_count       INTEGER NOT NULL DEFAULT 0,
    processing_time_ms  DOUBLE PRECISION,
    kafka_lag_ms        DOUBLE PRECISION,
    db_write_latency_ms DOUBLE PRECISION,
    records_per_minute  DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_source
    ON pipeline_metrics (source, created_at DESC);
"""

# -----------------------------------------------------------------------
# Phase 3 — Risk Scores
# -----------------------------------------------------------------------

RISK_SCORES_TABLE = """
CREATE TABLE IF NOT EXISTS risk_scores (
    id                  SERIAL PRIMARY KEY,
    region_name         VARCHAR(128) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    earthquake_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    wildfire_score      DOUBLE PRECISION NOT NULL DEFAULT 0,
    weather_score       DOUBLE PRECISION NOT NULL DEFAULT 0,
    air_quality_score   DOUBLE PRECISION NOT NULL DEFAULT 0,
    unified_score       DOUBLE PRECISION NOT NULL,
    risk_level          VARCHAR(16) NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_scores_computed
    ON risk_scores (computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_risk_scores_level
    ON risk_scores (risk_level);

CREATE INDEX IF NOT EXISTS idx_risk_scores_region
    ON risk_scores (region_name, computed_at DESC);
"""

# -----------------------------------------------------------------------
# Phase 3 — Alerts
# -----------------------------------------------------------------------

ALERTS_TABLE = """
CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    alert_type      VARCHAR(64) NOT NULL,
    source          VARCHAR(32) NOT NULL,
    severity        VARCHAR(16) NOT NULL,
    title           VARCHAR(256) NOT NULL,
    description     TEXT NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    region_name     VARCHAR(128),
    metadata        JSONB,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_created
    ON alerts (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_active
    ON alerts (is_active, severity);

CREATE INDEX IF NOT EXISTS idx_alerts_type
    ON alerts (alert_type, created_at DESC);
"""

# -----------------------------------------------------------------------
# Phase 3 — Forecasts
# -----------------------------------------------------------------------

FORECASTS_TABLE = """
CREATE TABLE IF NOT EXISTS forecasts (
    id              SERIAL PRIMARY KEY,
    metric          VARCHAR(64) NOT NULL,
    source          VARCHAR(32) NOT NULL,
    location_name   VARCHAR(128),
    horizon         VARCHAR(16) NOT NULL,
    forecast_time   TIMESTAMPTZ NOT NULL,
    predicted_value DOUBLE PRECISION NOT NULL,
    lower_bound     DOUBLE PRECISION,
    upper_bound     DOUBLE PRECISION,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_forecasts_metric
    ON forecasts (metric, horizon, forecast_time);

CREATE INDEX IF NOT EXISTS idx_forecasts_generated
    ON forecasts (generated_at DESC);
"""

# -----------------------------------------------------------------------
# Phase 3 — Analytics Snapshots
# -----------------------------------------------------------------------

ANALYTICS_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS analytics_snapshots (
    id                  SERIAL PRIMARY KEY,
    snapshot_type       VARCHAR(64) NOT NULL,
    payload             JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_snapshots_type
    ON analytics_snapshots (snapshot_type, created_at DESC);
"""

ALL_TABLES = [
    EARTHQUAKES_TABLE,
    WEATHER_TABLE,
    AIR_QUALITY_TABLE,
    WILDFIRES_TABLE,
    INGESTION_LOGS_TABLE,
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
]
