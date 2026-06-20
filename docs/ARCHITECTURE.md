# GRIP Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL DATA SOURCES                            │
│  USGS Earthquakes │ NASA FIRMS Wildfires │ Open-Meteo Weather & AQ     │
└────────┬──────────────────┬──────────────────────┬─────────────────────┘
         │                  │                      │
         ▼                  ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER (Phase 1)                           │
│  earthquake_producer │ wildfire_producer │ weather_producer │ aq_producer│
└────────┬──────────────────┬──────────────────────┬─────────────────────┘
         │                  │                      │
         ▼                  ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         APACHE KAFKA                                    │
│  Topics: earthquakes │ wildfires │ weather │ air_quality               │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   SPARK STRUCTURED STREAMING (Phase 2)                  │
│  Parse → Clean → Deduplicate → Enrich → Anomaly Detection → Write     │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         POSTGRESQL                                      │
│  *_raw │ *_processed │ anomaly_events │ pipeline_metrics │ alerts      │
│  risk_scores │ forecasts │ analytics_snapshots                         │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   INTELLIGENCE LAYER (Phase 3)                          │
│  Risk Scoring │ Alert Engine │ Prophet Forecasting │ Analytics         │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      FASTAPI + WEBSOCKET                                │
│  REST API │ WebSocket /ws/live │ Static Dashboard │ Export (CSV/JSON/PDF)│
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    INTERACTIVE DASHBOARD (9 Pages)                    │
│  Dashboard │ Map │ Earthquakes │ Wildfires │ Weather │ AQ │ Analytics│
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Ingestion** — Producers poll external APIs on configurable intervals and publish JSON events to Kafka
2. **Streaming ETL** — Spark reads from Kafka in 10-second micro-batches, applies enrichment and anomaly rules
3. **Storage** — Processed records written to PostgreSQL with quality and pipeline metrics
4. **Intelligence** — Background workers compute risk scores (60s), check alerts (30s), generate forecasts (5min)
5. **Delivery** — WebSocket broadcasts updates every 10s; REST API serves on-demand queries
6. **Visualization** — Dashboard pages fetch live data and render charts/maps

## Database Schema (20 Tables)

| Phase | Tables |
|-------|--------|
| 1 | earthquakes, weather, air_quality, wildfires, ingestion_logs |
| 2 | *_raw (4), *_processed (4), anomaly_events, data_quality_metrics, pipeline_metrics |
| 3 | risk_scores, alerts, forecasts, analytics_snapshots |

## Docker Services

9 containers orchestrated via Docker Compose on `grip-network`:

- zookeeper, kafka, kafka-init, postgres
- spark-master, spark-worker, spark-job
- backend-api, backend-producers
