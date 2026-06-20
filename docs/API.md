# GRIP API Documentation

Base URL: `http://localhost:8000`

Interactive documentation: `/docs` (Swagger) | `/redoc` (ReDoc)

## Health & Status

### GET /health
Liveness probe. Returns `{"status": "healthy", "service": "grip-api"}`.

### GET /status
Deep status: PostgreSQL, Kafka connectivity, per-source ingestion stats.

## Data Endpoints (Phase 2)

| Method | Path | Parameters | Description |
|--------|------|------------|-------------|
| GET | `/earthquakes` | min_magnitude, risk_category, limit, offset | Processed earthquakes |
| GET | `/weather` | location, min_wind, storm_severity, limit, offset | Processed weather |
| GET | `/air-quality` | location, min_aqi, aqi_category, limit, offset | Processed AQI |
| GET | `/wildfires` | min_severity, min_frp, limit, offset | Processed wildfires |
| GET | `/anomalies` | source, severity, event_type, limit, offset | Anomaly events |
| GET | `/metrics` | source | Pipeline and quality metrics |

## Analytics Endpoints (Phase 3)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/analytics/summary` | Dashboard summary metrics |
| GET | `/api/analytics/events-per-hour` | Hourly event counts by source |
| GET | `/api/analytics/risk-distribution` | Risk distribution across sources |
| GET | `/api/analytics/source-activity` | Ingestion activity per source |
| GET | `/api/analytics/regional-rankings` | Regions ranked by unified risk |
| GET | `/api/analytics/risk-scores` | Latest regional risk scores |
| GET | `/api/analytics/map-markers` | Geo markers for global map |
| GET | `/api/analytics/earthquakes` | Earthquake intelligence analytics |
| GET | `/api/analytics/wildfires` | Wildfire intelligence analytics |
| GET | `/api/analytics/weather` | Weather intelligence analytics |
| GET | `/api/analytics/air-quality` | Air quality intelligence analytics |

## Forecast Endpoints

| Method | Path | Parameters | Description |
|--------|------|------------|-------------|
| GET | `/api/forecasts` | metric, source, location, horizon | Get stored forecasts |
| POST | `/api/forecasts/generate` | — | Trigger forecast generation |

Horizons: `24h`, `7d`, `30d`

Metrics: `us_aqi`, `temperature_c`, `fire_count`, `event_count`

## Alert Endpoints

| Method | Path | Parameters | Description |
|--------|------|------------|-------------|
| GET | `/api/alerts/active` | limit | Active alerts |
| GET | `/api/alerts/history` | limit, offset, alert_type, severity | Alert history |

Alert types: `major_earthquake`, `extreme_aqi`, `severe_weather`, `major_wildfire`

## Monitoring

### GET /api/monitoring
Returns health for PostgreSQL, Kafka, Spark, API, plus pipeline metrics, ingestion rates, processing latency, and error counts.

## Export

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/export/csv/{type}` | CSV download |
| GET | `/api/export/json/{type}` | JSON download |
| GET | `/api/export/pdf/report` | PDF intelligence report |

Export types: `earthquakes`, `weather`, `air_quality`, `wildfires`, `anomalies`, `alerts`

## WebSocket

### WS /ws/live

Connect to receive real-time updates. Initial message type: `connected`.

Update message type: `update` — contains summary, risk_scores, alerts, map_markers.

Alert message type: `alerts` — new alerts generated.

## Rate Limiting

Default: 120 requests/minute per IP. Export and forecast generation have stricter limits.

429 response: `{"error": "rate_limit_exceeded", "detail": "..."}`

## Error Responses

| Status | Body |
|--------|------|
| 400 | `{"error": "invalid_request", "detail": "..."}` |
| 404 | `{"detail": "..."}` |
| 422 | Validation error details |
| 429 | Rate limit exceeded |
| 500 | `{"error": "internal_error", "detail": "An internal error occurred"}` |
