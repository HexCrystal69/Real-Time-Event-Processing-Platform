# GRIP — Global Risk Intelligence Platform

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104%2B-green.svg)](https://fastapi.tiangolo.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)

**GRIP (Global Risk Intelligence Platform)** is a production-grade, highly scalable, real-time global risk monitoring, alert propagation, and forecasting platform. The system leverages a **Kafka → Apache Spark → PostgreSQL** streaming architecture to ingest, process, deduplicate, and analyze high-throughput geospatial event data from multiple public sources (USGS Earthquakes, NASA FIRMS Wildfires, Open-Meteo Weather, and Air Quality). It exposes analytical query endpoints via **FastAPI**, broadcasts updates in real-time through **WebSockets**, and renders insights on a custom-designed **interactive control dashboard** built with Vanilla CSS, Chart.js, and Leaflet.js.

The platform provides:
*   **Real-Time Data Ingestion & Validation** from multiple live feeds.
*   **Micro-batch Stream Processing** using Spark Structured Streaming for enrichment, quality checks, and anomaly detection.
*   **Dynamic Unified Risk Scoring** for five critical global regions.
*   **Automated Event-Driven Alerts** for critical incidents (e.g. major earthquakes, extreme AQI).
*   **Time-Series Predictive Forecasting** using Facebook Prophet across 24-hour, 7-day, and 30-day horizons.
*   **A 9-page Command Center Web Interface** with real-time graphs, map visualizers, and system monitors.
*   **Reporting & Data Export** via CSV, JSON, and customized PDF Intelligence Briefs.

---

## 🏗️ System Architecture & Data Flow

The GRIP pipeline is built for scalability, robustness, and fault tolerance:

```
                  ┌─────────────────────────────────────────────────────────────┐
                  │                    EXTERNAL DATA SOURCES                    │
                  │  USGS Earthquakes │ NASA FIRMS Wildfires │ Open-Meteo API  │
                  └─────────┬───────────────────┬───────────────────┬───────────┘
                            │                   │                   │
                            ▼                   ▼                   ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │                 INGESTION LAYER (Producers)                 │
                  │  Polling loops, JSON schema validation, thread safety, rate │
                  │  limiting, sliding window deduplication, retry queues       │
                  └─────────┬───────────────────┬───────────────────┬───────────┘
                            │                   │                   │
                            ▼                   ▼                   ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │                        APACHE KAFKA                         │
                  │   Topics: 'earthquakes' | 'wildfires' | 'weather' | 'air_q' │
                  └─────────────────────────────┬───────────────────────────────┘
                                                │
                                                ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │             SPARK STRUCTURED STREAMING PROCESSOR            │
                  │  10-second micro-batches, schema parsing, geospatial        │
                  │  enrichment (bounding box match), anomaly threshold rules   │
                  └─────────────────────────────┬───────────────────────────────┘
                                                │
                                                ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │                     POSTGRESQL STORAGE                      │
                  │  - Raw tables (4)          - Anomaly logs & quality tables   │
                  │  - Processed tables (4)    - Performance & Ingestion metrics │
                  │  - Alert feed, Forecasts   - Risk scores & dynamic ratings   │
                  └─────────────────────────────┬───────────────────────────────┘
                                                │ (Periodic Worker Calculations)
                                                ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │                 INTELLIGENCE & WORKER LAYER                 │
                  │  - Risk Engine (60s): Dynamic weighted hazard assessment     │
                  │  - Alert Engine (30s): Severe condition evaluator           │
                  │  - Prophet Forecaster (5m): Time-series prediction models   │
                  └─────────────────────────────┬───────────────────────────────┘
                                                │
                                                ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │                    FASTAPI APPLICATION SERVER               │
                  │  - REST API (slowapi rate limited)  - CSV/JSON/PDF Exports  │
                  │  - WebSocket Hub (/ws/live)         - Live status monitor   │
                  └─────────────────────────────┬───────────────────────────────┘
                                                │
                                                ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │             COMMAND INTERACTIVE DASHBOARD (9 Pages)         │
                  │  Responsive UI: HTML5, Vanilla CSS, Chart.js, Leaflet map,  │
                  │  dynamic alerts, visual filters, auto-reconnecting WS client│
                  └─────────────────────────────────────────────────────────────┘
```

### End-to-End Execution Flow:
1.  **Ingestion**: Python-based producers fetch the latest records from USGS, NASA FIRMS, and Open-Meteo. The base producer prevents API throttling and verifies integrity before packaging events as JSON.
2.  **Streaming & ETL**: Spark reads JSON payloads from Kafka. It parses fields according to defined schemas, extracts spatial data, tags anomalies (e.g. out-of-bound temperatures or extreme depths), computes data quality statistics, and saves records into target database tables.
3.  **Storage**: A robust PostgreSQL database structure keeps tracks of pipeline activity.
4.  **Intelligence**: A dedicated background service worker handles core computational algorithms:
    *   **Risk Scoring**: Computes regional scores by blending earthquake magnitudes, AQI, temperatures, and wildfire counts for 5 predefined global cities (New York, London, Tokyo, Mumbai, São Paulo).
    *   **Alert Generation**: Analyzes newly written processed records, issues alerts for extreme events (e.g. Earthquake magnitude >= 6.0, wind speed > 100 km/h, PM2.5 > 150), and broadcasts them.
    *   **Forecasting**: Periodically runs time-series forecasting via Prophet, calculating point predictions and bounds (`yhat`, `yhat_lower`, `yhat_upper`) for three horizons (24h, 7d, 30d).
5.  **Delivery**: Web clients subscribe to `/ws/live`. Updates are pushed out periodically (every 10 seconds) or immediately on alert triggers. REST API queries handle user-triggered tasks like manual forecasting, reports downloading, or specific metrics lookups.

---

## 🗄️ Database Design (20 Tables)

GRIP divides storage into distinct operational layers inside PostgreSQL to maintain relational integrity and support high-speed analytical queries:

### 1. Ingestion Layer
*   `earthquakes`, `wildfires`, `weather`, `air_quality`: Primary transaction logs representing initial writes of incoming events.
*   `ingestion_logs`: Monitors producer success rates, record volumes, API latency, and run durations.

### 2. Streaming ETL & Processed Layer
*   `earthquakes_raw`, `wildfires_raw`, `weather_raw`, `air_quality_raw`: Archive of raw JSON payloads ingested from Kafka topics.
*   `earthquakes_processed`: Cleaned records with geo-coordinates, magnitude, depth, and matching closest monitored region name.
*   `wildfires_processed`: Validated fire events containing Brightness Temperature, Fire Radiative Power (FRP), and confidence indicators.
*   `weather_processed`: Regional atmospheric readings containing temperatures, wind speed, relative humidity, and wind direction.
*   `air_quality_processed`: Particulate values containing PM2.5, PM10, Nitrogen Dioxide, Carbon Monoxide, and composite AQI calculations.

### 3. Analytics, Health & Quality Monitoring
*   `anomaly_events`: Captures records that violate physical bounds (e.g., temperatures above 60°C or negative depths) with severity classification.
*   `data_quality_metrics`: Counts null columns, parsing errors, type mismatches, and schema validation failures per batch.
*   `pipeline_metrics`: Aggregates Spark processing stats, including rows per second, micro-batch processing time, and sink write latency.

### 4. Intelligence & Decision Layer
*   `risk_scores`: Historic regional risk values (0.0 to 10.0 scale) and severity tiers (Low, Moderate, High, Critical) computed dynamically.
*   `alerts`: Severe incident details including source, status (active/resolved), severity (warning/critical), and spatial scope.
*   `forecasts`: Predicted values, lower/upper confidence bounds, metrics, horizons, and model execution metadata.
*   `analytics_snapshots`: Aggregates hourly counters, throughput, and metrics for fast UI loads.

---

## 🖥️ Interactive Dashboard (9 Pages)

The user interface is designed with a premium, high-tech command center aesthetic. It uses dark modes, sleek glassmorphism, responsive grids, custom typography (Google Font "Outfit"), and interactive micro-animations.

| Page | File | Primary Features |
| :--- | :--- | :--- |
| **Command Dashboard** | [index.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/index.html) | Main hub displaying key system indicators, live scrolling alert feed, top cities ranked by current risk scores, and event rate distribution charts. |
| **Global Risk Map** | [map.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/map.html) | Interactive Leaflet.js world map with toggles for specific event layers. Event markers are scaled by severity (e.g., larger circles for higher magnitude earthquakes, red flame icons for active fires). |
| **Earthquake Intelligence** | [earthquakes.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/earthquakes.html) | Seismic details displaying magnitude histograms, depth distribution scatterplots, global activity trends, and a specific 7-day earthquake frequency forecast chart. |
| **Wildfire Intelligence** | [wildfires.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/wildfires.html) | Active fire hotspots mapped by Fire Radiative Power (FRP), daily burn rates, regional severity rankings, and fire growth forecasts. |
| **Weather Intelligence** | [weather.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/weather.html) | Severe weather monitors tracking wind speed vectors, temperature fluctuations, precipitation levels, storm tracks, and seasonal projections. |
| **Air Quality Intelligence** | [air-quality.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/air-quality.html) | AQI classification charts, PM2.5/PM10 hotspots, correlation metrics, regional comparisons, and air quality index forecasting. |
| **Forecasting Intelligence** | [forecasting.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/forecasting.html) | Dedicated forecasting station showing Prophet-generated models for all 4 event modules. Features a horizon switcher (24h, 7d, 30d) and a button to trigger model recalculations. |
| **System Analytics** | [analytics.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/analytics.html) | Real-time graphs showing stream throughput, data quality compliance, Spark task execution times, database write performance, and API rate limits. |
| **System Monitoring** | [monitoring.html](file:///d:/Mini/Real-Time-Event-Processing-Platform/frontend/monitoring.html) | Ingestion health checks monitoring database connections, Kafka offset lag, Spark cluster memory, API ping latency, and raw system logs. |

---

## ⚙️ Configuration & Environment Variables

Configure application settings by modifying the `.env` file in the project root:

```env
# ==========================================
# Database Configuration
# ==========================================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=grip_db
POSTGRES_USER=grip
POSTGRES_PASSWORD=grip_password

# ==========================================
# Ingestion & Message Broker
# ==========================================
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_EARTHQUAKES=earthquakes
KAFKA_TOPIC_WEATHER=weather
KAFKA_TOPIC_AIR_QUALITY=air_quality
KAFKA_TOPIC_WILDFIRES=wildfires

# Ingestion Polling Intervals (in seconds)
USGS_POLL_INTERVAL=60
WEATHER_POLL_INTERVAL=300
AIR_QUALITY_POLL_INTERVAL=300
WILDFIRE_POLL_INTERVAL=600

# ==========================================
# External API Credentials
# ==========================================
# Request your NASA FIRMS API key at: https://firms.modaps.eosdis.nasa.gov/api/map_key
NASA_FIRMS_MAP_KEY=your_nasa_firms_api_key_here

# ==========================================
# Intelligence Engine Intervals (in seconds)
# ==========================================
RISK_SCORE_INTERVAL_SECONDS=60
ALERT_CHECK_INTERVAL_SECONDS=30
FORECAST_INTERVAL_SECONDS=300
ANALYTICS_SNAPSHOT_INTERVAL_SECONDS=120
WEBSOCKET_POLL_INTERVAL_SECONDS=10

# ==========================================
# Spark & ML Modeling
# ==========================================
SPARK_MASTER_URL=http://spark-master:8080
RISK_LOOKBACK_HOURS=24
FORECAST_MIN_DATA_POINTS=10

# ==========================================
# API Server Configuration
# ==========================================
RATE_LIMIT_PER_MINUTE=120
LOG_LEVEL=INFO
```

---

## 🚀 Quick Start (Docker Compose)

The entire platform is containerized and can be spun up using Docker Compose:

### 1. Copy Environment Configuration
```bash
cp .env.example .env
# Edit .env and enter your NASA_FIRMS_MAP_KEY if available.
# The producers will automatically fall back to mock generators if no key is supplied.
```

### 2. Start the Docker Stack
```bash
docker compose up --build -d
```
This commands builds the backend images and runs 9 services:
*   `zookeeper` and `kafka`: Event streaming platform.
*   `kafka-init`: Creates topics (`earthquakes`, `weather`, `air_quality`, `wildfires`) with 3 partitions and a replication factor of 1.
*   `postgres`: SQL database initialized with schemas.
*   `spark-master` and `spark-worker`: Distributed compute engine.
*   `spark-job`: Submits the streaming pipeline job using Spark Submit.
*   `backend-api`: Exposes REST endpoints, runs background intelligence workers, and opens WebSocket connections.
*   `backend-producers`: Coordinates and runs data ingestion scripts in parallel.

### 3. Verify System Health
Check container statuses:
```bash
docker compose ps
```
Verify the API and components:
*   API Health: `curl http://localhost:8000/health`
*   Full Pipeline Monitor: `curl http://localhost:8000/api/monitoring`
*   Ingestion Activity: `curl http://localhost:8000/status`

### 4. Access Platform interfaces
*   **Command Dashboard**: [http://localhost:8000](http://localhost:8000)
*   **Interactive API documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
*   **Apache Spark Master UI**: [http://localhost:8080](http://localhost:8080)
*   **Apache Spark Worker UI**: [http://localhost:8081](http://localhost:8081)

### 5. Tear Down Stack
To stop services while keeping database records:
```bash
docker compose down
```
To stop services and wipe databases, queues, and checkpoints:
```bash
docker compose down -v
```

---

## 🛠️ Local Developer Setup (Without Docker)

If you prefer to run services individually for debugging:

### Prerequisites
*   Python 3.10 or higher.
*   PostgreSQL 15 or higher.
*   Java Development Kit (JDK 8 or 11) for local PySpark execution.
*   Apache Kafka broker.

### 1. Clone & Set Up Virtual Environment
```bash
# Setup python environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Local Env
Create a `.env` file pointing to your local database and message broker:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=grip_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_local_password
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### 3. Initialize Databases & Tables
Start your local PostgreSQL server, create the `grip_db` database, and run:
```bash
# Running backend server automatically executes database migrations via SQLAlchemy
python -m backend.main
```

### 4. Run Ingestion Producers
Execute the producers to begin publishing raw events to Kafka:
```bash
python -m backend.ingestion.run_producers
```

### 5. Submit Spark Streaming Job
Execute the Spark stream job using local submit:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  backend/spark/stream_processor.py
```

### 6. Run FastAPI Server
Start uvicorn in reload mode for real-time development updates:
```bash
uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```

---

## 📡 API Route Reference

### Health & Core
*   `GET /health` — Liveness probe (`{"status": "healthy"}`).
*   `GET /status` — Deep connectivity validation (PostgreSQL connection, Kafka accessibility, recent ingestion speeds).

### Data Access (Phase 2 Queries)
*   `GET /earthquakes` — Returns filtered seismic history. Query params: `min_magnitude`, `risk_category`, `limit`, `offset`.
*   `GET /weather` — Returns historical weather reports. Query params: `location`, `min_wind`, `storm_severity`.
*   `GET /air-quality` — Returns air index trends. Query params: `location`, `min_aqi`, `aqi_category`.
*   `GET /wildfires` — Returns historical fire records. Query params: `min_severity`, `min_frp`.
*   `GET /anomalies` — Lists telemetry records marked as anomalous. Query params: `source`, `severity`.
*   `GET /metrics` — Pipeline processing indicators. Query params: `source`.

### Analytics (Phase 3 Aggregations)
*   `GET /api/analytics/summary` — Returns snapshot variables for the dashboard counters.
*   `GET /api/analytics/events-per-hour` — Distribution of incoming events grouped by source over the last 24 hours.
*   `GET /api/analytics/risk-distribution` — Grouping of hazards by categories.
*   `GET /api/analytics/source-activity` — Operations tracking active updates per source.
*   `GET /api/analytics/regional-rankings` — Ranked listing of regions by composite risk score.
*   `GET /api/analytics/risk-scores` — The latest computed scoring arrays for monitored hubs.
*   `GET /api/analytics/map-markers` — Geographic coordinate objects for Leaflet maps.

### Forecasting
*   `GET /api/forecasts` — Returns existing predictions. Query params: `source` (e.g. `air_quality`), `metric` (e.g. `us_aqi`), `horizon` (`24h`, `7d`, `30d`).
*   `POST /api/forecasts/generate` — Manual request to trigger a Prophet model training run.

### Alerts
*   `GET /api/alerts/active` — Active high-severity alerts.
*   `GET /api/alerts/history` — Historical log of warnings and resolutions.

### Report Exports
*   `GET /api/export/csv/{type}` — Outputs database datasets to CSV format.
*   `GET /api/export/json/{type}` — Outputs database datasets to JSON structures.
*   `GET /api/export/pdf/report` — Compiles recent risk indicators and system health into a formatted PDF Intelligence Report.

---

## 🧪 Testing & Validation

Execute the Pytest test suite to verify ingestion pipelines, routing constraints, analytical computations, and web servers:

```bash
# Set PYTHONPATH to root directory
python -m pytest backend/tests/ -v --ignore=backend/tests/test_forecasting.py
```
*(Note: We ignore the forecasting tests during standard CI runs to avoid long training cycles since Facebook Prophet builds regression matrices dynamically).*

---

## 📂 Project Directory Structure

```
Real-Time-Event-Processing-Platform/
├── .env.example                     # Environment template configuration
├── .gitignore                       # Clean file exceptions listing (venv, keys, checkpoints)
├── Dockerfile                       # Python application image instructions
├── docker-compose.yml               # Complete service manager compose instructions
├── requirements.txt                 # Backend dependency list (FastAPI, Prophet, PySpark, etc.)
│
├── backend/                         # System backend processing layer
│   ├── config/                      # Ingestion configurations and global log settings
│   │   ├── logger.py
│   │   └── settings.py
│   ├── database/                    # Database interface definitions
│   │   ├── connection.py            # PostgreSQL engine wrappers
│   │   └── models.py                # SQL Alchemy schemas (20 tables)
│   ├── ingestion/                   # Aggregators and data publishers
│   │   ├── producers/               # API ingestion modules (USGS, FIRMS, Open-Meteo)
│   │   └── run_producers.py         # Multiplexing producer loop runner
│   ├── middleware/                  # Rate limiters & payload checkers
│   ├── routes/                      # REST API routing implementations
│   ├── services/                    # Central operational modules
│   │   ├── alert_engine.py          # Real-time incident evaluator
│   │   ├── analytics.py             # Numerical metric aggregators
│   │   ├── background_worker.py     # Background orchestrator loop
│   │   ├── export.py                # PDF builder and data formatting utilities
│   │   ├── forecasting.py           # Prophet engine interface
│   │   ├── monitoring.py            # Kafka and DB status checker
│   │   ├── risk_scoring.py          # Dynamic hazard scoring calculator
│   │   └── websocket_manager.py     # WebSocket broadcast router
│   ├── spark/                       # Spark processor
│   │   ├── spark_submit.sh          # Cluster submission script
│   │   └── stream_processor.py      # Structured Streaming pipeline
│   ├── tests/                       # Automated Pytest suite
│   └── main.py                      # FastAPI entry point
│
├── docs/                            # Internal reference guides
│   ├── API.md
│   ├── ARCHITECTURE.md
│   └── DEPLOYMENT.md
│
└── frontend/                        # Dashboard web screens
    ├── index.html                   # Command Center
    ├── map.html                     # Leaflet spatial visualizer
    ├── earthquakes.html             # Seismic charts & forecasts
    ├── wildfires.html               # Fire monitors & forecasts
    ├── weather.html                 # Severe weather & forecasts
    ├── air-quality.html             # AQI gauges & forecasts
    ├── forecasting.html             # Unified Prophet portal
    ├── analytics.html               # Stream throughput statistics
    ├── monitoring.html              # Service uptime tracking
    ├── about.html                   # Documentation guide
    │
    ├── css/                         # Custom style system tokens and layout structures
    │   └── styles.css
    └── js/                          # Application logic files
        ├── api.js                   # Endpoint communications wrapper
        ├── dashboard.js             # index widgets
        ├── map-page.js              # Leaflet configuration scripts
        ├── earthquakes.js           # Seismic charts updates
        ├── wildfires.js             # Thermal analytics charts updates
        ├── weather.js               # Atmospheric monitoring updates
        ├── air-quality.js           # AQI widgets updates
        ├── forecasting-page.js      # Horizon configurations and curves renderer
        ├── analytics-page.js        # Performance curves renderer
        └── monitoring-page.js       # Health checking updates
```

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
