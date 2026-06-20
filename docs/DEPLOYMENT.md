# GRIP Deployment Guide

## Prerequisites

- Docker Engine 24+ and Docker Compose v2
- 8 GB RAM minimum (Spark worker uses 2 GB)
- Ports available: 8000, 5432, 9092, 8080, 8081

## Production Deployment

### 1. Environment Configuration

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NASA_FIRMS_MAP_KEY` | (empty) | Required for wildfire data |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Internal Kafka listener |
| `POSTGRES_PASSWORD` | `grip_password` | Change in production |
| `RATE_LIMIT_PER_MINUTE` | `120` | API rate limit |
| `SPARK_MASTER_URL` | `http://spark-master:8080` | Spark health check |

### 2. Start the Stack

```bash
docker compose up --build -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

Expected startup order:
1. zookeeper → kafka → kafka-init
2. postgres (healthcheck)
3. spark-master → spark-worker → spark-job
4. backend-api, backend-producers

### 3. Verify Deployment

```bash
# API health
curl http://localhost:8000/health

# System status
curl http://localhost:8000/status

# Analytics (after data flows)
curl http://localhost:8000/api/analytics/summary

# Monitoring
curl http://localhost:8000/api/monitoring
```

Open dashboard: **http://localhost:8000**

### 4. Monitor Logs

```bash
docker compose logs -f backend-api
docker compose logs -f spark-job
docker compose logs -f backend-producers
```

### 5. Stop the Stack

```bash
docker compose down        # preserve volumes
docker compose down -v     # remove data volumes
```

## Scaling Considerations

- **Kafka**: Increase partitions for higher throughput
- **Spark**: Add workers via `docker compose scale spark-worker=2`
- **PostgreSQL**: Use managed service with connection pooling
- **API**: Scale `backend-api` replicas behind a load balancer with sticky WebSocket sessions

## Troubleshooting

| Issue | Solution |
|-------|----------|
| No wildfire data | Set `NASA_FIRMS_MAP_KEY` in `.env` |
| Empty dashboard | Wait 2-5 min for producers + Spark to populate data |
| Spark job restarting | Check `docker compose logs spark-job` |
| Forecasts empty | Requires 10+ data points; wait for historical accumulation |
| WebSocket disconnected | Auto-reconnects; check API logs |

## Security Recommendations

- Change default PostgreSQL credentials
- Place reverse proxy (nginx) with TLS in front of port 8000
- Restrict Kafka/PostgreSQL ports to internal network only
- Review rate limiting settings for production load
