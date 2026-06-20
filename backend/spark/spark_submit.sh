#!/bin/bash
# ============================================================
# GRIP — Spark job submission script
#
# Runs inside the spark-job container to submit the streaming
# processor with all required packages (Kafka connector + JDBC).
# ============================================================

set -euo pipefail

echo "=== GRIP Spark Job Submitter ==="
echo "Waiting for Spark master to be available..."

# Wait for Spark master
MAX_RETRIES=30
RETRY_DELAY=5
for i in $(seq 1 $MAX_RETRIES); do
    if curl -s http://spark-master:8080 > /dev/null 2>&1; then
        echo "Spark master is ready."
        break
    fi
    echo "Attempt $i/$MAX_RETRIES — Spark master not ready, waiting ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY
done

echo "Waiting for Kafka broker to be available..."
for i in $(seq 1 $MAX_RETRIES); do
    if echo > /dev/tcp/kafka/9092 2>/dev/null; then
        echo "Kafka broker is ready."
        break
    fi
    echo "Attempt $i/$MAX_RETRIES — Kafka not ready, waiting ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY
done

echo "Submitting Spark streaming job..."

/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
    /opt/bitnami/spark/apps/stream_processor.py

echo "Spark job exited."
