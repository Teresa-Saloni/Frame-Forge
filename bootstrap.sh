#!/usr/bin/env bash
# One-time setup after `docker compose up -d`:
#   1. buckets   2. Kafka topic   3. Iceberg namespaces + tables   4. YOLO model   5. sample video
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> 1/5 MinIO buckets"
docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null
docker compose exec -T minio mc mb --ignore-existing local/raw-video
docker compose exec -T minio mc mb --ignore-existing local/warehouse

echo "==> 2/5 Kafka topic video.ingested"
docker compose exec -T kafka kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic video.ingested --partitions 3 \
  --config retention.ms=604800000

echo "==> 3/5 Iceberg namespaces and tables"
docker compose exec -T spark-master spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
  -f /dev/stdin < sql/ddl.sql

echo "==> 4/5 YOLOv8n ONNX model"
if [ ! -f models/yolov8n.onnx ]; then
  echo "    models/yolov8n.onnx missing. Run: scripts/get-model.sh"
fi

echo "==> 5/5 Sample video"
if [ ! -f samples/demo.mp4 ]; then
  echo "    samples/demo.mp4 missing. Run: scripts/make-sample-video.sh"
fi

echo "Bootstrap complete. Trigger a run with: scripts/trigger-sample.sh"
