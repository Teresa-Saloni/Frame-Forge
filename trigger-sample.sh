#!/usr/bin/env bash
# Drops the sample video into the raw-video bucket under camera-01/<today>/.
# The ingest service sees it within 5 s, publishes a Kafka event, and triggers the Airflow DAG.
set -euo pipefail
cd "$(dirname "$0")/.."
DAY=$(date -u +%F)
docker compose cp samples/demo.mp4 minio:/tmp/demo.mp4
docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null
docker compose exec -T minio mc cp /tmp/demo.mp4 "local/raw-video/camera-01/${DAY}/demo-$(date +%s).mp4"
echo "Uploaded. Watch the DAG at http://localhost:8088 (admin/admin)"
