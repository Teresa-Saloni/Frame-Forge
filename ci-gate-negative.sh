#!/usr/bin/env bash
# Proves the quality gate actually blocks bad data: inject 5 corrupt label rows, run the gate on
# that fake video, and require it to FAIL. Then clean up.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> injecting corrupt rows"
docker compose exec -T trino trino --catalog lakehouse -f /dev/stdin < quality/inject_corrupt.sql

echo "==> running gate on corrupt-video (must fail)"
set +e
docker compose exec -T -e TRINO_HOST=trino airflow python /opt/quality/gate.py corrupt-video ci-negative
RC=$?
set -e

echo "==> cleaning up"
docker compose exec -T trino trino --catalog lakehouse --execute \
  "DELETE FROM lakehouse.silver.frames WHERE video_id = 'corrupt-video'"

if [ "$RC" -eq 0 ]; then
  echo "FAIL: gate passed corrupt data"; exit 1
fi
echo "PASS: gate rejected corrupt data (exit $RC)"
