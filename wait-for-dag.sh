#!/usr/bin/env bash
# Polls Airflow until the most recent run of a DAG reaches a terminal state. Used by CI.
# usage: scripts/wait-for-dag.sh <dag_id> [timeout_seconds]
set -euo pipefail
DAG=${1:-frameforge_video}
TIMEOUT=${2:-420}
URL=${AIRFLOW_URL:-http://localhost:8088}
for ((i=0; i<TIMEOUT; i+=10)); do
  STATE=$(curl -s -u admin:admin "$URL/api/v1/dags/$DAG/dagRuns?order_by=-execution_date&limit=1" \
          | python3 -c 'import sys,json; r=json.load(sys.stdin)["dag_runs"]; print(r[0]["state"] if r else "none")')
  echo "[$i s] $DAG -> $STATE"
  case "$STATE" in
    success) exit 0 ;;
    failed)  exit 1 ;;
  esac
  sleep 10
done
echo "timed out"; exit 2
