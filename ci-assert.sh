#!/usr/bin/env bash
# Runs every statement in sql/ci_assert.sql through Trino; fails if any returns ok=false.
set -euo pipefail
cd "$(dirname "$0")/.."
FAIL=0
# split on blank-line-separated statements
awk 'BEGIN{RS=""; FS="\n"} {gsub(/\n/," "); print}' sql/ci_assert.sql | grep -v '^--' | while read -r stmt; do
  [ -z "$stmt" ] && continue
  OUT=$(docker compose exec -T trino trino --catalog lakehouse --output-format CSV --execute "$stmt")
  NAME=$(echo "$OUT" | cut -d, -f2 | tr -d '"')
  OK=$(echo "$OUT" | cut -d, -f1 | tr -d '"')
  if [ "$OK" = "true" ]; then echo "PASS  $NAME"; else echo "FAIL  $NAME"; FAIL=1; fi
done
exit $FAIL
