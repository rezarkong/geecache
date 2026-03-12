#!/usr/bin/env bash

set -euo pipefail

URL="${1:-http://localhost:9999/api?key=Tom}"
REQUESTS="${2:-1000}"
CONCURRENCY="${3:-50}"

if command -v hey >/dev/null 2>&1; then
  echo "using hey: url=${URL} requests=${REQUESTS} concurrency=${CONCURRENCY}"
  hey -n "${REQUESTS}" -c "${CONCURRENCY}" "${URL}"
  exit 0
fi

echo "hey not found, falling back to curl loop"
for ((i=1; i<=REQUESTS; i++)); do
  curl -sS "${URL}" >/dev/null
done
echo "completed ${REQUESTS} requests"
