#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <grpc-port>" >&2
  exit 1
fi

port="$1"
if ! [[ "$port" =~ ^[0-9]+$ ]]; then
  echo "invalid port: $port" >&2
  exit 1
fi

if (( port < 1 || port > 64535 )); then
  echo "port must be between 1 and 64535 so api port can use port+1000" >&2
  exit 1
fi

api_port=$((port + 1000))

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

host="${NODE_HOST:-127.0.0.1}"
addr="${host}:${port}"
api_addr="${host}:${api_port}"

etcd_endpoints="${ETCD_ENDPOINTS:-127.0.0.1:2379}"
service_name="${SERVICE_NAME:-geecache}"
weight="${NODE_WEIGHT:-1}"
empty_ttl="${EMPTY_TTL:-30s}"
peer_retries="${PEER_RETRIES:-1}"
bloom_items="${BLOOM_ITEMS:-100000}"
bloom_fp_rate="${BLOOM_FP_RATE:-0.01}"
bloom_reject_on_miss="${BLOOM_REJECT_ON_MISS:-true}"
evictor="${EVICTOR:-lru}"

echo "starting geecache node"
echo "  grpc: ${addr}"
echo "  api: ${api_addr}"
echo "  etcd: ${etcd_endpoints}"
echo "  service: ${service_name}"
echo "  bloom_items: ${bloom_items}"
echo "  empty_ttl: ${empty_ttl}"

exec /usr/local/go/bin/go run ./cmd/server \
  -addr="${addr}" \
  -api=true \
  -api-addr="${api_addr}" \
  -use-etcd=true \
  -etcd-endpoints="${etcd_endpoints}" \
  -service-name="${service_name}" \
  -weight="${weight}" \
  -empty-ttl="${empty_ttl}" \
  -peer-retries="${peer_retries}" \
  -evictor="${evictor}" \
  -bloom-items="${bloom_items}" \
  -bloom-fp-rate="${bloom_fp_rate}" \
  -bloom-reject-on-miss="${bloom_reject_on_miss}"
