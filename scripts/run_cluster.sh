#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PID_FILE="${ROOT_DIR}/.cluster_pids"

mkdir -p "${ROOT_DIR}/logs"

echo "Building node binary..."
go build -o "${ROOT_DIR}/bin/node" "${ROOT_DIR}/cmd/node/main.go"

# 既存プロセスがあれば停止
if [ -f "${PID_FILE}" ]; then
  "${SCRIPT_DIR}/stop_cluster.sh" || true
fi

echo "Starting 3-node cluster..."
echo "  - Node 1: 127.0.0.1:8001 (Primary)"
echo "  - Node 2: 127.0.0.1:8002 (Backup)"
echo "  - Node 3: 127.0.0.1:8003 (Backup)"

"${ROOT_DIR}/bin/node" -id "node-1" -addr "127.0.0.1:8001" -peers "127.0.0.1:8002,127.0.0.1:8003" -role "Primary" > "${ROOT_DIR}/logs/node-1.log" 2>&1 &
PID1=$!

"${ROOT_DIR}/bin/node" -id "node-2" -addr "127.0.0.1:8002" -peers "127.0.0.1:8001,127.0.0.1:8003" -role "Backup" > "${ROOT_DIR}/logs/node-2.log" 2>&1 &
PID2=$!

"${ROOT_DIR}/bin/node" -id "node-3" -addr "127.0.0.1:8003" -peers "127.0.0.1:8001,127.0.0.1:8002" -role "Backup" > "${ROOT_DIR}/logs/node-3.log" 2>&1 &
PID3=$!

echo "${PID1} 127.0.0.1:8001 node-1" > "${PID_FILE}"
echo "${PID2} 127.0.0.1:8002 node-2" >> "${PID_FILE}"
echo "${PID3} 127.0.0.1:8003 node-3" >> "${PID_FILE}"

echo "Cluster started. PIDs: ${PID1}, ${PID2}, ${PID3}"
sleep 1
