#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PID_FILE="${ROOT_DIR}/.cluster_pids"

if [ ! -f "${PID_FILE}" ]; then
  echo "No active cluster PID file found."
  exit 0
fi

echo "Stopping cluster nodes..."
while IFS=' ' read -r pid addr id; do
  if kill -0 "${pid}" 2>/dev/null; then
    echo "  Killing ${id} (PID ${pid}, ${addr})..."
    kill -TERM "${pid}" 2>/dev/null || kill -9 "${pid}" 2>/dev/null || true
  fi
done < "${PID_FILE}"

rm -f "${PID_FILE}"
echo "All cluster nodes stopped."
