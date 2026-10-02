#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== [CHAOS TEST] Primary Node Down & Auto-Failover Test ==="

# 1. クラスタ起動
"${SCRIPT_DIR}/run_cluster.sh"
sleep 1

# テスターバイナリのビルド
go build -o "${ROOT_DIR}/bin/tester" "${ROOT_DIR}/cmd/tester/main.go"

# 2. 初期データ投入
echo ""
echo "-> 1. Writing initial key 'before-chaos' to primary..."
curl -s -X PUT -H "Content-Type: application/json" -d '{"value":"initial-pre-chaos-data"}' http://127.0.0.1:8001/v1/kv/before-chaos | grep -q "initial-pre-chaos-data"
echo "  [OK] Initial data written."

# 3. Primary (Node 1, PID $PID1) を強制キル
PID_FILE="${ROOT_DIR}/.cluster_pids"
PRIMARY_PID=$(head -n 1 "${PID_FILE}" | awk '{print $1}')
echo ""
echo "-> 2. Simulating sudden disaster: KILLING Primary Node 1 (PID: ${PRIMARY_PID})..."
kill -9 "${PRIMARY_PID}"

echo "  Primary killed. Waiting 1.5 seconds for Backup nodes to detect failure and elect a NEW leader..."
sleep 1.5

# 4. 新リーダーの確認
echo ""
echo "-> 3. Checking cluster status after failure..."
"${ROOT_DIR}/bin/tester" -nodes "127.0.0.1:8002,127.0.0.1:8003" -mode "status"

# 5. 新リーダーへの書き込みと旧データの残存確認
echo ""
echo "-> 4. Verifying write to new primary via smart tester..."
"${ROOT_DIR}/bin/tester" -nodes "127.0.0.1:8002,127.0.0.1:8003" -mode "consistency" -n 10

# 以前書き込んだキーが新クラスタに残っているか確認
echo ""
echo "-> 5. Verifying old key 'before-chaos' persists on surviving nodes..."
VAL=$(curl -s http://127.0.0.1:8002/v1/kv/before-chaos || true)
if echo "${VAL}" | grep -q "initial-pre-chaos-data"; then
  echo "  [PASS] Key 'before-chaos' safely preserved on Node 2!"
else
  VAL3=$(curl -s http://127.0.0.1:8003/v1/kv/before-chaos || true)
  if echo "${VAL3}" | grep -q "initial-pre-chaos-data"; then
    echo "  [PASS] Key 'before-chaos' safely preserved on Node 3!"
  else
    echo "  [FAIL] Pre-chaos data was LOST!"
    "${SCRIPT_DIR}/stop_cluster.sh"
    exit 1
  fi
fi

# クリーンアップ
"${SCRIPT_DIR}/stop_cluster.sh"
echo ""
echo "=== [CHAOS TEST PASSED] Automatic Failover and Data Persistence Confirmed! ==="
