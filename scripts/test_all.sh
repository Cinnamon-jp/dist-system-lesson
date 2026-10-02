#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=========================================================="
echo "    DISTRIBUTED KVS FULL AUTOMATED TEST SUITE"
echo "=========================================================="

echo ""
echo "[Step 1] Running Unit Tests (store & cluster)..."
go test -v ./internal/store/... ./internal/cluster/...

echo ""
echo "[Step 2] Starting Cluster and Running E2E Tester (Consistency & Benchmark)..."
"${SCRIPT_DIR}/run_cluster.sh"
sleep 1

# テスタービルド
go build -o "${ROOT_DIR}/bin/tester" "${ROOT_DIR}/cmd/tester/main.go"

# テスター実行 (allモード: status + consistency 30件 + benchmark 100件)
"${ROOT_DIR}/bin/tester" -mode "all" -n 30 -bench-n 100

# クラスタ停止
"${SCRIPT_DIR}/stop_cluster.sh"

echo ""
echo "[Step 3] Running Chaos Failover Disaster Test..."
"${SCRIPT_DIR}/run_failover_test.sh"

echo ""
echo "=========================================================="
echo "    ALL TEST SUITES COMPLETED AND PASSED (全テスト合格)!"
echo "=========================================================="
