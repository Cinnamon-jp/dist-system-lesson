# Distributed Key-Value Store (Go)

Go言語で実装する、Primary-Backupレプリケーション型の分散キーバリューストア（KVS）の練習・学習用リポジトリです。

分散システムのコアとなる以下の概念を、外部DBや巨大フレームワークに依存せずGo標準ライブラリ（`net/http`, `sync`, `context`）のみで体験・学習できます。

- **ノード間通信 & RPC**: HTTP/JSON によるクライアントAPIおよび内部協調API
- **Quorum（過半数）レプリケーション**: 3ノード中2ノード確定で書き込み成功とする高可用性
- **障害検知（Failure Detection）**: ハートビートとジッター付きタイムアウト監視
- **自動リーダー選出（Failover）**: Primaryノード障害時の投票と新リーダー昇格
- **整合性・カオステスト**: ノード強制キル時のデータ永続性と自動復旧の検証

---

## ドキュメント一覧

- [docs/system_design.md](file:///home/cinnamon-jp/dev/std/dist_system/docs/system_design.md): 詳細設計書（アーキテクチャ、API仕様、プロトコル設計）
- [docs/implementation_guide.md](file:///home/cinnamon-jp/dev/std/dist_system/docs/implementation_guide.md): 手動実装ガイド & 実装確認仕様書

---

## 状態と手動実装の流れ

1. **テストプログラム群**:
   - `cmd/tester/main.go`: 整合性検証、ベンチマーク、ステータス確認
   - `internal/store/store_test.go`: ストア単体テスト
   - `internal/cluster/cluster_test.go`: クラスタ結合単体テスト
   - `scripts/run_cluster.sh`: 3ノードクラスタ起動スクリプト
   - `scripts/stop_cluster.sh`: クラスタ停止スクリプト
   - `scripts/run_failover_test.sh`: カオス・フェイルオーバー検証スクリプト
   - `scripts/test_all.sh`: 全自動総合テストスクリプト
   ※事前にフル実装で全テスト100%通過を確認済みです。

2. **実装対象（本体コード）**:
   - `internal/store/store.go` (Step 1)
   - `internal/cluster/node.go` (Step 2)
   - `internal/server/server.go` (Step 3)
   - `cmd/node/main.go` (Step 4)

---

## テストの実行方法

各ステップの実装が進んだら、以下のコマンドで動作を確認できます。

### 単体テスト
```bash
go test -v ./internal/store/...
go test -v ./internal/cluster/...
```

### クラスタ手動起動 & テスター実行
```bash
./scripts/run_cluster.sh
go run cmd/tester/main.go -mode all
./scripts/stop_cluster.sh
```

### カオス・フェイルオーバーテスト
```bash
./scripts/run_failover_test.sh
```

### 全テストの一括自動実行（完成確認）
```bash
./scripts/test_all.sh
```
`ALL TEST SUITES COMPLETED AND PASSED (全テスト合格)!` が表示されれば完成です！
