# 分散KVS 手動実装ガイド & 実装確認仕様書

このドキュメントは、提供されているテストプログラム群（`cmd/tester`, `scripts/test_all.sh`, 各種単体テスト）をすべてパスさせるための、分散システム本体の完全な実装仕様とステップバイステップのガイドです。

---

## 1. 全体像と完成確認の流れ

本リポジトリには、動作確認済みのテストプログラム群がすでに配置されています。
あなたが本体コードを実装し、以下のコマンドを実行して**すべてPASS（終了コード0）**になれば完成です。

```bash
# 全テストの一括実行（単体テスト ➔ E2Eクラスタ整合性テスト ➔ カオス・フェイルオーバーテスト）
./scripts/test_all.sh
```

---

## 2. ディレクトリ構成と役割分担

```
.
├── cmd/
│   ├── node/              ★ 手動実装対象: ノード本体のエントリポイント
│   │   └── main.go
│   └── tester/            ✓ 完成済み: リクエスト送信・検証用CLIツール
│       └── main.go
├── internal/
│   ├── model/             ✓ 完成済み: 共通データモデル（リクエスト/レスポンス型）
│   │   └── model.go
│   ├── store/             ★ 手動実装対象: インメモリストア
│   │   ├── store.go       (実装対象)
│   │   └── store_test.go  (単体テスト)
│   ├── cluster/           ★ 手動実装対象: クラスタ管理、Quorumレプリケーション、選挙
│   │   ├── node.go        (実装対象)
│   │   └── cluster_test.go (結合単体テスト)
│   └── server/            ★ 手動実装対象: HTTPサーバー & ルーティング
│       └── server.go
├── deployments/           ✓ 完成済み: Podman & kind用設定
├── scripts/               ✓ 完成済み: 起動・停止・カオステスト用シェルスクリプト
│   ├── run_cluster.sh
│   ├── stop_cluster.sh
│   ├── run_failover_test.sh
│   └── test_all.sh
└── docs/
    ├── system_design.md
    └── implementation_guide.md (本ドキュメント)
```

---

## 3. ステップバイステップ実装ガイド

### Step 1: インメモリストア (`internal/store/store.go`)

スレッドセーフなオンメモリKVSを実装します。

#### 要件
1. **スレッドセーフ性**: `sync.RWMutex` を使用し、並行読み出し・排他書き込みを制御する。
2. **バージョニング（論理タイムスタンプ）**:
   - `Put(key, value string, version int64)`:
     - `version <= 0` の場合（Primaryによる書き込み）: 既存キーがあれば `Version + 1`、新規キーなら `1` を自動採番。
     - `version > 0` の場合（Backupへの複製適用）: 既存のバージョンより小さい値が渡された場合は古い更新（順序逆転）とみなし `ErrStaleVersion` を返して更新を拒否する。
3. **確認コマンド**:
   ```bash
   go test -v ./internal/store/...
   ```

---

### Step 2: クラスタ制御・レプリケーション・選挙 (`internal/cluster/node.go`)

分散システムの最も重要で中核となるロジックです。

#### 要件
1. **ノードの初期化 (`NewNode`)**:
   - 初期ロール（PrimaryまたはBackup）、ピアアドレス一覧、タイマー設定を保持。
2. **バックグラウンドループ (`runLoop`)**:
   - Primaryの場合: `HeartbeatInterval`（推奨: 150ms）ごとに全ピアへ `POST /internal/heartbeat` を送信。
   - Backupの場合: 一定時間（ジッター付き `400ms〜800ms`）ハートビートが途絶えたら `startElection()` をトリガー。
3. **過半数（Quorum）レプリケーション (`HandlePut`)**:
   - Primaryノードのみ受付（Backupの場合は `ErrNotPrimary` を返す）。
   - まずローカルストアに書き込む（1台確定）。
   - 全ピアへ並行して `POST /internal/replicate` を送信。
     > **重要ポイント**: ゴルーチン内でHTTPリクエストを送る際、クライアントリクエストのコンテキストに引きずられて途中でキャンセルされないよう、ゴルーチンごとに独立したタイムアウト付きコンテキストを作成すること。
   - 全ノード数 $N = 1 + len(Peers)$ に対し、過半数 $Q = (N / 2) + 1$。
   - 必要なAck数（$Q - 1$）が集まった時点で即座にクライアントへ成功を返却（遅延ノードをブロックして待たない）。
4. **ハートビート受信 (`HandleHeartbeat`)**:
   - 相手の `Term >= n.term` であれば自身を `Backup` に維持/降格し、`lastHeartbeat` を現在時刻に更新。
5. **リーダー選出 (`startElection`)**:
   - `role = Candidate`, `term++`, 自分自身に1票（`votes = 1`）。
   - 全ピアへ並行して `POST /internal/request-vote` を送信。
   - 自身を含めて過半数（$Q$ 票）を獲得したら `RolePrimary` に昇格し、即座にハートビートを送信して全ノードに新リーダー就任を周知する。
6. **確認コマンド**:
   ```bash
   go test -v ./internal/cluster/...
   ```

---

### Step 3: HTTPサーバー & ルーティング (`internal/server/server.go`)

Go 1.22+ の `http.ServeMux` ルーティングを用いて、クライアントAPIおよび内部協調APIを公開します。

#### クライアント向けエンドポイント
- `PUT /v1/kv/{key}`
  - Body: `{"value": "..."}`
  - 成功時: HTTP 200 + `{"key":"...","value":"...","version":...}`
  - 自身がPrimaryでない場合:
    - Primaryのアドレスが判明していれば `Location: http://<primaryAddr>/v1/kv/<key>` ヘッダを付与して `307 Temporary Redirect` を返す。
  - Quorumに達しなかった場合: `503 Service Unavailable`
- `GET /v1/kv/{key}`
  - 存在時: HTTP 200 + `{"key":"...","value":"...","version":...}`
  - 不在時: `404 Not Found`
- `GET /v1/cluster/status`
  - 現在のノード状態（NodeID, Role, Term, PrimaryAddr, Peers, KeyCount）をJSON返却。

#### 内部ノード間エンドポイント
- `POST /internal/replicate`: Backupノードでの複製反映
- `POST /internal/heartbeat`: ハートビート受信とリーダー維持
- `POST /internal/request-vote`: 投票要請の承認・拒否判定

---

### Step 4: CLIエントリポイント (`cmd/node/main.go`)

- コマンドラインフラグをパース：
  - `-id`: ノードID（例: `node-1`）
  - `-addr`: バインドアドレス（例: `127.0.0.1:8001`）
  - `-peers`: カンマ区切りのピアアドレス（例: `127.0.0.1:8002,127.0.0.1:8003`）
  - `-role`: 初期ロール（`Primary` または `Backup`）
- `Node` と `Server` を生成し、バックグラウンドループを開始してHTTPサーバーをリッスン。
- `os/signal`（SIGINT, SIGTERM）を受け取って Graceful Shutdown を実行。

---

### Step 5: テスターとスクリプトによる完成確認

本体実装が完了したら、以下のスクリプトを順に実行して動作を検証します。

1. **クラスタの手動起動とテスター実行**:
   ```bash
   # 3ノードをバックグラウンド起動
   ./scripts/run_cluster.sh

   # テスターを実行（ステータス確認 ➔ 30件連続書き込み&全ノード同期 ➔ 並行負荷テスト）
   go run cmd/tester/main.go -mode all

   # クラスタ停止
   ./scripts/stop_cluster.sh
   ```

2. **カオス・フェイルオーバーテスト**:
   ```bash
   # Primaryを強制キル (kill -9) し、自動選出とデータ永続を検証
   ./scripts/run_failover_test.sh
   ```

3. **全自動ワンコマンドテスト**:
   ```bash
   ./scripts/test_all.sh
   ```
   **`ALL TEST SUITES COMPLETED AND PASSED (全テスト合格)!`** と表示されれば、あなたの分散システムは完全に完成です！
