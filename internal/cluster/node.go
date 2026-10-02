package cluster

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"dist_system/internal/model"
	"dist_system/internal/store"
)

var (
	ErrNotPrimary     = errors.New("not primary node")
	ErrQuorumNotMet   = errors.New("failed to reach write quorum")
	ErrReplicateStale = errors.New("stale replication request")
)

type Config struct {
	ID                 string
	Addr               string
	Peers              []string
	InitialRole        model.Role
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

type Node struct {
	cfg        Config
	store      *store.Store
	httpClient *http.Client

	mu            sync.RWMutex
	role          model.Role
	term          int64
	primaryAddr   string
	votedFor      string
	lastHeartbeat time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
}

func NewNode(cfg Config, s *store.Store) *Node {
	// TODO: Node構造体の初期化とデフォルト設定を実装してください
	return &Node{
		cfg:        cfg,
		store:      s,
		httpClient: &http.Client{Timeout: 500 * time.Millisecond},
		role:       cfg.InitialRole,
		stopCh:     make(chan struct{}),
	}
}

// Start はノードのバックグラウンドタスク（ハートビート送信または選挙タイマー）を開始します
func (n *Node) Start(ctx context.Context) {
	// TODO: バックグラウンドゴルーチンで定期処理（ハートビート送信/選挙タイマー監視）を実装してください
}

// Stop はノードのバックグラウンド処理を停止します
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
}

// Addr はノードのアドレスを返します
func (n *Node) Addr() string {
	return n.cfg.Addr
}

// SetAddr はノードのアドレスを設定します（テスト用）
func (n *Node) SetAddr(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cfg.Addr = addr
}

// SetPeers はノードのピア一覧を更新します（テスト用）
func (n *Node) SetPeers(peers []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cfg.Peers = peers
}

// GetStatus は現在のノードのステータスを返します
func (n *Node) GetStatus() model.StatusResponse {
	// TODO: 現在のRole, Term, PrimaryAddr, Peers, 保持キー数などを返す処理を実装してください
	return model.StatusResponse{}
}

// HandlePut はクライアントからの書き込み要求を処理します（Primaryのみ）
// ローカルストアに書き込んだ後、全ピアへ並行レプリケーションを送信し、過半数（Quorum）の合意を確認して完了を返します
func (n *Node) HandlePut(ctx context.Context, key, value string) (model.Item, error) {
	// TODO: Quorumレプリケーション書き込み処理を実装してください
	return model.Item{}, nil
}

// HandleReplicate はBackupノードがPrimaryからの複製要求を処理します
func (n *Node) HandleReplicate(req model.ReplicateRequest) model.ReplicateResponse {
	// TODO: Primaryからのデータ複製要求をローカルストアに反映する処理を実装してください
	return model.ReplicateResponse{}
}

// HandleHeartbeat はハートビートを受信したときの処理です
func (n *Node) HandleHeartbeat(req model.HeartbeatRequest) model.HeartbeatResponse {
	// TODO: ハートビート受信とリーダーリース維持/タイマーリセット処理を実装してください
	return model.HeartbeatResponse{}
}

// HandleRequestVote はCandidateからの投票要請を処理します
func (n *Node) HandleRequestVote(req model.VoteRequest) model.VoteResponse {
	// TODO: 投票可否の判定（Term比較、1世代1票）処理を実装してください
	return model.VoteResponse{}
}

// GetKey はStoreからキーを取得します
func (n *Node) GetKey(key string) (model.Item, bool) {
	return n.store.Get(key)
}
