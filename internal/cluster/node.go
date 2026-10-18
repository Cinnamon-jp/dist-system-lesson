package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
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
	Addr               string   // IPアドレス+ポート
	Peers              []string // ペアのIPアドレス+ポート
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
	if cfg.InitialRole == "" {
		cfg.InitialRole = model.RoleBackup
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 400 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= 0 {
		cfg.ElectionTimeoutMax = 800 * time.Millisecond
	}

	primaryAddr := ""
	if cfg.InitialRole == model.RolePrimary {
		primaryAddr = cfg.Addr
	}

	return &Node{
		cfg:           cfg,
		store:         s,
		httpClient:    &http.Client{Timeout: 500 * time.Millisecond},
		role:          cfg.InitialRole,
		term:          1,
		primaryAddr:   primaryAddr,
		lastHeartbeat: time.Now(),
		stopCh:        make(chan struct{}),
	}
}

// Start はノードのバックグラウンドタスク（ハートビート送信または選挙タイマー）を開始します
func (n *Node) Start(ctx context.Context) {
	go n.runLoop(ctx)
}

func (n *Node) runLoop(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	electionTimeout := n.randomElectionTimeout()
	lastHeartbeatSent := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case now := <-ticker.C:
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			if role == model.RolePrimary {
				if now.Sub(lastHeartbeatSent) >= n.cfg.HeartbeatInterval {
					lastHeartbeatSent = now
					n.sendHeartbeats()
				}
			} else {
				n.mu.Lock()
				elapsed := now.Sub(n.lastHeartbeat)
				n.mu.Unlock()

				if elapsed >= electionTimeout {
					n.startElection(ctx)
					electionTimeout = n.randomElectionTimeout()
				}
			}
		}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.cfg.ElectionTimeoutMin
	max := n.cfg.ElectionTimeoutMax

	diff := max - min
	if diff <= 0 {
		return min
	}

	randomExtra := rand.N(diff)
	return min + randomExtra
}

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	if n.role != model.RolePrimary {
		n.mu.RUnlock()
		return
	}
	term := n.term
	id := n.cfg.ID
	addr := n.cfg.Addr
	peers := make([]string, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
	n.mu.RUnlock()

	reqBody := model.HeartbeatRequest{
		Term:        term,
		PrimaryID:   id,
		PrimaryAddr: addr,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return
	}

	for _, peer := range peers {
		go func(peerAddr string) {
			url := fmt.Sprintf("http://%s/internal/heartbeat", peerAddr)
			resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var hbResp model.HeartbeatResponse
			if err := json.NewDecoder(resp.Body).Decode(&hbResp); err != nil {
				return
			}

			if hbResp.Term > term {
				n.mu.Lock()
				if hbResp.Term > n.term {
					n.term = hbResp.Term
					n.role = model.RoleBackup
					n.primaryAddr = ""
					n.votedFor = ""
				}
				n.mu.Unlock()
			}
		}(peer)
	}
}

func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	if n.role == model.RolePrimary {
		n.mu.Unlock()
		return
	}

	n.role = model.RoleCandidate
	n.term++
	n.votedFor = n.cfg.ID
	n.lastHeartbeat = time.Now()
	currentTerm := n.term
	candidateID := n.cfg.ID
	candidateAddr := n.cfg.Addr
	peers := make([]string, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
	n.mu.Unlock()

	totalNodes := len(peers) + 1
	quorum := (totalNodes / 2) + 1

	if totalNodes == 1 {
		n.promoteToPrimary(currentTerm)
		return
	}

	reqBody := model.VoteRequest{
		Term:          currentTerm,
		CandidateID:   candidateID,
		CandidateAddr: candidateAddr,
	}
	data, _ := json.Marshal(reqBody)

	votesChan := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(peerAddr string) {
			url := fmt.Sprintf("http://%s/internal/request-vote", peerAddr)
			resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
			if err != nil {
				votesChan <- false
				return
			}
			defer resp.Body.Close()

			var vResp model.VoteResponse
			if err := json.NewDecoder(resp.Body).Decode(&vResp); err != nil {
				votesChan <- false
				return
			}

			if vResp.Term > currentTerm {
				n.mu.Lock()
				if vResp.Term > n.term {
					n.term = vResp.Term
					n.role = model.RoleBackup
					n.votedFor = ""
				}
				n.mu.Unlock()
			}

			votesChan <- vResp.VoteGranted
		}(peer)
	}

	grantedVotes := 1
	for i := 0; i < len(peers); i++ {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case granted := <-votesChan:
			if granted {
				grantedVotes++
			}
			if grantedVotes >= quorum {
				n.promoteToPrimary(currentTerm)
				return
			}
		}
	}
}

func (n *Node) promoteToPrimary(term int64) {
	n.mu.Lock()
	if n.role != model.RoleCandidate || n.term != term {
		n.mu.Unlock()
		return
	}
	n.role = model.RolePrimary
	n.primaryAddr = n.cfg.Addr
	n.mu.Unlock()

	n.sendHeartbeats()
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
