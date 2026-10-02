package model

import "time"

type Role string

const (
	RolePrimary   Role = "Primary"
	RoleBackup    Role = "Backup"
	RoleCandidate Role = "Candidate"
)

// Item はKVS内に保持される単一のエントリを表します
type Item struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// クライアント向けAPIリクエスト/レスポンス

type PutRequest struct {
	Value string `json:"value"`
}

type PutResponse struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version int64  `json:"version"`
}

type GetResponse struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

type StatusResponse struct {
	NodeID      string   `json:"node_id"`
	Role        Role     `json:"role"`
	Term        int64    `json:"term"`
	PrimaryAddr string   `json:"primary_addr,omitempty"`
	Peers       []string `json:"peers"`
	KeyCount    int      `json:"key_count"`
}

// 内部ノード間通信リクエスト/レスポンス

type ReplicateRequest struct {
	Term   int64  `json:"term"`
	Item   Item   `json:"item"`
	NodeID string `json:"node_id"`
}

type ReplicateResponse struct {
	Success bool   `json:"success"`
	Term    int64  `json:"term"`
	Message string `json:"message,omitempty"`
}

type HeartbeatRequest struct {
	Term        int64  `json:"term"`
	PrimaryID   string `json:"primary_id"`
	PrimaryAddr string `json:"primary_addr"`
}

type HeartbeatResponse struct {
	Success bool  `json:"success"`
	Term    int64 `json:"term"`
}

type VoteRequest struct {
	Term          int64  `json:"term"`
	CandidateID   string `json:"candidate_id"`
	CandidateAddr string `json:"candidate_addr"`
}

type VoteResponse struct {
	VoteGranted bool  `json:"vote_granted"`
	Term        int64 `json:"term"`
}
