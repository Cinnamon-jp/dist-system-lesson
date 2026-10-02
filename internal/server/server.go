package server

import (
	"net/http"

	"dist_system/internal/cluster"
)

type Server struct {
	node *cluster.Node
	mux  *http.ServeMux
}

func NewServer(node *cluster.Node) *Server {
	s := &Server{
		node: node,
		mux:  http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	// TODO: Go 1.22+ ルーティングを用いてエンドポイントを登録してください
	// クライアント向けAPI:
	//   PUT /v1/kv/{key}
	//   GET /v1/kv/{key}
	//   GET /v1/cluster/status
	// 内部ノード間協調API:
	//   POST /internal/replicate
	//   POST /internal/heartbeat
	//   POST /internal/request-vote

	s.mux.HandleFunc("PUT /v1/kv/{key}", s.handlePutKV)
	s.mux.HandleFunc("GET /v1/kv/{key}", s.handleGetKV)
	s.mux.HandleFunc("GET /v1/cluster/status", s.handleClusterStatus)
	s.mux.HandleFunc("POST /internal/replicate", s.handleReplicate)
	s.mux.HandleFunc("POST /internal/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /internal/request-vote", s.handleRequestVote)
}

func (s *Server) handlePutKV(w http.ResponseWriter, r *http.Request) {
	// TODO: PUT /v1/kv/{key} のハンドラを実装してください
	// - 自身がPrimaryでない場合は 307 Temporary Redirect（LocationヘッダにPrimaryのアドレス）
	// - Quorum不成立時は 503
	// - 成功時は 200 OK + PutResponse JSON
}

func (s *Server) handleGetKV(w http.ResponseWriter, r *http.Request) {
	// TODO: GET /v1/kv/{key} のハンドラを実装してください
	// - 存在時は 200 OK + GetResponse JSON
	// - 不在時は 404 Not Found
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	// TODO: GET /v1/cluster/status のハンドラを実装してください
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	// TODO: POST /internal/replicate のハンドラを実装してください
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// TODO: POST /internal/heartbeat のハンドラを実装してください
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	// TODO: POST /internal/request-vote のハンドラを実装してください
}
