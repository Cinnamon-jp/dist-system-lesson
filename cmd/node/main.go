package main

import (
	"flag"
	"strings"

	"dist_system/internal/model"
)

func main() {
	var (
		id        = flag.String("id", "node-1", "Unique node ID")
		addr      = flag.String("addr", "127.0.0.1:8001", "Listen address (host:port)")
		peersFlag = flag.String("peers", "", "Comma-separated list of peer addresses")
		roleStr   = flag.String("role", "Backup", "Initial role (Primary or Backup)")
		hbMs      = flag.Int("hb-ms", 150, "Heartbeat interval in milliseconds")
		elMinMs   = flag.Int("el-min-ms", 400, "Election timeout minimum in milliseconds")
		elMaxMs   = flag.Int("el-max-ms", 800, "Election timeout maximum in milliseconds")
	)
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			p = strings.TrimSpace(p)
			if p != "" && p != *addr {
				peers = append(peers, p)
			}
		}
	}

	_ = model.Role(*roleStr)
	_ = id
	_ = addr
	_ = peers
	_ = hbMs
	_ = elMinMs
	_ = elMaxMs

	// TODO: 以下の手順でノードを起動するメイン処理を実装してください:
	// 1. cluster.Config を構築
	// 2. store.NewStore() と cluster.NewNode() を作成
	// 3. server.NewServer() を作成
	// 4. http.Server を立ち上げ、node.Start(ctx) でバックグラウンド処理を開始
	// 5. os/signal で SIGINT / SIGTERM を待ち受け、Graceful Shutdown を行う
}
