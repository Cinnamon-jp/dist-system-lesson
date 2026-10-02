package cluster_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"dist_system/internal/cluster"
	"dist_system/internal/model"
	"dist_system/internal/server"
	"dist_system/internal/store"
)

// InProcessCluster はテスト用の3ノードクラスタをインメモリで立ち上げるヘルパーです
type InProcessCluster struct {
	Nodes   []*cluster.Node
	Servers []*httptest.Server
	Stores  []*store.Store
}

func newTestCluster(t *testing.T, count int) *InProcessCluster {
	t.Helper()

	c := &InProcessCluster{
		Nodes:   make([]*cluster.Node, count),
		Servers: make([]*httptest.Server, count),
		Stores:  make([]*store.Store, count),
	}

	// 1. 各ノードのStoreとNodeオブジェクト作成
	for i := 0; i < count; i++ {
		c.Stores[i] = store.NewStore()
		initialRole := model.RoleBackup
		if i == 0 {
			initialRole = model.RolePrimary
		}

		cfg := cluster.Config{
			ID:                 fmt.Sprintf("node-%d", i+1),
			InitialRole:        initialRole,
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		}
		c.Nodes[i] = cluster.NewNode(cfg, c.Stores[i])
	}

	// 2. httptest.Server を立ち上げてアドレスを解決
	for i := 0; i < count; i++ {
		srvHandler := server.NewServer(c.Nodes[i])
		ts := httptest.NewServer(srvHandler.Handler())
		c.Servers[i] = ts

		u, _ := url.Parse(ts.URL)
		c.Nodes[i].SetAddr(u.Host)
	}

	// 3. 各ノードに全Peerのアドレスを設定
	for i := 0; i < count; i++ {
		var peers []string
		for j := 0; j < count; j++ {
			if i != j {
				peers = append(peers, c.Nodes[j].Addr())
			}
		}
		c.Nodes[i].SetPeers(peers)
	}

	return c
}

func (c *InProcessCluster) Start(ctx context.Context) {
	for _, n := range c.Nodes {
		n.Start(ctx)
	}
}

func (c *InProcessCluster) Close() {
	for _, n := range c.Nodes {
		n.Stop()
	}
	for _, s := range c.Servers {
		s.Close()
	}
}

func TestCluster_ReplicationAndQuorum(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Primary は Node 1
	primary := c.Nodes[0]

	// 1. Primary に書き込み
	item, err := primary.HandlePut(ctx, "greeting", "hello-distributed")
	if err != nil {
		t.Fatalf("unexpected error on HandlePut: %v", err)
	}
	if item.Value != "hello-distributed" || item.Version != 1 {
		t.Fatalf("unexpected item: %+v", item)
	}

	// レプリケーション完了待ち（少し待機）
	time.Sleep(100 * time.Millisecond)

	// 2. 全ノードのStoreにデータが複製されているか確認
	for i, s := range c.Stores {
		val, ok := s.Get("greeting")
		if !ok {
			t.Fatalf("node-%d does not have 'greeting'", i+1)
		}
		if val.Value != "hello-distributed" || val.Version != 1 {
			t.Fatalf("node-%d has incorrect item: %+v", i+1, val)
		}
	}

	// 3. Backup(Node 2, 3)への直接書き込みが拒否されることを確認
	_, err = c.Nodes[1].HandlePut(ctx, "k2", "v2")
	if err == nil || !strings.Contains(err.Error(), cluster.ErrNotPrimary.Error()) {
		t.Fatalf("expected ErrNotPrimary on Backup node, got: %v", err)
	}
}

func TestCluster_QuorumToleratesSingleNodeFailure(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	primary := c.Nodes[0]

	// Node 3 (Backupの1つ) を停止
	c.Nodes[2].Stop()
	c.Servers[2].Close()

	// 3ノード中1ノードダウン（残り2ノード = 過半数 Quorum: 2）
	// Primaryは正常に書き込みできるはず
	item, err := primary.HandlePut(ctx, "resilient-key", "survives-single-failure")
	if err != nil {
		t.Fatalf("expected write to succeed with 1 dead backup (quorum 2/3), but got: %v", err)
	}
	if item.Value != "survives-single-failure" {
		t.Fatalf("unexpected value: %s", item.Value)
	}

	// Node 2 (生き残ったBackup) に反映されているか
	time.Sleep(50 * time.Millisecond)
	val, ok := c.Stores[1].Get("resilient-key")
	if !ok || val.Value != "survives-single-failure" {
		t.Fatalf("surviving backup node-2 missing data: %+v", val)
	}
}

func TestCluster_LeaderElectionOnPrimaryFailure(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	primary := c.Nodes[0]

	// データを1つ書き込んでおく
	_, err := primary.HandlePut(ctx, "leader-test", "val-init")
	if err != nil {
		t.Fatalf("HandlePut failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Primary (Node 1) を突然停止！
	t.Log("Stopping primary node-1...")
	c.Nodes[0].Stop()
	c.Servers[0].Close()

	// 生き残った Node 2, 3 のどちらかが選挙で新Primaryに昇格するのを待つ
	var newPrimary *cluster.Node
	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

findLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for new primary election")
		case <-ticker.C:
			for i := 1; i < 3; i++ {
				status := c.Nodes[i].GetStatus()
				if status.Role == model.RolePrimary {
					newPrimary = c.Nodes[i]
					t.Logf("Node %s successfully elected as NEW PRIMARY for Term %d!", status.NodeID, status.Term)
					break findLoop
				}
			}
		}
	}

	// 新Primaryに新しいデータが書き込めるか検証
	putResp, err := newPrimary.HandlePut(ctx, "after-election", "new-leader-works")
	if err != nil {
		t.Fatalf("failed to write to new primary: %v", err)
	}
	if putResp.Value != "new-leader-works" {
		t.Fatalf("unexpected put response: %+v", putResp)
	}

	// 以前書き込んだデータも新Primaryに残っているか検証
	item, ok := newPrimary.GetKey("leader-test")
	if !ok || item.Value != "val-init" {
		t.Fatalf("new primary lost pre-existing data: %+v", item)
	}
}
