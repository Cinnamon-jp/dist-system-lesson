package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dist_system/internal/model"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

type Tester struct {
	nodes      []string
	client     *http.Client
	httpClient *http.Client // リダイレクトを自動追従しないクライアント
}

func NewTester(nodes []string) *Tester {
	return &Tester{
		nodes: nodes,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // 307 を直接検知したい場合
			},
		},
	}
}

func main() {
	var (
		nodesFlag   = flag.String("nodes", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "Comma-separated node addresses")
		mode        = flag.String("mode", "all", "Test mode: status, consistency, benchmark, all")
		numKeys     = flag.Int("n", 30, "Number of keys for consistency test")
		concurrency = flag.Int("c", 10, "Concurrency level for benchmark")
		benchReqs   = flag.Int("bench-n", 100, "Number of requests for benchmark")
	)
	flag.Parse()

	var nodes []string
	for _, n := range strings.Split(*nodesFlag, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			nodes = append(nodes, n)
		}
	}

	if len(nodes) == 0 {
		fmt.Printf("%s[ERROR] No nodes specified%s\n", colorRed, colorReset)
		os.Exit(1)
	}

	tester := NewTester(nodes)

	fmt.Printf("%s%s=== 分散KVS 検証テストプログラム (Distributed KVS Tester) ===%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("Target Nodes: %v\n\n", nodes)

	allPassed := true

	switch *mode {
	case "status":
		tester.PrintClusterStatus()
	case "consistency":
		if err := tester.RunConsistencyTest(*numKeys); err != nil {
			allPassed = false
		}
	case "benchmark":
		if err := tester.RunBenchmark(*concurrency, *benchReqs); err != nil {
			allPassed = false
		}
	case "all":
		tester.PrintClusterStatus()
		fmt.Println()

		if err := tester.RunConsistencyTest(*numKeys); err != nil {
			allPassed = false
		}
		fmt.Println()

		if err := tester.RunBenchmark(*concurrency, *benchReqs); err != nil {
			allPassed = false
		}
	default:
		fmt.Printf("%sUnknown mode: %s%s\n", colorRed, *mode, colorReset)
		os.Exit(1)
	}

	fmt.Println()
	if allPassed {
		fmt.Printf("%s%s========================================%s\n", colorBold, colorGreen, colorReset)
		fmt.Printf("%s%s[OVERALL RESULT: ALL TESTS PASSED! (合格)]%s\n", colorBold, colorGreen, colorReset)
		fmt.Printf("%s%s========================================%s\n", colorBold, colorGreen, colorReset)
		os.Exit(0)
	} else {
		fmt.Printf("%s%s========================================%s\n", colorBold, colorRed, colorReset)
		fmt.Printf("%s%s[OVERALL RESULT: TESTS FAILED (不合格)]%s\n", colorBold, colorRed, colorReset)
		fmt.Printf("%s%s========================================%s\n", colorBold, colorRed, colorReset)
		os.Exit(1)
	}
}

// PrintClusterStatus は全ノードのステータスを取得して出力します
func (t *Tester) PrintClusterStatus() {
	fmt.Printf("%s--- 1. クラスタステータス確認 ---%s\n", colorBold, colorReset)
	for _, addr := range t.nodes {
		url := fmt.Sprintf("http://%s/v1/cluster/status", addr)
		resp, err := t.client.Get(url)
		if err != nil {
			fmt.Printf("  [%s] %sDOWN%s (%v)\n", addr, colorRed, colorReset, err)
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		var status model.StatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			fmt.Printf("  [%s] %sINVALID RESPONSE%s\n", addr, colorRed, colorReset)
			continue
		}

		roleColor := colorYellow
		if status.Role == model.RolePrimary {
			roleColor = colorGreen
		}

		fmt.Printf("  [%s] ID=%-7s Role=%s%-8s%s Term=%-2d Keys=%-3d PrimaryAddr=%s\n",
			addr, status.NodeID, roleColor, status.Role, colorReset, status.Term, status.KeyCount, status.PrimaryAddr)
	}
}

// findPrimary はクラスタ内のPrimaryノードのアドレスを探します
func (t *Tester) findPrimary() (string, error) {
	for _, addr := range t.nodes {
		url := fmt.Sprintf("http://%s/v1/cluster/status", addr)
		resp, err := t.client.Get(url)
		if err != nil {
			continue
		}
		var status model.StatusResponse
		_ = json.NewDecoder(resp.Body).Decode(&status)
		_ = resp.Body.Close()

		if status.Role == model.RolePrimary {
			return addr, nil
		}
		if status.PrimaryAddr != "" {
			return status.PrimaryAddr, nil
		}
	}
	return "", fmt.Errorf("no primary node found in cluster")
}

// PutWithSmartRedirect はPrimary宛てにPUTを送信し、必要に応じてリダイレクトを追従します
func (t *Tester) PutWithSmartRedirect(targetAddr, key, value string) (model.PutResponse, string, error) {
	putBody, _ := json.Marshal(model.PutRequest{Value: value})

	currentAddr := targetAddr
	for attempts := 0; attempts < 3; attempts++ {
		url := fmt.Sprintf("http://%s/v1/kv/%s", currentAddr, key)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(putBody))
		if err != nil {
			return model.PutResponse{}, currentAddr, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := t.httpClient.Do(req)
		if err != nil {
			return model.PutResponse{}, currentAddr, err
		}

		if resp.StatusCode == http.StatusTemporaryRedirect {
			loc := resp.Header.Get("Location")
			_ = resp.Body.Close()
			if loc != "" {
				// Location URLからホストを抽出
				loc = strings.TrimPrefix(loc, "http://")
				parts := strings.Split(loc, "/")
				currentAddr = parts[0]
				continue
			}
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			var putResp model.PutResponse
			if err := json.Unmarshal(bodyBytes, &putResp); err != nil {
				return model.PutResponse{}, currentAddr, err
			}
			return putResp, currentAddr, nil
		}

		return model.PutResponse{}, currentAddr, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return model.PutResponse{}, currentAddr, fmt.Errorf("exceeded redirect attempts")
}

// RunConsistencyTest はデータの書き込みと全ノードでの整合性（バージョン一致）を検証します
func (t *Tester) RunConsistencyTest(numKeys int) error {
	fmt.Printf("%s--- 2. データ整合性・レプリケーション検証 (Total Keys: %d) ---%s\n", colorBold, numKeys, colorReset)

	primaryAddr, err := t.findPrimary()
	if err != nil {
		fmt.Printf("  %s[FAIL] Primaryノードの検出に失敗: %v%s\n", colorRed, err, colorReset)
		return err
	}
	fmt.Printf("  Detected Primary: %s%s%s\n", colorGreen, primaryAddr, colorReset)

	// A. 順次書き込み & 全ノード整合性チェック
	failed := false
	startTime := time.Now()

	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("test-key-%03d", i)
		expectedVal := fmt.Sprintf("val-%d-%d", i, time.Now().UnixNano())

		// Primaryへ書き込み
		putResp, actualPrimary, err := t.PutWithSmartRedirect(primaryAddr, key, expectedVal)
		if err != nil {
			fmt.Printf("  %s[FAIL]%s Key=%s PUT失敗: %v\n", colorRed, colorReset, key, err)
			failed = true
			break
		}
		primaryAddr = actualPrimary

		if putResp.Value != expectedVal {
			fmt.Printf("  %s[FAIL]%s Key=%s PUT値不一致: expected=%s, got=%s\n", colorRed, colorReset, key, expectedVal, putResp.Value)
			failed = true
			break
		}

		// 全ノードからGETして値とバージョンが同一か確認（伝播待ちのリトライ付き）
		for _, nodeAddr := range t.nodes {
			nodeOK := false
			var lastErr string

			for retry := 0; retry < 10; retry++ {
				url := fmt.Sprintf("http://%s/v1/kv/%s", nodeAddr, key)
				getResp, err := t.client.Get(url)
				if err != nil {
					lastErr = fmt.Sprintf("unreachable: %v", err)
					time.Sleep(20 * time.Millisecond)
					continue
				}

				if getResp.StatusCode == http.StatusOK {
					var item model.GetResponse
					_ = json.NewDecoder(getResp.Body).Decode(&item)
					_ = getResp.Body.Close()

					if item.Value == expectedVal && item.Version == putResp.Version {
						nodeOK = true
						break
					}
					lastErr = fmt.Sprintf("inconsistent: expected (%s, v%d), got (%s, v%d)",
						expectedVal, putResp.Version, item.Value, item.Version)
				} else {
					body, _ := io.ReadAll(getResp.Body)
					_ = getResp.Body.Close()
					lastErr = fmt.Sprintf("HTTP %d: %s", getResp.StatusCode, string(body))
				}

				time.Sleep(20 * time.Millisecond)
			}

			if !nodeOK {
				fmt.Printf("  %s[FAIL]%s Node=%s Key=%s 読み出し検証失敗: %s\n",
					colorRed, colorReset, nodeAddr, key, lastErr)
				failed = true
				break
			}
		}

		if failed {
			break
		}
	}

	// B. 存在しないキーの404チェック
	dummyURL := fmt.Sprintf("http://%s/v1/kv/non-existent-key-9999", primaryAddr)
	resp404, err := t.client.Get(dummyURL)
	if err == nil {
		defer func() { _ = resp404.Body.Close() }()
		if resp404.StatusCode != http.StatusNotFound {
			fmt.Printf("  %s[FAIL]%s Expected 404 for missing key, got %d\n", colorRed, colorReset, resp404.StatusCode)
			failed = true
		}
	}

	elapsed := time.Since(startTime)
	if failed {
		fmt.Printf("  %s[FAIL] 整合性テストに失敗しました%s\n", colorRed, colorReset)
		return fmt.Errorf("consistency test failed")
	}

	fmt.Printf("  %s[PASS]%s 全 %d 件の書き込みと全ノード同期の検証に成功しました (所要時間: %v)\n",
		colorGreen, colorReset, numKeys, elapsed)
	return nil
}

// RunBenchmark は並行負荷テストを実行します
func (t *Tester) RunBenchmark(concurrency, totalReqs int) error {
	fmt.Printf("%s--- 3. 並行負荷テスト (Workers: %d, Total Requests: %d) ---%s\n", colorBold, concurrency, totalReqs, colorReset)

	primaryAddr, err := t.findPrimary()
	if err != nil {
		fmt.Printf("  %s[FAIL] Primaryノードの検出に失敗: %v%s\n", colorRed, err, colorReset)
		return err
	}

	var (
		successCount atomic.Int64
		failCount    atomic.Int64
		wg           sync.WaitGroup
	)

	reqsPerWorker := totalReqs / concurrency
	startTime := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < reqsPerWorker; i++ {
				key := fmt.Sprintf("bench-k-%d", rand.Intn(20)) // 競合が起きるようキー数を絞る
				val := fmt.Sprintf("v-%d-%d", workerID, i)

				_, _, err := t.PutWithSmartRedirect(primaryAddr, key, val)
				if err != nil {
					failCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	sCount := successCount.Load()
	fCount := failCount.Load()
	total := sCount + fCount
	rps := float64(total) / elapsed.Seconds()

	fmt.Printf("  Total: %d reqs, Success: %s%d%s, Failed: %s%d%s\n",
		total, colorGreen, sCount, colorReset, colorRed, fCount, colorReset)
	fmt.Printf("  Elapsed: %v, Throughput: %.1f req/sec\n", elapsed, rps)

	if fCount > 0 {
		fmt.Printf("  %s[FAIL] 一部の並行リクエストが失敗しました%s\n", colorRed, colorReset)
		return fmt.Errorf("benchmark failed with %d errors", fCount)
	}

	fmt.Printf("  %s[PASS]%s 並行リクエストが整合性を崩さず正常に完了しました\n", colorGreen, colorReset)
	return nil
}
