// Command loadgen replays multi-turn conversations against the gateway and
// reports latency percentiles, throughput and how requests were routed. Run
// it with prefix affinity on and off to measure the value of KV cache reuse.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type result struct {
	ttft, total time.Duration
	tokens      int
	worker      string
	route       string
	cache       string
	err         error
}

func main() {
	var (
		url           = flag.String("url", "http://localhost:8080", "gateway base URL")
		key           = flag.String("key", os.Getenv("GATEWAY_API_KEY"), "API key")
		model         = flag.String("model", "llama3-8b", "model to request")
		conversations = flag.Int("conversations", 48, "number of conversations")
		turns         = flag.Int("turns", 4, "user turns per conversation")
		concurrency   = flag.Int("concurrency", 16, "conversations in flight")
		systemPrompts = flag.Int("system-prompts", 4, "distinct shared system prompts")
		systemWords   = flag.Int("system-words", 1200, "words per system prompt (the shared prefix)")
		maxTokens     = flag.Int("max-tokens", 48, "max_tokens per request")
		adminToken    = flag.String("admin-token", os.Getenv("GATEWAY_ADMIN_TOKEN"), "admin token, to print worker prefix-cache hit rates")
	)
	flag.Parse()

	prompts := make([]string, *systemPrompts)
	for i := range prompts {
		prompts[i] = systemPrompt(i, *systemWords)
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	results := make(chan result, *conversations**turns)
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *conversations; c++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(c int) {
			defer wg.Done()
			defer func() { <-sem }()
			msgs := []message{{"system", prompts[c%len(prompts)]}}
			for t := 0; t < *turns; t++ {
				msgs = append(msgs, message{"user", fmt.Sprintf("Conversation %d, question %d: what should I check next?", c, t)})
				r, reply := call(client, *url, *key, *model, msgs, *maxTokens)
				results <- r
				if r.err != nil {
					return
				}
				msgs = append(msgs, message{"assistant", reply})
			}
		}(c)
	}
	wg.Wait()
	close(results)
	report(results, time.Since(start))
	if *adminToken != "" {
		printWorkers(client, *url, *adminToken)
	}
}

func call(client *http.Client, url, key, model string, msgs []message, maxTokens int) (result, string) {
	body, _ := json.Marshal(map[string]any{
		"model": model, "messages": msgs, "stream": true, "max_tokens": maxTokens,
		"stream_options": map[string]bool{"include_usage": true},
	})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, strings.TrimRight(url, "/")+"/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return result{err: err}, ""
	}
	defer resp.Body.Close()
	r := result{worker: resp.Header.Get("X-Gateway-Worker"), route: resp.Header.Get("X-Gateway-Route"), cache: resp.Header.Get("X-Gateway-Cache")}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		r.err = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		return r, ""
	}
	var reply strings.Builder
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *struct {
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		if chunk.Error != nil {
			r.err = fmt.Errorf("stream error: %s", chunk.Error.Message)
			return r, ""
		}
		if chunk.Usage != nil {
			r.tokens = chunk.Usage.CompletionTokens
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
			if r.ttft == 0 {
				r.ttft = time.Since(start)
			}
			reply.WriteString(chunk.Choices[0].Delta.Content)
		}
	}
	r.total = time.Since(start)
	return r, reply.String()
}

func report(results <-chan result, elapsed time.Duration) {
	var ttfts, totals []time.Duration
	tokens, errs := 0, 0
	workers, routes, caches := map[string]int{}, map[string]int{}, map[string]int{}
	var firstErr error
	for r := range results {
		if r.err != nil {
			errs++
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		ttfts = append(ttfts, r.ttft)
		totals = append(totals, r.total)
		tokens += r.tokens
		workers[r.worker]++
		routes[r.route]++
		caches[r.cache]++
	}
	fmt.Printf("requests      %d ok, %d failed in %s\n", len(ttfts), errs, elapsed.Round(time.Millisecond))
	if firstErr != nil {
		fmt.Printf("first error   %v\n", firstErr)
	}
	if len(ttfts) == 0 {
		return
	}
	fmt.Printf("TTFT          p50 %-8s p95 %-8s p99 %s\n", pct(ttfts, .5), pct(ttfts, .95), pct(ttfts, .99))
	fmt.Printf("latency       p50 %-8s p95 %-8s p99 %s\n", pct(totals, .5), pct(totals, .95), pct(totals, .99))
	fmt.Printf("throughput    %.0f completion tok/s, %.1f req/s\n", float64(tokens)/elapsed.Seconds(), float64(len(ttfts))/elapsed.Seconds())
	fmt.Printf("workers       %s\n", fmtMap(workers))
	fmt.Printf("routes        %s\n", fmtMap(routes))
	fmt.Printf("cache         %s\n", fmtMap(caches))
}

func printWorkers(client *http.Client, url, token string) {
	req, _ := http.NewRequest(http.MethodGet, strings.TrimRight(url, "/")+"/admin/v1/workers", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var out struct {
		Workers []struct {
			ID        string  `json:"id"`
			PrefixHit float64 `json:"prefix_cache_hit_rate"`
			TTFTp99   float64 `json:"ttft_p99_ms"`
		} `json:"workers"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return
	}
	for _, w := range out.Workers {
		fmt.Printf("worker %-10s prefix-cache hit %5.1f%%   ttft p99 %.0fms\n", w.ID, 100*w.PrefixHit, w.TTFTp99)
	}
}

func pct(ds []time.Duration, q float64) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(int(q*float64(len(s))), len(s)-1)].Round(time.Millisecond)
}

func fmtMap(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		name := k
		if name == "" {
			name = "-"
		}
		parts[i] = fmt.Sprintf("%s=%d", name, m[k])
	}
	return strings.Join(parts, " ")
}

var words = strings.Fields(`policy contract clause liability indemnity warranty termination notice
jurisdiction arbitration confidentiality assignment payment schedule milestone acceptance criteria
service level availability incident severity escalation audit compliance retention encryption`)

// systemPrompt builds a long, deterministic system prompt: the shared prefix
// that prefix caching can reuse across requests.
func systemPrompt(i, n int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "You are assistant #%d. Follow the policy below exactly.\n", i)
	for w := 0; w < n; w++ {
		sb.WriteString(words[(w*7+i*13)%len(words)])
		if w%12 == 11 {
			sb.WriteString(".\n")
		} else {
			sb.WriteByte(' ')
		}
	}
	return sb.String()
}
