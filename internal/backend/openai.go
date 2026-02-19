package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// openAIAdapter talks to vLLM and other OpenAI-compatible servers.
type openAIAdapter struct {
	client        *http.Client
	kind          Kind
	scrapeMetrics bool
}

func (a *openAIAdapter) Kind() Kind { return a.kind }

func (a *openAIAdapter) ChatStream(ctx context.Context, baseURL string, req *openai.ChatCompletionRequest) (Stream, error) {
	up := req.Clone()
	up.Stream = true
	up.StreamOptions = &openai.StreamOptions{IncludeUsage: true}
	body, err := json.Marshal(up)
	if err != nil {
		return nil, err
	}
	httpReq, err := newRequest(ctx, http.MethodPost, joinURL(baseURL, "/v1/chat/completions"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "text/event-stream")
	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if err := checkStatus(resp); err != nil {
		return nil, err
	}
	return &sseStream{body: resp.Body, sc: newScanner(resp.Body)}, nil
}

type sseStream struct {
	body io.ReadCloser
	sc   *bufio.Scanner
	done bool
}

func newScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	return sc
}

func (s *sseStream) Recv() (Chunk, error) {
	if s.done {
		return Chunk{}, io.EOF
	}
	for s.sc.Scan() {
		line := s.sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // comments, event names, blank separators
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			s.done = true
			return Chunk{}, io.EOF
		}
		var c openai.ChatCompletionChunk
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			return Chunk{}, fmt.Errorf("%w: %v", ErrStreamProtocol, err)
		}
		out := Chunk{Usage: c.Usage}
		if len(c.Choices) > 0 {
			out.Content = c.Choices[0].Delta.Content
			if fr := c.Choices[0].FinishReason; fr != nil {
				out.FinishReason = *fr
			}
		}
		return out, nil
	}
	if err := s.sc.Err(); err != nil {
		return Chunk{}, err
	}
	// The server closed the stream without [DONE]; treat as truncated.
	return Chunk{}, io.ErrUnexpectedEOF
}

func (s *sseStream) Close() error { return s.body.Close() }

func (a *openAIAdapter) Probe(ctx context.Context, baseURL string) (ProbeResult, error) {
	var res ProbeResult
	if a.kind == KindVLLM {
		if err := a.get(ctx, joinURL(baseURL, "/health"), nil); err != nil {
			return res, err
		}
	}
	var models openai.ModelList
	if err := a.get(ctx, joinURL(baseURL, "/v1/models"), &models); err != nil {
		return res, err
	}
	for _, m := range models.Data {
		res.Models = append(res.Models, m.ID)
	}
	res.Healthy = true
	if a.scrapeMetrics {
		// Metrics are best-effort: a worker without /metrics is still healthy.
		_ = a.scrape(ctx, baseURL, &res)
	}
	return res, nil
}

func (a *openAIAdapter) get(ctx context.Context, url string, into any) error {
	req, err := newRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	if err := checkStatus(resp); err != nil {
		return err
	}
	defer resp.Body.Close()
	if into == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

func (a *openAIAdapter) scrape(ctx context.Context, baseURL string, res *ProbeResult) error {
	req, err := newRequest(ctx, http.MethodGet, joinURL(baseURL, "/metrics"), nil)
	if err != nil {
		return err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	if err := checkStatus(resp); err != nil {
		return err
	}
	defer resp.Body.Close()
	m := ParsePromText(resp.Body)
	res.Running = int(m["vllm:num_requests_running"])
	res.Waiting = int(m["vllm:num_requests_waiting"])
	res.KVCacheUsage = firstOf(m, "vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc")
	res.PrefixHit = firstOf(m, "vllm:gpu_prefix_cache_hit_rate", "vllm:prefix_cache_hit_rate")
	res.GPUUtil = firstOf(m, "gpu_utilization", "DCGM_FI_DEV_GPU_UTIL") // DCGM reports percent
	if res.GPUUtil > 1 {
		res.GPUUtil /= 100
	}
	return nil
}

func firstOf(m map[string]float64, names ...string) float64 {
	for _, n := range names {
		if v, ok := m[n]; ok {
			return v
		}
	}
	return 0
}

// ParsePromText parses Prometheus text exposition into metric name → value,
// summing series of the same metric across label sets. Histograms and
// summaries are reduced to their _sum/_count series like any other.
func ParsePromText(r io.Reader) map[string]float64 {
	out := map[string]float64{}
	sc := newScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		rest := line[len(name):]
		if strings.HasPrefix(rest, "{") {
			end := strings.LastIndex(rest, "}")
			if end < 0 {
				continue
			}
			rest = rest[end+1:]
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out[name] += v
	}
	return out
}
