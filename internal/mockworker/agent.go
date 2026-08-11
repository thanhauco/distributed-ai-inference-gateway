package mockworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Agent registers a worker with the gateway's control plane and keeps it
// alive with heartbeats. The same loop can run as a sidecar next to a real
// vLLM or Ollama server: new workers announce themselves and the gateway
// starts routing to them without a restart.
type Agent struct {
	GatewayURL   string
	Token        string
	ID           string
	AdvertiseURL string
	Kind         string
	Models       []string
	Logger       *slog.Logger
	Client       *http.Client
}

// PullConfig fetches this worker's model assignment. It returns nil models
// when the gateway has no assignment for the worker.
func (a *Agent) PullConfig(ctx context.Context) ([]string, error) {
	var out struct {
		Assignment struct {
			Models []string `json:"models"`
		} `json:"assignment"`
	}
	status, err := a.do(ctx, http.MethodGet, "/internal/v1/worker-config/"+a.ID, nil, &out)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out.Assignment.Models, nil
}

// Run registers, then heartbeats until ctx is done, re-registering whenever
// the gateway forgets the worker (e.g. after a restart).
func (a *Agent) Run(ctx context.Context) {
	interval := 10 * time.Second
	registered := false
	for {
		if !registered {
			var resp struct {
				HeartbeatInterval string `json:"heartbeat_interval"`
			}
			_, err := a.do(ctx, http.MethodPost, "/admin/v1/workers", map[string]any{
				"id": a.ID, "url": a.AdvertiseURL, "kind": a.Kind, "models": a.Models,
			}, &resp)
			if err != nil {
				a.Logger.Warn("registration failed; retrying", "gateway", a.GatewayURL, "error", err)
			} else {
				registered = true
				if d, perr := time.ParseDuration(resp.HeartbeatInterval); perr == nil && d > 0 {
					interval = d
				}
				a.Logger.Info("registered with gateway", "gateway", a.GatewayURL, "models", a.Models, "heartbeat", interval.String())
			}
		} else {
			status, err := a.do(ctx, http.MethodPost, "/admin/v1/workers/"+a.ID+"/heartbeat", map[string]any{"models": a.Models}, nil)
			if status == http.StatusNotFound {
				registered = false
				continue
			}
			if err != nil {
				a.Logger.Warn("heartbeat failed", "error", err)
			}
		}
		wait := interval
		if !registered {
			wait = 2 * time.Second
		}
		select {
		case <-ctx.Done():
			dctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = a.do(dctx, http.MethodDelete, "/admin/v1/workers/"+a.ID, nil, nil)
			cancel()
			return
		case <-time.After(wait):
		}
	}
}

func (a *Agent) do(ctx context.Context, method, path string, body, into any) (int, error) {
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.GatewayURL, "/")+path, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+a.Token)
	req.Header.Set("Content-Type", "application/json")
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return resp.StatusCode, fmt.Errorf("%s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if into != nil {
		return resp.StatusCode, json.NewDecoder(resp.Body).Decode(into)
	}
	return resp.StatusCode, nil
}
