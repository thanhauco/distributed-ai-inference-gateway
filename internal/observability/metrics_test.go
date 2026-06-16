package observability

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.Requests.WithLabelValues("llama3-8b", "200", "miss").Inc()
	m.WorkerHealthy.WithLabelValues("w1").Set(1)
	m.TTFT.WithLabelValues("llama3-8b", "w1").Observe(0.2)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`gateway_requests_total{cache="miss",model="llama3-8b",status="200"} 1`,
		`gateway_worker_healthy{worker="w1"} 1`,
		`gateway_time_to_first_token_seconds_count{model="llama3-8b",worker="w1"} 1`,
		"go_goroutines",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing %s", want)
		}
	}

	m.DeleteWorker("w1")
	rec = httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ = io.ReadAll(rec.Body)
	if strings.Contains(string(body), `worker="w1"`) {
		t.Error("worker series not deleted")
	}
}
