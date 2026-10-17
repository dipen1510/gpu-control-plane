//go:build integration

package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/metrics"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// TestMetricsCreateResultsIntegration drives every create outcome and a
// delete through the real stack, wired as cmd/apiserver wires it, and checks
// what /metrics reports.
func TestMetricsCreateResultsIntegration(t *testing.T) {
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(4) // 2 H100 + 2 A100, 8 GPUs each
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	m.Registry().MustRegister(metrics.NewDBCollector(db, log))
	st := store.New(db, scheduler.New(m.ReserveConflicts, scheduler.WithPlaceDuration(m.PlaceDuration)),
		store.WithTransitionObserver(m))
	srv := httptest.NewServer(NewHandler(st, lifecycle.New(st, log), log, WithMetrics(m), WithReadiness(db)))
	t.Cleanup(srv.Close)

	post := func(tenant, key, body string, want int) instanceResponse {
		t.Helper()
		resp, inst := postInstance(t, srv, tenant, key, body)
		if resp.StatusCode != want {
			t.Fatalf("POST %s/%s %s = %d, want %d", tenant, key, body, resp.StatusCode, want)
		}
		return inst
	}
	a100 := post("t1", "k1", `{"gpu_type":"A100","gpu_count":1}`, http.StatusAccepted)
	post("t1", "k1", `{"gpu_type":"A100","gpu_count":1}`, http.StatusAccepted)  // replayed
	post("t1", "k1", `{"gpu_type":"A100","gpu_count":2}`, http.StatusConflict)  // key reused
	post("tq", "ka", `{"gpu_type":"H100","gpu_count":8}`, http.StatusAccepted)  // fills one H100 node
	post("tq", "kb", `{"gpu_type":"H100","gpu_count":8}`, http.StatusAccepted)  // fills the other; tq at 16
	post("tq", "kc", `{"gpu_type":"H100","gpu_count":1}`, http.StatusForbidden) // quota
	post("t2", "kd", `{"gpu_type":"H100","gpu_count":1}`, http.StatusServiceUnavailable)
	post("t2", "ke", `{"gpu_type":"H100","gpu_count":3}`, http.StatusBadRequest)
	if resp, b := do(t, http.MethodDelete, srv.URL+"/v1/instances/"+a100.ID, map[string]string{headerTenant: "t1"}, ""); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("DELETE = %d %s", resp.StatusCode, b)
	}

	resp, body := do(t, http.MethodGet, srv.URL+"/metrics", nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", resp.StatusCode)
	}
	text := string(body)
	for _, want := range []string{
		`gpucp_create_results_total{result="scheduled"} 3`,
		`gpucp_create_results_total{result="replayed"} 1`,
		`gpucp_create_results_total{result="key_reused"} 1`,
		`gpucp_create_results_total{result="quota_exceeded"} 1`,
		`gpucp_create_results_total{result="insufficient_capacity"} 1`,
		`gpucp_create_results_total{result="invalid"} 1`,
		`gpucp_create_results_total{result="conflict_retry"} 0`,
		`gpucp_create_results_total{result="error"} 0`,
		`gpucp_api_requests_total{code="202",route="POST /v1/instances"} 4`,
		`gpucp_api_requests_total{code="202",route="DELETE /v1/instances/{id}"} 1`,
		// Place runs for the 3 fresh creates and the 503; quota is
		// refused before Place, replays before quota.
		`gpucp_scheduler_place_duration_seconds_count 4`,
		`gpucp_scheduler_reserve_conflicts_total 0`,
		`gpucp_instance_transitions_total{from="SCHEDULED",to="PROVISIONING"} 3`,
		`gpucp_instance_transitions_total{from="PROVISIONING",to="RUNNING"} 3`,
		`gpucp_instance_transitions_total{from="RUNNING",to="TERMINATING"} 1`,
		`gpucp_instance_transitions_total{from="TERMINATING",to="TERMINATED"} 1`,
		`gpucp_nodes_gpus{gpu_type="H100",kind="allocated"} 16`,
		`gpucp_nodes_gpus{gpu_type="A100",kind="allocated"} 0`,
		`gpucp_nodes_gpus{gpu_type="A100",kind="capacity"} 16`,
		`gpucp_scheduler_fragmented_gpus{gpu_type="H100"} 0`,
		`gpucp_store_allocation_drift_nodes 0`,
		`gpucp_store_stuck_terminating_instances 0`,
		// 3 creates × (scheduled, provisioning, running) + terminating, terminated.
		`gpucp_outbox_unpublished_rows 11`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}

	if resp, _ := do(t, http.MethodGet, srv.URL+"/readyz", nil, ""); resp.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz with the database up = %d, want 200", resp.StatusCode)
	}
	db.Close()
	if resp, _ := do(t, http.MethodGet, srv.URL+"/readyz", nil, ""); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz with the pool closed = %d, want 503", resp.StatusCode)
	}
	if resp, _ := do(t, http.MethodGet, srv.URL+"/healthz", nil, ""); resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz with the pool closed = %d, want 200", resp.StatusCode)
	}
	// The API metrics are still served while the database gauges fail.
	if resp, b := do(t, http.MethodGet, srv.URL+"/metrics", nil, ""); resp.StatusCode != http.StatusOK ||
		!strings.Contains(string(b), `gpucp_create_results_total{result="scheduled"} 3`) {
		t.Errorf("GET /metrics with the pool closed = %d, want 200 with the API metrics", resp.StatusCode)
	}
}
