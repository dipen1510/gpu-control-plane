package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/dipen1510/gpu-control-plane/internal/metrics"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// series returns every series of the metric family name in g, as label set
// → value (a histogram's value is its sample count).
func series(t *testing.T, g prometheus.Gatherer, name string) map[string]float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			out[labelKey(m)] = sampleValue(m)
		}
	}
	return out
}

func labelKey(m *dto.Metric) string {
	var parts []string
	for _, l := range m.GetLabel() {
		parts = append(parts, l.GetName()+"="+l.GetValue())
	}
	return strings.Join(parts, ",")
}

func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetHistogram() != nil:
		return float64(m.GetHistogram().GetSampleCount())
	}
	return 0
}

func newMetricsHandler(s Store, lc Lifecycle, opts ...Option) (*Handler, *metrics.Metrics) {
	m := metrics.New()
	h := NewHandler(s, lc, slog.New(slog.NewTextHandler(io.Discard, nil)), append(opts, WithMetrics(m))...)
	return h, m
}

// deadlockErr is what pgx returns when Postgres aborts a transaction to break
// a deadlock, wrapped the way the store wraps it.
func deadlockErr() error {
	return fmt.Errorf("create instance inst_x: reserve quota: %w",
		&pgconn.PgError{Code: "40P01", Message: "deadlock detected"})
}

// TestMetricsCreateResults: each create outcome increments exactly its own
// gpucp_create_results_total{result} series, by one.
func TestMetricsCreateResults(t *testing.T) {
	const body = `{"gpu_type":"H100","gpu_count":2}`
	replayed := store.CreateResult{Replayed: true, Instance: store.Instance{
		ID: "inst_earlier", State: statemachine.Running, ResourceVersion: 3,
	}}
	tests := []struct {
		result string
		fs     *fakeStore
		body   string
		status int
	}{
		{result: metrics.CreateScheduled, fs: &fakeStore{}, body: body, status: http.StatusAccepted},
		{result: metrics.CreateReplayed, fs: &fakeStore{createResult: &replayed}, body: body, status: http.StatusAccepted},
		{result: metrics.CreateQuotaExceeded, fs: &fakeStore{createErr: store.ErrQuotaExceeded}, body: body, status: http.StatusForbidden},
		{result: metrics.CreateInsufficientCapacity, fs: &fakeStore{createErr: store.ErrInsufficientCapacity}, body: body, status: http.StatusServiceUnavailable},
		{result: metrics.CreateKeyReused, fs: &fakeStore{createErr: store.ErrIdempotencyKeyReused}, body: body, status: http.StatusConflict},
		{result: metrics.CreateInvalid, fs: &fakeStore{}, body: `{"gpu_type":"H100","gpu_count":3}`, status: http.StatusBadRequest},
		{result: metrics.CreateConflictRetry, fs: &fakeStore{createErr: deadlockErr()}, body: body, status: http.StatusServiceUnavailable},
		{result: metrics.CreateError, fs: &fakeStore{createErr: errors.New("boom")}, body: body, status: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.result, func(t *testing.T) {
			h, m := newMetricsHandler(tc.fs, &fakeLifecycle{})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, newCreateRequest(strings.NewReader(tc.body)))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.status, rec.Body.String())
			}
			got := series(t, m.Registry(), "gpucp_create_results_total")
			for label, v := range got {
				want := 0.0
				if label == "result="+tc.result {
					want = 1
				}
				if v != want {
					t.Errorf("%s = %v, want %v", label, v, want)
				}
			}
			if _, ok := got["result="+tc.result]; !ok {
				t.Errorf("no series result=%s; have %v", tc.result, got)
			}
		})
	}
}

// TestRouteLabelCardinality: the route label is the matched pattern, so many
// distinct ids and junk paths give a small fixed set of series; health and
// metrics scrapes are not counted.
func TestRouteLabelCardinality(t *testing.T) {
	fs := &fakeStore{getInst: instanceAt(statemachine.Running, 3)}
	h, m := newMetricsHandler(fs, &fakeLifecycle{}, WithReadiness(okPinger{}))
	serve := func(method, path string) {
		req := httptest.NewRequest(method, path, strings.NewReader(`{"gpu_type":"H100","gpu_count":1}`))
		req.Header.Set(headerTenant, "t1")
		req.Header.Set(headerIdempotencyKey, "k1")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	for i := range 50 {
		id := fmt.Sprintf("inst_%03d", i)
		serve(http.MethodGet, "/v1/instances/"+id)
		serve(http.MethodDelete, "/v1/instances/"+id)
		serve(http.MethodGet, fmt.Sprintf("/scan/%d", i)) // 404
		serve(http.MethodPut, "/v1/instances/"+id)        // 405
	}
	serve(http.MethodPost, "/v1/instances")
	serve(http.MethodGet, "/healthz")
	serve(http.MethodGet, "/readyz")
	serve(http.MethodGet, "/metrics")

	var routes []string
	for label := range series(t, m.Registry(), "gpucp_api_request_duration_seconds") {
		routes = append(routes, label)
	}
	slices.Sort(routes)
	want := []string{
		"route=DELETE /v1/instances/{id}",
		"route=GET /v1/instances/{id}",
		"route=POST /v1/instances",
		"route=unmatched",
	}
	if !slices.Equal(routes, want) {
		t.Errorf("duration routes = %q, want %q", routes, want)
	}

	got := series(t, m.Registry(), "gpucp_api_requests_total")
	wantCounts := map[string]float64{
		"code=200,route=GET /v1/instances/{id}":    50,
		"code=202,route=DELETE /v1/instances/{id}": 50,
		"code=202,route=POST /v1/instances":        1,
		"code=404,route=unmatched":                 50,
		"code=405,route=unmatched":                 50,
	}
	if len(got) != len(wantCounts) {
		t.Errorf("request series = %v, want %v", got, wantCounts)
	}
	for k, v := range wantCounts {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
}

type okPinger struct{}

func (okPinger) Ping(context.Context) error { return nil }

type errPinger struct{ err error }

func (p errPinger) Ping(context.Context) error { return p.err }

func TestHealthz(t *testing.T) {
	// Liveness does not depend on the database: a down DB must not get the
	// process restarted.
	h := NewHandler(&fakeStore{}, &fakeLifecycle{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithReadiness(errPinger{err: errors.New("db down")}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want int
	}{
		{name: "db reachable", opts: []Option{WithReadiness(okPinger{})}, want: http.StatusOK},
		{name: "db unreachable", opts: []Option{WithReadiness(errPinger{err: errors.New("connection refused")})}, want: http.StatusServiceUnavailable},
		{name: "no check configured", want: http.StatusServiceUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(&fakeStore{}, &fakeLifecycle{}, slog.New(slog.NewTextHandler(io.Discard, nil)), tc.opts...)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != tc.want {
				t.Fatalf("GET /readyz = %d, want %d", rec.Code, tc.want)
			}
			if tc.want != http.StatusOK {
				if got := decodeError(t, rec); got.Code != codeNotReady || strings.Contains(got.Message, "refused") {
					t.Errorf("error = %+v, want code %s and no driver detail", got, codeNotReady)
				}
			}
		})
	}
}

// TestReadyzDBDown uses a real *pgxpool.Pool that cannot reach a database:
// once with an address nothing listens on, once closed.
func TestReadyzDBDown(t *testing.T) {
	newPool := func(t *testing.T) *pgxpool.Pool {
		t.Helper()
		// Port 1 on loopback: connection refused at once. pgxpool.New does
		// not connect, so this needs no database.
		pool, err := pgxpool.New(t.Context(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
		if err != nil {
			t.Fatalf("new pool: %v", err)
		}
		return pool
	}
	tests := []struct {
		name string
		pool func(t *testing.T) *pgxpool.Pool
	}{
		{name: "unreachable", pool: func(t *testing.T) *pgxpool.Pool {
			p := newPool(t)
			t.Cleanup(p.Close)
			return p
		}},
		{name: "closed pool", pool: func(t *testing.T) *pgxpool.Pool {
			p := newPool(t)
			p.Close()
			return p
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(&fakeStore{}, &fakeLifecycle{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
				WithReadiness(tc.pool(t)))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("GET /readyz = %d, want 503 (body %s)", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h, _ := newMetricsHandler(&fakeStore{}, &fakeLifecycle{})
	h.ServeHTTP(httptest.NewRecorder(), newCreateRequest(strings.NewReader(`{"gpu_type":"H100","gpu_count":2}`)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	for _, want := range []string{
		`gpucp_create_results_total{result="scheduled"} 1`,
		`gpucp_api_requests_total{code="202",route="POST /v1/instances"} 1`,
		`gpucp_create_results_total{result="quota_exceeded"} 0`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

// TestDeadlockMapsTo503: a transaction Postgres aborted with 40P01 is a
// retryable 503 conflict_retry with a short Retry-After, logged at Error;
// other database errors stay 500.
func TestDeadlockMapsTo503(t *testing.T) {
	const body = `{"gpu_type":"H100","gpu_count":2}`
	tests := []struct {
		name       string
		serve      func(*Handler, *httptest.ResponseRecorder)
		fs         *fakeStore
		lc         *fakeLifecycle
		wantStatus int
		wantCode   string
	}{
		{
			name: "create deadlock",
			fs:   &fakeStore{createErr: deadlockErr()},
			lc:   &fakeLifecycle{},
			serve: func(h *Handler, rec *httptest.ResponseRecorder) {
				h.ServeHTTP(rec, newCreateRequest(strings.NewReader(body)))
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: codeConflictRetry,
		},
		{
			name: "create other SQLSTATE",
			fs:   &fakeStore{createErr: fmt.Errorf("x: %w", &pgconn.PgError{Code: "23514"})},
			lc:   &fakeLifecycle{},
			serve: func(h *Handler, rec *httptest.ResponseRecorder) {
				h.ServeHTTP(rec, newCreateRequest(strings.NewReader(body)))
			},
			wantStatus: http.StatusInternalServerError, wantCode: codeInternal,
		},
		{
			name: "delete walk deadlock before anything committed",
			fs:   &fakeStore{getInst: instanceAt(statemachine.Running, 3)},
			lc:   &fakeLifecycle{deleteResults: []deleteResult{{err: deadlockErr()}}},
			serve: func(h *Handler, rec *httptest.ResponseRecorder) {
				h.ServeHTTP(rec, deleteRequest(t.Context(), nil))
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: codeConflictRetry,
		},
		{
			name: "delete read deadlock",
			fs:   &fakeStore{getErr: deadlockErr()},
			lc:   &fakeLifecycle{},
			serve: func(h *Handler, rec *httptest.ResponseRecorder) {
				h.ServeHTTP(rec, deleteRequest(t.Context(), nil))
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: codeConflictRetry,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			h := NewHandler(tc.fs, tc.lc, slog.New(slog.NewJSONHandler(&logs, nil)))
			rec := httptest.NewRecorder()
			tc.serve(h, rec)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			got := decodeError(t, rec)
			if got.Code != tc.wantCode {
				t.Errorf("code = %q, want %q", got.Code, tc.wantCode)
			}
			if strings.Contains(rec.Body.String(), "deadlock") || strings.Contains(rec.Body.String(), "40P01") {
				t.Errorf("body leaks the database error: %s", rec.Body.String())
			}
			if tc.wantCode != codeConflictRetry {
				return
			}
			if ra := rec.Header().Get("Retry-After"); ra != retryAfterConflictSeconds {
				t.Errorf("Retry-After = %q, want %q", ra, retryAfterConflictSeconds)
			}
			if !strings.Contains(logs.String(), `"level":"ERROR"`) || !strings.Contains(logs.String(), "40P01") {
				t.Errorf("deadlock not logged at ERROR with its SQLSTATE: %s", logs.String())
			}
		})
	}
}

// deleteRequest is DELETE /v1/instances/inst_x for tenant t1.
func deleteRequest(ctx context.Context, headers map[string]string) *http.Request {
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete, "/v1/instances/inst_x", nil)
	req.Header.Set(headerTenant, "t1")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// failingCollector reports one metric that fails to collect, as DBCollector
// does when the database is down.
type failingCollector struct{ desc *prometheus.Desc }

func (c failingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }
func (c failingCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.NewInvalidMetric(c.desc, errors.New("injected: database unreachable"))
}

// TestPromLoggerLogsReadableError: a scrape error reaches the log as text
// that names the cause, not as {} (S1-13 review: slog's JSON handler renders
// an error inside a []any as an empty object).
func TestPromLoggerLogsReadableError(t *testing.T) {
	t.Run("scrape with a failing collector", func(t *testing.T) {
		var logs bytes.Buffer
		m := metrics.New()
		m.Registry().MustRegister(failingCollector{desc: prometheus.NewDesc("gpucp_test_failing", "test", nil, nil)})
		h := NewHandler(&fakeStore{}, &fakeLifecycle{}, slog.New(slog.NewJSONHandler(&logs, nil)), WithMetrics(m))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /metrics = %d, want 200 (ContinueOnError)", rec.Code)
		}
		var entry struct {
			Msg    string `json:"msg"`
			Detail string `json:"detail"`
		}
		if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
			t.Fatalf("log %q is not one JSON entry with a string detail: %v", logs.String(), err)
		}
		if entry.Msg != "metrics: scrape error" ||
			!strings.HasPrefix(entry.Detail, "error gathering metrics: ") ||
			!strings.Contains(entry.Detail, "injected: database unreachable") {
			t.Errorf("log entry = %+v, want the scrape error with its cause as text", entry)
		}
	})
	t.Run("operands formatted like Println", func(t *testing.T) {
		var logs bytes.Buffer
		promLogger{log: slog.New(slog.NewJSONHandler(&logs, nil))}.Println("error gathering metrics:", errors.New("boom"), 3)
		if want := `"detail":"error gathering metrics: boom 3"`; !strings.Contains(logs.String(), want) {
			t.Errorf("log = %s, want it to contain %s", logs.String(), want)
		}
	})
}
