//go:build integration

package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// newServer returns an API server backed by a fresh database with 4 seeded
// nodes (2 H100, 2 A100, 8 GPUs each).
func newServer(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	h := NewHandler(store.New(db, scheduler.New(nil)), slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, db
}

func do(t *testing.T, method, url string, headers map[string]string, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, b
}

func createInstance(t *testing.T, srv *httptest.Server, tenant, key, body string) instanceResponse {
	t.Helper()
	resp, b := do(t, http.MethodPost, srv.URL+"/v1/instances",
		map[string]string{headerTenant: tenant, headerIdempotencyKey: key}, body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create: status %d body %s, want 202", resp.StatusCode, b)
	}
	var inst instanceResponse
	if err := json.Unmarshal(b, &inst); err != nil {
		t.Fatalf("decode create body %s: %v", b, err)
	}
	return inst
}

// storedInstance reads the instance as it is in the database right now.
func storedInstance(t *testing.T, db *pgxpool.Pool, id string) instanceResponse {
	t.Helper()
	var inst instanceResponse
	if err := db.QueryRow(t.Context(), `SELECT id, state, node_id, gpu_type, gpu_count, resource_version
		FROM instances WHERE id = $1`, id).Scan(
		&inst.ID, &inst.State, &inst.NodeID, &inst.GPUType, &inst.GPUCount, &inst.ResourceVersion); err != nil {
		t.Fatalf("read instance %s: %v", id, err)
	}
	return inst
}

func TestCreateHappyPath(t *testing.T) {
	srv, db := newServer(t)

	resp, b := do(t, http.MethodPost, srv.URL+"/v1/instances",
		map[string]string{headerTenant: "t1", headerIdempotencyKey: "k1"},
		`{"gpu_type":"H100","gpu_count":4}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d body %s, want 202", resp.StatusCode, b)
	}
	var got instanceResponse
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode body %s: %v", b, err)
	}
	// The create response is the state its own transaction committed.
	if got.State != "SCHEDULED" || got.ResourceVersion != 1 || got.GPUType != "H100" ||
		got.GPUCount != 4 || got.NodeID == "" || !strings.HasPrefix(got.ID, "inst_") {
		t.Errorf("body = %+v, want SCHEDULED rv 1 H100 x4 on a node", got)
	}
	if etag := resp.Header.Get("ETag"); etag != `"1"` {
		t.Errorf("ETag = %q, want %q", etag, `"1"`)
	}

	var instances, events, instanceEvents int
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM instances),
		(SELECT count(*) FROM outbox WHERE event_type = 'instance.scheduled'),
		(SELECT count(*) FROM outbox WHERE aggregate_id = $1)`, got.ID).Scan(&instances, &events, &instanceEvents); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	// Create commits exactly one event, of type instance.scheduled.
	// NOTE(S1-14): once the lifecycle driver runs after create, this
	// instance also gets instance.provisioning and instance.running rows;
	// that story must update the total to the full expected history.
	if instances != 1 || events != 1 || instanceEvents != 1 {
		t.Errorf("instances = %d, instance.scheduled events = %d, events for %s = %d; want 1, 1, 1",
			instances, events, got.ID, instanceEvents)
	}

	var ev struct {
		aggregate, id, tenant, gpuType, node, state string
		count, rv                                   int
	}
	if err := db.QueryRow(t.Context(), `SELECT aggregate_id, payload->>'id', payload->>'tenant_id',
		payload->>'gpu_type', (payload->>'gpu_count')::int, payload->>'node_id', payload->>'state',
		(payload->>'resource_version')::int
		FROM outbox WHERE event_type = 'instance.scheduled'`).Scan(
		&ev.aggregate, &ev.id, &ev.tenant, &ev.gpuType, &ev.count, &ev.node, &ev.state, &ev.rv); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if ev.aggregate != got.ID || ev.id != got.ID || ev.tenant != "t1" || ev.gpuType != "H100" ||
		ev.count != 4 || ev.node != got.NodeID || ev.state != "SCHEDULED" || ev.rv != 1 {
		t.Errorf("event = %+v, want it to describe %+v for tenant t1", ev, got)
	}

	var nodeAlloc, totalAlloc, used int
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT allocated FROM nodes WHERE id = $1),
		(SELECT sum(allocated) FROM nodes),
		(SELECT used FROM tenant_quota WHERE tenant_id = 't1' AND gpu_type = 'H100')`,
		got.NodeID).Scan(&nodeAlloc, &totalAlloc, &used); err != nil {
		t.Fatalf("read accounting: %v", err)
	}
	// Seeded nodes start at allocated 0 and the tenant has no quota row.
	if nodeAlloc != 4 || totalAlloc != 4 || used != 4 {
		t.Errorf("allocated on %s = %d, total allocated = %d, used = %d; want 4, 4, 4",
			got.NodeID, nodeAlloc, totalAlloc, used)
	}
}

func TestGetConditional(t *testing.T) {
	srv, db := newServer(t)
	created := createInstance(t, srv, "t1", "k1", `{"gpu_type":"A100","gpu_count":2}`)
	url := srv.URL + "/v1/instances/" + created.ID
	tenant := map[string]string{headerTenant: "t1"}

	// Plain GET returns the instance exactly as currently stored.
	resp, b := do(t, http.MethodGet, url, tenant, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: status %d body %s, want 200", resp.StatusCode, b)
	}
	var got instanceResponse
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode body %s: %v", b, err)
	}
	stored := storedInstance(t, db, created.ID)
	if got != stored {
		t.Errorf("GET body = %+v, want stored %+v", got, stored)
	}
	etag := resp.Header.Get("ETag")
	if want := `"` + strconv.FormatInt(stored.ResourceVersion, 10) + `"`; etag != want {
		t.Fatalf("ETag = %q, want %q", etag, want)
	}

	// Matching If-None-Match: 304, no body.
	resp, b = do(t, http.MethodGet, url, map[string]string{headerTenant: "t1", "If-None-Match": etag}, "")
	if resp.StatusCode != http.StatusNotModified || len(b) != 0 {
		t.Errorf("matching If-None-Match: status %d body %q, want 304 and no body", resp.StatusCode, b)
	}
	if resp.Header.Get("ETag") != etag {
		t.Errorf("304 ETag = %q, want %q", resp.Header.Get("ETag"), etag)
	}

	// Non-matching If-None-Match: 200 with the instance.
	resp, b = do(t, http.MethodGet, url, map[string]string{headerTenant: "t1", "If-None-Match": `"999"`}, "")
	if resp.StatusCode != http.StatusOK || len(b) == 0 {
		t.Errorf("non-matching If-None-Match: status %d body %q, want 200 with body", resp.StatusCode, b)
	}

	// The instance changes (as a lifecycle step would): the old ETag no
	// longer matches, and the client gets the new state and ETag.
	if _, err := db.Exec(t.Context(), `UPDATE instances
		SET state = 'PROVISIONING', resource_version = resource_version + 1 WHERE id = $1`, created.ID); err != nil {
		t.Fatalf("advance instance: %v", err)
	}
	resp, b = do(t, http.MethodGet, url, map[string]string{headerTenant: "t1", "If-None-Match": etag}, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stale If-None-Match: status %d body %s, want 200", resp.StatusCode, b)
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode body %s: %v", b, err)
	}
	stored = storedInstance(t, db, created.ID)
	if got != stored {
		t.Errorf("GET after change = %+v, want stored %+v", got, stored)
	}
	if want := `"` + strconv.FormatInt(stored.ResourceVersion, 10) + `"`; resp.Header.Get("ETag") != want {
		t.Errorf("ETag after change = %q, want %q", resp.Header.Get("ETag"), want)
	}
}

func TestGetOtherTenantIs404(t *testing.T) {
	srv, _ := newServer(t)
	created := createInstance(t, srv, "t1", "k1", `{"gpu_type":"H100","gpu_count":1}`)

	for _, tc := range []struct{ name, tenant, id string }{
		{name: "other tenant", tenant: "t2", id: created.ID},
		{name: "unknown id", tenant: "t1", id: "inst_doesnotexist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, b := do(t, http.MethodGet, srv.URL+"/v1/instances/"+tc.id,
				map[string]string{headerTenant: tc.tenant}, "")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status %d body %s, want 404", resp.StatusCode, b)
			}
			var e errorBody
			if err := json.Unmarshal(b, &e); err != nil || e.Error.Code != codeNotFound {
				t.Errorf("body %s, want not_found envelope", b)
			}
		})
	}

	// The owner still sees it.
	resp, b := do(t, http.MethodGet, srv.URL+"/v1/instances/"+created.ID, map[string]string{headerTenant: "t1"}, "")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("owner GET: status %d body %s, want 200", resp.StatusCode, b)
	}
}
