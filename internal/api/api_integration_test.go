//go:build integration

package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// newServer returns an API server backed by a fresh database with 4 seeded
// nodes (2 H100, 2 A100, 8 GPUs each), running the M1 lifecycle driver.
func newServer(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	srv, db, _ := newServerWithDriver(t)
	return srv, db
}

// newServerWithDriver is newServer that also returns the driver, so tests
// can run the delete walk directly (HTTP DELETE is S1-10).
func newServerWithDriver(t *testing.T) (*httptest.Server, *pgxpool.Pool, *lifecycle.Driver) {
	t.Helper()
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := store.New(db, scheduler.New(nil))
	driver := lifecycle.New(st, log)
	srv := httptest.NewServer(NewHandler(st, driver, log))
	t.Cleanup(srv.Close)
	return srv, db, driver
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
	// Create commits exactly one event, of type instance.scheduled; the M1
	// lifecycle driver then adds one per step, instance.provisioning and
	// instance.running (ADR-0001 Amendment 1), so 3 in all.
	if instances != 1 || events != 1 || instanceEvents != 3 {
		t.Errorf("instances = %d, instance.scheduled events = %d, events for %s = %d; want 1, 1, 3",
			instances, events, got.ID, instanceEvents)
	}
	if types := outboxTypes(t, db, got.ID); !slices.Equal(types,
		[]string{"instance.scheduled", "instance.provisioning", "instance.running"}) {
		t.Errorf("events for %s = %v, want scheduled, provisioning, running", got.ID, types)
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
	// The instance is RUNNING now and still holds its GPUs.
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
		SET state = 'TERMINATING', resource_version = resource_version + 1 WHERE id = $1`, created.ID); err != nil {
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

// writes is everything a create writes; a rejected create must change none of it.
type writes struct {
	instances, events, keys int
	allocated               map[string]int // node id → allocated
	used                    map[string]int // "tenant/gpu_type" → used
}

func readWrites(t *testing.T, db *pgxpool.Pool) writes {
	t.Helper()
	w := writes{allocated: map[string]int{}, used: map[string]int{}}
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM instances), (SELECT count(*) FROM outbox), (SELECT count(*) FROM idempotency_keys)`).
		Scan(&w.instances, &w.events, &w.keys); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	rows, err := db.Query(t.Context(), `SELECT id, allocated FROM nodes`)
	if err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan node: %v", err)
		}
		w.allocated[id] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	rows, err = db.Query(t.Context(), `SELECT tenant_id || '/' || gpu_type, used FROM tenant_quota`)
	if err != nil {
		t.Fatalf("read quota: %v", err)
	}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			t.Fatalf("scan quota: %v", err)
		}
		w.used[k] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read quota: %v", err)
	}
	return w
}

func assertNothingWritten(t *testing.T, before, after writes) {
	t.Helper()
	if after.instances != before.instances || after.events != before.events || after.keys != before.keys {
		t.Errorf("instances/outbox/idempotency_keys went from %d/%d/%d to %d/%d/%d, want unchanged",
			before.instances, before.events, before.keys, after.instances, after.events, after.keys)
	}
	if !maps.Equal(before.allocated, after.allocated) {
		t.Errorf("nodes.allocated went from %v to %v, want unchanged", before.allocated, after.allocated)
	}
	if !maps.Equal(before.used, after.used) {
		t.Errorf("tenant_quota.used went from %v to %v, want unchanged", before.used, after.used)
	}
}

// postCreate sends a create and decodes the error envelope (zero if none).
func postCreate(t *testing.T, srv *httptest.Server, tenant, key, body string) (*http.Response, errorDetail) {
	t.Helper()
	resp, b := do(t, http.MethodPost, srv.URL+"/v1/instances",
		map[string]string{headerTenant: tenant, headerIdempotencyKey: key}, body)
	var e errorBody
	if resp.StatusCode != http.StatusAccepted {
		if err := json.Unmarshal(b, &e); err != nil {
			t.Fatalf("status %d body %s is not the error envelope: %v", resp.StatusCode, b, err)
		}
	}
	return resp, e.Error
}

// TestCreateQuotaExceeded: the quota row is set directly, and the nodes stay
// empty, so quota is the only thing that can refuse. If the reserve UPDATE
// lost its `used + n <= quota_limit` guard, the CHECK constraint would turn
// the over-quota write into a 500, which this test rejects.
func TestCreateQuotaExceeded(t *testing.T) {
	tests := []struct {
		name  string
		used  int
		count int
	}{
		{name: "at the limit", used: 16, count: 1},
		{name: "would cross the limit", used: 12, count: 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, db := newServer(t)
			if _, err := db.Exec(t.Context(),
				`INSERT INTO tenant_quota (tenant_id, gpu_type, used) VALUES ('t1', 'H100', $1)`, tc.used); err != nil {
				t.Fatalf("set quota: %v", err)
			}
			before := readWrites(t, db)

			resp, e := postCreate(t, srv, "t1", "k1", `{"gpu_type":"H100","gpu_count":`+strconv.Itoa(tc.count)+`}`)
			if resp.StatusCode != http.StatusForbidden || e.Code != codeQuotaExceeded {
				t.Fatalf("status %d error %+v, want 403 quota_exceeded", resp.StatusCode, e)
			}
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				t.Errorf("Retry-After = %q on 403, want none (retrying will not help)", ra)
			}
			if !strings.Contains(e.Message, "H100") {
				t.Errorf("message %q does not name the gpu_type", e.Message)
			}
			after := readWrites(t, db)
			assertNothingWritten(t, before, after)
			if after.used["t1/H100"] != tc.used {
				t.Errorf("used = %d, want %d", after.used["t1/H100"], tc.used)
			}
		})
	}
}

func TestCreateInsufficientCapacity(t *testing.T) {
	srv, db := newServer(t)
	// t1 already holds 4 A100s, so its quota row exists with used 4; the
	// failed create must roll back its own reservation of that row.
	createInstance(t, srv, "t1", "k1", `{"gpu_type":"A100","gpu_count":4}`)
	// Other tenants fill the rest of the A100 capacity (2 nodes × 8).
	createInstance(t, srv, "t2", "k1", `{"gpu_type":"A100","gpu_count":8}`)
	createInstance(t, srv, "t3", "k1", `{"gpu_type":"A100","gpu_count":4}`)
	before := readWrites(t, db)
	if before.used["t1/A100"] != 4 {
		t.Fatalf("setup: t1 A100 used = %d, want 4", before.used["t1/A100"])
	}

	resp, e := postCreate(t, srv, "t1", "k2", `{"gpu_type":"A100","gpu_count":1}`)
	if resp.StatusCode != http.StatusServiceUnavailable || e.Code != codeInsufficientCapacity {
		t.Fatalf("status %d error %+v, want 503 insufficient_capacity", resp.StatusCode, e)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "30" {
		t.Errorf("Retry-After = %q, want %q", ra, "30")
	}
	if !strings.Contains(e.Message, "A100") {
		t.Errorf("message %q does not name the gpu_type A100", e.Message)
	}
	assertNothingWritten(t, before, readWrites(t, db))

	// H100 capacity is untouched, so the same tenant can still get those.
	createInstance(t, srv, "t1", "k3", `{"gpu_type":"H100","gpu_count":8}`)
}

// TestCreateFragmentedCapacity: 8 A100s are free in total, but split 4 + 4
// across two nodes, and an instance must fit on one node (D1).
func TestCreateFragmentedCapacity(t *testing.T) {
	srv, db := newServer(t)
	if _, err := db.Exec(t.Context(), `UPDATE nodes SET allocated = 4 WHERE gpu_type = 'A100'`); err != nil {
		t.Fatalf("fragment nodes: %v", err)
	}
	before := readWrites(t, db)

	resp, e := postCreate(t, srv, "t1", "k1", `{"gpu_type":"A100","gpu_count":8}`)
	if resp.StatusCode != http.StatusServiceUnavailable || e.Code != codeInsufficientCapacity {
		t.Fatalf("status %d error %+v, want 503 insufficient_capacity", resp.StatusCode, e)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "30" {
		t.Errorf("Retry-After = %q, want %q", ra, "30")
	}
	assertNothingWritten(t, before, readWrites(t, db))

	// A request that fits one node's free GPUs still succeeds.
	createInstance(t, srv, "t1", "k2", `{"gpu_type":"A100","gpu_count":4}`)
}

// TestQuotaIsPerGPUType: a tenant reaches its H100 limit exactly (8 + 8 = 16
// is allowed), one more H100 is refused, and A100 has its own 16.
func TestQuotaIsPerGPUType(t *testing.T) {
	srv, db := newServer(t)
	createInstance(t, srv, "t1", "k1", `{"gpu_type":"H100","gpu_count":8}`)
	createInstance(t, srv, "t1", "k2", `{"gpu_type":"H100","gpu_count":8}`)

	resp, e := postCreate(t, srv, "t1", "k3", `{"gpu_type":"H100","gpu_count":1}`)
	if resp.StatusCode != http.StatusForbidden || e.Code != codeQuotaExceeded {
		t.Fatalf("17th H100: status %d error %+v, want 403 quota_exceeded", resp.StatusCode, e)
	}

	inst := createInstance(t, srv, "t1", "k4", `{"gpu_type":"A100","gpu_count":8}`)
	if inst.GPUType != "A100" {
		t.Errorf("created %+v, want an A100 instance", inst)
	}
	w := readWrites(t, db)
	if w.used["t1/H100"] != 16 || w.used["t1/A100"] != 8 {
		t.Errorf("used = %v, want t1/H100 16 and t1/A100 8", w.used)
	}
}

// postInstance sends a create and decodes a 202 body (zero otherwise).
func postInstance(t *testing.T, srv *httptest.Server, tenant, key, body string) (*http.Response, instanceResponse) {
	t.Helper()
	resp, b := do(t, http.MethodPost, srv.URL+"/v1/instances",
		map[string]string{headerTenant: tenant, headerIdempotencyKey: key}, body)
	var inst instanceResponse
	if resp.StatusCode == http.StatusAccepted {
		if err := json.Unmarshal(b, &inst); err != nil {
			t.Fatalf("decode create body %s: %v", b, err)
		}
	}
	return resp, inst
}

func TestIdempotentReplay(t *testing.T) {
	srv, db := newServer(t)
	const body = `{"gpu_type":"H100","gpu_count":4}`
	resp, first := postInstance(t, srv, "t1", "k1", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first create: status %d, want 202", resp.StatusCode)
	}
	if got := resp.Header.Get(headerReplayed); got != "" {
		t.Errorf("first create %s = %q, want none", headerReplayed, got)
	}
	before := readWrites(t, db)

	resp, again := postInstance(t, srv, "t1", "k1", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("replay: status %d, want 202", resp.StatusCode)
	}
	if got := resp.Header.Get(headerReplayed); got != "true" {
		t.Errorf("replay %s = %q, want %q", headerReplayed, got, "true")
	}
	// A replay reports the instance as it is now: the first create answered
	// with its commit (SCHEDULED rv 1), and the lifecycle driver has since
	// moved it to RUNNING rv 3.
	if first.State != "SCHEDULED" || first.ResourceVersion != 1 {
		t.Errorf("first create body = %+v, want SCHEDULED rv 1", first)
	}
	if stored := storedInstance(t, db, first.ID); again != stored || again.State != "RUNNING" ||
		again.ResourceVersion != 3 {
		t.Errorf("replay body = %+v, want current %+v (RUNNING rv 3)", again, stored)
	}
	if etag := resp.Header.Get("ETag"); etag != `"3"` {
		t.Errorf("replay ETag = %q, want %q", etag, `"3"`)
	}

	// Nothing new: the replay wrote no instance, no outbox row (the
	// lifecycle did not run again), no key, and counted quota and GPUs
	// once. The first create's 3 events are all there is.
	after := readWrites(t, db)
	assertNothingWritten(t, before, after)
	if after.instances != 1 || after.events != 3 || after.keys != 1 || after.used["t1/H100"] != 4 {
		t.Errorf("instances %d, outbox %d, keys %d, used %d; want 1, 3, 1, 4",
			after.instances, after.events, after.keys, after.used["t1/H100"])
	}

	// The instance moves on again (as a delete would): a later replay
	// reports that state, still writing nothing.
	if _, err := db.Exec(t.Context(), `UPDATE instances
		SET state = 'TERMINATING', resource_version = 4 WHERE id = $1`, first.ID); err != nil {
		t.Fatalf("advance instance: %v", err)
	}
	before = readWrites(t, db)
	resp, later := postInstance(t, srv, "t1", "k1", body)
	if resp.StatusCode != http.StatusAccepted || resp.Header.Get(headerReplayed) != "true" {
		t.Fatalf("second replay: status %d %s %q, want 202 true", resp.StatusCode,
			headerReplayed, resp.Header.Get(headerReplayed))
	}
	if stored := storedInstance(t, db, first.ID); later != stored || later.State != "TERMINATING" {
		t.Errorf("second replay body = %+v, want current %+v (TERMINATING)", later, stored)
	}
	if etag := resp.Header.Get("ETag"); etag != `"4"` {
		t.Errorf("second replay ETag = %q, want %q", etag, `"4"`)
	}
	assertNothingWritten(t, before, readWrites(t, db))
}

// outboxTypes returns the event types written for instance id, in order.
func outboxTypes(t *testing.T, db *pgxpool.Pool, id string) []string {
	t.Helper()
	rows, err := db.Query(t.Context(), `SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	return types
}

// TestCreateThenGetIsRunning: the create answers with the state its own
// transaction committed, and the M1 lifecycle driver has walked the
// instance to RUNNING before that answer is sent (ADR-0001 Amendment 1).
func TestCreateThenGetIsRunning(t *testing.T) {
	srv, db := newServer(t)
	created := createInstance(t, srv, "t1", "k1", `{"gpu_type":"H100","gpu_count":2}`)
	if created.State != "SCHEDULED" || created.ResourceVersion != 1 {
		t.Errorf("create body = %+v, want SCHEDULED rv 1", created)
	}

	resp, b := do(t, http.MethodGet, srv.URL+"/v1/instances/"+created.ID, map[string]string{headerTenant: "t1"}, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: status %d body %s, want 200", resp.StatusCode, b)
	}
	var got instanceResponse
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode body %s: %v", b, err)
	}
	want := created
	want.State, want.ResourceVersion = "RUNNING", 3
	if got != want {
		t.Errorf("GET right after create = %+v, want %+v", got, want)
	}
	if etag := resp.Header.Get("ETag"); etag != `"3"` {
		t.Errorf("ETag = %q, want %q", etag, `"3"`)
	}
	if stored := storedInstance(t, db, created.ID); stored != want {
		t.Errorf("stored = %+v, want %+v", stored, want)
	}
}

// TestDriverOutboxOnePerStep: a create over HTTP and a delete walk give
// exactly one outbox row per committed state, in order, with the state and
// version of each step in its payload (invariant 3).
func TestDriverOutboxOnePerStep(t *testing.T) {
	srv, db, driver := newServerWithDriver(t)
	created := createInstance(t, srv, "t1", "k1", `{"gpu_type":"A100","gpu_count":4}`)
	if _, err := driver.Delete(t.Context(), created.ID, 3); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rows, err := db.Query(t.Context(), `SELECT event_type, aggregate_id, payload->>'state',
		(payload->>'resource_version')::int FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	type event struct {
		Type, Aggregate, State string
		RV                     int
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[event])
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var want []event
	for i, state := range []string{"SCHEDULED", "PROVISIONING", "RUNNING", "TERMINATING", "TERMINATED"} {
		want = append(want, event{Type: "instance." + strings.ToLower(state), Aggregate: created.ID,
			State: state, RV: i + 1})
	}
	if !slices.Equal(got, want) {
		t.Errorf("outbox =\n  %+v\nwant\n  %+v", got, want)
	}
}

func TestIdempotentReplayIgnoresFormatting(t *testing.T) {
	srv, db := newServer(t)
	_, first := postInstance(t, srv, "t1", "k1", `{"gpu_type":"A100","gpu_count":2}`)
	before := readWrites(t, db)

	resp, again := postInstance(t, srv, "t1", "k1", "{\n  \"gpu_count\" : 2,\n\t\"gpu_type\":\"A100\"\n}\n")
	if resp.StatusCode != http.StatusAccepted || resp.Header.Get(headerReplayed) != "true" {
		t.Fatalf("reformatted replay: status %d %s %q, want 202 true", resp.StatusCode,
			headerReplayed, resp.Header.Get(headerReplayed))
	}
	if again.ID != first.ID {
		t.Errorf("replay id = %s, want %s", again.ID, first.ID)
	}
	assertNothingWritten(t, before, readWrites(t, db))
}

func TestIdempotencyKeyReused(t *testing.T) {
	srv, db := newServer(t)
	const body = `{"gpu_type":"H100","gpu_count":2}`
	_, first := postInstance(t, srv, "t1", "k1", body)
	before := readWrites(t, db)

	for _, other := range []string{
		`{"gpu_type":"H100","gpu_count":4}`,
		`{"gpu_type":"A100","gpu_count":2}`,
	} {
		resp, e := postCreate(t, srv, "t1", "k1", other)
		if resp.StatusCode != http.StatusConflict || e.Code != codeKeyReused {
			t.Errorf("body %s: status %d error %+v, want 409 %s", other, resp.StatusCode, e, codeKeyReused)
		}
	}
	assertNothingWritten(t, before, readWrites(t, db))

	// The key still belongs to the first request.
	resp, again := postInstance(t, srv, "t1", "k1", body)
	if resp.StatusCode != http.StatusAccepted || again.ID != first.ID {
		t.Errorf("original body after 409: status %d id %s, want 202 %s", resp.StatusCode, again.ID, first.ID)
	}
}

func TestIdempotencyKeyScopedToTenant(t *testing.T) {
	srv, db := newServer(t)
	const body = `{"gpu_type":"H100","gpu_count":1}`
	_, a := postInstance(t, srv, "t1", "k1", body)
	resp, b := postInstance(t, srv, "t2", "k1", body)
	if resp.StatusCode != http.StatusAccepted || resp.Header.Get(headerReplayed) != "" {
		t.Fatalf("t2 create: status %d %s %q, want a fresh 202", resp.StatusCode,
			headerReplayed, resp.Header.Get(headerReplayed))
	}
	if a.ID == "" || a.ID == b.ID {
		t.Errorf("t1 id %q, t2 id %q, want two different instances", a.ID, b.ID)
	}
	w := readWrites(t, db)
	if w.instances != 2 || w.keys != 2 || w.used["t1/H100"] != 1 || w.used["t2/H100"] != 1 {
		t.Errorf("instances %d keys %d used %v, want 2, 2, 1 each", w.instances, w.keys, w.used)
	}
	// Each tenant still only sees its own instance.
	if resp, _ := do(t, http.MethodGet, srv.URL+"/v1/instances/"+a.ID,
		map[string]string{headerTenant: "t2"}, ""); resp.StatusCode != http.StatusNotFound {
		t.Errorf("t2 GET of t1's instance: status %d, want 404", resp.StatusCode)
	}
}

// TestFailedCreateDoesNotKeepKey: a refused create rolls its key back, so
// the same key works once the reason goes away (ADR-0001 Q2).
func TestFailedCreateDoesNotKeepKey(t *testing.T) {
	tests := []struct {
		name       string
		block      string // SQL that makes the create fail
		unblock    string // SQL that removes the reason
		wantStatus int
		wantCode   string
	}{
		{
			name:       "503 then capacity frees",
			block:      `UPDATE nodes SET allocated = capacity WHERE gpu_type = 'A100'`,
			unblock:    `UPDATE nodes SET allocated = 0 WHERE gpu_type = 'A100'`,
			wantStatus: http.StatusServiceUnavailable, wantCode: codeInsufficientCapacity,
		},
		{
			name:       "403 then quota frees",
			block:      `INSERT INTO tenant_quota (tenant_id, gpu_type, used) VALUES ('t1', 'A100', 16)`,
			unblock:    `UPDATE tenant_quota SET used = 0 WHERE tenant_id = 't1' AND gpu_type = 'A100'`,
			wantStatus: http.StatusForbidden, wantCode: codeQuotaExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, db := newServer(t)
			const body = `{"gpu_type":"A100","gpu_count":2}`
			if _, err := db.Exec(t.Context(), tc.block); err != nil {
				t.Fatalf("block: %v", err)
			}

			resp, e := postCreate(t, srv, "t1", "k1", body)
			if resp.StatusCode != tc.wantStatus || e.Code != tc.wantCode {
				t.Fatalf("status %d error %+v, want %d %s", resp.StatusCode, e, tc.wantStatus, tc.wantCode)
			}
			if w := readWrites(t, db); w.keys != 0 {
				t.Fatalf("idempotency_keys has %d rows after a refused create, want 0", w.keys)
			}

			if _, err := db.Exec(t.Context(), tc.unblock); err != nil {
				t.Fatalf("unblock: %v", err)
			}
			resp, inst := postInstance(t, srv, "t1", "k1", body)
			if resp.StatusCode != http.StatusAccepted || resp.Header.Get(headerReplayed) != "" {
				t.Fatalf("retry: status %d %s %q, want a fresh 202", resp.StatusCode,
					headerReplayed, resp.Header.Get(headerReplayed))
			}
			if inst.State != "SCHEDULED" || inst.ResourceVersion != 1 {
				t.Errorf("retry created %+v, want SCHEDULED rv 1", inst)
			}
			if w := readWrites(t, db); w.instances != 1 || w.keys != 1 {
				t.Errorf("instances %d keys %d, want 1, 1", w.instances, w.keys)
			}
		})
	}
}
