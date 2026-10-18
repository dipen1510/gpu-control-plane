//go:build integration

package api

// Helpers for the S1-11 concurrency tests (s111_concurrency_integration_test.go).
// Every identifier is prefixed with s111 so it can't clash with other test
// files in this package.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

const (
	// s111Replicas independent API stacks (handler, store, scheduler, driver
	// and pgxpool each), standing in for N apiserver replicas.
	s111Replicas = 4
	// s111ConnsPerReplica: 4 × 8 = 32 server connections in total, so many
	// transactions really are in flight at once (the default pool is ~4 on 2
	// CPUs). Postgres allows 100, shared with other test packages.
	s111ConnsPerReplica = 8
	// s111TestTimeout bounds one test's whole run.
	s111TestTimeout = 90 * time.Second
	// s111Quota is the default per-tenant, per-GPU-type limit (Discussion D2).
	s111Quota = 16
)

// s111HoldingStates are the states that hold GPUs and quota (ADR-0001
// §Data model). Written from the spec, not taken from the statemachine
// package, so a wrong HoldsCapacity there is caught here.
var s111HoldingStates = []string{"SCHEDULED", "PROVISIONING", "RUNNING", "TERMINATING"}

// s111Edges are the legal transitions from the CLAUDE.md diagram.
var s111Edges = map[string][]string{
	"PENDING":      {"SCHEDULED", "FAILED"},
	"SCHEDULED":    {"PROVISIONING", "TERMINATING", "FAILED"},
	"PROVISIONING": {"RUNNING", "TERMINATING", "FAILED"},
	"RUNNING":      {"TERMINATING", "FAILED"}, // D29, S2-9
	"TERMINATING":  {"TERMINATED"},
}

// s111Env is a database with seeded nodes behind s111Replicas API servers.
type s111Env struct {
	db     *pgxpool.Pool // the test's own pool: setup and assertions only
	urls   []string      // one per replica
	client *http.Client
	logs   *s111LogStore
}

// s111NewEnv seeds nodes fake nodes (seed.Nodes: odd numbers H100, even A100,
// capacity 8 each) and starts the replicas.
func s111NewEnv(t *testing.T, nodes int) *s111Env {
	t.Helper()
	db := testutil.NewDB(t)
	ns, err := seed.Nodes(nodes)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, ns); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	env := &s111Env{db: db, logs: &s111LogStore{}}
	log := slog.New(&s111LogHandler{store: env.logs})
	for range s111Replicas {
		cfg := db.Config()
		cfg.MaxConns = s111ConnsPerReplica
		cfg.MinConns = 0
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
		if err != nil {
			t.Fatalf("replica pool: %v", err)
		}
		t.Cleanup(pool.Close)
		st := store.New(pool, scheduler.New(nil))
		srv := httptest.NewServer(NewHandler(st, lifecycle.New(st, log), log))
		t.Cleanup(srv.Close) // runs before pool.Close (LIFO)
		env.urls = append(env.urls, srv.URL)
	}
	env.client = &http.Client{
		Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 256},
		Timeout:   30 * time.Second,
	}
	t.Cleanup(env.client.CloseIdleConnections)
	return env
}

// url returns replica i's base URL (i is any int; wraps around).
func (e *s111Env) url(i int) string { return e.urls[i%len(e.urls)] }

// s111Resp is one HTTP answer, decoded.
type s111Resp struct {
	Status int
	Code   string // error code from the envelope, if any
	ID     string
	State  string
	NodeID string
	GPUs   int
	RV     int64
}

type s111Body struct {
	ID              string `json:"id"`
	State           string `json:"state"`
	NodeID          string `json:"node_id"`
	GPUCount        int    `json:"gpu_count"`
	ResourceVersion int64  `json:"resource_version"`
	Error           *struct {
		Code string `json:"code"`
	} `json:"error"`
}

// s111Do sends one request. Goroutine-safe: it never calls t.Fatal.
func s111Do(ctx context.Context, c *http.Client, method, url, tenant, key, body string) (s111Resp, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return s111Resp{}, err
	}
	req.Header.Set("X-Tenant-ID", tenant)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.Do(req)
	if err != nil {
		return s111Resp{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return s111Resp{}, err
	}
	out := s111Resp{Status: resp.StatusCode}
	if len(raw) == 0 {
		return out, nil
	}
	var b s111Body
	if err := json.Unmarshal(raw, &b); err != nil {
		return out, fmt.Errorf("%s %s: status %d, undecodable body %q: %w", method, url, resp.StatusCode, raw, err)
	}
	out.ID, out.State, out.NodeID, out.GPUs, out.RV = b.ID, b.State, b.NodeID, b.GPUCount, b.ResourceVersion
	if b.Error != nil {
		out.Code = b.Error.Code
	}
	return out, nil
}

func (e *s111Env) create(ctx context.Context, replica int, tenant, key, gpuType string, n int) (s111Resp, error) {
	return s111Do(ctx, e.client, http.MethodPost, e.url(replica)+"/v1/instances", tenant, key,
		fmt.Sprintf(`{"gpu_type":%q,"gpu_count":%d}`, gpuType, n))
}

// del sends DELETE without If-Match.
func (e *s111Env) del(ctx context.Context, replica int, tenant, id string) (s111Resp, error) {
	return s111Do(ctx, e.client, http.MethodDelete, e.url(replica)+"/v1/instances/"+id, tenant, "", "")
}

// s111CheckCreateResp returns a non-empty description if a create answer is
// not one the spec allows under load: 202 SCHEDULED, 403 quota_exceeded or
// 503 insufficient_capacity. A 500, or a 503 with another code (where a
// mapped deadlock would show up), is a failure.
func s111CheckCreateResp(r s111Resp) string {
	switch {
	case r.Status == http.StatusAccepted && r.State == "SCHEDULED" && r.ID != "" && r.NodeID != "" && r.RV == 1:
		return ""
	case r.Status == http.StatusForbidden && r.Code == "quota_exceeded":
		return ""
	case r.Status == http.StatusServiceUnavailable && r.Code == "insufficient_capacity":
		return ""
	case r.Status == http.StatusInternalServerError:
		return fmt.Sprintf("HTTP 500 (code %q): an unexpected server error under concurrency; "+
			"check the logged error below for SQLSTATE 40P01 (deadlock) or 23514 (CHECK hit)", r.Code)
	default:
		return fmt.Sprintf("unexpected create answer: status %d code %q state %q id %q node %q rv %d",
			r.Status, r.Code, r.State, r.ID, r.NodeID, r.RV)
	}
}

// s111RunConcurrently runs fn(0..n-1) on n goroutines that all wait at a
// start barrier until every one of them is ready, then go at once.
func s111RunConcurrently(n int, fn func(i int)) {
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	for i := range n {
		done.Go(func() {
			ready.Done()
			<-start
			fn(i)
		})
	}
	ready.Wait()
	close(start)
	done.Wait()
}

// s111Tally counts answers by "METHOD status code" and keeps the first few
// problems. Goroutine-safe.
type s111Tally struct {
	mu       sync.Mutex
	counts   map[string]int
	problems []string
}

func (s *s111Tally) add(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = map[string]int{}
	}
	s.counts[key]++
}

func (s *s111Tally) problem(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.problems = append(s.problems, fmt.Sprintf(format, args...))
}

func (s *s111Tally) get(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

// report fails the test with every collected problem (first 20).
func (s *s111Tally) report(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.problems) == 0 {
		return
	}
	shown := s.problems
	if len(shown) > 20 {
		shown = shown[:20]
	}
	t.Errorf("%d bad answers under concurrency (first %d):\n  %s", len(s.problems), len(shown),
		strings.Join(shown, "\n  "))
}

// s111Violations checks invariants 1, 6 and 7 in two single statements (each
// sees one snapshot, and reserve/release commit together with the instance
// row, so they must hold at every instant, not only at the end):
//   - every node: 0 <= allocated <= capacity, and allocated = SUM(gpu_count)
//     of instances in a capacity-holding state on that node
//   - every (tenant, gpu_type): 0 <= used <= quota_limit, and used = SUM of
//     the same, and no holding instance without a quota row
func s111Violations(ctx context.Context, db *pgxpool.Pool) ([]string, error) {
	var out []string
	rows, err := db.Query(ctx, `
		SELECT n.id, n.allocated, n.capacity, COALESCE(h.gpus, 0)
		FROM nodes n
		LEFT JOIN (SELECT node_id, SUM(gpu_count)::int AS gpus FROM instances
		           WHERE state = ANY($1) GROUP BY node_id) h ON h.node_id = n.id
		WHERE n.allocated > n.capacity OR n.allocated < 0 OR n.allocated <> COALESCE(h.gpus, 0)`,
		s111HoldingStates)
	if err != nil {
		return nil, fmt.Errorf("node accounting query: %w", err)
	}
	for rows.Next() {
		var id string
		var alloc, capacity, sum int
		if err := rows.Scan(&id, &alloc, &capacity, &sum); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, fmt.Sprintf("node %s: allocated %d, capacity %d, SUM(gpu_count) of holding instances %d "+
			"(invariant 1: allocated <= capacity; invariant 7: allocated == SUM)", id, alloc, capacity, sum))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = db.Query(ctx, `
		SELECT COALESCE(q.tenant_id, h.tenant_id), COALESCE(q.gpu_type, h.gpu_type),
		       COALESCE(q.used, -1), COALESCE(q.quota_limit, -1), COALESCE(h.gpus, 0)
		FROM tenant_quota q
		FULL JOIN (SELECT tenant_id, gpu_type, SUM(gpu_count)::int AS gpus FROM instances
		           WHERE state = ANY($1) GROUP BY tenant_id, gpu_type) h
		  ON h.tenant_id = q.tenant_id AND h.gpu_type = q.gpu_type
		WHERE q.tenant_id IS NULL OR q.used > q.quota_limit OR q.used < 0 OR q.used <> COALESCE(h.gpus, 0)`,
		s111HoldingStates)
	if err != nil {
		return nil, fmt.Errorf("quota accounting query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tenant, gpuType string
		var used, limit, sum int
		if err := rows.Scan(&tenant, &gpuType, &used, &limit, &sum); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("quota %s/%s: used %d, quota_limit %d, SUM(gpu_count) of holding instances %d "+
			"(invariant 6: used <= quota_limit; invariant 7: used == SUM; -1 means no quota row)",
			tenant, gpuType, used, limit, sum))
	}
	return out, rows.Err()
}

// s111CheckAccounting fails the test on any invariant 1/6/7 violation.
func s111CheckAccounting(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	t.Helper()
	v, err := s111Violations(ctx, db)
	if err != nil {
		t.Fatalf("accounting check: %v", err)
	}
	if len(v) > 0 {
		t.Errorf("accounting broken (%d rows):\n  %s", len(v), strings.Join(v, "\n  "))
	}
}

// s111Totals returns SUM(allocated) over nodes and SUM(used) over quota rows.
func s111Totals(t *testing.T, ctx context.Context, db *pgxpool.Pool) (allocated, used int) {
	t.Helper()
	if err := db.QueryRow(ctx, `SELECT
		(SELECT COALESCE(SUM(allocated), 0)::int FROM nodes),
		(SELECT COALESCE(SUM(used), 0)::int FROM tenant_quota)`).Scan(&allocated, &used); err != nil {
		t.Fatalf("read totals: %v", err)
	}
	return allocated, used
}

// s111CheckHistories checks invariant 3 for every instance: its outbox rows,
// in insertion order (the steps of one instance are serialized, so id order
// is their order), are one row per state it passed through: they start at
// instance.scheduled, follow legal edges only, contain no state twice, end
// at the instance's current state, and number exactly resource_version (rv
// starts at 1 and each transition adds 1). It also checks no outbox row
// belongs to an instance that does not exist (a rolled-back create must leave
// no event) and that every instance's state is in finalStates. It returns
// how many instances skipped RUNNING (a DELETE cut their create walk short).
func s111CheckHistories(t *testing.T, ctx context.Context, db *pgxpool.Pool, finalStates ...string) (cutShort int) {
	t.Helper()
	var orphans int
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM outbox o WHERE NOT EXISTS (SELECT 1 FROM instances i WHERE i.id = o.aggregate_id)`).
		Scan(&orphans); err != nil {
		t.Fatalf("orphan outbox rows: %v", err)
	}
	if orphans != 0 {
		t.Errorf("%d outbox rows belong to no instance (a failed create left an event: invariant 3)", orphans)
	}

	rows, err := db.Query(ctx, `
		SELECT i.id, i.state, i.resource_version,
		       COALESCE(array_agg(o.event_type ORDER BY o.id) FILTER (WHERE o.id IS NOT NULL), '{}')
		FROM instances i LEFT JOIN outbox o ON o.aggregate_id = i.id
		GROUP BY i.id, i.state, i.resource_version`)
	if err != nil {
		t.Fatalf("read histories: %v", err)
	}
	defer rows.Close()
	allowedFinal := map[string]bool{}
	for _, s := range finalStates {
		allowedFinal[s] = true
	}
	var bad []string
	for rows.Next() {
		var id, state string
		var rv int64
		var events []string
		if err := rows.Scan(&id, &state, &rv, &events); err != nil {
			t.Fatalf("scan history: %v", err)
		}
		if !allowedFinal[state] {
			bad = append(bad, fmt.Sprintf("%s: state %s, want one of %v (stuck mid-walk?)", id, state, finalStates))
		}
		if msg := s111HistoryProblem(state, rv, events); msg != "" {
			bad = append(bad, fmt.Sprintf("%s: %s; events %v", id, msg, events))
		}
		if !s111Contains(events, "instance.running") {
			cutShort++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read histories: %v", err)
	}
	if len(bad) > 0 {
		shown := bad
		if len(shown) > 20 {
			shown = shown[:20]
		}
		t.Errorf("%d instances with a bad state or outbox history (first %d):\n  %s", len(bad), len(shown),
			strings.Join(shown, "\n  "))
	}
	return cutShort
}

func s111HistoryProblem(state string, rv int64, events []string) string {
	if int64(len(events)) != rv {
		return fmt.Sprintf("%d outbox rows but resource_version %d (want one row per transition)", len(events), rv)
	}
	if len(events) == 0 || events[0] != "instance.scheduled" {
		return "first event is not instance.scheduled"
	}
	seen := map[string]bool{}
	prev := ""
	for _, ev := range events {
		s := strings.ToUpper(strings.TrimPrefix(ev, "instance."))
		if seen[s] {
			return "duplicate event " + ev
		}
		seen[s] = true
		if prev != "" && !s111Contains(s111Edges[prev], s) {
			return fmt.Sprintf("illegal edge %s -> %s", prev, s)
		}
		prev = s
	}
	if prev != state {
		return fmt.Sprintf("last event state %s != instance state %s", prev, state)
	}
	return ""
}

func s111Contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// s111InstanceIDForKey polls until the create under (tenant, key) has
// committed and returns its instance id, so a DELETE can race the create's
// lifecycle walk (which runs before the POST answers). It returns ctx's
// error if the create never commits.
func s111InstanceIDForKey(ctx context.Context, db *pgxpool.Pool, tenant, key string) (string, error) {
	for {
		var id string
		err := db.QueryRow(ctx, `SELECT instance_id FROM idempotency_keys WHERE tenant_id = $1 AND key = $2`,
			tenant, key).Scan(&id)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(300 * time.Microsecond):
		}
	}
}

// s111SlowWrites installs triggers in this test's own throwaway database that
// sleep inside the writing transaction: instMs after each INSERT INTO
// instances (a create that already reserved quota and a node, still holding
// both row locks) and outboxMs after each INSERT INTO outbox (every create
// and every lifecycle step, just before commit). It stands in for network
// latency between real replicas and Postgres. Without it, on a small machine
// a winner often commits before a loser reaches the same row, so the loser's
// UPDATE never waits and the "waited, then lost the re-check" path (ADR-0001
// Amendment 2, D18 Lesson 1) is never exercised. Production code is not
// touched.
func s111SlowWrites(t *testing.T, db *pgxpool.Pool, instMs, outboxMs int) {
	t.Helper()
	stmts := []string{
		fmt.Sprintf(`CREATE FUNCTION s111_slow_instance() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_sleep(%f); RETURN NEW; END $$`, float64(instMs)/1000),
		`CREATE TRIGGER s111_slow_instance AFTER INSERT ON instances
			FOR EACH ROW EXECUTE FUNCTION s111_slow_instance()`,
		fmt.Sprintf(`CREATE FUNCTION s111_slow_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_sleep(%f); RETURN NEW; END $$`, float64(outboxMs)/1000),
		`CREATE TRIGGER s111_slow_outbox AFTER INSERT ON outbox
			FOR EACH ROW EXECUTE FUNCTION s111_slow_outbox()`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(t.Context(), s); err != nil {
			t.Fatalf("install latency trigger: %v", err)
		}
	}
}

// s111LogStore keeps every server log record that signals a problem: any
// Warn or Error (a 500, a lifecycle step that failed for a reason other than
// losing a race, an instance stuck in TERMINATING), and any record at any
// level whose attributes carry SQLSTATE 40P01. The lifecycle driver only logs
// its errors (the create still answers 202), so the logs are the only place a
// deadlock in a lifecycle step shows up.
type s111LogStore struct {
	mu        sync.Mutex
	problems  []string
	deadlocks []string
}

type s111LogHandler struct {
	store *s111LogStore
	attrs []slog.Attr
}

func (h *s111LogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *s111LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &s111LogHandler{store: h.store, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}

func (h *s111LogHandler) WithGroup(string) slog.Handler { return h }

func (h *s111LogHandler) Handle(_ context.Context, r slog.Record) error {
	deadlock := false
	parts := []string{r.Level.String(), r.Message}
	visit := func(a slog.Attr) bool {
		v := a.Value.Resolve()
		if err, ok := v.Any().(error); ok {
			var pg *pgconn.PgError
			if errors.As(err, &pg) && pg.Code == "40P01" {
				deadlock = true
			}
		}
		s := v.String()
		if strings.Contains(s, "40P01") {
			deadlock = true
		}
		parts = append(parts, a.Key+"="+s)
		return true
	}
	for _, a := range h.attrs {
		visit(a)
	}
	r.Attrs(visit)
	if !deadlock && r.Level < slog.LevelWarn {
		return nil
	}
	line := strings.Join(parts, " ")
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if deadlock {
		h.store.deadlocks = append(h.store.deadlocks, line)
	} else {
		h.store.problems = append(h.store.problems, line)
	}
	return nil
}

// check fails the test on any recorded deadlock or warning/error.
func (s *s111LogStore) check(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := len(s.deadlocks); n > 0 {
		t.Errorf("DEADLOCK: Postgres aborted %d transactions with SQLSTATE 40P01. Some code path takes row locks "+
			"out of the documented order (idempotency_keys -> tenant_quota -> nodes -> instances -> outbox) or "+
			"keeps a lost-race lock (ADR-0001 Amendment 2, Discussion D18). First: %s", n, s.deadlocks[0])
	}
	if n := len(s.problems); n > 0 {
		shown := s.problems
		if len(shown) > 10 {
			shown = shown[:10]
		}
		t.Errorf("server logged %d warnings/errors under concurrency (500s, failed lifecycle steps or "+
			"instances stuck in TERMINATING); first %d:\n  %s", n, len(shown), strings.Join(shown, "\n  "))
	}
}
