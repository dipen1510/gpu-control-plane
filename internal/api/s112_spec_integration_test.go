//go:build integration

// S1-12 spec tests for idempotent create under concurrency, written from
// ADR-0001 (§The create transaction step 1, §API (M1), Amendment 2 item 3)
// and CLAUDE.md invariant 2, not from the code. They go through real HTTP,
// the real store and the M1 lifecycle driver.
//
// External package api_test: only the exported contract is used. Every
// helper is prefixed s112 to avoid collisions with other stories' helpers.
package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/api"
	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// s112PoolConns: the testutil default pool is max(4, NumCPU) connections,
// which would queue the racers in the pool instead of racing them in
// Postgres (S1-8 review). Postgres allows 100 in total, so stay modest.
const s112PoolConns = 25

const s112Racers = 100

// Header names from ADR-0001 §API (M1) / Amendment 2 item 3.
const (
	s112HdrTenant   = "X-Tenant-ID"
	s112HdrKey      = "Idempotency-Key"
	s112HdrReplayed = "Idempotent-Replayed"
)

type s112Instance struct {
	ID              string `json:"id"`
	State           string `json:"state"`
	NodeID          string `json:"node_id"`
	GPUType         string `json:"gpu_type"`
	GPUCount        int    `json:"gpu_count"`
	ResourceVersion int64  `json:"resource_version"`
}

type s112ErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// s112Server is an API server on a fresh database with 4 seeded nodes (2
// H100, 2 A100, 8 GPUs each), a pool of s112PoolConns, the real store and
// the M1 lifecycle driver.
func s112Server(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	small := testutil.NewDB(t)
	cfg := small.Config()
	cfg.MaxConns = s112PoolConns
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("s112: open pool: %v", err)
	}
	t.Cleanup(db.Close) // runs before testutil drops the database (LIFO)

	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("s112: seed: %v", err)
	}
	if err := seed.Upsert(ctx, db, nodes); err != nil {
		t.Fatalf("s112: seed: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := store.New(db, scheduler.New(nil))
	srv := httptest.NewServer(api.NewHandler(st, lifecycle.New(st, log), log))
	t.Cleanup(srv.Close)
	return srv, db
}

// s112Client allows every racer its own keep-alive connection.
func s112Client(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{MaxIdleConns: 2 * s112Racers, MaxIdleConnsPerHost: 2 * s112Racers}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 60 * time.Second}
}

type s112Response struct {
	status   int
	replayed string
	body     []byte
	err      error
}

// s112RaceCreates sends one POST per body (all at once after a barrier) with
// the same tenant and key, and returns the responses in body order.
func s112RaceCreates(t *testing.T, srv *httptest.Server, tenant, key string, bodies []string) []s112Response {
	t.Helper()
	client := s112Client(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	out := make([]s112Response, len(bodies))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/instances",
				strings.NewReader(body))
			if err != nil {
				out[i].err = err
				return
			}
			req.Header.Set(s112HdrTenant, tenant)
			req.Header.Set(s112HdrKey, key)
			<-start
			resp, err := client.Do(req)
			if err != nil {
				out[i].err = err
				return
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			out[i] = s112Response{status: resp.StatusCode, replayed: resp.Header.Get(s112HdrReplayed), body: b, err: err}
		}()
	}
	close(start)
	wg.Wait()
	return out
}

func s112Count(t *testing.T, db *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("s112: %s: %v", sql, err)
	}
	return n
}

// s112Used is tenant_quota.used for tenant/gpuType, 0 if there is no row.
func s112Used(t *testing.T, db *pgxpool.Pool, tenant, gpuType string) int {
	t.Helper()
	return s112Count(t, db, `SELECT COALESCE((SELECT used FROM tenant_quota
		WHERE tenant_id = $1 AND gpu_type = $2), 0)`, tenant, gpuType)
}

// s112CheckOneInstance asserts what invariant 2 promises after any race on
// one key: one instance for the tenant, one key row, one instance.scheduled
// event, the M1 driver ran once (exactly one event per state it walked),
// quota and node GPUs counted once. It returns the stored instance.
func s112CheckOneInstance(t *testing.T, db *pgxpool.Pool, tenant string) s112Instance {
	t.Helper()
	if n := s112Count(t, db, `SELECT count(*) FROM instances WHERE tenant_id = $1`, tenant); n != 1 {
		t.Fatalf("%d instances for tenant %s, want exactly 1", n, tenant)
	}
	var inst s112Instance
	if err := db.QueryRow(t.Context(), `SELECT id, state, node_id, gpu_type, gpu_count, resource_version
		FROM instances WHERE tenant_id = $1`, tenant).Scan(
		&inst.ID, &inst.State, &inst.NodeID, &inst.GPUType, &inst.GPUCount, &inst.ResourceVersion); err != nil {
		t.Fatalf("s112: read instance: %v", err)
	}
	if n := s112Count(t, db, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = $1`, tenant); n != 1 {
		t.Errorf("%d idempotency_keys rows, want 1", n)
	}
	if n := s112Count(t, db, `SELECT count(*) FROM outbox
		WHERE aggregate_id = $1 AND event_type = 'instance.scheduled'`, inst.ID); n != 1 {
		t.Errorf("%d instance.scheduled outbox rows, want exactly 1", n)
	}
	// Replays must not re-run the lifecycle driver: one event per step.
	rows, err := db.Query(t.Context(), `SELECT event_type, count(*) FROM outbox
		WHERE aggregate_id = $1 GROUP BY event_type`, inst.ID)
	if err != nil {
		t.Fatalf("s112: read outbox: %v", err)
	}
	for rows.Next() {
		var et string
		var n int
		if err := rows.Scan(&et, &n); err != nil {
			t.Fatalf("s112: scan outbox: %v", err)
		}
		if n != 1 {
			t.Errorf("outbox has %d %s rows for the instance, want 1 (driver ran more than once?)", n, et)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("s112: read outbox: %v", err)
	}
	if n := s112Count(t, db, `SELECT count(*) FROM outbox o
		WHERE NOT EXISTS (SELECT 1 FROM instances i WHERE i.id = o.aggregate_id)`); n != 0 {
		t.Errorf("%d outbox rows for instances that do not exist (losers leaked events)", n)
	}
	if inst.State != "TERMINATED" && inst.State != "FAILED" {
		if used := s112Used(t, db, tenant, inst.GPUType); used != inst.GPUCount {
			t.Errorf("tenant_quota.used = %d, want %d (quota counted once)", used, inst.GPUCount)
		}
	}
	// GPUs reserved once: every node's allocated equals the GPUs of the
	// capacity-holding instances on it (invariant 7). A loser that reserved
	// and kept GPUs would show up here.
	if n := s112Count(t, db, `SELECT count(*) FROM nodes n WHERE n.allocated <>
		COALESCE((SELECT SUM(i.gpu_count) FROM instances i WHERE i.node_id = n.id
		          AND i.state IN ('SCHEDULED','PROVISIONING','RUNNING','TERMINATING')), 0)`); n != 0 {
		t.Errorf("%d nodes whose allocated differs from their instances (GPUs reserved more than once?)", n)
	}
	return inst
}

// TestConcurrentSameKeyCreatesOneInstance: 100 clients retry the same create
// (same tenant, key and body) at the same moment. Invariant 2 says that is
// one request: one instance, one instance.scheduled event, quota counted
// once, and every client gets 202 with the same id; exactly one response is
// the original (no Idempotent-Replayed), the rest are replays.
func TestConcurrentSameKeyCreatesOneInstance(t *testing.T) {
	srv, db := s112Server(t)
	for round := range 3 {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			tenant := fmt.Sprintf("s112-same-%d", round)
			bodies := make([]string, s112Racers)
			for i := range bodies {
				bodies[i] = `{"gpu_type":"H100","gpu_count":2}`
			}
			resps := s112RaceCreates(t, srv, tenant, "s112-key", bodies)

			var originals int
			ids := map[string]int{}
			for i, r := range resps {
				if r.err != nil {
					t.Errorf("client %d: %v", i, r.err)
					continue
				}
				if r.status != http.StatusAccepted {
					t.Errorf("client %d: status %d body %s, want 202", i, r.status, r.body)
					continue
				}
				var got s112Instance
				if err := json.Unmarshal(r.body, &got); err != nil {
					t.Errorf("client %d: decode %s: %v", i, r.body, err)
					continue
				}
				ids[got.ID]++
				if got.GPUType != "H100" || got.GPUCount != 2 || got.NodeID == "" {
					t.Errorf("client %d: body %+v, want H100 x2 on a node", i, got)
				}
				switch r.replayed {
				case "":
					originals++
					if got.State != "SCHEDULED" || got.ResourceVersion != 1 {
						t.Errorf("client %d: original response %s rv %d, want SCHEDULED rv 1",
							i, got.State, got.ResourceVersion)
					}
				case "true":
				default:
					t.Errorf("client %d: %s: %q, want \"true\" or absent", i, s112HdrReplayed, r.replayed)
				}
			}
			if len(ids) != 1 {
				t.Errorf("responses carry %d distinct ids %v, want exactly 1", len(ids), ids)
			}
			if originals != 1 {
				t.Errorf("%d responses without %s, want exactly 1", originals, s112HdrReplayed)
			}

			inst := s112CheckOneInstance(t, db, tenant)
			if _, ok := ids[inst.ID]; !ok && len(ids) == 1 {
				t.Errorf("responses carry id %v, stored instance is %s", ids, inst.ID)
			}
		})
	}
}

// s112Body is one logical create body plus its spelling. Two spellings of
// the same (gpu_type, gpu_count) are the SAME request: the hash is over the
// parsed body (ADR-0001 §The create transaction).
type s112Body struct {
	gpuType string
	count   int
	json    string
}

// TestConcurrentSameKeyDifferentBodies: 100 clients race on one key with
// several different bodies. Exactly one body wins and becomes the only
// instance. Every client that sent the winning body (in any spelling) gets
// 202 with the winner's id; every client that sent another body gets
// 409 idempotency_key_reused. Nothing else (no 500, no second instance, no
// quota charged for a losing body).
func TestConcurrentSameKeyDifferentBodies(t *testing.T) {
	variants := []s112Body{
		{"H100", 1, `{"gpu_type":"H100","gpu_count":1}`},
		{"H100", 1, `{ "gpu_count": 1, "gpu_type": "H100" }`}, // same request, other spelling
		{"H100", 2, `{"gpu_type":"H100","gpu_count":2}`},
		{"A100", 1, `{"gpu_type":"A100","gpu_count":1}`},
		{"A100", 4, `{"gpu_type":"A100","gpu_count":4}`},
	}
	srv, db := s112Server(t)
	for round := range 3 {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			tenant := fmt.Sprintf("s112-diff-%d", round)
			sent := make([]s112Body, s112Racers)
			bodies := make([]string, s112Racers)
			for i := range sent {
				sent[i] = variants[(i+round)%len(variants)]
				bodies[i] = sent[i].json
			}
			resps := s112RaceCreates(t, srv, tenant, "s112-key", bodies)

			inst := s112CheckOneInstance(t, db, tenant)
			type logical struct {
				gpuType string
				count   int
			}
			winner := logical{inst.GPUType, inst.GPUCount}

			var originals, replays, conflicts int
			for i, r := range resps {
				if r.err != nil {
					t.Errorf("client %d: %v", i, r.err)
					continue
				}
				mine := logical{sent[i].gpuType, sent[i].count}
				switch r.status {
				case http.StatusAccepted:
					var got s112Instance
					if err := json.Unmarshal(r.body, &got); err != nil {
						t.Errorf("client %d: decode %s: %v", i, r.body, err)
						continue
					}
					if mine != winner {
						t.Errorf("client %d sent %+v but got 202 %+v; the winner is %+v", i, mine, got, winner)
					}
					if got.ID != inst.ID || got.GPUType != inst.GPUType || got.GPUCount != inst.GPUCount ||
						got.NodeID != inst.NodeID {
						t.Errorf("client %d: 202 body %+v, want the winner %+v", i, got, inst)
					}
					if r.replayed == "true" {
						replays++
					} else {
						originals++
					}
				case http.StatusConflict:
					conflicts++
					var eb s112ErrorBody
					if err := json.Unmarshal(r.body, &eb); err != nil || eb.Error.Code != "idempotency_key_reused" {
						t.Errorf("client %d: 409 body %s, want code idempotency_key_reused", i, r.body)
					}
					if mine == winner {
						t.Errorf("client %d sent the winning body %+v but got 409", i, mine)
					}
				default:
					t.Errorf("client %d: status %d body %s, want 202 or 409", i, r.status, r.body)
				}
			}
			if originals != 1 {
				t.Errorf("%d original (non-replayed) 202s, want exactly 1", originals)
			}
			t.Logf("winner %+v: 1 original, %d replays, %d conflicts", winner, replays, conflicts)

			// Losing bodies must not have charged quota for their GPU type.
			for _, gt := range []string{"H100", "A100"} {
				want := 0
				if gt == inst.GPUType {
					want = inst.GPUCount
				}
				if used := s112Used(t, db, tenant, gt); used != want {
					t.Errorf("tenant_quota.used for %s = %d, want %d", gt, used, want)
				}
			}
		})
	}
}
