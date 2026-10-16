//go:build integration

package api

import (
	"context"
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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// send is a goroutine-safe request helper (no t.Fatal): it returns the
// status code, or an error.
func send(ctx context.Context, client *http.Client, method, url string, headers map[string]string, body string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}

// waitForInstanceID polls the idempotency key until the create commits, so a
// DELETE can race the create's lifecycle walk (which runs before the POST
// response is sent).
func waitForInstanceID(ctx context.Context, db *pgxpool.Pool, tenant string) (string, error) {
	for {
		var id string
		err := db.QueryRow(ctx, `SELECT instance_id FROM idempotency_keys WHERE tenant_id = $1 AND key = 'k'`,
			tenant).Scan(&id)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(200 * time.Microsecond):
		}
	}
}

// TestDeleteConcurrentWithCreateWalk: each instance is created while several
// DELETEs (no If-Match) race its create walk and each other. Every DELETE
// must be 202 (a DELETE that keeps losing to the others still sees the
// instance end TERMINATED, so it is not a 409), every instance ends
// TERMINATED with exactly one instance.terminated event, and all GPUs and
// quota come back.
func TestDeleteConcurrentWithCreateWalk(t *testing.T) {
	const (
		rounds              = 8
		instancesPerRound   = 10
		deletersPerInstance = 5 // 10 × (1 creator + 5 deleters) = 60 goroutines per round
	)
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(20) // 10 H100 nodes, 80 GPUs: capacity never refuses
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	// The server gets its own pool, big enough for real contention.
	cfg := db.Config()
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("server pool: %v", err)
	}
	t.Cleanup(pool.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := store.New(pool, scheduler.New(nil))
	srv := httptest.NewServer(NewHandler(st, lifecycle.New(st, log), log))
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}, Timeout: 30 * time.Second}
	t.Cleanup(client.CloseIdleConnections)

	type result struct {
		op     string
		tenant string
		status int
		err    error
	}
	var ids []string
	statuses := map[string]int{}
	for round := range rounds {
		start := make(chan struct{})
		results := make([]result, instancesPerRound*(1+deletersPerInstance))
		var wg sync.WaitGroup
		for i := range instancesPerRound {
			tenant := fmt.Sprintf("tc-%d-%d", round, i)
			base := i * (1 + deletersPerInstance)
			wg.Go(func() {
				<-start
				status, err := send(t.Context(), client, http.MethodPost, srv.URL+"/v1/instances",
					map[string]string{headerTenant: tenant, headerIdempotencyKey: "k"}, `{"gpu_type":"H100","gpu_count":1}`)
				results[base] = result{op: "POST", tenant: tenant, status: status, err: err}
			})
			for d := 1; d <= deletersPerInstance; d++ {
				wg.Go(func() {
					<-start
					ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
					defer cancel()
					id, err := waitForInstanceID(ctx, db, tenant)
					if err != nil {
						results[base+d] = result{op: "DELETE", tenant: tenant, err: fmt.Errorf("wait for create: %w", err)}
						return
					}
					status, err := send(ctx, client, http.MethodDelete, srv.URL+"/v1/instances/"+id,
						map[string]string{headerTenant: tenant}, "")
					results[base+d] = result{op: "DELETE", tenant: tenant, status: status, err: err}
				})
			}
		}
		close(start)
		wg.Wait()

		for _, r := range results {
			if r.err != nil {
				t.Fatalf("round %d %s %s: %v", round, r.op, r.tenant, r.err)
			}
			statuses[fmt.Sprintf("%s %d", r.op, r.status)]++
		}
		for i := range instancesPerRound {
			id, err := waitForInstanceID(t.Context(), db, fmt.Sprintf("tc-%d-%d", round, i))
			if err != nil {
				t.Fatalf("read instance id: %v", err)
			}
			ids = append(ids, id)
		}
	}

	want := map[string]int{
		"POST 202":   rounds * instancesPerRound,
		"DELETE 202": rounds * instancesPerRound * deletersPerInstance,
	}
	if fmt.Sprint(statuses) != fmt.Sprint(want) {
		t.Errorf("responses = %v, want %v (no 409, no 500)", statuses, want)
	}

	var notTerminated, badEvents, allocated, used int
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM instances WHERE id = ANY($1) AND state <> 'TERMINATED'),
		(SELECT count(*) FROM (SELECT i.id FROM instances i
			LEFT JOIN outbox o ON o.aggregate_id = i.id AND o.event_type = 'instance.terminated'
			WHERE i.id = ANY($1) GROUP BY i.id HAVING count(o.id) <> 1) x),
		(SELECT COALESCE(sum(allocated), 0) FROM nodes),
		(SELECT COALESCE(sum(used), 0) FROM tenant_quota)`, ids).
		Scan(&notTerminated, &badEvents, &allocated, &used); err != nil {
		t.Fatalf("read results: %v", err)
	}
	if len(ids) != rounds*instancesPerRound || notTerminated != 0 || badEvents != 0 || allocated != 0 || used != 0 {
		t.Errorf("%d instances: %d not TERMINATED, %d without exactly one instance.terminated, allocated %d, used %d; want %d, 0, 0, 0, 0",
			len(ids), notTerminated, badEvents, allocated, used, rounds*instancesPerRound)
	}
}
