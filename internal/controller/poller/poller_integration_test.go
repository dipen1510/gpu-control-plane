//go:build integration

package poller

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

const insertSQL = `INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, state)
VALUES ($1, 't1', 'H100', 1, $2)`

func newPoller(t *testing.T, db DB, cfg Config) (*Poller, chan Event) {
	t.Helper()
	ch := make(chan Event, 100)
	p, err := New(db, ch, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p, ch
}

// TestPollerSeesChangeCommittedLate: a transaction stamps updated_at = now()
// (its start time) before a poll, but commits after it. The next poll must
// still report the row; that is what the overlap is for.
func TestPollerSeesChangeCommittedLate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap time.Duration
		want    bool
	}{
		{"default 5s overlap reports it", 0, true},
		{"no overlap misses it (proves the test can fail)", time.Nanosecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.NewDB(t)
			ctx := t.Context()
			p, ch := newPoller(t, db, Config{Overlap: tc.overlap})

			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // no-op after commit
			if _, err := tx.Exec(ctx, insertSQL, "inst_late", "SCHEDULED"); err != nil {
				t.Fatal(err)
			}
			// Previous poll: runs after the tx stamped updated_at, cannot see it yet.
			if _, err := p.Resync(ctx); err != nil {
				t.Fatal(err)
			}
			if got := drain(ch); len(got) != 0 {
				t.Fatalf("uncommitted row visible: %v", got)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := p.PollChanged(ctx); err != nil {
				t.Fatal(err)
			}
			got := slices.Contains(drain(ch), "inst_late")
			if got != tc.want {
				t.Fatalf("late-committed row reported = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestPollerResyncListsNonTerminal(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()
	states := map[string]string{
		"i_sched": "SCHEDULED", "i_prov": "PROVISIONING", "i_run": "RUNNING",
		"i_term": "TERMINATING", "i_done": "TERMINATED", "i_fail": "FAILED",
	}
	for id, st := range states {
		if _, err := db.Exec(ctx, insertSQL, id, st); err != nil {
			t.Fatal(err)
		}
	}
	p, ch := newPoller(t, db, Config{})
	n, err := p.Resync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(ch)
	slices.Sort(got)
	want := []string{"i_prov", "i_run", "i_sched", "i_term"}
	if !slices.Equal(got, want) || n != len(want) {
		t.Fatalf("resync = %v (n=%d); want %v", got, n, want)
	}
}

// TestPollerIssuesNoWrites runs the poller over a pool whose sessions are
// read-only: any write it attempted would fail and be counted.
func TestPollerIssuesNoWrites(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := t.Context()
	if _, err := db.Exec(ctx, insertSQL, "i1", "RUNNING"); err != nil {
		t.Fatal(err)
	}
	cfg := db.Config().Copy()
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	ro, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ro.Close)
	// The guard is real: a write through ro fails.
	if _, err := ro.Exec(ctx, `UPDATE instances SET failure_reason = 'x'`); err == nil {
		t.Fatal("read-only pool accepted a write")
	}

	p, ch := newPoller(t, ro, Config{Interval: 10 * time.Millisecond, ResyncInterval: 30 * time.Millisecond})
	runCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	go func() {
		for range ch { // drained until Start closes it
		}
	}()
	if err := p.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	if got := promtest.ToFloat64(p.errs); got != 0 {
		t.Fatalf("poller errors on read-only pool = %v; want 0", got)
	}
	if got := promtest.ToFloat64(p.idsResync); got == 0 {
		t.Fatal("poller sent nothing; the test exercised no queries")
	}
}
