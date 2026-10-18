package poller

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeRow scans a fixed (now, ids) result or returns err.
type fakeRow struct {
	now time.Time
	ids []string
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*time.Time) = r.now
	if len(dest) > 1 {
		*dest[1].(*[]string) = r.ids
	}
	return nil
}

// fakeDB answers each query with the next scripted row (the last repeats).
type fakeDB struct {
	mu    sync.Mutex
	rows  []fakeRow
	calls int
	args  [][]any
}

func (f *fakeDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := min(f.calls, len(f.rows)-1)
	f.calls++
	f.args = append(f.args, args)
	return f.rows[i]
}

func drain(ch <-chan Event) []string {
	var ids []string
	for {
		select {
		case e := <-ch:
			ids = append(ids, e.Object)
		default:
			return ids
		}
	}
}

func TestPollChangedUsesPreviousDBTimeMinusOverlap(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	db := &fakeDB{rows: []fakeRow{
		{now: t0, ids: []string{"a"}},                       // resync
		{now: t0.Add(time.Second), ids: []string{"b", "c"}}, // changed
		{now: t0.Add(2 * time.Second)},                      // changed, none
	}}
	ch := make(chan Event, 10)
	reg := prometheus.NewRegistry()
	p, err := New(db, ch, Config{}, reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n, err := p.PollChanged(t.Context()); err != nil || n != 2 {
		t.Fatalf("PollChanged = %d, %v; want 2, nil", n, err)
	}
	if n, err := p.PollChanged(t.Context()); err != nil || n != 0 {
		t.Fatalf("PollChanged = %d, %v; want 0, nil", n, err)
	}
	if got := drain(ch); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("ids = %v", got)
	}
	// Each changed-poll passes the previous DB now() and the 5s overlap.
	if got := db.args[1]; got[0] != t0 || got[1] != 5.0 {
		t.Fatalf("first changed args = %v; want [%v 5]", got, t0)
	}
	if got := db.args[2][0]; got != t0.Add(time.Second) {
		t.Fatalf("second changed last = %v; want %v", got, t0.Add(time.Second))
	}
	if got := testutil.ToFloat64(p.idsChanged); got != 2 {
		t.Fatalf("changed counter = %v", got)
	}
	if got := testutil.ToFloat64(p.idsResync); got != 1 {
		t.Fatalf("resync counter = %v", got)
	}
}

func TestPollChangedKeepsLastOnError(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	db := &fakeDB{rows: []fakeRow{
		{now: t0},
		{err: errors.New("boom")},
		{now: t0.Add(3 * time.Second)},
	}}
	p, err := New(db, make(chan Event, 1), Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PollChanged(t.Context()); err == nil {
		t.Fatal("want error")
	}
	if _, err := p.PollChanged(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The retry still starts from the last successful DB time.
	if got := db.args[2][0]; got != t0 {
		t.Fatalf("retry last = %v; want %v", got, t0)
	}
}

func TestStartCountsErrorsAndKeepsRunning(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{{err: errors.New("db down")}}}
	ch := make(chan Event, 1)
	p, err := New(db, ch, Config{Interval: time.Millisecond, ResyncInterval: time.Hour}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for testutil.ToFloat64(p.errs) < 3 {
		if time.Now().After(deadline) {
			t.Fatal("errors not counted")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start = %v; want nil", err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("Start must close the channel on return")
	}
}

func TestSendStopsOnCancel(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{{ids: []string{"a", "b"}}}}
	ch := make(chan Event) // unbuffered, nobody reads
	p, err := New(db, ch, Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n, err := p.Resync(ctx); !errors.Is(err, errStopped) || n != 0 {
		t.Fatalf("Resync = %d, %v; want 0, errStopped", n, err)
	}
}

// TestNewTwiceOnOneRegistryReusesCollectors: a second poller on the same
// registry (a second Manager in one process) must not fail, and both share
// the registered counters.
func TestNewTwiceOnOneRegistryReusesCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	p1, err := New(&fakeDB{}, make(chan Event, 1), Config{}, reg, nil)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	p2, err := New(&fakeDB{}, make(chan Event, 1), Config{}, reg, nil)
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	p2.errs.Inc()
	if got := testutil.ToFloat64(p1.errs); got != 1 {
		t.Errorf("first poller's errors counter = %v, want 1 (shared collector)", got)
	}
}
