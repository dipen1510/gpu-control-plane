// Package poller is the controller's DB event source (ADR-0002 M2-2,
// Discussion D20). New instances exist only in Postgres, so a goroutine polls
// the instances table and sends instance ids (never objects) into a channel
// that the Manager wires to the instance workqueue with source.Channel.
//
// Two queries, both read-only:
//   - every Interval: ids WHERE updated_at > last - Overlap, where last is
//     the DB now() returned by the previous poll. Using DB time avoids clock
//     skew between hosts; the overlap catches a row whose transaction started
//     (and stamped updated_at = now()) before the previous poll but committed
//     after it.
//   - every ResyncInterval: every non-terminal id, the safety net for
//     anything the incremental poll missed.
//
// Duplicates are harmless: the workqueue dedupes keys and the reconcile reads
// current state. DB errors are logged, counted and retried on the next tick.
package poller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// Defaults from D20.
const (
	DefaultInterval       = time.Second
	DefaultResyncInterval = 30 * time.Second
	DefaultOverlap        = 5 * time.Second
)

// Event is what the poller sends: Object is an instance id.
type Event = event.TypedGenericEvent[string]

// DB is what the poller needs from Postgres. *pgxpool.Pool satisfies it.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Config holds the poll intervals. Zero values take the defaults.
type Config struct {
	Interval       time.Duration
	ResyncInterval time.Duration
	Overlap        time.Duration
}

// Poller polls the instances table. Start runs it; it is a manager.Runnable
// and, by not implementing LeaderElectionRunnable, runs only on the leader.
type Poller struct {
	db  DB
	out chan<- Event
	cfg Config
	log *slog.Logger

	idsChanged prometheus.Counter
	idsResync  prometheus.Counter
	errs       prometheus.Counter

	last    time.Time // DB now() of the previous successful changed-poll
	hasLast bool
}

// New returns a Poller that sends into out. Start closes out when it returns
// (the poller is the only sender). Metrics are registered on reg.
func New(db DB, out chan<- Event, cfg Config, reg prometheus.Registerer, log *slog.Logger) (*Poller, error) {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.ResyncInterval <= 0 {
		cfg.ResyncInterval = DefaultResyncInterval
	}
	if cfg.Overlap <= 0 {
		cfg.Overlap = DefaultOverlap
	}
	if log == nil {
		log = slog.Default()
	}
	ids := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpucp_controller_poller_ids_total",
		Help: "Instance ids sent to the workqueue by the DB poller.",
	}, []string{"kind"})
	errs := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gpucp_controller_poller_errors_total",
		Help: "DB poller queries that failed (retried next tick).",
	})
	if reg != nil {
		var err error
		if ids, err = registerOrReuse(reg, ids); err != nil {
			return nil, err
		}
		if errs, err = registerOrReuse(reg, errs); err != nil {
			return nil, err
		}
	}
	return &Poller{
		db: db, out: out, cfg: cfg, log: log.With("component", "poller"),
		idsChanged: ids.WithLabelValues("changed"),
		idsResync:  ids.WithLabelValues("resync"),
		errs:       errs,
	}, nil
}

// registerOrReuse registers c on reg. If an identical collector is already
// registered (a second Manager in one process, e.g. tests, since
// controller-runtime's metrics.Registry is process-wide), it returns that
// one, the same pattern as controller.register.
func registerOrReuse[C prometheus.Collector](reg prometheus.Registerer, c C) (C, error) {
	err := reg.Register(c)
	if err == nil {
		return c, nil
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(C); ok {
			return existing, nil
		}
	}
	var zero C
	return zero, fmt.Errorf("register poller metrics: %w", err)
}

// Start resyncs once, then polls until ctx is cancelled. It returns nil on
// cancellation; DB errors never stop it.
func (p *Poller) Start(ctx context.Context) error {
	defer close(p.out)
	poll := time.NewTicker(p.cfg.Interval)
	defer poll.Stop()
	resync := time.NewTicker(p.cfg.ResyncInterval)
	defer resync.Stop()

	p.tick(ctx, p.Resync)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
			p.tick(ctx, p.PollChanged)
		case <-resync.C:
			p.tick(ctx, p.Resync)
		}
	}
}

func (p *Poller) tick(ctx context.Context, f func(context.Context) (int, error)) {
	if _, err := f(ctx); err != nil && ctx.Err() == nil {
		p.errs.Inc()
		p.log.Error("poll failed; retrying next tick", "err", err)
	}
}

const changedSQL = `
SELECT now(), COALESCE(array_agg(id), '{}')
FROM instances
WHERE updated_at > $1::timestamptz - make_interval(secs => $2)`

const resyncSQL = `
SELECT now(), COALESCE(array_agg(id), '{}')
FROM instances
WHERE state NOT IN ('TERMINATED', 'FAILED')`

// PollChanged sends the ids updated since the previous poll (minus the
// overlap) and returns how many it sent. The first call, with no previous
// DB time, sends nothing but records now(); Start's initial resync covers it.
func (p *Poller) PollChanged(ctx context.Context) (int, error) {
	if !p.hasLast {
		var now time.Time
		if err := p.db.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
			return 0, fmt.Errorf("poll changed: read db time: %w", err)
		}
		p.last, p.hasLast = now, true
		return 0, nil
	}
	now, ids, err := p.query(ctx, changedSQL, p.last, p.cfg.Overlap.Seconds())
	if err != nil {
		return 0, fmt.Errorf("poll changed: %w", err)
	}
	n, err := p.send(ctx, ids, p.idsChanged)
	if err != nil {
		return n, err
	}
	p.last = now
	return n, nil
}

// Resync sends every non-terminal id and returns how many it sent. If no
// changed-poll has run yet, it also sets the starting DB time for it.
func (p *Poller) Resync(ctx context.Context) (int, error) {
	now, ids, err := p.query(ctx, resyncSQL)
	if err != nil {
		return 0, fmt.Errorf("resync: %w", err)
	}
	n, err := p.send(ctx, ids, p.idsResync)
	if err != nil {
		return n, err
	}
	if !p.hasLast {
		p.last, p.hasLast = now, true
	}
	return n, nil
}

func (p *Poller) query(ctx context.Context, sql string, args ...any) (time.Time, []string, error) {
	var now time.Time
	var ids []string
	if err := p.db.QueryRow(ctx, sql, args...).Scan(&now, &ids); err != nil {
		return time.Time{}, nil, err
	}
	return now, ids, nil
}

// errStopped is returned when ctx ends during a send.
var errStopped = errors.New("poller stopped")

// send blocks until each id is accepted or ctx ends. The channel is bounded;
// a full queue applies backpressure instead of dropping ids.
func (p *Poller) send(ctx context.Context, ids []string, c prometheus.Counter) (int, error) {
	for i, id := range ids {
		select {
		case p.out <- Event{Object: id}:
			c.Inc()
		case <-ctx.Done():
			return i, errStopped
		}
	}
	return len(ids), nil
}
