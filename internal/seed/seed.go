// Package seed fills the nodes table with fake GPU nodes. In M1 there is no
// node sync loop, so this is how placement gets inventory (ADR-0001 §Context
// "How M1 works without a controller"). The node sync loop replaces it as the
// inventory writer in M2.
//
// Column ownership (Discussion D15): the seed writes only the inventory
// columns gpu_type, capacity, schedulable, failure_domain and last_seen. It
// never writes allocated, which belongs to placement and release; rewriting
// it here could erase a reservation made since the seed last ran and allow
// over-allocation (invariant 1).
//
// Re-running is an upsert keyed on stable node IDs. Seeding fewer nodes than
// a previous run leaves the extra nodes untouched: the seed never deletes or
// disables a node (D15: a removed node keeps its row).
package seed

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dipen1510/gpu-control-plane/internal/migrate"
)

// Values every seeded node gets.
const (
	GPUsPerNode = 8
	// Racks is how many failure domains the nodes are spread over.
	Racks = 4
)

// ErrInvalidCount is returned when asked to seed fewer than one node.
var ErrInvalidCount = errors.New("node count must be at least 1")

// Node is the inventory part of a nodes row: exactly the columns the seed
// owns, apart from last_seen, which is set to the database's now().
type Node struct {
	ID            string
	GPUType       string
	Capacity      int
	Schedulable   bool
	FailureDomain string
}

// Nodes returns the n fake nodes, numbered 1..n. Every attribute depends only
// on the node's number, never on n, so seeding a different n never changes
// an existing node's type or rack (changing a node's gpu_type under running
// instances would be wrong). Odd numbers are H100 and even numbers A100, so
// for odd n there is one more H100 than A100. Consecutive pairs share a rack
// (nodes 1-2 in rack-0, 3-4 in rack-1, ...), so both GPU types are spread
// across every rack.
func Nodes(n int) ([]Node, error) {
	if n < 1 {
		return nil, fmt.Errorf("seed %d nodes: %w", n, ErrInvalidCount)
	}
	nodes := make([]Node, 0, n)
	for i := 1; i <= n; i++ {
		gpuType := "H100"
		if i%2 == 0 {
			gpuType = "A100"
		}
		nodes = append(nodes, Node{
			ID:            fmt.Sprintf("node-%04d", i),
			GPUType:       gpuType,
			Capacity:      GPUsPerNode,
			Schedulable:   true,
			FailureDomain: fmt.Sprintf("rack-%d", ((i-1)/2)%Racks),
		})
	}
	return nodes, nil
}

// Execer runs a statement. *pgxpool.Pool, *pgx.Conn and pgx.Tx satisfy it.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// upsertSQL inserts every node in one statement, so a run is all-or-nothing.
// The DO UPDATE SET list names only seed-owned columns; allocated must never
// appear in it (D15). A new row gets allocated's column default of 0.
const upsertSQL = `
INSERT INTO nodes (id, gpu_type, capacity, schedulable, failure_domain, last_seen)
SELECT id, gpu_type, capacity, schedulable, failure_domain, now()
FROM unnest($1::text[], $2::text[], $3::integer[], $4::boolean[], $5::text[])
    AS n(id, gpu_type, capacity, schedulable, failure_domain)
ON CONFLICT (id) DO UPDATE SET
    gpu_type       = EXCLUDED.gpu_type,
    capacity       = EXCLUDED.capacity,
    schedulable    = EXCLUDED.schedulable,
    failure_domain = EXCLUDED.failure_domain,
    last_seen      = EXCLUDED.last_seen`

// Upsert writes nodes to the nodes table, inserting new IDs and refreshing
// the inventory columns of existing ones.
func Upsert(ctx context.Context, db Execer, nodes []Node) error {
	ids := make([]string, len(nodes))
	types := make([]string, len(nodes))
	caps := make([]int32, len(nodes))
	sched := make([]bool, len(nodes))
	domains := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i], types[i], caps[i], sched[i], domains[i] =
			n.ID, n.GPUType, int32(n.Capacity), n.Schedulable, n.FailureDomain
	}
	if _, err := db.Exec(ctx, upsertSQL, ids, types, caps, sched, domains); err != nil {
		return fmt.Errorf("upsert %d nodes: %w", len(nodes), err)
	}
	return nil
}

// DB is what Run needs: transactions for the migrations and Exec for the
// upsert. *pgxpool.Pool and *pgx.Conn satisfy it.
type DB interface {
	migrate.DB
	Execer
}

// Run applies any pending migrations, then upserts n fake nodes. Migrating
// first makes `make seed-nodes` work on a fresh database; migrate.Up is a
// no-op when the schema is current and safe alongside other migrators.
func Run(ctx context.Context, db DB, n int) error {
	nodes, err := Nodes(n)
	if err != nil {
		return err
	}
	if err := migrate.Up(ctx, db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return Upsert(ctx, db, nodes)
}
