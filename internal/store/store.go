// Package store is the only code that writes instances. Every state change is
// one Postgres transaction that also writes its outbox event (invariant 3),
// so a committed change can never lose its event and a failed one leaves no
// trace (ADR-0001 §The create transaction).
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

// Sentinel errors callers branch on with errors.Is. The API maps them to HTTP
// status codes at the edge.
var (
	// ErrNotFound means no instance with that id exists for that tenant.
	// Another tenant's instance is also "not found", so ids don't leak.
	ErrNotFound = errors.New("instance not found")
	// ErrQuotaExceeded means the tenant has no quota left for the GPU type.
	// Retrying will not help until the tenant frees GPUs (Discussion D2).
	ErrQuotaExceeded = errors.New("quota exceeded")
	// ErrInsufficientCapacity is scheduler.ErrInsufficientCapacity, re-exported
	// so callers of the store need not import the scheduler. It is the same
	// value, so errors.Is matches either name.
	ErrInsufficientCapacity = scheduler.ErrInsufficientCapacity
)

// EventInstanceScheduled is the outbox event type written by Create.
const EventInstanceScheduled = "instance.scheduled"

// DB is what the store needs from Postgres. *pgxpool.Pool satisfies it.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Placer reserves GPUs on a node inside the caller's transaction.
// *scheduler.Scheduler satisfies it.
type Placer interface {
	Place(ctx context.Context, tx pgx.Tx, req scheduler.Request) (scheduler.NodeID, error)
}

// Store reads and writes instances. It holds no per-request state and is
// safe for concurrent use; correctness across replicas comes from row locks
// and conditional updates in Postgres, not from anything in here.
type Store struct {
	db     DB
	placer Placer

	// afterPlace, if set, runs inside the create transaction right after
	// Place succeeds. Tests use it to inject a failure there; production
	// leaves it nil.
	afterPlace func(ctx context.Context, tx pgx.Tx) error
}

// New returns a Store that uses db and places instances with placer.
func New(db DB, placer Placer) *Store {
	return &Store{db: db, placer: placer}
}

// Instance is one row of the instances table.
type Instance struct {
	ID              string
	TenantID        string
	GPUType         string
	GPUCount        int
	NodeID          string // empty until placed (PENDING, v2 only)
	State           statemachine.State
	ResourceVersion int64
	FailureReason   string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// CreateRequest is a validated create. The caller generates ID before the
// transaction (ADR-0001: "generate the instance id" happens before BEGIN).
type CreateRequest struct {
	ID       string
	TenantID string
	GPUType  string
	GPUCount int

	// IdempotencyKey and RequestHash are carried for S1-8, which inserts
	// them into idempotency_keys as step 1 of the same transaction. Create
	// does not use them yet.
	IdempotencyKey string
	RequestHash    []byte
}

// Create runs the create transaction (ADR-0001 §The create transaction):
//
//  1. (S1-8) idempotency key
//  2. reserve quota: tenant_quota row, created at the default limit if missing,
//     used += n only if it stays within quota_limit
//  3. scheduler.Place: reserve n GPUs on one node
//  4. INSERT the instance in SCHEDULED with resource_version 1
//  5. INSERT the outbox event instance.scheduled
//
// Locks are taken in the documented order (idempotency_keys → tenant_quota →
// nodes → instances → outbox) so two creates can never deadlock. Any error
// rolls the whole transaction back: no quota, no GPUs, no row, no event.
//
// It returns ErrQuotaExceeded or ErrInsufficientCapacity (wrapped) when the
// request cannot be satisfied.
func (s *Store) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Instance{}, fmt.Errorf("create instance %s: begin: %w", req.ID, err)
	}
	// Rollback after a successful Commit returns ErrTxClosed; nothing to undo.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Step 1 (S1-8) goes here: INSERT INTO idempotency_keys first, so a
	// duplicate stops before it locks quota or nodes.

	if err := reserveQuota(ctx, tx, req.TenantID, req.GPUType, req.GPUCount); err != nil {
		return Instance{}, fmt.Errorf("create instance %s: %w", req.ID, err)
	}

	node, err := s.placer.Place(ctx, tx, scheduler.Request{
		TenantID: req.TenantID, GPUType: req.GPUType, GPUCount: req.GPUCount,
	})
	if err != nil {
		return Instance{}, fmt.Errorf("create instance %s: %w", req.ID, err)
	}

	if s.afterPlace != nil {
		if err := s.afterPlace(ctx, tx); err != nil {
			return Instance{}, fmt.Errorf("create instance %s: %w", req.ID, err)
		}
	}

	inst := Instance{
		ID: req.ID, TenantID: req.TenantID, GPUType: req.GPUType, GPUCount: req.GPUCount,
		NodeID: string(node), State: statemachine.Scheduled, ResourceVersion: 1,
	}
	if err := tx.QueryRow(ctx, insertInstanceSQL,
		inst.ID, inst.TenantID, inst.GPUType, inst.GPUCount, inst.NodeID,
		string(inst.State), inst.ResourceVersion,
	).Scan(&inst.CreatedAt, &inst.UpdatedAt); err != nil {
		return Instance{}, fmt.Errorf("create instance %s: insert instance: %w", req.ID, err)
	}

	if err := insertOutbox(ctx, tx, EventInstanceScheduled, inst); err != nil {
		return Instance{}, fmt.Errorf("create instance %s: %w", req.ID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Instance{}, fmt.Errorf("create instance %s: commit: %w", req.ID, err)
	}
	return inst, nil
}

// Get returns the instance with id owned by tenantID, or ErrNotFound.
func (s *Store) Get(ctx context.Context, tenantID, id string) (Instance, error) {
	var inst Instance
	var state string
	err := s.db.QueryRow(ctx, getInstanceSQL, id, tenantID).Scan(
		&inst.ID, &inst.TenantID, &inst.GPUType, &inst.GPUCount, &inst.NodeID, &state,
		&inst.ResourceVersion, &inst.FailureReason, &inst.CreatedAt, &inst.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Instance{}, fmt.Errorf("get instance %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return Instance{}, fmt.Errorf("get instance %s: %w", id, err)
	}
	inst.State = statemachine.State(state)
	return inst, nil
}

// ensureQuotaSQL creates the tenant's quota row on first use; quota_limit
// takes its column default (16, Discussion D2). If a concurrent create is
// inserting the same row, this waits for it to commit and then does nothing.
const ensureQuotaSQL = `
INSERT INTO tenant_quota (tenant_id, gpu_type) VALUES ($1, $2)
ON CONFLICT (tenant_id, gpu_type) DO NOTHING`

// reserveQuotaSQL is check-and-reserve in one statement. Under READ
// COMMITTED a concurrent create of the same tenant waits for this row lock
// and then re-checks the WHERE against the new used, so two creates can never
// both pass a check against the same old value.
const reserveQuotaSQL = `
UPDATE tenant_quota SET used = used + $3
WHERE tenant_id = $1 AND gpu_type = $2 AND used + $3 <= quota_limit`

func reserveQuota(ctx context.Context, tx pgx.Tx, tenantID, gpuType string, n int) error {
	if _, err := tx.Exec(ctx, ensureQuotaSQL, tenantID, gpuType); err != nil {
		return fmt.Errorf("ensure quota row: %w", err)
	}
	tag, err := tx.Exec(ctx, reserveQuotaSQL, tenantID, gpuType, n)
	if err != nil {
		return fmt.Errorf("reserve quota: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("reserve %d %s GPUs for tenant %s: %w", n, gpuType, tenantID, ErrQuotaExceeded)
	}
	return nil
}

const insertInstanceSQL = `
INSERT INTO instances (id, tenant_id, gpu_type, gpu_count, node_id, state, resource_version)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING created_at, updated_at`

// getInstanceSQL is scoped by tenant so one tenant can never read another's
// instance, even by guessing its id.
const getInstanceSQL = `
SELECT id, tenant_id, gpu_type, gpu_count, COALESCE(node_id, ''), state,
       resource_version, failure_reason, created_at, updated_at
FROM instances WHERE id = $1 AND tenant_id = $2`

const insertOutboxSQL = `
INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
VALUES ($1, $2, $3, $4)`

// eventPayload is the outbox payload for instance events: the instance as of
// the change, so a consumer needs no extra read.
type eventPayload struct {
	ID              string `json:"id"`
	TenantID        string `json:"tenant_id"`
	GPUType         string `json:"gpu_type"`
	GPUCount        int    `json:"gpu_count"`
	NodeID          string `json:"node_id"`
	State           string `json:"state"`
	ResourceVersion int64  `json:"resource_version"`
}

// insertOutbox writes one event for inst inside tx. The event_id is generated
// here, not by the database, so it is known before commit and consumers can
// dedupe on it (invariant 3).
func insertOutbox(ctx context.Context, tx pgx.Tx, eventType string, inst Instance) error {
	payload, err := json.Marshal(eventPayload{
		ID: inst.ID, TenantID: inst.TenantID, GPUType: inst.GPUType, GPUCount: inst.GPUCount,
		NodeID: inst.NodeID, State: string(inst.State), ResourceVersion: inst.ResourceVersion,
	})
	if err != nil {
		return fmt.Errorf("encode %s event: %w", eventType, err)
	}
	if _, err := tx.Exec(ctx, insertOutboxSQL, newEventID(), inst.ID, eventType, string(payload)); err != nil {
		return fmt.Errorf("insert %s event: %w", eventType, err)
	}
	return nil
}

// newEventID returns a random (version 4) UUID in its canonical text form.
func newEventID() string {
	var b [16]byte
	// Since Go 1.24 crypto/rand.Read never returns an error (it crashes the
	// program instead), so there is nothing to handle.
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
