package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
)

func TestEventType(t *testing.T) {
	tests := []struct {
		state statemachine.State
		want  string
	}{
		{statemachine.Scheduled, "instance.scheduled"},
		{statemachine.Provisioning, "instance.provisioning"},
		{statemachine.Running, "instance.running"},
		{statemachine.Terminating, "instance.terminating"},
		{statemachine.Terminated, "instance.terminated"},
		{statemachine.Failed, "instance.failed"},
	}
	for _, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := eventType(tc.state); got != tc.want {
				t.Errorf("eventType(%s) = %q, want %q", tc.state, got, tc.want)
			}
		})
	}
	// Create's event must be the same name Transition would use.
	if got := eventType(statemachine.Scheduled); got != EventInstanceScheduled {
		t.Errorf("eventType(SCHEDULED) = %q, want EventInstanceScheduled %q", got, EventInstanceScheduled)
	}
}

// readOnlyDB serves the unlocked read from a fixed instance (or no rows)
// and records whether a transaction was ever started.
type readOnlyDB struct {
	inst    *Instance
	begunTx bool
}

func (d *readOnlyDB) Begin(context.Context) (pgx.Tx, error) {
	d.begunTx = true
	return nil, errors.New("readOnlyDB: Begin not allowed")
}

func (d *readOnlyDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return instanceRow{inst: d.inst}
}

type instanceRow struct{ inst *Instance }

func (r instanceRow) Scan(dest ...any) error {
	if r.inst == nil {
		return pgx.ErrNoRows
	}
	// Column order of getInstanceByIDSQL.
	*dest[0].(*string) = r.inst.ID
	*dest[1].(*string) = r.inst.TenantID
	*dest[2].(*string) = r.inst.GPUType
	*dest[3].(*int) = r.inst.GPUCount
	*dest[4].(*string) = r.inst.NodeID
	*dest[5].(*string) = string(r.inst.State)
	*dest[6].(*int64) = r.inst.ResourceVersion
	*dest[7].(*string) = r.inst.FailureReason
	*dest[8].(*time.Time) = r.inst.CreatedAt
	*dest[9].(*time.Time) = r.inst.UpdatedAt
	return nil
}

// TestTransitionRejectsWithoutWriting checks that requests the unlocked read
// already rules out never open a transaction, so they take no locks and
// cannot write anything.
func TestTransitionRejectsWithoutWriting(t *testing.T) {
	running := &Instance{
		ID: "inst_1", TenantID: "t1", GPUType: "H100", GPUCount: 2, NodeID: "n1",
		State: statemachine.Running, ResourceVersion: 3,
	}
	terminated := &Instance{
		ID: "inst_2", TenantID: "t1", GPUType: "H100", GPUCount: 2, NodeID: "n1",
		State: statemachine.Terminated, ResourceVersion: 5,
	}
	tests := []struct {
		name    string
		inst    *Instance
		to      statemachine.State
		rv      int64
		wantErr error
	}{
		{name: "unknown id", inst: nil, to: statemachine.Terminating, rv: 1, wantErr: ErrNotFound},
		// A retry of PROVISIONING → RUNNING sent with the version it read
		// before the first attempt applied: the version is checked first.
		{name: "repeat", inst: running, to: statemachine.Running, rv: 2, wantErr: ErrStaleVersion},
		{name: "self at current version", inst: running, to: statemachine.Running, rv: 3, wantErr: ErrInvalidTransition},
		{name: "illegal and stale", inst: terminated, to: statemachine.Terminating, rv: 4, wantErr: ErrStaleVersion},
		{name: "backward", inst: running, to: statemachine.Provisioning, rv: 3, wantErr: ErrInvalidTransition},
		{name: "out of terminal", inst: terminated, to: statemachine.Terminating, rv: 5, wantErr: ErrInvalidTransition},
		{name: "stale version", inst: running, to: statemachine.Terminating, rv: 2, wantErr: ErrStaleVersion},
		{name: "future version", inst: running, to: statemachine.Terminating, rv: 4, wantErr: ErrStaleVersion},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &readOnlyDB{inst: tc.inst}
			s := New(db, nil)
			_, err := s.Transition(t.Context(), "inst_x", tc.to, tc.rv, "")
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("error = %v, want %v", err, tc.wantErr)
			}
			if db.begunTx {
				t.Errorf("a transaction was started for a request the read already rejects")
			}
		})
	}
}
