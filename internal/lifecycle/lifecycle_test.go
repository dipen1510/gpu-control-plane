package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// step is one Transition call as the fake saw it.
type step struct {
	to         statemachine.State
	expectedRV int64
}

// fakeStore succeeds on every call except number failAt (1-based), which
// returns failErr. A successful call bumps the version by one, like the
// real store.
type fakeStore struct {
	failAt  int
	failErr error
	calls   []step
}

func (f *fakeStore) Transition(_ context.Context, id string, to statemachine.State, expectedRV int64, _ string) (store.Instance, error) {
	f.calls = append(f.calls, step{to: to, expectedRV: expectedRV})
	if len(f.calls) == f.failAt {
		return store.Instance{}, f.failErr
	}
	return store.Instance{ID: id, State: to, ResourceVersion: expectedRV + 1}, nil
}

// newDriver returns a driver over fs whose JSON log goes to the returned buffer.
func newDriver(fs *fakeStore) (*Driver, *bytes.Buffer) {
	var logs bytes.Buffer
	return New(fs, slog.New(slog.NewJSONHandler(&logs, nil))), &logs
}

func TestDriverWalksPath(t *testing.T) {
	tests := []struct {
		name      string
		run       func(*Driver) (store.Instance, error)
		wantCalls []step
		wantState statemachine.State
		wantRV    int64
	}{
		{
			name: "AfterCreate",
			run: func(d *Driver) (store.Instance, error) {
				return d.AfterCreate(context.Background(), "inst_1", 1)
			},
			wantCalls: []step{{statemachine.Provisioning, 1}, {statemachine.Running, 2}},
			wantState: statemachine.Running, wantRV: 3,
		},
		{
			name: "Delete",
			run: func(d *Driver) (store.Instance, error) {
				return d.Delete(context.Background(), "inst_1", statemachine.Running, 3)
			},
			wantCalls: []step{{statemachine.Terminating, 3}, {statemachine.Terminated, 4}},
			wantState: statemachine.Terminated, wantRV: 5,
		},
		{
			name: "Delete from SCHEDULED",
			run: func(d *Driver) (store.Instance, error) {
				return d.Delete(context.Background(), "inst_1", statemachine.Scheduled, 1)
			},
			wantCalls: []step{{statemachine.Terminating, 1}, {statemachine.Terminated, 2}},
			wantState: statemachine.Terminated, wantRV: 3,
		},
		{
			// A walk left in TERMINATING (crash) resumes with the last step
			// only; asking for TERMINATING again would be an illegal move.
			name: "Delete resumes from TERMINATING",
			run: func(d *Driver) (store.Instance, error) {
				return d.Delete(context.Background(), "inst_1", statemachine.Terminating, 4)
			},
			wantCalls: []step{{statemachine.Terminated, 4}},
			wantState: statemachine.Terminated, wantRV: 5,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{}
			d, logs := newDriver(fs)
			got, err := tc.run(d)
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if got.State != tc.wantState || got.ResourceVersion != tc.wantRV {
				t.Errorf("result %s rv %d, want %s rv %d", got.State, got.ResourceVersion, tc.wantState, tc.wantRV)
			}
			if !slices.Equal(fs.calls, tc.wantCalls) {
				t.Errorf("Transition calls = %v, want %v (each step chaining the returned version)",
					fs.calls, tc.wantCalls)
			}
			if logs.Len() != 0 {
				t.Errorf("successful walk logged %q, want nothing", logs.String())
			}
		})
	}
}

// TestDriverStopsOnError: whichever step fails, and however, the driver logs
// it with the instance id, the state the instance was left in and the error,
// returns the last committed instance, and makes no further calls.
func TestDriverStopsOnError(t *testing.T) {
	injected := errors.New("injected: connection reset")
	stale := errors.Join(errors.New("transition: at version 9, expected 1"), store.ErrStaleVersion)

	type walkCase struct {
		name      string
		run       func(*Driver) (store.Instance, error)
		failAt    int
		wantCalls int
		wantLeft  string             // "state" logged
		wantLast  statemachine.State // state returned ("" = zero instance)
		wantTo    statemachine.State // step that failed
	}
	walks := []walkCase{
		{
			name: "AfterCreate step 1", failAt: 1, wantCalls: 1,
			run:      func(d *Driver) (store.Instance, error) { return d.AfterCreate(context.Background(), "inst_1", 1) },
			wantLeft: "SCHEDULED", wantLast: "", wantTo: statemachine.Provisioning,
		},
		{
			name: "AfterCreate step 2", failAt: 2, wantCalls: 2,
			run:      func(d *Driver) (store.Instance, error) { return d.AfterCreate(context.Background(), "inst_1", 1) },
			wantLeft: "PROVISIONING", wantLast: statemachine.Provisioning, wantTo: statemachine.Running,
		},
		{
			name: "Delete step 1", failAt: 1, wantCalls: 1,
			run: func(d *Driver) (store.Instance, error) {
				return d.Delete(context.Background(), "inst_1", statemachine.Running, 3)
			},
			wantLeft: "RUNNING", wantLast: "", wantTo: statemachine.Terminating,
		},
		{
			name: "Delete step 2", failAt: 2, wantCalls: 2,
			run: func(d *Driver) (store.Instance, error) {
				return d.Delete(context.Background(), "inst_1", statemachine.Running, 3)
			},
			wantLeft: "TERMINATING", wantLast: statemachine.Terminating, wantTo: statemachine.Terminated,
		},
		{
			name: "Delete resume step", failAt: 1, wantCalls: 1,
			run: func(d *Driver) (store.Instance, error) {
				return d.Delete(context.Background(), "inst_1", statemachine.Terminating, 4)
			},
			wantLeft: "TERMINATING", wantLast: "", wantTo: statemachine.Terminated,
		},
	}
	errs := []struct {
		name      string
		err       error
		wantLevel string
	}{
		{name: "injected error", err: injected, wantLevel: "ERROR"},
		{name: "stale version", err: stale, wantLevel: "INFO"},
	}

	for _, w := range walks {
		for _, e := range errs {
			t.Run(w.name+"/"+e.name, func(t *testing.T) {
				fs := &fakeStore{failAt: w.failAt, failErr: e.err}
				d, logs := newDriver(fs)
				got, err := w.run(d)

				if !errors.Is(err, e.err) {
					t.Errorf("err = %v, want it to wrap %v", err, e.err)
				}
				if len(fs.calls) != w.wantCalls {
					t.Errorf("Transition called %d times, want %d (no steps after the failure)",
						len(fs.calls), w.wantCalls)
				}
				if got.State != w.wantLast {
					t.Errorf("returned state %q, want last committed %q", got.State, w.wantLast)
				}

				lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
				if len(lines) != 1 {
					t.Fatalf("logged %d lines %q, want 1", len(lines), logs.String())
				}
				var rec map[string]any
				if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
					t.Fatalf("decode log %q: %v", lines[0], err)
				}
				if rec["level"] != e.wantLevel || rec["instance_id"] != "inst_1" ||
					rec["state"] != w.wantLeft || rec["to"] != string(w.wantTo) ||
					!strings.Contains(fmt.Sprint(rec["err"]), e.err.Error()) {
					t.Errorf("log = %v, want level %s instance_id inst_1 state %s to %s err %q",
						rec, e.wantLevel, w.wantLeft, w.wantTo, e.err)
				}
			})
		}
	}
}
