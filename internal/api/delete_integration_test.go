//go:build integration

package api

import (
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dipen1510/gpu-control-plane/internal/lifecycle"
	"github.com/dipen1510/gpu-control-plane/internal/scheduler"
	"github.com/dipen1510/gpu-control-plane/internal/seed"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
	"github.com/dipen1510/gpu-control-plane/internal/testutil"
)

// newServerWithStore is newServer that also returns the store, so a test
// can put an instance in any state with store.Transition.
func newServerWithStore(t *testing.T) (*httptest.Server, *pgxpool.Pool, *store.Store) {
	t.Helper()
	db := testutil.NewDB(t)
	nodes, err := seed.Nodes(4)
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := seed.Upsert(t.Context(), db, nodes); err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := store.New(db, scheduler.New(nil))
	srv := httptest.NewServer(NewHandler(st, lifecycle.New(st, log), log))
	t.Cleanup(srv.Close)
	return srv, db, st
}

// pathTo is the legal walk from SCHEDULED to each state, and the outbox
// events it writes (after instance.scheduled).
var pathTo = map[statemachine.State][]statemachine.State{
	statemachine.Scheduled:    nil,
	statemachine.Provisioning: {statemachine.Provisioning},
	statemachine.Running:      {statemachine.Provisioning, statemachine.Running},
	statemachine.Terminating:  {statemachine.Provisioning, statemachine.Running, statemachine.Terminating},
	statemachine.Terminated: {statemachine.Provisioning, statemachine.Running, statemachine.Terminating,
		statemachine.Terminated},
	statemachine.Failed: {statemachine.Failed},
}

// createAt commits a 4-GPU H100 instance for t1 straight through the store
// (no lifecycle walk) and moves it to state with store.Transition.
func createAt(t *testing.T, st *store.Store, id string, state statemachine.State) store.Instance {
	t.Helper()
	hash := sha256.Sum256([]byte(id))
	res, err := st.Create(t.Context(), store.CreateRequest{
		ID: id, TenantID: "t1", GPUType: "H100", GPUCount: 4,
		IdempotencyKey: "key-" + id, RequestHash: hash[:],
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	inst := res.Instance
	for _, to := range pathTo[state] {
		if inst, err = st.Transition(t.Context(), id, to, inst.ResourceVersion, "injected for test"); err != nil {
			t.Fatalf("move %s to %s: %v", id, to, err)
		}
	}
	return inst
}

// eventsFor is the outbox event types written walking SCHEDULED → state.
func eventsFor(state statemachine.State) []string {
	out := []string{"instance.scheduled"}
	for _, s := range pathTo[state] {
		out = append(out, "instance."+strings.ToLower(string(s)))
	}
	return out
}

// totals is what a release changes: GPUs allocated on all nodes, and t1's
// H100 quota used (0 before the quota row exists).
type totals struct{ allocated, used int }

func readTotals(t *testing.T, db *pgxpool.Pool) totals {
	t.Helper()
	var tt totals
	if err := db.QueryRow(t.Context(), `SELECT
		(SELECT COALESCE(sum(allocated), 0) FROM nodes),
		(SELECT COALESCE(sum(used), 0) FROM tenant_quota WHERE tenant_id = 't1' AND gpu_type = 'H100')`).
		Scan(&tt.allocated, &tt.used); err != nil {
		t.Fatalf("read totals: %v", err)
	}
	return tt
}

// deleteInstance sends DELETE as tenant, with If-Match unless it is "".
func deleteInstance(t *testing.T, srv *httptest.Server, tenant, id, ifMatch string) (*http.Response, []byte) {
	t.Helper()
	headers := map[string]string{headerTenant: tenant}
	if ifMatch != "" {
		headers["If-Match"] = ifMatch
	}
	return do(t, http.MethodDelete, srv.URL+"/v1/instances/"+id, headers, "")
}

// wantInstance checks a response with status and the instance in state at
// version rv, with the matching ETag.
func wantInstance(t *testing.T, resp *http.Response, b []byte, status int, state statemachine.State, rv int64) {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status %d body %s, want %d", resp.StatusCode, b, status)
	}
	var got instanceResponse
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decode body %s: %v", b, err)
	}
	if got.State != string(state) || got.ResourceVersion != rv {
		t.Errorf("body = %+v, want %s rv %d", got, state, rv)
	}
	if want := etagFor(rv); resp.Header.Get("ETag") != want {
		t.Errorf("ETag = %q, want %q", resp.Header.Get("ETag"), want)
	}
}

func wantErrorCode(t *testing.T, resp *http.Response, b []byte, status int, code string) {
	t.Helper()
	var e errorBody
	if resp.StatusCode != status || json.Unmarshal(b, &e) != nil || e.Error.Code != code {
		t.Fatalf("status %d body %s, want %d %s", resp.StatusCode, b, status, code)
	}
}

func getInstance(t *testing.T, srv *httptest.Server, tenant, id string) (*http.Response, []byte) {
	t.Helper()
	return do(t, http.MethodGet, srv.URL+"/v1/instances/"+id, map[string]string{headerTenant: tenant}, "")
}

// TestDeleteFromEachStateEndsTerminated: from each state that holds GPUs,
// DELETE answers 202 TERMINATING; a GET right after shows TERMINATED, the
// GPUs and quota are back to their pre-create values, and exactly one
// instance.terminated event was written.
func TestDeleteFromEachStateEndsTerminated(t *testing.T) {
	for _, start := range []statemachine.State{statemachine.Scheduled, statemachine.Provisioning, statemachine.Running} {
		t.Run(string(start), func(t *testing.T) {
			srv, db, st := newServerWithStore(t)
			before := readTotals(t, db)
			inst := createAt(t, st, "inst_del", start)
			if held := readTotals(t, db); held != (totals{before.allocated + 4, before.used + 4}) {
				t.Fatalf("while %s totals = %+v, want %+v plus 4 each", start, held, before)
			}

			resp, b := deleteInstance(t, srv, "t1", inst.ID, "")
			wantInstance(t, resp, b, http.StatusAccepted, statemachine.Terminating, inst.ResourceVersion+1)

			resp, b = getInstance(t, srv, "t1", inst.ID)
			wantInstance(t, resp, b, http.StatusOK, statemachine.Terminated, inst.ResourceVersion+2)

			if after := readTotals(t, db); after != before {
				t.Errorf("after delete totals = %+v, want pre-create %+v", after, before)
			}
			want := append(eventsFor(start), "instance.terminating", "instance.terminated")
			if got := outboxTypes(t, db, inst.ID); !slices.Equal(got, want) {
				t.Errorf("events = %v, want %v (one instance.terminated)", got, want)
			}
		})
	}
}

// TestDeleteStaleIfMatch: an If-Match that is not the current version is
// 409 stale_resource_version and changes nothing; the current one works.
func TestDeleteStaleIfMatch(t *testing.T) {
	srv, db := newServer(t)
	created := createInstance(t, srv, "t1", "k1", `{"gpu_type":"H100","gpu_count":4}`) // RUNNING rv 3 after the walk
	before := readWrites(t, db)

	for _, stale := range []string{`"1"`, `"2"`, `W/"3"`} {
		resp, b := deleteInstance(t, srv, "t1", created.ID, stale)
		wantErrorCode(t, resp, b, http.StatusConflict, codeStaleVersion)
	}
	assertNothingWritten(t, before, readWrites(t, db))
	if got := storedInstance(t, db, created.ID); got.State != "RUNNING" || got.ResourceVersion != 3 {
		t.Errorf("stored %+v, want RUNNING rv 3 (unchanged)", got)
	}

	resp, b := deleteInstance(t, srv, "t1", created.ID, `"3"`)
	wantInstance(t, resp, b, http.StatusAccepted, statemachine.Terminating, 4)
	if got := storedInstance(t, db, created.ID); got.State != "TERMINATED" || got.ResourceVersion != 5 {
		t.Errorf("stored %+v, want TERMINATED rv 5", got)
	}
}

// TestDeleteAlreadyTerminal: DELETE on a TERMINATED or FAILED instance is
// 202 with its current state and writes nothing (no outbox row, no release).
func TestDeleteAlreadyTerminal(t *testing.T) {
	for _, state := range []statemachine.State{statemachine.Terminated, statemachine.Failed} {
		t.Run(string(state), func(t *testing.T) {
			srv, db, st := newServerWithStore(t)
			inst := createAt(t, st, "inst_gone", state)
			before := readWrites(t, db)

			for range 2 {
				resp, b := deleteInstance(t, srv, "t1", inst.ID, "")
				wantInstance(t, resp, b, http.StatusAccepted, state, inst.ResourceVersion)
			}
			assertNothingWritten(t, before, readWrites(t, db))
			if got := outboxTypes(t, db, inst.ID); !slices.Equal(got, eventsFor(state)) {
				t.Errorf("events = %v, want %v", got, eventsFor(state))
			}
		})
	}
}

// TestDeleteResumesTerminating: an instance a crash left in TERMINATING
// (still holding its GPUs) is finished by DELETE: 202 TERMINATING at its
// current version, then TERMINATED with the GPUs released exactly once.
func TestDeleteResumesTerminating(t *testing.T) {
	srv, db, st := newServerWithStore(t)
	before := readTotals(t, db)
	inst := createAt(t, st, "inst_stuck", statemachine.Terminating) // rv 4
	if held := readTotals(t, db); held != (totals{before.allocated + 4, before.used + 4}) {
		t.Fatalf("while TERMINATING totals = %+v, want %+v plus 4 each", held, before)
	}

	resp, b := deleteInstance(t, srv, "t1", inst.ID, "")
	wantInstance(t, resp, b, http.StatusAccepted, statemachine.Terminating, 4)
	resp, b = getInstance(t, srv, "t1", inst.ID)
	wantInstance(t, resp, b, http.StatusOK, statemachine.Terminated, 5)
	if after := readTotals(t, db); after != before {
		t.Errorf("after delete totals = %+v, want pre-create %+v", after, before)
	}
	want := eventsFor(statemachine.Terminated) // one terminating, one terminated
	if got := outboxTypes(t, db, inst.ID); !slices.Equal(got, want) {
		t.Errorf("events = %v, want %v", got, want)
	}

	// Again: already TERMINATED, nothing is released twice.
	snapshot := readWrites(t, db)
	resp, b = deleteInstance(t, srv, "t1", inst.ID, "")
	wantInstance(t, resp, b, http.StatusAccepted, statemachine.Terminated, 5)
	assertNothingWritten(t, snapshot, readWrites(t, db))
}

// TestDeleteOtherTenantIs404: another tenant's instance and an unknown id
// are both 404 not_found (existence is not leaked), and nothing changes.
func TestDeleteOtherTenantIs404(t *testing.T) {
	srv, db := newServer(t)
	created := createInstance(t, srv, "t1", "k1", `{"gpu_type":"H100","gpu_count":2}`)
	before := readWrites(t, db)

	for _, tc := range []struct{ name, tenant, id string }{
		{name: "other tenant", tenant: "t2", id: created.ID},
		{name: "unknown id", tenant: "t1", id: "inst_doesnotexist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, b := deleteInstance(t, srv, tc.tenant, tc.id, "")
			wantErrorCode(t, resp, b, http.StatusNotFound, codeNotFound)
		})
	}
	assertNothingWritten(t, before, readWrites(t, db))
	if got := storedInstance(t, db, created.ID); got.State != "RUNNING" {
		t.Errorf("stored %+v, want RUNNING (untouched)", got)
	}
}

// TestDeleteFreesQuota: a tenant at its 16-GPU limit deletes everything and
// can then create 16 GPUs again.
func TestDeleteFreesQuota(t *testing.T) {
	srv, db := newServer(t)
	const body8 = `{"gpu_type":"H100","gpu_count":8}`
	a := createInstance(t, srv, "t1", "k1", body8)
	b := createInstance(t, srv, "t1", "k2", body8)
	if resp, e := postCreate(t, srv, "t1", "k3", `{"gpu_type":"H100","gpu_count":1}`); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("17th GPU: status %d error %+v, want 403", resp.StatusCode, e)
	}

	for _, inst := range []instanceResponse{a, b} {
		resp, body := deleteInstance(t, srv, "t1", inst.ID, "")
		wantInstance(t, resp, body, http.StatusAccepted, statemachine.Terminating, 4)
	}
	if got := readTotals(t, db); got != (totals{}) {
		t.Fatalf("after deleting everything totals = %+v, want zero", got)
	}

	createInstance(t, srv, "t1", "k4", body8)
	createInstance(t, srv, "t1", "k5", body8)
	if got := readTotals(t, db); got != (totals{allocated: 16, used: 16}) {
		t.Errorf("after re-creating totals = %+v, want 16 allocated, 16 used", got)
	}
}
