package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

var errStale = errors.Join(errors.New("transition: at version 9, expected 3"), store.ErrStaleVersion)

func instanceAt(state statemachine.State, rv int64) store.Instance {
	return store.Instance{ID: "inst_x", TenantID: "t1", GPUType: "H100", GPUCount: 4,
		NodeID: "node-0001", State: state, ResourceVersion: rv}
}

// serveDelete sends DELETE /v1/instances/inst_x with tenant t1 (unless
// headers set it) and the given headers.
func serveDelete(fs *fakeStore, lc *fakeLifecycle, headers map[string]string) *httptest.ResponseRecorder {
	return serveDeleteCtx(context.Background(), fs, lc, headers)
}

func serveDeleteCtx(ctx context.Context, fs *fakeStore, lc *fakeLifecycle, headers map[string]string) *httptest.ResponseRecorder {
	rec, _ := serveDeleteLogged(ctx, fs, lc, headers)
	return rec
}

// serveDeleteLogged is serveDeleteCtx that also returns the handler's JSON log.
func serveDeleteLogged(ctx context.Context, fs *fakeStore, lc *fakeLifecycle, headers map[string]string) (*httptest.ResponseRecorder, *bytes.Buffer) {
	var logs bytes.Buffer
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete, "/v1/instances/inst_x", nil)
	req.Header.Set(headerTenant, "t1")
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	NewHandler(fs, lc, slog.New(slog.NewJSONHandler(&logs, nil))).ServeHTTP(rec, req)
	return rec, &logs
}

// assertAccepted checks a 202 with the instance in state at version rv,
// and the matching ETag.
func assertAccepted(t *testing.T, rec *httptest.ResponseRecorder, state statemachine.State, rv int64) {
	t.Helper()
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	var got instanceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body %s: %v", rec.Body.String(), err)
	}
	if got.ID != "inst_x" || got.State != string(state) || got.ResourceVersion != rv {
		t.Errorf("body = %+v, want inst_x %s rv %d", got, state, rv)
	}
	if want := etagFor(rv); rec.Header().Get("ETag") != want {
		t.Errorf("ETag = %q, want %q", rec.Header().Get("ETag"), want)
	}
}

func assertError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body.String())
	}
	if got := decodeError(t, rec); got.Code != code || got.Message == "" {
		t.Errorf("error = %+v, want code %q and a message", got, code)
	}
}

func deleteSteps(lc *fakeLifecycle) []deleteCall {
	out := make([]deleteCall, len(lc.deleteCalls))
	for i, c := range lc.deleteCalls {
		out[i] = deleteCall{id: c.id, from: c.from, rv: c.rv}
	}
	return out
}

// TestDeleteHeaderValidation: X-Tenant-ID follows the same rules as for GET
// and POST, and If-Match must be "*" or a list of quoted ETags. A bad header
// is 400 before the store or the lifecycle is called, and is not echoed.
func TestDeleteHeaderValidation(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantStatus int
	}{
		{name: "valid", wantStatus: http.StatusAccepted},
		{name: "missing tenant", headers: map[string]string{headerTenant: ""}, wantStatus: http.StatusBadRequest},
		{name: "tenant at 64 chars", headers: map[string]string{headerTenant: strings.Repeat("t", 64)}, wantStatus: http.StatusAccepted},
		{name: "tenant at 65 chars", headers: map[string]string{headerTenant: strings.Repeat("t", 65)}, wantStatus: http.StatusBadRequest},
		{name: "tenant with space", headers: map[string]string{headerTenant: "t 1"}, wantStatus: http.StatusBadRequest},
		{name: "tenant with control char", headers: map[string]string{headerTenant: "t\x01"}, wantStatus: http.StatusBadRequest},
		{name: "tenant non-ASCII", headers: map[string]string{headerTenant: "té"}, wantStatus: http.StatusBadRequest},
		{name: "If-Match quoted", headers: map[string]string{"If-Match": `"3"`}, wantStatus: http.StatusAccepted},
		{name: "If-Match star", headers: map[string]string{"If-Match": `*`}, wantStatus: http.StatusAccepted},
		{name: "If-Match list", headers: map[string]string{"If-Match": `W/"3", "3"`}, wantStatus: http.StatusAccepted},
		{name: "If-Match unquoted", headers: map[string]string{"If-Match": `3`}, wantStatus: http.StatusBadRequest},
		{name: "If-Match half quoted", headers: map[string]string{"If-Match": `"3`}, wantStatus: http.StatusBadRequest},
		{name: "If-Match weak unquoted", headers: map[string]string{"If-Match": `W/3`}, wantStatus: http.StatusBadRequest},
		{name: "If-Match star in list", headers: map[string]string{"If-Match": `*, "3"`}, wantStatus: http.StatusBadRequest},
		{name: "If-Match empty list entry", headers: map[string]string{"If-Match": `"3",`}, wantStatus: http.StatusBadRequest},
		{name: "If-Match inner quote", headers: map[string]string{"If-Match": `"3"secret"`}, wantStatus: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{getInst: instanceAt(statemachine.Running, 3)}
			lc := &fakeLifecycle{}
			rec := serveDelete(fs, lc, tc.headers)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusAccepted {
				if len(lc.deleteCalls) != 1 {
					t.Errorf("lifecycle Delete called %d times, want 1", len(lc.deleteCalls))
				}
				return
			}
			if got := decodeError(t, rec); got.Code != codeInvalidRequest || got.Message == "" {
				t.Errorf("error = %+v, want code %q and a message", got, codeInvalidRequest)
			}
			if strings.Contains(rec.Body.String(), "secret") {
				t.Errorf("body %s echoes the header value", rec.Body.String())
			}
			if fs.getCalls != 0 || len(lc.deleteCalls) != 0 {
				t.Errorf("Get called %d times, Delete %d times on a bad request; want 0, 0",
					fs.getCalls, len(lc.deleteCalls))
			}
		})
	}
}

// TestDeleteResponse: the handler asks the lifecycle to delete the instance
// from the state and version it read, on a context the client hanging up
// does not cancel but that has a deadline, and answers 202 with the
// instance at TERMINATING (the version before the final TERMINATED).
func TestDeleteResponse(t *testing.T) {
	fs := &fakeStore{getInst: instanceAt(statemachine.Running, 3)}
	lc := &fakeLifecycle{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // the client is gone before the walk starts; the fake store ignores ctx
	rec := serveDeleteCtx(ctx, fs, lc, nil)

	assertAccepted(t, rec, statemachine.Terminating, 4)
	if len(lc.deleteCalls) != 1 {
		t.Fatalf("Delete called %d times, want 1", len(lc.deleteCalls))
	}
	call := lc.deleteCalls[0]
	if call.id != "inst_x" || call.from != statemachine.Running || call.rv != 3 {
		t.Errorf("Delete(%s, %s, %d), want (inst_x, RUNNING, 3)", call.id, call.from, call.rv)
	}
	if call.ctxErr != nil || !call.hasDeadline {
		t.Errorf("Delete ctx err %v, deadline %v; want not canceled by the client, with a deadline",
			call.ctxErr, call.hasDeadline)
	}
}

// TestDeleteIfMatch: If-Match is compared strongly with the current
// version; a mismatch is 409 stale_resource_version without calling the
// lifecycle.
func TestDeleteIfMatch(t *testing.T) {
	tests := []struct {
		ifMatch   string
		wantCalls int
	}{
		{ifMatch: "", wantCalls: 1},
		{ifMatch: `"3"`, wantCalls: 1},
		{ifMatch: `"1", "3"`, wantCalls: 1},
		{ifMatch: `*`, wantCalls: 1},
		{ifMatch: `"2"`, wantCalls: 0},
		{ifMatch: `"33"`, wantCalls: 0},
		{ifMatch: `W/"3"`, wantCalls: 0}, // weak never matches If-Match (RFC 9110 §13.1.1)
		{ifMatch: `"1", W/"3"`, wantCalls: 0},
	}
	for _, tc := range tests {
		t.Run(tc.ifMatch, func(t *testing.T) {
			fs := &fakeStore{getInst: instanceAt(statemachine.Running, 3)}
			lc := &fakeLifecycle{}
			rec := serveDelete(fs, lc, map[string]string{"If-Match": tc.ifMatch})
			if tc.wantCalls == 1 {
				assertAccepted(t, rec, statemachine.Terminating, 4)
			} else {
				assertError(t, rec, http.StatusConflict, codeStaleVersion)
			}
			if len(lc.deleteCalls) != tc.wantCalls {
				t.Errorf("Delete called %d times, want %d", len(lc.deleteCalls), tc.wantCalls)
			}
		})
	}
}

// TestDeleteWithoutIfMatchRetries: losing a race without If-Match re-reads
// and resumes from the state the instance has now; after 1 + deleteRetries
// lost attempts it gives up with 409. With If-Match it never retries.
func TestDeleteWithoutIfMatchRetries(t *testing.T) {
	stale := deleteResult{err: errStale}
	tests := []struct {
		name      string
		ifMatch   string
		reads     []store.Instance
		results   []deleteResult
		wantCalls []deleteCall
		wantGets  int
		// 202 with wantState/wantRV, or 409 stale if wantStale.
		wantStale bool
		wantState statemachine.State
		wantRV    int64
	}{
		{
			// The create walk moved PROVISIONING → RUNNING after our read.
			name:      "stale once then succeeds from the new state",
			reads:     []store.Instance{instanceAt(statemachine.Provisioning, 2), instanceAt(statemachine.Running, 3)},
			results:   []deleteResult{stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Provisioning, rv: 2}, {id: "inst_x", from: statemachine.Running, rv: 3}},
			wantGets:  2, wantState: statemachine.Terminating, wantRV: 4,
		},
		{
			// Another walk committed TERMINATING and stopped: resume it, do
			// not ask for TERMINATING again.
			name:      "re-read finds TERMINATING and resumes",
			reads:     []store.Instance{instanceAt(statemachine.Running, 3), instanceAt(statemachine.Terminating, 4)},
			results:   []deleteResult{stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Running, rv: 3}, {id: "inst_x", from: statemachine.Terminating, rv: 4}},
			wantGets:  2, wantState: statemachine.Terminating, wantRV: 4,
		},
		{
			// A concurrent DELETE won and finished: 202 with what it left.
			name:      "re-read finds TERMINATED",
			reads:     []store.Instance{instanceAt(statemachine.Running, 3), instanceAt(statemachine.Terminated, 5)},
			results:   []deleteResult{stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Running, rv: 3}},
			wantGets:  2, wantState: statemachine.Terminated, wantRV: 5,
		},
		{
			name: "gives up after 3 retries",
			reads: []store.Instance{instanceAt(statemachine.Scheduled, 1), instanceAt(statemachine.Provisioning, 2),
				instanceAt(statemachine.Running, 3), instanceAt(statemachine.Running, 9)},
			results: []deleteResult{stale, stale, stale, stale, {}},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Scheduled, rv: 1},
				{id: "inst_x", from: statemachine.Provisioning, rv: 2}, {id: "inst_x", from: statemachine.Running, rv: 3},
				{id: "inst_x", from: statemachine.Running, rv: 9}},
			wantGets: 5, wantStale: true, // the 5th read still shows RUNNING
		},
		{
			// A concurrent DELETE finished the instance while we kept
			// losing: the last re-read shows TERMINATED, so 202, not 409.
			name: "retries exhausted but the final re-read is TERMINATED",
			reads: []store.Instance{instanceAt(statemachine.Scheduled, 1), instanceAt(statemachine.Provisioning, 2),
				instanceAt(statemachine.Running, 3), instanceAt(statemachine.Terminating, 4),
				instanceAt(statemachine.Terminated, 5)},
			results: []deleteResult{stale, stale, stale, stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Scheduled, rv: 1},
				{id: "inst_x", from: statemachine.Provisioning, rv: 2}, {id: "inst_x", from: statemachine.Running, rv: 3},
				{id: "inst_x", from: statemachine.Terminating, rv: 4}},
			wantGets: 5, wantState: statemachine.Terminated, wantRV: 5,
		},
		{
			// Resuming TERMINATING loses to a concurrent DELETE that
			// released it: re-read shows TERMINATED → 202.
			name:      "resume step stale and re-read is TERMINATED",
			reads:     []store.Instance{instanceAt(statemachine.Terminating, 4), instanceAt(statemachine.Terminated, 5)},
			results:   []deleteResult{stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Terminating, rv: 4}},
			wantGets:  2, wantState: statemachine.Terminated, wantRV: 5,
		},
		{
			// With If-Match the client asked for version 4 exactly; it is
			// gone, so 409 even though the instance ended TERMINATED.
			name:      "If-Match: resume step stale is 409",
			ifMatch:   `"4"`,
			reads:     []store.Instance{instanceAt(statemachine.Terminating, 4), instanceAt(statemachine.Terminated, 5)},
			results:   []deleteResult{stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Terminating, rv: 4}},
			wantGets:  1, wantStale: true,
		},
		{
			name:      "If-Match: stale walk is 409 at once",
			ifMatch:   `"3"`,
			reads:     []store.Instance{instanceAt(statemachine.Running, 3), instanceAt(statemachine.Running, 9)},
			results:   []deleteResult{stale},
			wantCalls: []deleteCall{{id: "inst_x", from: statemachine.Running, rv: 3}},
			wantGets:  1, wantStale: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{getSeq: tc.reads, getInst: instanceAt(statemachine.Running, 9)}
			lc := &fakeLifecycle{deleteResults: tc.results}
			rec := serveDelete(fs, lc, map[string]string{"If-Match": tc.ifMatch})
			if tc.wantStale {
				assertError(t, rec, http.StatusConflict, codeStaleVersion)
			} else {
				assertAccepted(t, rec, tc.wantState, tc.wantRV)
			}
			if got := deleteSteps(lc); !slices.Equal(got, tc.wantCalls) {
				t.Errorf("Delete calls = %+v, want %+v", got, tc.wantCalls)
			}
			if fs.getCalls != tc.wantGets {
				t.Errorf("Get called %d times, want %d", fs.getCalls, tc.wantGets)
			}
		})
	}
}

// TestDeleteStep2FailureIs202: once TERMINATING is committed, the delete is
// accepted whatever went wrong after it: 202, never 409 or 500, and no new
// walk (a retry would ask for TERMINATING again). If the release step lost
// to a concurrent DELETE, the instance as re-read is the body and the log
// says so at Info; a real error leaves it TERMINATING and logs a Warn.
func TestDeleteStep2FailureIs202(t *testing.T) {
	boom := errors.New("injected: connection reset")
	terminating4 := instanceAt(statemachine.Terminating, 4)
	tests := []struct {
		name      string
		reads     []store.Instance
		ifMatch   string
		result    deleteResult
		wantState statemachine.State
		wantRV    int64
		wantGets  int
		wantStuck bool // Warn "stuck in TERMINATING" logged
	}{
		{name: "stale after TERMINATING, finished elsewhere",
			reads:  []store.Instance{instanceAt(statemachine.Running, 3), instanceAt(statemachine.Terminated, 5)},
			result: deleteResult{inst: terminating4, err: errStale}, wantState: statemachine.Terminated, wantRV: 5, wantGets: 2},
		{name: "stale after TERMINATING with If-Match, finished elsewhere", ifMatch: `"3"`,
			reads:  []store.Instance{instanceAt(statemachine.Running, 3), instanceAt(statemachine.Terminated, 5)},
			result: deleteResult{inst: terminating4, err: errStale}, wantState: statemachine.Terminated, wantRV: 5, wantGets: 2},
		{name: "stale after TERMINATING, re-read not terminal",
			reads:  []store.Instance{instanceAt(statemachine.Running, 3), terminating4},
			result: deleteResult{inst: terminating4, err: errStale}, wantState: statemachine.Terminating, wantRV: 4, wantGets: 2},
		{name: "error after TERMINATING",
			reads:  []store.Instance{instanceAt(statemachine.Running, 3)},
			result: deleteResult{inst: terminating4, err: boom}, wantState: statemachine.Terminating, wantRV: 4, wantGets: 1,
			wantStuck: true},
		{name: "resume fails",
			reads:  []store.Instance{terminating4},
			result: deleteResult{err: boom}, wantState: statemachine.Terminating, wantRV: 4, wantGets: 1, wantStuck: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{getSeq: tc.reads, getInst: instanceAt(statemachine.Running, 99)}
			lc := &fakeLifecycle{deleteResults: []deleteResult{tc.result}}
			rec, logs := serveDeleteLogged(context.Background(), fs, lc, map[string]string{"If-Match": tc.ifMatch})
			assertAccepted(t, rec, tc.wantState, tc.wantRV)
			if len(lc.deleteCalls) != 1 || fs.getCalls != tc.wantGets {
				t.Errorf("Delete called %d times, Get %d times; want 1, %d (no new walk)",
					len(lc.deleteCalls), fs.getCalls, tc.wantGets)
			}
			stuck := strings.Contains(logs.String(), "stuck in TERMINATING")
			concurrent := strings.Contains(logs.String(), "delete finished by a concurrent request")
			if stuck != tc.wantStuck || concurrent == tc.wantStuck {
				t.Errorf("logged stuck=%v concurrent=%v, want stuck=%v concurrent=%v; log:\n%s",
					stuck, concurrent, tc.wantStuck, !tc.wantStuck, logs.String())
			}
		})
	}
}

// TestDeleteAlreadyTerminalUnit: TERMINATED and FAILED are answered 202 with
// the current state without calling the lifecycle, even with a stale
// If-Match (the instance is gone either way).
func TestDeleteAlreadyTerminalUnit(t *testing.T) {
	for _, state := range []statemachine.State{statemachine.Terminated, statemachine.Failed} {
		for _, ifMatch := range []string{"", `"1"`} {
			t.Run(string(state)+" If-Match "+ifMatch, func(t *testing.T) {
				fs := &fakeStore{getInst: instanceAt(state, 5)}
				lc := &fakeLifecycle{}
				rec := serveDelete(fs, lc, map[string]string{"If-Match": ifMatch})
				assertAccepted(t, rec, state, 5)
				if len(lc.deleteCalls) != 0 {
					t.Errorf("Delete called %d times, want 0", len(lc.deleteCalls))
				}
			})
		}
	}
}

// TestDeleteErrorMapping: domain errors map to their codes; anything else is
// a 500 that does not leak the underlying error.
func TestDeleteErrorMapping(t *testing.T) {
	secret := errors.New("pq: connection to 10.0.0.5 refused")
	tests := []struct {
		name       string
		fs         *fakeStore
		result     *deleteResult
		wantStatus int
		wantCode   string
	}{
		{name: "unknown or other tenant", fs: &fakeStore{getErr: store.ErrNotFound},
			wantStatus: http.StatusNotFound, wantCode: codeNotFound},
		{name: "read fails", fs: &fakeStore{getErr: secret},
			wantStatus: http.StatusInternalServerError, wantCode: codeInternal},
		{name: "invalid transition", fs: &fakeStore{getInst: instanceAt(statemachine.Pending, 1)},
			result:     &deleteResult{err: store.ErrInvalidTransition},
			wantStatus: http.StatusConflict, wantCode: codeInvalidTransition},
		{name: "walk finds no instance", fs: &fakeStore{getInst: instanceAt(statemachine.Running, 3)},
			result:     &deleteResult{err: store.ErrNotFound},
			wantStatus: http.StatusNotFound, wantCode: codeNotFound},
		{name: "first step fails", fs: &fakeStore{getInst: instanceAt(statemachine.Running, 3)},
			result:     &deleteResult{err: secret},
			wantStatus: http.StatusInternalServerError, wantCode: codeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lc := &fakeLifecycle{}
			if tc.result != nil {
				lc.deleteResults = []deleteResult{*tc.result}
			}
			rec := serveDelete(tc.fs, lc, nil)
			assertError(t, rec, tc.wantStatus, tc.wantCode)
			if strings.Contains(rec.Body.String(), "10.0.0.5") {
				t.Errorf("body %s leaks the underlying error", rec.Body.String())
			}
		})
	}
}
