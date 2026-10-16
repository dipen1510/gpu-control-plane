package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// fakeStore records calls and returns canned results.
type fakeStore struct {
	createCalls []store.CreateRequest
	createErr   error
	getInst     store.Instance
	getErr      error
}

func (f *fakeStore) Create(_ context.Context, req store.CreateRequest) (store.Instance, error) {
	f.createCalls = append(f.createCalls, req)
	if f.createErr != nil {
		return store.Instance{}, f.createErr
	}
	return store.Instance{
		ID: req.ID, TenantID: req.TenantID, GPUType: req.GPUType, GPUCount: req.GPUCount,
		NodeID: "node-0001", State: statemachine.Scheduled, ResourceVersion: 1,
	}, nil
}

func (f *fakeStore) Get(context.Context, string, string) (store.Instance, error) {
	return f.getInst, f.getErr
}

func newTestHandler(s Store) *Handler {
	return NewHandler(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorDetail {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body %q is not the envelope: %v", rec.Body.String(), err)
	}
	return body.Error
}

func TestCreateValidation(t *testing.T) {
	const valid = `{"gpu_type":"H100","gpu_count":2}`
	tests := []struct {
		name       string
		tenant     string
		key        string
		body       string
		wantStatus int
	}{
		{name: "valid", tenant: "t1", key: "k1", body: valid, wantStatus: http.StatusAccepted},
		{name: "valid A100 x8", tenant: "t1", key: "k1", body: `{"gpu_type":"A100","gpu_count":8}`, wantStatus: http.StatusAccepted},
		{name: "missing tenant", key: "k1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "missing idempotency key", tenant: "t1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "unknown gpu_type", tenant: "t1", key: "k1", body: `{"gpu_type":"V100","gpu_count":2}`, wantStatus: http.StatusBadRequest},
		{name: "lower-case gpu_type", tenant: "t1", key: "k1", body: `{"gpu_type":"h100","gpu_count":2}`, wantStatus: http.StatusBadRequest},
		{name: "missing gpu_type", tenant: "t1", key: "k1", body: `{"gpu_count":2}`, wantStatus: http.StatusBadRequest},
		{name: "gpu_count 3", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":3}`, wantStatus: http.StatusBadRequest},
		{name: "gpu_count 0", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":0}`, wantStatus: http.StatusBadRequest},
		{name: "gpu_count 16", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":16}`, wantStatus: http.StatusBadRequest},
		{name: "gpu_count negative", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":-2}`, wantStatus: http.StatusBadRequest},
		{name: "gpu_count fractional", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":2.5}`, wantStatus: http.StatusBadRequest},
		{name: "gpu_count string", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":"2"}`, wantStatus: http.StatusBadRequest},
		{name: "missing gpu_count", tenant: "t1", key: "k1", body: `{"gpu_type":"H100"}`, wantStatus: http.StatusBadRequest},
		{name: "malformed JSON", tenant: "t1", key: "k1", body: `{"gpu_type":`, wantStatus: http.StatusBadRequest},
		{name: "empty body", tenant: "t1", key: "k1", body: ``, wantStatus: http.StatusBadRequest},
		{name: "unknown field", tenant: "t1", key: "k1", body: `{"gpu_type":"H100","gpu_count":2,"region":"x"}`, wantStatus: http.StatusBadRequest},
		{name: "trailing data", tenant: "t1", key: "k1", body: valid + `{}`, wantStatus: http.StatusBadRequest},
		{name: "array body", tenant: "t1", key: "k1", body: `[1]`, wantStatus: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{}
			req := httptest.NewRequest(http.MethodPost, "/v1/instances", strings.NewReader(tc.body))
			if tc.tenant != "" {
				req.Header.Set(headerTenant, tc.tenant)
			}
			if tc.key != "" {
				req.Header.Set(headerIdempotencyKey, tc.key)
			}
			rec := httptest.NewRecorder()
			newTestHandler(fs).ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusAccepted {
				if len(fs.createCalls) != 1 {
					t.Fatalf("store.Create called %d times, want 1", len(fs.createCalls))
				}
				return
			}
			if got := decodeError(t, rec); got.Code != codeInvalidRequest || got.Message == "" {
				t.Errorf("error = %+v, want code %q and a message", got, codeInvalidRequest)
			}
			if len(fs.createCalls) != 0 {
				t.Errorf("store.Create called on an invalid request")
			}
		})
	}
}

func TestCreateResponse(t *testing.T) {
	fs := &fakeStore{}
	req := httptest.NewRequest(http.MethodPost, "/v1/instances", strings.NewReader(`{"gpu_type":"A100","gpu_count":4}`))
	req.Header.Set(headerTenant, "t1")
	req.Header.Set(headerIdempotencyKey, "k1")
	rec := httptest.NewRecorder()
	newTestHandler(fs).ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	call := fs.createCalls[0]
	if !strings.HasPrefix(call.ID, "inst_") || len(call.ID) <= len("inst_") {
		t.Errorf("instance id = %q, want inst_<random>", call.ID)
	}
	if call.TenantID != "t1" || call.IdempotencyKey != "k1" || call.GPUType != "A100" || call.GPUCount != 4 {
		t.Errorf("store.Create got %+v", call)
	}
	var got instanceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	want := instanceResponse{ID: call.ID, State: "SCHEDULED", NodeID: "node-0001", GPUType: "A100", GPUCount: 4, ResourceVersion: 1}
	if got != want {
		t.Errorf("body = %+v, want %+v", got, want)
	}
	if etag := rec.Header().Get("ETag"); etag != `"1"` {
		t.Errorf("ETag = %q, want %q", etag, `"1"`)
	}
}

func TestInstanceIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := newInstanceID()
		if seen[id] {
			t.Fatalf("newInstanceID returned %q twice", id)
		}
		seen[id] = true
	}
}

func TestStoreErrorsAreInternal(t *testing.T) {
	secret := errors.New("pq: connection to 10.0.0.5 refused")
	fs := &fakeStore{createErr: secret, getErr: secret}
	h := newTestHandler(fs)

	post := httptest.NewRequest(http.MethodPost, "/v1/instances", strings.NewReader(`{"gpu_type":"H100","gpu_count":1}`))
	post.Header.Set(headerTenant, "t1")
	post.Header.Set(headerIdempotencyKey, "k1")
	get := httptest.NewRequest(http.MethodGet, "/v1/instances/inst_x", nil)
	get.Header.Set(headerTenant, "t1")

	for _, req := range []*http.Request{post, get} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s: status = %d, want 500", req.Method, rec.Code)
		}
		if got := decodeError(t, rec); got.Code != codeInternal || strings.Contains(got.Message, "10.0.0.5") {
			t.Errorf("%s: error = %+v, want code internal without internals", req.Method, got)
		}
	}
}

func TestGetValidationAndNotFound(t *testing.T) {
	fs := &fakeStore{getErr: store.ErrNotFound}
	h := newTestHandler(fs)

	noTenant := httptest.NewRequest(http.MethodGet, "/v1/instances/inst_x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, noTenant)
	if rec.Code != http.StatusBadRequest || decodeError(t, rec).Code != codeInvalidRequest {
		t.Errorf("missing tenant: status %d body %s, want 400 invalid_request", rec.Code, rec.Body.String())
	}

	missing := httptest.NewRequest(http.MethodGet, "/v1/instances/inst_x", nil)
	missing.Header.Set(headerTenant, "t1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, missing)
	if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != codeNotFound {
		t.Errorf("unknown id: status %d body %s, want 404 not_found", rec.Code, rec.Body.String())
	}
}

func TestEtagMatches(t *testing.T) {
	tests := []struct {
		header string
		want   bool
	}{
		{header: `"3"`, want: true},
		{header: `W/"3"`, want: true},
		{header: `"1", "3"`, want: true},
		{header: `*`, want: true},
		{header: ``, want: false},
		{header: `"4"`, want: false},
		{header: `3`, want: false},
		{header: `"33"`, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			if got := etagMatches(tc.header, `"3"`); got != tc.want {
				t.Errorf("etagMatches(%q, \"3\") = %v, want %v", tc.header, got, tc.want)
			}
		})
	}
}
