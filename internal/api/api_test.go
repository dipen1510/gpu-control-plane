package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// fakeStore records calls and returns canned results.
type fakeStore struct {
	createCalls  []store.CreateRequest
	createErr    error
	createResult *store.CreateResult // if set, returned instead of a fresh instance
	getInst      store.Instance
	getErr       error
}

func (f *fakeStore) Create(_ context.Context, req store.CreateRequest) (store.CreateResult, error) {
	f.createCalls = append(f.createCalls, req)
	if f.createErr != nil {
		return store.CreateResult{}, f.createErr
	}
	if f.createResult != nil {
		return *f.createResult, nil
	}
	return store.CreateResult{Instance: store.Instance{
		ID: req.ID, TenantID: req.TenantID, GPUType: req.GPUType, GPUCount: req.GPUCount,
		NodeID: "node-0001", State: statemachine.Scheduled, ResourceVersion: 1,
	}}, nil
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
		// Header limits: both values go into primary keys, whose btree
		// entries are capped by Postgres; too long must be 400, not 500.
		{name: "tenant at 64 chars", tenant: strings.Repeat("t", 64), key: "k1", body: valid, wantStatus: http.StatusAccepted},
		{name: "tenant at 65 chars", tenant: strings.Repeat("t", 65), key: "k1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "tenant 6 KB", tenant: strings.Repeat("t", 6<<10), key: "k1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "tenant with space", tenant: "t 1", key: "k1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "tenant non-ASCII", tenant: "t\u00e9", key: "k1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "key at 255 chars", tenant: "t1", key: strings.Repeat("k", 255), body: valid, wantStatus: http.StatusAccepted},
		{name: "key at 256 chars", tenant: "t1", key: strings.Repeat("k", 256), body: valid, wantStatus: http.StatusBadRequest},
		{name: "key UUID", tenant: "t1", key: "6f1c2b9e-4a7d-4c2e-9b1a-0d3e5f7a9c1b", body: valid, wantStatus: http.StatusAccepted},
		{name: "key with tab", tenant: "t1", key: "k\t1", body: valid, wantStatus: http.StatusBadRequest},
		{name: "key with DEL", tenant: "t1", key: "k\x7f", body: valid, wantStatus: http.StatusBadRequest},
		{name: "key non-ASCII", tenant: "t1", key: "cl\u00e9", body: valid, wantStatus: http.StatusBadRequest},
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
	if got := rec.Header().Get(headerReplayed); got != "" {
		t.Errorf("%s = %q on a fresh create, want none", headerReplayed, got)
	}
	if len(call.RequestHash) != sha256.Size {
		t.Errorf("request hash is %d bytes, want %d", len(call.RequestHash), sha256.Size)
	}
}

// TestCreateReplayResponse: a replay returns the earlier instance as it is
// now (not as it was created), with Idempotent-Replayed: true.
func TestCreateReplayResponse(t *testing.T) {
	earlier := store.Instance{
		ID: "inst_earlier", TenantID: "t1", GPUType: "A100", GPUCount: 4,
		NodeID: "node-0002", State: statemachine.Running, ResourceVersion: 3,
	}
	fs := &fakeStore{createResult: &store.CreateResult{Instance: earlier, Replayed: true}}
	rec := httptest.NewRecorder()
	newTestHandler(fs).ServeHTTP(rec, newCreateRequest(strings.NewReader(`{"gpu_type":"A100","gpu_count":4}`)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(headerReplayed); got != "true" {
		t.Errorf("%s = %q, want %q", headerReplayed, got, "true")
	}
	if etag := rec.Header().Get("ETag"); etag != `"3"` {
		t.Errorf("ETag = %q, want %q", etag, `"3"`)
	}
	var got instanceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if want := toResponse(earlier); got != want {
		t.Errorf("body = %+v, want %+v", got, want)
	}
}

// TestRequestHashIsCanonical: the hash the handler passes to the store
// depends on the parsed values only, not on whitespace or field order.
func TestRequestHashIsCanonical(t *testing.T) {
	hashOf := func(t *testing.T, body string) []byte {
		t.Helper()
		fs := &fakeStore{}
		rec := httptest.NewRecorder()
		newTestHandler(fs).ServeHTTP(rec, newCreateRequest(strings.NewReader(body)))
		if rec.Code != http.StatusAccepted || len(fs.createCalls) != 1 {
			t.Fatalf("body %s: status %d, %d store calls; want 202 and 1", body, rec.Code, len(fs.createCalls))
		}
		return fs.createCalls[0].RequestHash
	}
	base := hashOf(t, `{"gpu_type":"H100","gpu_count":2}`)
	for _, tc := range []struct {
		name, body string
		same       bool
	}{
		{name: "whitespace", body: "{ \"gpu_type\" : \"H100\",\n\t\"gpu_count\": 2 }\n", same: true},
		{name: "field order", body: `{"gpu_count":2,"gpu_type":"H100"}`, same: true},
		{name: "other count", body: `{"gpu_type":"H100","gpu_count":4}`, same: false},
		{name: "other type", body: `{"gpu_type":"A100","gpu_count":2}`, same: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hashOf(t, tc.body); bytes.Equal(got, base) != tc.same {
				t.Errorf("hash(%s) equal to base = %v, want %v", tc.body, !tc.same, tc.same)
			}
		})
	}
}

func TestGetHeaderLimits(t *testing.T) {
	for _, tc := range []struct {
		name, tenant string
		wantStatus   int
	}{
		{name: "tenant at 64 chars", tenant: strings.Repeat("t", 64), wantStatus: http.StatusOK},
		{name: "tenant at 65 chars", tenant: strings.Repeat("t", 65), wantStatus: http.StatusBadRequest},
		{name: "tenant with control char", tenant: "t\x01", wantStatus: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/instances/inst_x", nil)
			req.Header.Set(headerTenant, tc.tenant)
			rec := httptest.NewRecorder()
			newTestHandler(&fakeStore{getInst: store.Instance{ID: "inst_x", ResourceVersion: 1}}).ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusBadRequest && decodeError(t, rec).Code != codeInvalidRequest {
				t.Errorf("body %s, want invalid_request", rec.Body.String())
			}
		})
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

func newCreateRequest(body io.Reader) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/instances", body)
	req.Header.Set(headerTenant, "t1")
	req.Header.Set(headerIdempotencyKey, "k1")
	return req
}

func TestCreateStoreErrorMapping(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantStatus     int
		wantCode       string
		wantRetryAfter string
	}{
		{
			name: "quota exceeded",
			// Wrapped the way store.Create wraps it.
			err:        fmt.Errorf("create instance inst_x: reserve 8 A100 GPUs for tenant t1: %w", store.ErrQuotaExceeded),
			wantStatus: http.StatusForbidden, wantCode: codeQuotaExceeded,
		},
		{
			name:       "insufficient capacity",
			err:        fmt.Errorf("create instance inst_x: place 8 A100 GPUs (0 candidates): %w", store.ErrInsufficientCapacity),
			wantStatus: http.StatusServiceUnavailable, wantCode: codeInsufficientCapacity, wantRetryAfter: "30",
		},
		{
			name:       "idempotency key reused",
			err:        fmt.Errorf("create instance inst_x: key used by instance inst_y: %w", store.ErrIdempotencyKeyReused),
			wantStatus: http.StatusConflict, wantCode: codeKeyReused,
		},
		{
			name:       "other error",
			err:        errors.New("connection refused"),
			wantStatus: http.StatusInternalServerError, wantCode: codeInternal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			newTestHandler(&fakeStore{createErr: tc.err}).ServeHTTP(rec,
				newCreateRequest(strings.NewReader(`{"gpu_type":"A100","gpu_count":8}`)))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			got := decodeError(t, rec)
			if got.Code != tc.wantCode {
				t.Errorf("code = %q, want %q", got.Code, tc.wantCode)
			}
			if ra := rec.Header().Get("Retry-After"); ra != tc.wantRetryAfter {
				t.Errorf("Retry-After = %q, want %q", ra, tc.wantRetryAfter)
			}
			if (tc.wantStatus == http.StatusForbidden || tc.wantStatus == http.StatusServiceUnavailable) &&
				!strings.Contains(got.Message, "A100") {
				t.Errorf("message %q does not name the gpu_type A100", got.Message)
			}
			if strings.Contains(got.Message, "inst_") || strings.Contains(got.Message, "connection refused") {
				t.Errorf("message %q leaks the store error", got.Message)
			}
		})
	}
}

// errReader fails every Read with err.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// timeoutError is what a read past the connection's deadline returns.
type timeoutError struct{ msg string }

func (e timeoutError) Error() string   { return e.msg }
func (e timeoutError) Timeout() bool   { return true }
func (e timeoutError) Temporary() bool { return true }

func TestCreateBodyErrors(t *testing.T) {
	const secret = "10.0.0.5:51234"
	tests := []struct {
		name        string
		body        io.Reader
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{
			name: "malformed JSON", body: strings.NewReader(`{"gpu_type":`),
			wantStatus: http.StatusBadRequest, wantCode: codeInvalidRequest, wantMessage: msgMalformedBody,
		},
		{
			name: "wrong type", body: strings.NewReader(`{"gpu_type":"H100","gpu_count":"2"}`),
			wantStatus: http.StatusBadRequest, wantCode: codeInvalidRequest, wantMessage: msgMalformedBody,
		},
		{
			name: "trailing value", body: strings.NewReader(`{"gpu_type":"H100","gpu_count":2}{}`),
			wantStatus: http.StatusBadRequest, wantCode: codeInvalidRequest, wantMessage: msgMalformedBody,
		},
		{
			name:       "over the limit inside the object",
			body:       strings.NewReader(`{"gpu_type":"` + strings.Repeat("x", maxBodyBytes) + `","gpu_count":2}`),
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: codeRequestTooLarge,
			wantMessage: "request body exceeds 4096 bytes",
		},
		{
			name:       "over the limit after the object",
			body:       strings.NewReader(`{"gpu_type":"H100","gpu_count":2}` + strings.Repeat(" ", maxBodyBytes)),
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: codeRequestTooLarge,
			wantMessage: "request body exceeds 4096 bytes",
		},
		{
			name:       "read timeout",
			body:       errReader{&net.OpError{Op: "read", Net: "tcp", Err: timeoutError{"i/o timeout from " + secret}}},
			wantStatus: http.StatusRequestTimeout, wantCode: codeRequestTimeout, wantMessage: msgBodyTimeout,
		},
		{
			name:       "connection reset mid-body",
			body:       errReader{&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer " + secret)}},
			wantStatus: http.StatusBadRequest, wantCode: codeInvalidRequest, wantMessage: msgMalformedBody,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			fs := &fakeStore{}
			h := NewHandler(fs, slog.New(slog.NewTextHandler(&logs, nil)))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, newCreateRequest(tc.body))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if got := decodeError(t, rec); got.Code != tc.wantCode || got.Message != tc.wantMessage {
				t.Errorf("error = %+v, want code %q message %q", got, tc.wantCode, tc.wantMessage)
			}
			if strings.Contains(rec.Body.String(), secret) {
				t.Errorf("response %s leaks %s", rec.Body.String(), secret)
			}
			// The underlying error is not lost: it is in the log.
			if !strings.Contains(logs.String(), "create request rejected") || !strings.Contains(logs.String(), "err=") {
				t.Errorf("log %q does not record the underlying error", logs.String())
			}
			if len(fs.createCalls) != 0 {
				t.Errorf("store.Create called on a bad body")
			}
		})
	}
}

// pipeListener hands out one in-memory connection, so an http.Server can run
// inside a synctest bubble where its read deadline uses the fake clock.
type pipeListener struct {
	conns chan net.Conn
	once  sync.Once
	done  chan struct{}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{} }

// TestCreateBodyReadTimeout checks that what net/http really returns when the
// server's ReadTimeout fires mid-body is classified as 408, not 400.
func TestCreateBodyReadTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srvConn, cliConn := net.Pipe()
		ln := &pipeListener{conns: make(chan net.Conn, 1), done: make(chan struct{})}
		ln.conns <- srvConn
		srv := &http.Server{Handler: newTestHandler(&fakeStore{}), ReadTimeout: 10 * time.Second}
		served := make(chan error, 1)
		go func() { served <- srv.Serve(ln) }()

		// Headers promise 100 bytes, then the client sends 12 and stalls.
		start := time.Now()
		if _, err := io.WriteString(cliConn, "POST /v1/instances HTTP/1.1\r\nHost: x\r\n"+
			headerTenant+": t1\r\n"+headerIdempotencyKey+": k1\r\nContent-Length: 100\r\n\r\n"+
			`{"gpu_type":`); err != nil {
			t.Fatalf("write request: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(cliConn), nil)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close() // in-memory body; nothing to report
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != http.StatusRequestTimeout {
			t.Errorf("status = %d body %s, want 408", resp.StatusCode, b)
		}
		var e errorBody
		if err := json.Unmarshal(b, &e); err != nil || e.Error.Code != codeRequestTimeout || e.Error.Message != msgBodyTimeout {
			t.Errorf("body %s, want request_timeout with the fixed message", b)
		}
		if waited := time.Since(start); waited < 10*time.Second {
			t.Errorf("answered after %v, before the 10s ReadTimeout", waited)
		}

		_ = cliConn.Close() // in-memory pipe; nothing to report
		if err := srv.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve: %v", err)
		}
	})
}
