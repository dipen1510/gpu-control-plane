package api

import (
	"bufio"
	"bytes"
	"context"
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
			if tc.wantStatus != http.StatusInternalServerError && !strings.Contains(got.Message, "A100") {
				t.Errorf("message %q does not name the gpu_type A100", got.Message)
			}
			if strings.Contains(got.Message, "inst_x") || strings.Contains(got.Message, "connection refused") {
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
