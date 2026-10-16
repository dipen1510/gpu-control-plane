// Package api is the HTTP edge of the apiserver (ADR-0001 §API (M1)). It
// validates requests, calls the store, and maps domain errors to HTTP status
// codes. It holds no business rules: quota, placement and state live in the
// store, which runs each request as one transaction.
package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// Store is what the handlers need from the store. *store.Store satisfies it.
type Store interface {
	Create(ctx context.Context, req store.CreateRequest) (store.CreateResult, error)
	Get(ctx context.Context, tenantID, id string) (store.Instance, error)
}

// Lifecycle moves an instance on after the API has committed it. In M1 it is
// *lifecycle.Driver, which walks a new instance to RUNNING within the request
// (ADR-0001 Amendment 1); the M2 controller replaces it.
type Lifecycle interface {
	// AfterCreate is called once per fresh create, with the instance's id
	// and its resource_version at the create commit. It logs its own
	// errors; the instance stays in its last committed state.
	AfterCreate(ctx context.Context, id string, rv int64) (store.Instance, error)
}

const (
	headerTenant         = "X-Tenant-ID"
	headerIdempotencyKey = "Idempotency-Key"
	headerReplayed       = "Idempotent-Replayed"

	// Header length limits. Both values end up in primary keys, and a
	// Postgres btree entry cannot exceed ~2.7 KB, so an unbounded value
	// would turn into a 500 instead of a 400.
	maxTenantLen = 64
	maxKeyLen    = 255

	// maxBodyBytes bounds the create body; a valid one is ~40 bytes.
	maxBodyBytes = 4 << 10

	// retryAfterSeconds is the Retry-After on 503 insufficient_capacity
	// (ADR-0001 resolved questions).
	retryAfterSeconds = "30"

	// requestTimeout bounds the database work of one request, so a slow or
	// stuck Postgres fails the request instead of piling up goroutines.
	requestTimeout = 10 * time.Second

	// lifecycleTimeout bounds the M1 lifecycle walk after a create (two
	// small transactions). It runs after requestTimeout's work, and the two
	// together stay under the server's 15s WriteTimeout (cmd/apiserver).
	lifecycleTimeout = 3 * time.Second
)

// Error codes in the {"error": {"code", "message"}} envelope.
const (
	codeInvalidRequest       = "invalid_request"
	codeRequestTooLarge      = "request_too_large"
	codeRequestTimeout       = "request_timeout"
	codeQuotaExceeded        = "quota_exceeded"
	codeKeyReused            = "idempotency_key_reused"
	codeNotFound             = "not_found"
	codeInsufficientCapacity = "insufficient_capacity"
	codeInternal             = "internal"
)

// Fixed messages for body errors. The decoder's own error can include socket
// addresses or other internals, so it is logged and never sent.
const (
	msgMalformedBody = "malformed JSON body"
	msgBodyTimeout   = "timed out reading the request body"
)

// D1: one instance per request, fitting on a single node.
var (
	validGPUTypes  = map[string]bool{"H100": true, "A100": true}
	validGPUCounts = map[int]bool{1: true, 2: true, 4: true, 8: true}
)

// Handler serves the /v1 instance API.
type Handler struct {
	store     Store
	lifecycle Lifecycle
	log       *slog.Logger
	mux       *http.ServeMux
}

// NewHandler returns the API's HTTP handler. lc is run after every fresh
// create, before the response is written.
func NewHandler(s Store, lc Lifecycle, log *slog.Logger) *Handler {
	h := &Handler{store: s, lifecycle: lc, log: log, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/instances", h.create)
	h.mux.HandleFunc("GET /v1/instances/{id}", h.get)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// createRequest is the create body (Discussion D1).
type createRequest struct {
	GPUType  string `json:"gpu_type"`
	GPUCount int    `json:"gpu_count"`
}

// instanceResponse is the instance as clients see it.
type instanceResponse struct {
	ID              string `json:"id"`
	State           string `json:"state"`
	NodeID          string `json:"node_id"`
	GPUType         string `json:"gpu_type"`
	GPUCount        int    `json:"gpu_count"`
	ResourceVersion int64  `json:"resource_version"`
}

func toResponse(inst store.Instance) instanceResponse {
	return instanceResponse{
		ID: inst.ID, State: string(inst.State), NodeID: inst.NodeID,
		GPUType: inst.GPUType, GPUCount: inst.GPUCount, ResourceVersion: inst.ResourceVersion,
	}
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	tenant, reqErr := headerValue(r, headerTenant, maxTenantLen)
	if reqErr != nil {
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	key, reqErr := headerValue(r, headerIdempotencyKey, maxKeyLen)
	if reqErr != nil {
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	body, reqErr := decodeCreate(w, r)
	if reqErr != nil {
		if reqErr.cause != nil {
			h.log.Info("create request rejected", "tenant", tenant, "status", reqErr.status,
				"code", reqErr.code, "err", reqErr.cause)
		}
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}

	hash, err := requestHash(body)
	if err != nil {
		h.internalError(w, "create instance", err, "tenant", tenant)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := h.store.Create(ctx, store.CreateRequest{
		ID: newInstanceID(), TenantID: tenant, GPUType: body.GPUType, GPUCount: body.GPUCount,
		IdempotencyKey: key, RequestHash: hash,
	})
	switch {
	case errors.Is(err, store.ErrIdempotencyKeyReused):
		// 409: the client sent a different request under a key it already
		// used. Retrying the same request will not help.
		h.log.Info("create rejected", "tenant", tenant, "gpu_type", body.GPUType,
			"gpu_count", body.GPUCount, "result", codeKeyReused)
		writeError(w, http.StatusConflict, codeKeyReused,
			headerIdempotencyKey+" was already used with a different request body")
		return
	case errors.Is(err, store.ErrQuotaExceeded):
		// 403: retrying will not help until the tenant frees GPUs (D2).
		h.log.Info("create rejected", "tenant", tenant, "gpu_type", body.GPUType,
			"gpu_count", body.GPUCount, "result", codeQuotaExceeded)
		writeError(w, http.StatusForbidden, codeQuotaExceeded, fmt.Sprintf(
			"%s GPU quota exceeded: %d more would go over the tenant's limit", body.GPUType, body.GPUCount))
		return
	case errors.Is(err, store.ErrInsufficientCapacity):
		// 503 + Retry-After: capacity may free up, so the client should
		// retry later (D3). The message names the GPU type so it can't be
		// mistaken for a quota error.
		h.log.Info("create rejected", "tenant", tenant, "gpu_type", body.GPUType,
			"gpu_count", body.GPUCount, "result", codeInsufficientCapacity)
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, codeInsufficientCapacity, fmt.Sprintf(
			"no node has %d free %s GPUs; retry later", body.GPUCount, body.GPUType))
		return
	case err != nil:
		h.internalError(w, "create instance", err, "tenant", tenant,
			"gpu_type", body.GPUType, "gpu_count", body.GPUCount)
		return
	}
	if res.Replayed {
		// Same key, same body: the earlier instance as it is now, nothing
		// written (ADR-0001 §API).
		h.log.Info("create replayed", "instance_id", res.ID, "tenant", tenant, "node_id", res.NodeID,
			"state", res.State, "resource_version", res.ResourceVersion, "result", "replayed")
		w.Header().Set(headerReplayed, "true")
		writeInstance(w, http.StatusAccepted, res.Instance)
		return
	}
	h.log.Info("instance created", "instance_id", res.ID, "tenant", tenant, "node_id", res.NodeID,
		"state", res.State, "resource_version", res.ResourceVersion, "result", "scheduled")

	// The create is committed; now walk the new instance on (M1: to
	// RUNNING), before responding so a GET right after sees the result.
	// The walk is detached from client cancellation: a client hanging up
	// must not leave the instance half-walked. Replays never get here, so
	// a retried create can't run the walk twice.
	lcCtx, lcCancel := context.WithTimeout(context.WithoutCancel(r.Context()), lifecycleTimeout)
	defer lcCancel()
	// The driver has logged any error, and the instance stays in its last
	// committed state; the create itself succeeded, so the client still
	// gets 202.
	_, _ = h.lifecycle.AfterCreate(lcCtx, res.ID, res.ResourceVersion)

	// The response is the state the create transaction committed
	// (SCHEDULED, rv 1), as it will be in M2 (ADR-0001 Amendment 1).
	writeInstance(w, http.StatusAccepted, res.Instance)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	tenant, reqErr := headerValue(r, headerTenant, maxTenantLen)
	if reqErr != nil {
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	inst, err := h.store.Get(ctx, tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "instance "+id+" not found")
		return
	}
	if err != nil {
		h.internalError(w, "get instance", err, "tenant", tenant, "instance_id", id)
		return
	}

	etag := etagFor(inst.ResourceVersion)
	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		// Nothing changed since the client's copy (Discussion D16).
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeInstance(w, http.StatusOK, inst)
}

// requestError is a client error found before calling the store. message is
// sent to the client; cause, if set, is the underlying error, which is logged
// but never sent.
type requestError struct {
	status  int
	code    string
	message string
	cause   error
}

func invalid(message string) *requestError {
	return &requestError{status: http.StatusBadRequest, code: codeInvalidRequest, message: message}
}

// bodyError classifies an error from reading or decoding the body: over the
// size limit → 413, the server's read deadline → 408, anything else (bad
// JSON, wrong types, client hung up mid-body) → 400. All three carry a fixed
// message.
func bodyError(err error) *requestError {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return &requestError{status: http.StatusRequestEntityTooLarge, code: codeRequestTooLarge,
			message: fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes), cause: err}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &requestError{status: http.StatusRequestTimeout, code: codeRequestTimeout,
			message: msgBodyTimeout, cause: err}
	}
	return &requestError{status: http.StatusBadRequest, code: codeInvalidRequest,
		message: msgMalformedBody, cause: err}
}

// decodeCreate parses and validates the create body.
func decodeCreate(w http.ResponseWriter, r *http.Request) (createRequest, *requestError) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	var body createRequest
	if err := dec.Decode(&body); err != nil {
		return createRequest{}, bodyError(err)
	}
	switch err := dec.Decode(&struct{}{}); {
	case err == nil:
		return createRequest{}, bodyError(errors.New("unexpected JSON value after the object"))
	case !errors.Is(err, io.EOF):
		return createRequest{}, bodyError(fmt.Errorf("after the object: %w", err))
	}
	if !validGPUTypes[body.GPUType] {
		return createRequest{}, invalid(fmt.Sprintf("gpu_type must be H100 or A100, got %q", body.GPUType))
	}
	if !validGPUCounts[body.GPUCount] {
		return createRequest{}, invalid(fmt.Sprintf("gpu_count must be 1, 2, 4 or 8, got %d", body.GPUCount))
	}
	return body, nil
}

// headerValue returns the named header, which must be present, at most
// maxLen bytes, and only visible ASCII (0x21–0x7E: no spaces, control
// characters or non-ASCII bytes). The value is never echoed back.
func headerValue(r *http.Request, name string, maxLen int) (string, *requestError) {
	v := r.Header.Get(name)
	if v == "" {
		return "", invalid("missing " + name + " header")
	}
	if len(v) > maxLen {
		return "", invalid(fmt.Sprintf("%s header is longer than %d characters", name, maxLen))
	}
	for i := range len(v) {
		if v[i] < 0x21 || v[i] > 0x7e {
			return "", invalid(name + " header must be printable ASCII with no spaces")
		}
	}
	return v, nil
}

// requestHash is the SHA-256 of the parsed body re-encoded by encoding/json,
// which writes struct fields in declaration order with no whitespace. So
// formatting and field order in the client's JSON do not count as a
// different request (ADR-0001 §The create transaction), and a field added to
// createRequest later is part of the hash automatically.
func requestHash(body createRequest) ([]byte, error) {
	canonical, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode canonical body: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

// newInstanceID returns "inst_" plus 128 random bits (base32, lower case).
func newInstanceID() string {
	return "inst_" + strings.ToLower(rand.Text())
}

func etagFor(rv int64) string {
	return `"` + strconv.FormatInt(rv, 10) + `"`
}

// etagMatches reports whether an If-None-Match header value matches etag.
// It accepts "*", a comma-separated list, and weak validators (W/"3"), which
// RFC 9110 says compare weakly for If-None-Match.
func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}

func writeInstance(w http.ResponseWriter, status int, inst store.Instance) {
	w.Header().Set("ETag", etagFor(inst.ResourceVersion))
	writeJSON(w, status, toResponse(inst))
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

// internalError logs err with its details and returns a generic 500, so
// database internals never reach the client.
func (h *Handler) internalError(w http.ResponseWriter, op string, err error, attrs ...any) {
	h.log.Error(op+" failed", append(attrs, "err", err)...)
	writeError(w, http.StatusInternalServerError, codeInternal, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line is already sent; a write error means the client went
	// away and there is nobody left to tell.
	_ = json.NewEncoder(w).Encode(v)
}
