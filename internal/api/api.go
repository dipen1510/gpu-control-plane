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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dipen1510/gpu-control-plane/internal/metrics"
	"github.com/dipen1510/gpu-control-plane/internal/statemachine"
	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// Store is what the handlers need from the store. *store.Store satisfies it.
type Store interface {
	Create(ctx context.Context, req store.CreateRequest) (store.CreateResult, error)
	Get(ctx context.Context, tenantID, id string) (store.Instance, error)
}

// Lifecycle moves an instance on after the API has committed it. In M1 it is
// *lifecycle.Driver, which walks a new instance to RUNNING and a deleted one
// to TERMINATED within the request (ADR-0001 Amendment 1); the M2 controller
// replaces it.
type Lifecycle interface {
	// AfterCreate is called once per fresh create, with the instance's id
	// and its resource_version at the create commit. It logs its own
	// errors; the instance stays in its last committed state.
	AfterCreate(ctx context.Context, id string, rv int64) (store.Instance, error)
	// Delete moves an instance that is in state from at version expectedRV
	// to TERMINATING and then TERMINATED (from TERMINATING: only the last
	// step). It returns the instance as its last successful step left it
	// (zero if none) and the error that stopped it, already logged. A
	// non-zero instance with an error means TERMINATING was committed.
	Delete(ctx context.Context, id string, from statemachine.State, expectedRV int64) (store.Instance, error)
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

	// retryAfterConflictSeconds is the Retry-After on 503 conflict_retry.
	// A deadlock is between two specific transactions; Postgres has already
	// aborted one, so an immediate retry almost always succeeds. Capacity's
	// 30s would make the client wait for nothing.
	retryAfterConflictSeconds = "1"

	// requestTimeout bounds the database work of one request, so a slow or
	// stuck Postgres fails the request instead of piling up goroutines.
	requestTimeout = 10 * time.Second

	// lifecycleTimeout bounds the M1 lifecycle work of one request: the
	// walk after a create, or all delete walks of one DELETE together
	// (small transactions each). It runs on top of requestTimeout's work,
	// and the two together stay under the server's 15s WriteTimeout
	// (cmd/apiserver).
	lifecycleTimeout = 3 * time.Second

	// deleteRetries is how many times a DELETE without If-Match re-reads
	// the instance and tries again after losing a race (ErrStaleVersion),
	// before giving up with 409 (ADR-0001 Amendment 1). So the walk is
	// attempted at most 1 + deleteRetries times.
	deleteRetries = 3
)

// Error codes in the {"error": {"code", "message"}} envelope.
const (
	codeInvalidRequest       = "invalid_request"
	codeRequestTooLarge      = "request_too_large"
	codeRequestTimeout       = "request_timeout"
	codeQuotaExceeded        = "quota_exceeded"
	codeKeyReused            = "idempotency_key_reused"
	codeNotFound             = "not_found"
	codeStaleVersion         = "stale_resource_version"
	codeInvalidTransition    = "invalid_transition"
	codeInsufficientCapacity = "insufficient_capacity"
	codeConflictRetry        = "conflict_retry"
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

	metrics      *metrics.Metrics
	serveMetrics bool   // GET /metrics is registered (WithMetrics)
	ready        Pinger // GET /readyz pings it; nil → 503
}

// NewHandler returns the apiserver's HTTP handler: the /v1 API plus
// GET /healthz, GET /readyz and (WithMetrics) GET /metrics. lc is run after
// every fresh create, before the response is written.
func NewHandler(s Store, lc Lifecycle, log *slog.Logger, opts ...Option) *Handler {
	h := &Handler{store: s, lifecycle: lc, log: log, mux: http.NewServeMux()}
	for _, o := range opts {
		o(h)
	}
	if h.metrics == nil {
		h.metrics = metrics.New()
	}
	h.mux.HandleFunc("POST /v1/instances", h.create)
	h.mux.HandleFunc("GET /v1/instances/{id}", h.get)
	h.mux.HandleFunc("DELETE /v1/instances/{id}", h.delete)
	h.mux.HandleFunc(routeHealthz, h.healthz)
	h.mux.HandleFunc(routeReadyz, h.readyz)
	if h.serveMetrics {
		h.mux.Handle(routeMetrics, h.metrics.Handler(promLogger{log: log}))
	}
	return h
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
		h.metrics.CreateResult(metrics.CreateInvalid)
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	key, reqErr := headerValue(r, headerIdempotencyKey, maxKeyLen)
	if reqErr != nil {
		h.metrics.CreateResult(metrics.CreateInvalid)
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	body, reqErr := decodeCreate(w, r)
	if reqErr != nil {
		if reqErr.cause != nil {
			h.log.Info("create request rejected", "tenant", tenant, "status", reqErr.status,
				"code", reqErr.code, "err", reqErr.cause)
		}
		h.metrics.CreateResult(metrics.CreateInvalid)
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}

	hash, err := requestHash(body)
	if err != nil {
		h.metrics.CreateResult(metrics.CreateError)
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
		h.metrics.CreateResult(metrics.CreateKeyReused)
		writeError(w, http.StatusConflict, codeKeyReused,
			headerIdempotencyKey+" was already used with a different request body")
		return
	case errors.Is(err, store.ErrQuotaExceeded):
		// 403: retrying will not help until the tenant frees GPUs (D2).
		h.log.Info("create rejected", "tenant", tenant, "gpu_type", body.GPUType,
			"gpu_count", body.GPUCount, "result", codeQuotaExceeded)
		h.metrics.CreateResult(metrics.CreateQuotaExceeded)
		writeError(w, http.StatusForbidden, codeQuotaExceeded, fmt.Sprintf(
			"%s GPU quota exceeded: %d more would go over the tenant's limit", body.GPUType, body.GPUCount))
		return
	case errors.Is(err, store.ErrInsufficientCapacity):
		// 503 + Retry-After: capacity may free up, so the client should
		// retry later (D3). The message names the GPU type so it can't be
		// mistaken for a quota error.
		h.log.Info("create rejected", "tenant", tenant, "gpu_type", body.GPUType,
			"gpu_count", body.GPUCount, "result", codeInsufficientCapacity)
		h.metrics.CreateResult(metrics.CreateInsufficientCapacity)
		w.Header().Set("Retry-After", retryAfterSeconds)
		writeError(w, http.StatusServiceUnavailable, codeInsufficientCapacity, fmt.Sprintf(
			"no node has %d free %s GPUs; retry later", body.GPUCount, body.GPUType))
		return
	case store.IsDeadlock(err):
		h.metrics.CreateResult(metrics.CreateConflictRetry)
		h.conflictRetry(w, "create instance", err, "tenant", tenant,
			"gpu_type", body.GPUType, "gpu_count", body.GPUCount)
		return
	case err != nil:
		h.metrics.CreateResult(metrics.CreateError)
		h.internalError(w, "create instance", err, "tenant", tenant,
			"gpu_type", body.GPUType, "gpu_count", body.GPUCount)
		return
	}
	if res.Replayed {
		// Same key, same body: the earlier instance as it is now, nothing
		// written (ADR-0001 §API).
		h.log.Info("create replayed", "instance_id", res.ID, "tenant", tenant, "node_id", res.NodeID,
			"state", res.State, "resource_version", res.ResourceVersion, "result", "replayed")
		h.metrics.CreateResult(metrics.CreateReplayed)
		w.Header().Set(headerReplayed, "true")
		writeInstance(w, http.StatusAccepted, res.Instance)
		return
	}
	h.log.Info("instance created", "instance_id", res.ID, "tenant", tenant, "node_id", res.NodeID,
		"state", res.State, "resource_version", res.ResourceVersion, "result", "scheduled")
	h.metrics.CreateResult(metrics.CreateScheduled)

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

// delete asks for an instance to be stopped (ADR-0001 §API (M1),
// Amendment 1). Order of checks: the instance must exist for this tenant
// (404); an instance already TERMINATED or FAILED is answered 202 with its
// current state and nothing is written; a stale If-Match is 409; otherwise
// the lifecycle walks it to TERMINATED, releasing its GPUs and quota in the
// last step (invariant 5).
//
// The response is 202 with the instance at TERMINATING and its ETag, as in
// M2, even though a GET right after already shows TERMINATED. When the walk
// resumes an instance a crash left in TERMINATING, that is the instance as
// read (this request committed only TERMINATED).
//
// Without If-Match, losing a race (another writer moved the instance between
// our read and the walk) re-reads and tries again from the state it now has,
// up to deleteRetries times. If a concurrent DELETE finished the instance,
// the answer is 202 with that state; only an instance still not terminal
// after the last re-read is 409. With If-Match, the client asked for one
// specific version, so losing the race is 409 at once.
func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	tenant, reqErr := headerValue(r, headerTenant, maxTenantLen)
	if reqErr != nil {
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	precondition, reqErr := parseIfMatch(r.Header.Get("If-Match"))
	if reqErr != nil {
		writeError(w, reqErr.status, reqErr.code, reqErr.message)
		return
	}
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	// The walks are detached from client cancellation, like the create walk:
	// a client hanging up must not stop a walk between TERMINATING and the
	// release. One budget covers every attempt of this request.
	lcCtx, lcCancel := context.WithTimeout(context.WithoutCancel(r.Context()), lifecycleTimeout)
	defer lcCancel()

	// Ownership is checked here: Get is scoped to the tenant, the
	// lifecycle (store.Transition) is not.
	inst, err := h.store.Get(ctx, tenant, id)
	for attempt := 0; ; attempt++ {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, codeNotFound, "instance "+id+" not found")
			return
		case store.IsDeadlock(err):
			h.conflictRetry(w, "delete instance: read", err, "tenant", tenant, "instance_id", id)
			return
		case err != nil:
			h.internalError(w, "delete instance: read", err, "tenant", tenant, "instance_id", id)
			return
		}

		if statemachine.IsTerminal(inst.State) {
			// Already gone: DELETE is idempotent, nothing is written.
			h.log.Info("delete of a terminal instance", "instance_id", id, "tenant", tenant,
				"node_id", inst.NodeID, "state", inst.State, "resource_version", inst.ResourceVersion, "result", "already_terminal")
			writeInstance(w, http.StatusAccepted, inst)
			return
		}
		if precondition != nil && !precondition.matches(inst.ResourceVersion) {
			h.log.Info("delete rejected", "instance_id", id, "tenant", tenant, "node_id", inst.NodeID, "state", inst.State,
				"resource_version", inst.ResourceVersion, "result", codeStaleVersion)
			writeStale(w, id)
			return
		}

		last, walkErr := h.lifecycle.Delete(lcCtx, id, inst.State, inst.ResourceVersion)
		switch {
		case walkErr == nil:
			h.log.Info("instance deleted", "instance_id", id, "tenant", tenant, "node_id", last.NodeID,
				"from", inst.State, "state", last.State, "resource_version", last.ResourceVersion, "result", "terminated")
			writeInstance(w, http.StatusAccepted, terminatingView(last))
			return
		case last.ID != "" && errors.Is(walkErr, store.ErrStaleVersion):
			// This walk committed TERMINATING, then lost the release step:
			// another DELETE finished the instance first. The delete was
			// accepted, so this is never a 409; the instance as it is now
			// decides the body (TERMINATED, normally).
			h.log.Info("delete finished by a concurrent request", "instance_id", id, "tenant", tenant,
				"node_id", last.NodeID, "state", last.State, "resource_version", last.ResourceVersion,
				"result", "terminated_elsewhere")
			if now, rerr := h.store.Get(ctx, tenant, id); rerr == nil && statemachine.IsTerminal(now.State) {
				writeInstance(w, http.StatusAccepted, now)
				return
			}
			// Re-read failed or not terminal yet: what we committed is true.
			writeInstance(w, http.StatusAccepted, last)
			return
		case last.ID != "" || (inst.State == statemachine.Terminating && !errors.Is(walkErr, store.ErrStaleVersion)):
			// TERMINATING is committed (by this walk, or an earlier one we
			// were resuming) and the release step failed with a real error.
			// The delete was accepted, so this is never a 409 or 500: the
			// client sees TERMINATING, and in M1 another DELETE resumes the
			// walk. The driver has logged the cause.
			at := inst
			if last.ID != "" {
				at = last
			}
			h.log.Warn("delete accepted but the instance is stuck in TERMINATING", "instance_id", id,
				"tenant", tenant, "node_id", at.NodeID, "state", at.State, "resource_version", at.ResourceVersion, "result", "terminating", "err", walkErr)
			writeInstance(w, http.StatusAccepted, at)
			return
		case errors.Is(walkErr, store.ErrStaleVersion):
			// Nothing changed; someone moved the instance after we read it.
			if precondition != nil {
				// The client asked to delete one specific version, and the
				// instance is no longer at it: 409 even if a concurrent
				// DELETE has since finished it, so the client re-reads.
				h.log.Info("delete rejected", "instance_id", id, "tenant", tenant, "node_id", inst.NodeID, "state", inst.State,
					"resource_version", inst.ResourceVersion, "attempts", attempt+1, "result", codeStaleVersion)
				writeStale(w, id)
				return
			}
			// Re-read and decide again from the state it has now: it may
			// have moved on (create walk), be TERMINATING (resume) or be
			// terminal (another DELETE won; the loop answers 202).
			inst, err = h.store.Get(ctx, tenant, id)
			if attempt < deleteRetries || err != nil || statemachine.IsTerminal(inst.State) {
				if err == nil && statemachine.IsTerminal(inst.State) {
					h.log.Info("delete finished by a concurrent request", "instance_id", id, "tenant", tenant,
						"node_id", inst.NodeID, "state", inst.State, "resource_version", inst.ResourceVersion, "attempts", attempt+1)
				}
				continue
			}
			// Out of retries and the instance is still not terminal.
			h.log.Info("delete rejected", "instance_id", id, "tenant", tenant, "node_id", inst.NodeID, "state", inst.State,
				"resource_version", inst.ResourceVersion, "attempts", attempt+1, "result", codeStaleVersion)
			writeStale(w, id)
			return
		case errors.Is(walkErr, store.ErrInvalidTransition):
			// SCHEDULED, PROVISIONING and RUNNING all have an edge to
			// TERMINATING; PENDING (v2) does not (ADR-0001 §API).
			writeError(w, http.StatusConflict, codeInvalidTransition,
				"instance "+id+" cannot be deleted in state "+string(inst.State))
			return
		case errors.Is(walkErr, store.ErrNotFound):
			writeError(w, http.StatusNotFound, codeNotFound, "instance "+id+" not found")
			return
		case store.IsDeadlock(walkErr):
			// Nothing was committed (last is zero and the instance was not
			// already TERMINATING), so a retry starts cleanly.
			h.conflictRetry(w, "delete instance", walkErr, "tenant", tenant, "instance_id", id,
				"node_id", inst.NodeID, "state", inst.State)
			return
		default:
			h.internalError(w, "delete instance", walkErr, "tenant", tenant, "instance_id", id,
				"node_id", inst.NodeID, "state", inst.State)
			return
		}
	}
}

// terminatingView is the instance as the delete's TERMINATING step left it,
// given the instance after the walk's last step, TERMINATING → TERMINATED.
// store.Transition bumps resource_version by exactly 1 per step, so
// TERMINATING was the version before. The API reports TERMINATING so the
// DELETE response is the same in M1 and M2 (ADR-0001 Amendment 1).
func terminatingView(final store.Instance) store.Instance {
	if final.State != statemachine.Terminated {
		return final
	}
	view := final
	view.State = statemachine.Terminating
	view.ResourceVersion--
	return view
}

func writeStale(w http.ResponseWriter, id string) {
	writeError(w, http.StatusConflict, codeStaleVersion,
		"instance "+id+" has changed; read it again and retry")
}

// ifMatch is a parsed If-Match header: the resource versions the client
// accepts.
type ifMatch struct {
	versions []string // ETags, quotes included
}

func (m *ifMatch) matches(rv int64) bool {
	return slices.Contains(m.versions, etagFor(rv))
}

// parseIfMatch parses an If-Match header into the versions it accepts. It
// returns nil (no precondition) if the header is absent or "*" (any current
// instance, RFC 9110 §13.1.1). Otherwise it is a comma-separated list of
// entity tags. Each must be quoted ("3") or weak (W/"3"); anything else is
// 400. Comparison is strong, as RFC 9110 requires for If-Match: a weak tag
// is valid but never matches. The value is never echoed back.
func parseIfMatch(header string) (*ifMatch, *requestError) {
	header = strings.TrimSpace(header)
	if header == "" || header == "*" {
		return nil, nil
	}
	m := &ifMatch{}
	for _, tag := range strings.Split(header, ",") {
		tag = strings.TrimSpace(tag)
		weak := strings.HasPrefix(tag, "W/")
		opaque := strings.TrimPrefix(tag, "W/")
		if len(opaque) < 2 || opaque[0] != '"' || opaque[len(opaque)-1] != '"' ||
			strings.Contains(opaque[1:len(opaque)-1], `"`) {
			return nil, invalid(`If-Match must be "*" or a list of quoted ETags such as "3"`)
		}
		if !weak {
			m.versions = append(m.versions, opaque)
		}
	}
	return m, nil
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

// conflictRetry answers a transaction Postgres aborted to break a deadlock
// (SQLSTATE 40P01) with 503 conflict_retry and a short Retry-After. The
// transaction rolled back as a whole (for a create, its idempotency key too),
// so retrying, with the same key, is safe. The code is not
// insufficient_capacity: capacity was not the problem, and a dashboard or
// client must be able to tell the two apart. It is logged at Error because
// with the documented lock order a deadlock is a bug (ADR-0001 §The create
// transaction); the error is never sent to the client.
func (h *Handler) conflictRetry(w http.ResponseWriter, op string, err error, attrs ...any) {
	h.log.Error(op+" aborted by a database deadlock (40P01); answered 503", append(attrs, "err", err)...)
	w.Header().Set("Retry-After", retryAfterConflictSeconds)
	writeError(w, http.StatusServiceUnavailable, codeConflictRetry,
		"the request conflicted with another one and was rolled back; retry")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line is already sent; a write error means the client went
	// away and there is nobody left to tell.
	_ = json.NewEncoder(w).Encode(v)
}
