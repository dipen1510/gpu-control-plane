// Package api is the HTTP edge of the apiserver (ADR-0001 §API (M1)). It
// validates requests, calls the store, and maps domain errors to HTTP status
// codes. It holds no business rules: quota, placement and state live in the
// store, which runs each request as one transaction.
package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dipen1510/gpu-control-plane/internal/store"
)

// Store is what the handlers need from the store. *store.Store satisfies it.
type Store interface {
	Create(ctx context.Context, req store.CreateRequest) (store.Instance, error)
	Get(ctx context.Context, tenantID, id string) (store.Instance, error)
}

const (
	headerTenant         = "X-Tenant-ID"
	headerIdempotencyKey = "Idempotency-Key"

	// maxBodyBytes bounds the create body; a valid one is ~40 bytes.
	maxBodyBytes = 4 << 10

	// requestTimeout bounds the database work of one request, so a slow or
	// stuck Postgres fails the request instead of piling up goroutines.
	requestTimeout = 10 * time.Second
)

// Error codes in the {"error": {"code", "message"}} envelope.
const (
	codeInvalidRequest = "invalid_request"
	codeNotFound       = "not_found"
	codeInternal       = "internal"
)

// D1: one instance per request, fitting on a single node.
var (
	validGPUTypes  = map[string]bool{"H100": true, "A100": true}
	validGPUCounts = map[int]bool{1: true, 2: true, 4: true, 8: true}
)

// Handler serves the /v1 instance API.
type Handler struct {
	store Store
	log   *slog.Logger
	mux   *http.ServeMux
}

// NewHandler returns the API's HTTP handler.
func NewHandler(s Store, log *slog.Logger) *Handler {
	h := &Handler{store: s, log: log, mux: http.NewServeMux()}
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
	tenant := r.Header.Get(headerTenant)
	if tenant == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "missing "+headerTenant+" header")
		return
	}
	key := r.Header.Get(headerIdempotencyKey)
	if key == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "missing "+headerIdempotencyKey+" header")
		return
	}
	body, err := decodeCreate(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	inst, err := h.store.Create(ctx, store.CreateRequest{
		ID: newInstanceID(), TenantID: tenant, GPUType: body.GPUType, GPUCount: body.GPUCount,
		IdempotencyKey: key, // RequestHash: S1-8
	})
	if err != nil {
		// TODO(S1-7): ErrQuotaExceeded → 403 quota_exceeded,
		// ErrInsufficientCapacity → 503 insufficient_capacity + Retry-After.
		h.internalError(w, "create instance", err, "tenant", tenant,
			"gpu_type", body.GPUType, "gpu_count", body.GPUCount)
		return
	}
	h.log.Info("instance created", "instance_id", inst.ID, "tenant", tenant, "node_id", inst.NodeID,
		"state", inst.State, "resource_version", inst.ResourceVersion, "result", "scheduled")
	writeInstance(w, http.StatusAccepted, inst)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	tenant := r.Header.Get(headerTenant)
	if tenant == "" {
		writeError(w, http.StatusBadRequest, codeInvalidRequest, "missing "+headerTenant+" header")
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

// decodeCreate parses and validates the create body.
func decodeCreate(w http.ResponseWriter, r *http.Request) (createRequest, error) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	var body createRequest
	if err := dec.Decode(&body); err != nil {
		return createRequest{}, fmt.Errorf("malformed JSON body: %v", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return createRequest{}, errors.New("malformed JSON body: unexpected data after the object")
	}
	if !validGPUTypes[body.GPUType] {
		return createRequest{}, fmt.Errorf("gpu_type must be H100 or A100, got %q", body.GPUType)
	}
	if !validGPUCounts[body.GPUCount] {
		return createRequest{}, fmt.Errorf("gpu_count must be 1, 2, 4 or 8, got %d", body.GPUCount)
	}
	return body, nil
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
