package store

import (
	"crypto/sha256"
	"regexp"
	"strings"
	"testing"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewEventIDIsUniqueUUIDv4(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := newEventID()
		if !uuidV4.MatchString(id) {
			t.Fatalf("newEventID() = %q, want a canonical version 4 UUID", id)
		}
		if seen[id] {
			t.Fatalf("newEventID() returned %q twice", id)
		}
		seen[id] = true
	}
}

// TestCreateRequiresKeyAndHash: a create without a key or with a malformed
// hash is refused before any database work (the store has no DB here), so
// no create can bypass idempotency (invariant 2).
func TestCreateRequiresKeyAndHash(t *testing.T) {
	hash := sha256.Sum256([]byte("body"))
	tests := []struct {
		name string
		key  string
		hash []byte
		want string
	}{
		{name: "no key", key: "", hash: hash[:], want: "idempotency key is required"},
		{name: "no hash", key: "k1", hash: nil, want: "request hash is 0 bytes"},
		{name: "short hash", key: "k1", hash: hash[:16], want: "request hash is 16 bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(nil, nil)
			_, err := s.Create(t.Context(), CreateRequest{
				ID: "inst_x", TenantID: "t1", GPUType: "H100", GPUCount: 1,
				IdempotencyKey: tc.key, RequestHash: tc.hash,
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Create error = %v, want one containing %q", err, tc.want)
			}
		})
	}
}
