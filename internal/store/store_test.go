package store

import (
	"regexp"
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
