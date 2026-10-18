package store

import (
	"strings"
	"testing"
)

// The node sync SQL must never name the allocation column (D15 column
// ownership, invariant 7): only placement and release write it.
func TestSyncNodeSQLNeverMentionsAllocated(t *testing.T) {
	if strings.Contains(strings.ToLower(syncNodeSQL), "allocated") {
		t.Fatalf("syncNodeSQL mentions allocated:\n%s", syncNodeSQL)
	}
}
