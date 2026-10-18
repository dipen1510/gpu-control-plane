package store

import (
	"strings"
	"testing"
)

// The node sync SQL must never name the allocation column (D15 column
// ownership, invariant 7): only placement and release write it.
func TestSyncNodeSQLNeverMentionsAllocated(t *testing.T) {
	for name, sql := range map[string]string{
		"syncNodeSQL": syncNodeSQL, "markNotReadySQL": markNotReadySQL,
		"sweepNodesSQL": sweepNodesSQL, "nodeCountsSQL": nodeCountsSQL,
	} {
		if strings.Contains(strings.ToLower(sql), "allocated") {
			t.Fatalf("%s mentions allocated:\n%s", name, sql)
		}
		// Every write sets status explicitly (the column default is READY).
		if name != "nodeCountsSQL" && !strings.Contains(sql, "status") {
			t.Fatalf("%s does not set status", name)
		}
	}
}
