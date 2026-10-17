package seed

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestNodes(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		wantH100 int
		wantA100 int
	}{
		{"one node is H100", 1, 1, 0},
		{"even count splits evenly", 10, 5, 5},
		{"odd count gives H100 the extra node", 7, 4, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes, err := Nodes(tc.n)
			if err != nil {
				t.Fatalf("Nodes(%d): %v", tc.n, err)
			}
			if len(nodes) != tc.n {
				t.Fatalf("got %d nodes, want %d", len(nodes), tc.n)
			}
			count := map[string]int{}
			ids := map[string]bool{}
			for _, nd := range nodes {
				count[nd.GPUType]++
				if ids[nd.ID] {
					t.Errorf("duplicate id %q", nd.ID)
				}
				ids[nd.ID] = true
				if nd.Capacity != 8 || nd.Status != "READY" || nd.FailureDomain == "" {
					t.Errorf("node %+v: want capacity 8, READY, failure domain set", nd)
				}
			}
			if count["H100"] != tc.wantH100 || count["A100"] != tc.wantA100 {
				t.Errorf("H100=%d A100=%d, want %d and %d",
					count["H100"], count["A100"], tc.wantH100, tc.wantA100)
			}
		})
	}
}

func TestNodesRejectsNonPositiveCount(t *testing.T) {
	for _, n := range []int{0, -3} {
		if _, err := Nodes(n); !errors.Is(err, ErrInvalidCount) {
			t.Errorf("Nodes(%d) error = %v, want ErrInvalidCount", n, err)
		}
	}
}

// A node's attributes must not depend on how many nodes are seeded, or
// re-seeding with a different N would retype or move existing nodes.
func TestNodesAreStableAcrossCounts(t *testing.T) {
	small, err := Nodes(5)
	if err != nil {
		t.Fatal(err)
	}
	large, err := Nodes(200)
	if err != nil {
		t.Fatal(err)
	}
	for i := range small {
		if small[i] != large[i] {
			t.Errorf("node %d differs: N=5 gives %+v, N=200 gives %+v", i+1, small[i], large[i])
		}
	}
}

func TestNodesSpreadEachTypeOverEveryRack(t *testing.T) {
	nodes, err := Nodes(2 * Racks)
	if err != nil {
		t.Fatal(err)
	}
	racks := map[string]map[string]bool{} // gpu type -> racks
	for _, nd := range nodes {
		if racks[nd.GPUType] == nil {
			racks[nd.GPUType] = map[string]bool{}
		}
		racks[nd.GPUType][nd.FailureDomain] = true
	}
	for _, gpu := range []string{"H100", "A100"} {
		if len(racks[gpu]) != Racks {
			t.Errorf("%s nodes are in %d racks, want %d", gpu, len(racks[gpu]), Racks)
		}
	}
}

// Guards D15 column ownership at the source: the conflict update list must
// not name allocated. The integration tests check the behavior too.
func TestUpsertSQLNeverSetsAllocated(t *testing.T) {
	_, update, ok := strings.Cut(upsertSQL, "DO UPDATE SET")
	if !ok {
		t.Fatal("upsertSQL has no DO UPDATE SET clause")
	}
	if regexp.MustCompile(`(?i)\ballocated\b`).MatchString(update) {
		t.Errorf("DO UPDATE SET mentions allocated:\n%s", update)
	}
	if regexp.MustCompile(`(?i)\ballocated\b`).MatchString(upsertSQL) {
		t.Errorf("upsertSQL mentions allocated; new rows must take the column default")
	}
}
