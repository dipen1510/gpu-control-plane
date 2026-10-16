package migrate

import (
	"testing"
	"testing/fstest"
)

func TestLoad(t *testing.T) {
	file := func(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }

	tests := []struct {
		name     string
		files    fstest.MapFS
		wantErr  bool
		wantVers []int64
	}{
		{
			name: "sorted by numeric version, not by file order",
			files: fstest.MapFS{
				"m/10_ten.sql":   file("SELECT 10"),
				"m/0002_two.sql": file("SELECT 2"),
				"m/1_one.sql":    file("SELECT 1"),
			},
			wantVers: []int64{1, 2, 10},
		},
		{
			name:    "name without version is rejected",
			files:   fstest.MapFS{"m/nodes.sql": file("")},
			wantErr: true,
		},
		{
			name:    "uppercase name is rejected",
			files:   fstest.MapFS{"m/0001_Nodes.sql": file("")},
			wantErr: true,
		},
		{
			name:    "version zero is rejected",
			files:   fstest.MapFS{"m/0000_zero.sql": file("")},
			wantErr: true,
		},
		{
			name: "duplicate version is rejected",
			files: fstest.MapFS{
				"m/0001_a.sql": file(""),
				"m/1_b.sql":    file(""),
			},
			wantErr: true,
		},
		{
			name:    "empty directory is rejected",
			files:   fstest.MapFS{"m/sub/0001_a.sql": file("")},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			migs, err := load(tc.files, "m")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("load: want error, got %d migrations", len(migs))
				}
				return
			}
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if len(migs) != len(tc.wantVers) {
				t.Fatalf("got %d migrations, want %d", len(migs), len(tc.wantVers))
			}
			for i, m := range migs {
				if m.Version != tc.wantVers[i] {
					t.Errorf("migs[%d].Version = %d, want %d", i, m.Version, tc.wantVers[i])
				}
			}
		})
	}
}

func TestEmbeddedMigrationsAreNumberedFromOne(t *testing.T) {
	migs, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	// Contiguous numbering makes a missing or renumbered file obvious in review.
	for i, m := range migs {
		if want := int64(i + 1); m.Version != want {
			t.Errorf("migration %d (%s) has version %d, want %d", i, m.Name, m.Version, want)
		}
		if m.SQL == "" {
			t.Errorf("migration %04d_%s is empty", m.Version, m.Name)
		}
	}
}

func TestChecksumIsStable(t *testing.T) {
	a, err := Checksum()
	if err != nil {
		t.Fatalf("Checksum: %v", err)
	}
	b, err := Checksum()
	if err != nil {
		t.Fatalf("Checksum: %v", err)
	}
	if a != b || len(a) != 16 {
		t.Fatalf("Checksum() = %q then %q; want equal 16-char values", a, b)
	}
}
