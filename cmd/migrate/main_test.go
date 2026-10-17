package main

import "testing"

func TestRunRequiresDatabaseURL(t *testing.T) {
	if err := run(t.Context(), ""); err == nil {
		t.Fatal("run with empty DATABASE_URL: want error, got nil")
	}
}
