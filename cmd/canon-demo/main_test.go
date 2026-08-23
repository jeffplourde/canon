package main

import (
	"testing"

	"github.com/jeffplourde/canon"
)

func TestDemoGate(t *testing.T) {
	got, err := run(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Passed {
		t.Fatalf("demo failed: %+v", got)
	}
	if countStatus(got.ConcurrentStatuses, canon.ResultInserted) != 1 ||
		countStatus(got.ConcurrentStatuses, canon.ResultConflicted) != 1 {
		t.Fatalf("concurrent statuses = %v, want inserted/conflicted", got.ConcurrentStatuses)
	}
	if got.DuplicateStatus != canon.ResultCollapsed {
		t.Fatalf("duplicate status = %q, want collapsed", got.DuplicateStatus)
	}
	if got.ConflictCount != 1 || !got.ConflictPreservesClaims {
		t.Fatalf("conflict result = count %d preserves %v", got.ConflictCount, got.ConflictPreservesClaims)
	}
	if got.Candidates != 1 {
		t.Fatalf("same-fact-different-key candidates = %d, want 1", got.Candidates)
	}
	if got.SilentClobbers != 0 {
		t.Fatalf("silent clobbers = %d", got.SilentClobbers)
	}
	if len(got.CanonicalNotes) != 1 || got.CanonicalNotes[0] != "project-canon.md" {
		t.Fatalf("canonical notes = %v", got.CanonicalNotes)
	}
}
