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
	if got.ProjectionClaimLines != 3 || got.SearchLicenseResults != 1 {
		t.Fatalf("projection/search = lines %d search %d", got.ProjectionClaimLines, got.SearchLicenseResults)
	}
	if got.SupersedeProjectionLines != 1 || len(got.SupersedeSearchValues) != 1 || got.SupersedeSearchValues[0] != "green" {
		t.Fatalf("supersede probe = lines %d values %v", got.SupersedeProjectionLines, got.SupersedeSearchValues)
	}
	if got.TwoFileScanImported != 2 || got.TwoFileScanConflicts != 2 {
		t.Fatalf("two-file scan = imported %d conflicts %d", got.TwoFileScanImported, got.TwoFileScanConflicts)
	}
	if len(got.DivergenceNotes) != 2 || got.DivergenceSearchResults != 2 {
		t.Fatalf("divergence probe = notes %v search %d", got.DivergenceNotes, got.DivergenceSearchResults)
	}
}
