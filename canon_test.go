package canon

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 23, 2, 0, 0, 0, time.UTC)
	s.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	return s
}

func TestPutClaimInsertCollapseConflict(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{
		Entity:     "Project Canon",
		Key:        "status",
		Value:      "ratified",
		Provenance: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != ResultInserted {
		t.Fatalf("first status = %q, want inserted", first.Status)
	}

	dup, err := s.PutClaim(ClaimInput{
		Entity:     "project canon",
		Key:        "Status",
		Value:      "Ratified",
		Provenance: "agent-b",
		BaseHash:   first.IndexHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Status != ResultCollapsed || dup.RedirectTo != first.Claim.ID {
		t.Fatalf("duplicate result = %+v, want collapse redirect to %s", dup, first.Claim.ID)
	}

	conflicted, err := s.PutClaim(ClaimInput{
		Entity:     "Project Canon",
		Key:        "status",
		Value:      "still debating",
		Provenance: "agent-c",
		BaseHash:   dup.IndexHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if conflicted.Status != ResultConflicted || conflicted.Conflict == nil {
		t.Fatalf("conflict result = %+v, want conflict", conflicted)
	}
	if conflicted.Conflict.Existing.Value != "ratified" || conflicted.Conflict.Incoming.Value != "still debating" {
		t.Fatalf("conflict did not preserve both values: %+v", conflicted.Conflict)
	}

	idx, err := s.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Claims) != 2 {
		t.Fatalf("claims = %d, want original + duplicate claim event; conflict candidate is preserved in conflict", len(idx.Claims))
	}
	if len(idx.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(idx.Conflicts))
	}
	if idx.Redirects[dup.Claim.ID] != first.Claim.ID {
		t.Fatalf("redirect missing: %+v", idx.Redirects)
	}
}

func TestDifferentKeysSameFactCollapseDeterministically(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{
		Entity:     "Acme Inc",
		Key:        "founding date",
		Value:      "2024-05-01",
		Provenance: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	dup, err := s.PutClaim(ClaimInput{
		Entity:     "ACME Inc.",
		Key:        "first day",
		Value:      "2024-05-01",
		Provenance: "agent-b",
		BaseHash:   first.IndexHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Status != ResultCollapsed {
		t.Fatalf("status = %q, want collapsed for same fact under different key", dup.Status)
	}
	if dup.Claim.DuplicateReason != "same_entity_equal_value_different_key" {
		t.Fatalf("duplicate reason = %q", dup.Claim.DuplicateReason)
	}
	if dup.RedirectTo != first.Claim.ID {
		t.Fatalf("redirect = %q, want %q", dup.RedirectTo, first.Claim.ID)
	}
}

func TestDifferentPhrasingEquivalentKeyConflicts(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{
		Entity:     "Acme",
		Key:        "headquarters",
		Value:      "Paris",
		Provenance: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PutClaim(ClaimInput{
		Entity:     "Acme",
		Key:        "HQ location",
		Value:      "Berlin",
		Provenance: "agent-b",
		BaseHash:   first.IndexHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ResultConflicted {
		t.Fatalf("status = %q, want conflict for equivalent key with different value", got.Status)
	}
	if got.Conflict == nil || got.Conflict.Existing.Provenance != "agent-a" || got.Conflict.Incoming.Provenance != "agent-b" {
		t.Fatalf("conflict provenance not preserved: %+v", got.Conflict)
	}
}

func TestCASPreventsLWWClobber(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{
		Entity:     "User Jeff",
		Key:        "prefers",
		Value:      "concise reports",
		Provenance: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutClaim(ClaimInput{
		Entity:     "Other",
		Key:        "status",
		Value:      "active",
		Provenance: "agent-b",
		BaseHash:   first.IndexHash,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = s.PutClaim(ClaimInput{
		Entity:     "User Jeff",
		Key:        "prefers",
		Value:      "detailed reports",
		Provenance: "agent-c",
		BaseHash:   first.IndexHash,
	})
	if !errors.Is(err, ErrCASMismatch) {
		t.Fatalf("err = %v, want CAS mismatch", err)
	}
}

func TestResolveConflictAndRetractAreRecorded(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "license", Value: "Apache-2.0", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	conflicted, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "license", Value: "AGPL-3.0", Provenance: "agent-b", BaseHash: first.IndexHash})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.ResolveConflict(conflicted.Conflict.ID, first.Claim.ID, "", "jeff")
	if err != nil {
		t.Fatal(err)
	}
	if res.ConflictID != conflicted.Conflict.ID {
		t.Fatalf("resolution = %+v", res)
	}
	ret, err := s.RetractClaim(first.Claim.ID, "cleanup")
	if err != nil {
		t.Fatal(err)
	}
	if ret.ClaimID != first.Claim.ID {
		t.Fatalf("retraction = %+v", ret)
	}
	idx, err := s.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Conflicts[conflicted.Conflict.ID].Resolved {
		t.Fatalf("conflict not marked resolved: %+v", idx.Conflicts[conflicted.Conflict.ID])
	}
	if !idx.Claims[first.Claim.ID].Retracted {
		t.Fatalf("claim not marked retracted: %+v", idx.Claims[first.Claim.ID])
	}
}

func TestProjectionAndConflictFilesAreLegible(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "promise", Value: "A folder does not lose truth", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	conflicted, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "promise", Value: "A memory server", Provenance: "agent-b", BaseHash: first.IndexHash})
	if err != nil {
		t.Fatal(err)
	}
	note := readFile(t, filepath.Join(s.root, "canon.md"))
	if !strings.Contains(note, "canon-claim") || !strings.Contains(note, "A folder does not lose truth") {
		t.Fatalf("projection missing claim block:\n%s", note)
	}
	conflict := readFile(t, filepath.Join(s.root, "conflicts", conflicted.Conflict.ID+".md"))
	if !strings.Contains(conflict, "A folder does not lose truth") || !strings.Contains(conflict, "A memory server") {
		t.Fatalf("conflict projection did not preserve both values:\n%s", conflict)
	}
}

func TestEventLogIsAppendOnlyJSONL(t *testing.T) {
	s := testStore(t)
	if _, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "agent-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "agent-b"}); err != nil {
		t.Fatal(err)
	}
	data := readFile(t, s.canonPath(eventLogFile))
	lines := strings.Split(strings.TrimSpace(data), "\n")
	if len(lines) != 2 {
		t.Fatalf("event lines = %d, want 2\n%s", len(lines), data)
	}
	if !strings.Contains(lines[0], `"type":"claim"`) || !strings.Contains(lines[1], `"duplicate_of"`) {
		t.Fatalf("event log did not record claim + duplicate claim:\n%s", data)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
