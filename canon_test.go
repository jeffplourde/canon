package canon

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	var mu sync.Mutex
	s.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
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
		t.Fatalf("claims = %d, want original + duplicate claim event; the conflicting claim is preserved in the conflict", len(idx.Claims))
	}
	if len(idx.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(idx.Conflicts))
	}
	if idx.Redirects[dup.Claim.ID] != first.Claim.ID {
		t.Fatalf("redirect missing: %+v", idx.Redirects)
	}
}

func TestDifferentKeysSameFactDoNotAutoCollapse(t *testing.T) {
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
	if dup.Status != ResultInserted {
		t.Fatalf("status = %q, want inserted for different key without declared alias", dup.Status)
	}
	if dup.RedirectTo != "" || dup.Claim.DuplicateOf != "" {
		t.Fatalf("different key auto-collapsed: %+v", dup)
	}
}

func TestDifferentKeysDifferentSpellingsDoNotSilentlyMerge(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{
		Entity:     "Acme",
		Key:        "founding date",
		Value:      "May 1, 2024",
		Provenance: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PutClaim(ClaimInput{
		Entity:     "Acme",
		Key:        "first day",
		Value:      "2024-05-01",
		Provenance: "agent-b",
		BaseHash:   first.IndexHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ResultInserted {
		t.Fatalf("status = %q, want a separate claim until an alias is declared", got.Status)
	}
}

func TestEqualValuesUnderDifferentKeysDoNotCollapse(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Acme", Key: "employees", Value: "100", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PutClaim(ClaimInput{Entity: "Acme", Key: "offices", Value: "100", Provenance: "agent-b", BaseHash: first.IndexHash})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ResultInserted || got.RedirectTo != "" {
		t.Fatalf("employees/offices equal value collapsed: %+v", got)
	}
	idx, err := s.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Current) != 2 {
		t.Fatalf("current slots = %d, want employees and offices held separately", len(idx.Current))
	}
	if len(idx.Redirects) != 0 {
		t.Fatalf("redirects = %v, want none for distinct keys", idx.Redirects)
	}
}

func TestConcurrentPutClaimsSerializeToConflict(t *testing.T) {
	s := testStore(t)
	var wg sync.WaitGroup
	results := make(chan PutResult, 2)
	errs := make(chan error, 2)
	for _, in := range []ClaimInput{
		{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "alice"},
		{Entity: "Canon", Key: "status", Value: "debating", Provenance: "bob"},
	} {
		wg.Add(1)
		go func(in ClaimInput) {
			defer wg.Done()
			got, err := s.PutClaim(in)
			if err != nil {
				errs <- err
				return
			}
			results <- got
		}(in)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for got := range results {
		counts[got.Status]++
	}
	if counts[ResultInserted] != 1 || counts[ResultConflicted] != 1 {
		t.Fatalf("statuses = %+v, want one inserted and one conflicted", counts)
	}
	idx, err := s.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(idx.Conflicts))
	}
}

func TestRebuildRepairsRacyLogAsConflict(t *testing.T) {
	s := testStore(t)
	a := newClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "alice"}, s.now())
	b := newClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "debating", Provenance: "bob"}, s.now())
	if err := s.appendEvent(Event{Type: EventClaim, ID: a.ID, TS: a.TS, Claim: &a}); err != nil {
		t.Fatal(err)
	}
	if err := s.appendEvent(Event{Type: EventClaim, ID: b.ID, TS: b.TS, Claim: &b}); err != nil {
		t.Fatal(err)
	}
	idx, err := s.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want replay-created conflict", len(idx.Conflicts))
	}
	if idx.Current[slotKey(a.CanonicalEntity, a.CanonicalKey)] != a.ID {
		t.Fatalf("current was overwritten by racy claim: %+v", idx.Current)
	}
	note := readFile(t, filepath.Join(s.root, "canon.md"))
	if strings.Count(note, "canon-claim") != 1 || strings.Contains(note, "debating") {
		t.Fatalf("projection leaked conflicted claim:\n%s", note)
	}
	claims, err := s.Search("status")
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Value != "ratified" {
		t.Fatalf("search claims = %+v, want only current ratified fact", claims)
	}
	files, err := filepath.Glob(filepath.Join(s.root, "conflicts", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("conflict files = %d, want 1", len(files))
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

func TestResolveConflictChooseIncomingLeavesOneProjectedFact(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "license", Value: "AGPL-3.0", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	conflicted, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "license", Value: "Apache-2.0", Provenance: "agent-b", BaseHash: first.IndexHash})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveConflict(conflicted.Conflict.ID, conflicted.Conflict.Incoming.ID, "", "jeff"); err != nil {
		t.Fatal(err)
	}
	note := readFile(t, filepath.Join(s.root, "canon.md"))
	if strings.Count(note, "canon-claim") != 1 {
		t.Fatalf("projection has multiple facts after choose-incoming:\n%s", note)
	}
	if strings.Contains(note, "AGPL") || !strings.Contains(note, "Apache-2.0") {
		t.Fatalf("projection did not keep only incoming value:\n%s", note)
	}
	claims, err := s.Search("license")
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Value != "Apache-2.0" {
		t.Fatalf("search claims = %+v, want only chosen incoming", claims)
	}
}

func TestResolveConflictSupersedeLeavesOneProjectedFact(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "color", Value: "red", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	conflicted, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "color", Value: "blue", Provenance: "agent-b", BaseHash: first.IndexHash})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveConflict(conflicted.Conflict.ID, "", "green", "jeff"); err != nil {
		t.Fatal(err)
	}
	note := readFile(t, filepath.Join(s.root, "canon.md"))
	if strings.Count(note, "canon-claim") != 1 {
		t.Fatalf("projection has multiple facts after supersede:\n%s", note)
	}
	if strings.Contains(note, "red") || strings.Contains(note, "blue") || !strings.Contains(note, "green") {
		t.Fatalf("projection did not keep only superseding value:\n%s", note)
	}
	claims, err := s.Search("color")
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Value != "green" {
		t.Fatalf("search claims = %+v, want only superseding value", claims)
	}
}

func TestRepeatedReassertGetsUniqueClaimID(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	dup, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "agent-a", BaseHash: first.IndexHash})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Claim.ID == first.Claim.ID {
		t.Fatalf("duplicate reassert reused claim id %q", dup.Claim.ID)
	}
	if dup.RedirectTo != first.Claim.ID {
		t.Fatalf("duplicate redirect = %q, want %q", dup.RedirectTo, first.Claim.ID)
	}
}

func TestRetractDeletesProjection(t *testing.T) {
	s := testStore(t)
	first, err := s.PutClaim(ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.root, "canon.md")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetractClaim(first.Claim.ID, "cleanup"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("projection still exists after retract: err=%v", err)
	}
}

func TestScanExternalChangesSnapshotsAllFilesBeforeImport(t *testing.T) {
	s := testStore(t)
	if _, err := s.PutClaim(ClaimInput{Entity: "Acme", Key: "status", Value: "active", Provenance: "agent-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutClaim(ClaimInput{Entity: "Beta", Key: "status", Value: "active", Provenance: "agent-b"}); err != nil {
		t.Fatal(err)
	}
	acmePath := filepath.Join(s.root, "acme.md")
	betaPath := filepath.Join(s.root, "beta.md")
	writeFile(t, acmePath, strings.Replace(readFile(t, acmePath), "active", "paused", 1))
	writeFile(t, betaPath, strings.Replace(readFile(t, betaPath), "active", "paused", 1))

	got, err := s.ScanExternalChanges("human-edit")
	if err != nil {
		t.Fatal(err)
	}
	if got.Imported != 2 {
		t.Fatalf("imported = %d, want 2; result=%+v", got.Imported, got)
	}
	idx, err := s.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Conflicts) != 2 {
		t.Fatalf("conflicts = %d, want 2 after two-file scan", len(idx.Conflicts))
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

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
