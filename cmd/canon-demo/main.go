package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jeffplourde/canon"
)

type summary struct {
	Root                          string   `json:"root"`
	ConcurrentStatuses            []string `json:"concurrent_statuses"`
	DuplicateStatus               string   `json:"duplicate_status"`
	ConflictCount                 int      `json:"conflict_count"`
	ConflictPreservesClaims       bool     `json:"conflict_preserves_claims"`
	DuplicateRedirects            int      `json:"duplicate_redirects"`
	Tombstones                    int      `json:"tombstones"`
	Candidates                    int      `json:"same_fact_different_key_candidates"`
	CanonicalNotes                []string `json:"canonical_notes"`
	ProjectionClaimLines          int      `json:"projection_claim_lines"`
	ProjectionContainsCurrentFact bool     `json:"projection_contains_current_fact"`
	SearchLicenseResults          int      `json:"search_license_results"`
	SupersedeProjectionLines      int      `json:"supersede_projection_lines"`
	SupersedeSearchValues         []string `json:"supersede_search_values"`
	TwoFileScanImported           int      `json:"two_file_scan_imported"`
	TwoFileScanConflicts          int      `json:"two_file_scan_conflicts"`
	DivergenceNotes               []string `json:"entity_divergence_notes"`
	DivergenceSearchResults       int      `json:"different_spelling_search_results"`
	SilentClobbers                int      `json:"silent_clobbers"`
	Passed                        bool     `json:"passed"`
}

func main() {
	rootFlag := flag.String("root", "", "folder for the demo; defaults to a temp dir")
	keep := flag.Bool("keep", false, "keep temp demo folder")
	flag.Parse()

	root := *rootFlag
	var cleanup func()
	if root == "" {
		dir, err := os.MkdirTemp("", "canon-demo-*")
		if err != nil {
			fail(err)
		}
		root = dir
		cleanup = func() {
			if !*keep {
				_ = os.RemoveAll(dir)
			}
		}
	} else {
		cleanup = func() {}
	}
	defer cleanup()

	if err := os.MkdirAll(root, 0o755); err != nil {
		fail(err)
	}
	got, err := run(root)
	if err != nil {
		fail(err)
	}
	data, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		fail(err)
	}
	fmt.Println(string(data))
	if !got.Passed {
		os.Exit(1)
	}
}

func run(root string) (summary, error) {
	mainRoot := filepath.Join(root, "main")
	if err := os.MkdirAll(mainRoot, 0o755); err != nil {
		return summary{}, err
	}
	store, err := canon.Open(mainRoot)
	if err != nil {
		return summary{}, err
	}

	var wg sync.WaitGroup
	results := make(chan canon.PutResult, 3)
	errs := make(chan error, 3)
	for _, in := range []canon.ClaimInput{
		{Entity: "Project Canon", Key: "license", Value: "Apache-2.0", Provenance: "alice"},
		{Entity: "Project Canon", Key: "license", Value: "AGPL-3.0", Provenance: "bob"},
	} {
		wg.Add(1)
		go func(in canon.ClaimInput) {
			defer wg.Done()
			got, err := store.PutClaim(in)
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
		return summary{}, err
	}

	var statuses []string
	var conflictID string
	for got := range results {
		statuses = append(statuses, got.Status)
		if got.Conflict != nil {
			conflictID = got.Conflict.ID
		}
	}
	if conflictID == "" {
		return summary{}, errors.New("demo produced no conflict")
	}

	current, err := store.Search("license")
	if err != nil {
		return summary{}, err
	}
	if len(current) != 1 {
		return summary{}, fmt.Errorf("search license returned %d current claims, want 1", len(current))
	}
	dup, err := store.PutClaim(canon.ClaimInput{
		Entity:     "Project Canon",
		Key:        "license",
		Value:      current[0].Value,
		Provenance: "carol",
	})
	if err != nil {
		return summary{}, err
	}

	_, err = store.PutClaim(canon.ClaimInput{
		Entity:     "Project Canon",
		Key:        "title",
		Value:      "OSS wedge",
		Provenance: "alice",
	})
	if err != nil {
		return summary{}, err
	}
	_, err = store.PutClaim(canon.ClaimInput{
		Entity:     "Project Canon",
		Key:        "role",
		Value:      "OSS wedge",
		Provenance: "bob",
	})
	if err != nil {
		return summary{}, err
	}

	idx, err := store.Rebuild()
	if err != nil {
		return summary{}, err
	}
	conflict := idx.Conflicts[conflictID]
	preserves := conflict.Existing.Provenance != "" && conflict.Incoming.Provenance != "" &&
		conflict.Existing.Value != conflict.Incoming.Value

	notes, err := filepath.Glob(filepath.Join(mainRoot, "*.md"))
	if err != nil {
		return summary{}, err
	}
	for i := range notes {
		notes[i] = filepath.Base(notes[i])
	}
	noteBody, err := os.ReadFile(filepath.Join(mainRoot, "project-canon.md"))
	if err != nil {
		return summary{}, err
	}
	searchLicense, err := store.Search("license")
	if err != nil {
		return summary{}, err
	}
	supersedeLines, supersedeValues, err := runSupersedeProbe(filepath.Join(root, "supersede"))
	if err != nil {
		return summary{}, err
	}
	scanImported, scanConflicts, err := runTwoFileScanProbe(filepath.Join(root, "scan"))
	if err != nil {
		return summary{}, err
	}
	divergenceNotes, divergenceSearch, err := runDivergenceProbe(filepath.Join(root, "divergence"))
	if err != nil {
		return summary{}, err
	}

	out := summary{
		Root:                          root,
		ConcurrentStatuses:            statuses,
		DuplicateStatus:               dup.Status,
		ConflictCount:                 len(idx.Conflicts),
		ConflictPreservesClaims:       preserves,
		DuplicateRedirects:            len(idx.Redirects),
		Tombstones:                    len(idx.Tombstones),
		Candidates:                    len(idx.Candidates),
		CanonicalNotes:                notes,
		ProjectionClaimLines:          strings.Count(string(noteBody), "canon-claim"),
		ProjectionContainsCurrentFact: strings.Contains(string(noteBody), current[0].Value),
		SearchLicenseResults:          len(searchLicense),
		SupersedeProjectionLines:      supersedeLines,
		SupersedeSearchValues:         supersedeValues,
		TwoFileScanImported:           scanImported,
		TwoFileScanConflicts:          scanConflicts,
		DivergenceNotes:               divergenceNotes,
		DivergenceSearchResults:       divergenceSearch,
		SilentClobbers:                silentClobbers(idx),
	}
	out.Passed = out.ConflictCount == 1 &&
		out.ConflictPreservesClaims &&
		out.DuplicateRedirects >= 1 &&
		out.Tombstones >= 1 &&
		out.Candidates >= 1 &&
		len(out.CanonicalNotes) == 1 &&
		out.CanonicalNotes[0] == "project-canon.md" &&
		out.ProjectionClaimLines == 3 &&
		out.ProjectionContainsCurrentFact &&
		out.SearchLicenseResults == 1 &&
		out.SupersedeProjectionLines == 1 &&
		len(out.SupersedeSearchValues) == 1 &&
		out.SupersedeSearchValues[0] == "green" &&
		out.TwoFileScanImported == 2 &&
		out.TwoFileScanConflicts == 2 &&
		len(out.DivergenceNotes) == 2 &&
		out.DivergenceSearchResults == 2 &&
		out.SilentClobbers == 0 &&
		countStatus(out.ConcurrentStatuses, canon.ResultInserted) == 1 &&
		countStatus(out.ConcurrentStatuses, canon.ResultConflicted) == 1 &&
		out.DuplicateStatus == canon.ResultCollapsed
	return out, nil
}

func runSupersedeProbe(root string) (int, []string, error) {
	store, err := canon.Open(root)
	if err != nil {
		return 0, nil, err
	}
	first, err := store.PutClaim(canon.ClaimInput{Entity: "Light", Key: "color", Value: "red", Provenance: "alice"})
	if err != nil {
		return 0, nil, err
	}
	conflicted, err := store.PutClaim(canon.ClaimInput{Entity: "Light", Key: "color", Value: "blue", Provenance: "bob", BaseHash: first.IndexHash})
	if err != nil {
		return 0, nil, err
	}
	if _, err := store.ResolveConflict(conflicted.Conflict.ID, "", "green", "jeff"); err != nil {
		return 0, nil, err
	}
	body, err := os.ReadFile(filepath.Join(root, "light.md"))
	if err != nil {
		return 0, nil, err
	}
	claims, err := store.Search("color")
	if err != nil {
		return 0, nil, err
	}
	values := make([]string, 0, len(claims))
	for _, claim := range claims {
		values = append(values, claim.Value)
	}
	return strings.Count(string(body), "canon-claim"), values, nil
}

func runTwoFileScanProbe(root string) (int, int, error) {
	store, err := canon.Open(root)
	if err != nil {
		return 0, 0, err
	}
	if _, err := store.PutClaim(canon.ClaimInput{Entity: "Acme", Key: "status", Value: "active", Provenance: "alice"}); err != nil {
		return 0, 0, err
	}
	if _, err := store.PutClaim(canon.ClaimInput{Entity: "Beta", Key: "status", Value: "active", Provenance: "bob"}); err != nil {
		return 0, 0, err
	}
	for _, file := range []string{"acme.md", "beta.md"} {
		path := filepath.Join(root, file)
		data, err := os.ReadFile(path)
		if err != nil {
			return 0, 0, err
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(data), "active", "paused", 1)), 0o644); err != nil {
			return 0, 0, err
		}
	}
	scan, err := store.ScanExternalChanges("human-edit")
	if err != nil {
		return 0, 0, err
	}
	idx, err := store.Rebuild()
	if err != nil {
		return 0, 0, err
	}
	return scan.Imported, len(idx.Conflicts), nil
}

func runDivergenceProbe(root string) ([]string, int, error) {
	store, err := canon.Open(root)
	if err != nil {
		return nil, 0, err
	}
	if _, err := store.PutClaim(canon.ClaimInput{Entity: "Acme", Key: "founding date", Value: "May 1, 2024", Provenance: "alice"}); err != nil {
		return nil, 0, err
	}
	if _, err := store.PutClaim(canon.ClaimInput{Entity: "Acme Inc", Key: "first day", Value: "2024-05-01", Provenance: "bob"}); err != nil {
		return nil, 0, err
	}
	notes, err := filepath.Glob(filepath.Join(root, "*.md"))
	if err != nil {
		return nil, 0, err
	}
	for i := range notes {
		notes[i] = filepath.Base(notes[i])
	}
	claims, err := store.Search("2024")
	if err != nil {
		return nil, 0, err
	}
	return notes, len(claims), nil
}

func silentClobbers(idx canon.Index) int {
	var n int
	for _, conflict := range idx.Conflicts {
		if conflict.Existing.ID == "" || conflict.Incoming.ID == "" {
			n++
		}
	}
	for _, candidate := range idx.Candidates {
		if candidate.Left.ID == "" || candidate.Right.ID == "" {
			n++
		}
	}
	return n
}

func countStatus(statuses []string, want string) int {
	var n int
	for _, status := range statuses {
		if status == want {
			n++
		}
	}
	return n
}

func fail(err error) {
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		msg = "unknown error"
	}
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
