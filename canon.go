// Package canon implements the deterministic claim engine behind canon's
// folder projection.
package canon

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	EventClaim      = "claim"
	EventConflict   = "conflict"
	EventResolution = "resolution"
	EventRetraction = "retraction"

	ResultInserted   = "inserted"
	ResultCollapsed  = "collapsed"
	ResultConflicted = "conflicted"
	ResultRetracted  = "retracted"
	ResultResolved   = "resolved"

	canonDir     = ".canon"
	eventLogFile = "events.jsonl"
	indexFile    = "index.json"
	lockFile     = "events.lock"
)

var (
	ErrCASMismatch      = errors.New("canon: base hash mismatch")
	ErrConflictNotFound = errors.New("canon: conflict not found")
	ErrClaimNotFound    = errors.New("canon: claim not found")
)

type Store struct {
	root string
	now  func() time.Time
	mu   sync.Mutex
}

type ClaimInput struct {
	Entity     string `json:"entity"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	Provenance string `json:"provenance"`
	BaseHash   string `json:"base_hash,omitempty"`
}

type Claim struct {
	ID                 string    `json:"id"`
	Entity             string    `json:"entity"`
	CanonicalEntity    string    `json:"canonical_entity"`
	Key                string    `json:"key"`
	CanonicalKey       string    `json:"canonical_key"`
	Value              string    `json:"value"`
	Provenance         string    `json:"provenance"`
	TS                 time.Time `json:"ts"`
	Hash               string    `json:"hash"`
	ValueHash          string    `json:"value_hash"`
	DuplicateOf        string    `json:"duplicate_of,omitempty"`
	DuplicateReason    string    `json:"duplicate_reason,omitempty"`
	Retracted          bool      `json:"retracted,omitempty"`
	RetractedBy        string    `json:"retracted_by,omitempty"`
	ResolvedConflictID string    `json:"resolved_conflict_id,omitempty"`
}

type Conflict struct {
	ID           string    `json:"id"`
	Entity       string    `json:"entity"`
	CanonicalKey string    `json:"canonical_key"`
	Existing     Claim     `json:"existing"`
	Incoming     Claim     `json:"incoming"`
	TS           time.Time `json:"ts"`
	Resolved     bool      `json:"resolved,omitempty"`
	ResolutionID string    `json:"resolution_id,omitempty"`
}

type Resolution struct {
	ID               string    `json:"id"`
	ConflictID       string    `json:"conflict_id"`
	ChosenClaimID    string    `json:"chosen_claim_id,omitempty"`
	SupersedingValue string    `json:"superseding_value,omitempty"`
	Provenance       string    `json:"provenance"`
	TS               time.Time `json:"ts"`
	Hash             string    `json:"hash"`
}

type Retraction struct {
	ID         string    `json:"id"`
	ClaimID    string    `json:"claim_id"`
	Provenance string    `json:"provenance"`
	TS         time.Time `json:"ts"`
}

type ScanResult struct {
	FilesScanned int      `json:"files_scanned"`
	Imported     int      `json:"imported"`
	Skipped      int      `json:"skipped"`
	Ambiguous    []string `json:"ambiguous,omitempty"`
}

type Candidate struct {
	Entity string `json:"entity"`
	Left   Claim  `json:"left"`
	Right  Claim  `json:"right"`
	Reason string `json:"reason"`
}

type Event struct {
	Type       string      `json:"type"`
	ID         string      `json:"id"`
	TS         time.Time   `json:"ts"`
	Claim      *Claim      `json:"claim,omitempty"`
	Conflict   *Conflict   `json:"conflict,omitempty"`
	Resolution *Resolution `json:"resolution,omitempty"`
	Retraction *Retraction `json:"retraction,omitempty"`
}

type PutResult struct {
	Status     string    `json:"status"`
	Claim      Claim     `json:"claim"`
	Existing   *Claim    `json:"existing,omitempty"`
	Conflict   *Conflict `json:"conflict,omitempty"`
	RedirectTo string    `json:"redirect_to,omitempty"`
	IndexHash  string    `json:"index_hash"`
}

type Index struct {
	Version      int                   `json:"version"`
	Hash         string                `json:"hash"`
	Claims       map[string]Claim      `json:"claims"`
	Conflicts    map[string]Conflict   `json:"conflicts"`
	Candidates   []Candidate           `json:"candidates,omitempty"`
	Resolutions  map[string]Resolution `json:"resolutions"`
	EntityClaims map[string][]string   `json:"entity_claims"`
	Current      map[string]string     `json:"current"`
	Redirects    map[string]string     `json:"redirects"`
	Tombstones   map[string]string     `json:"tombstones"`
}

func Open(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("root is required")
	}
	s := &Store{root: root, now: func() time.Time { return time.Now().UTC() }}
	if err := os.MkdirAll(s.canonPath(), 0o755); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Root() string {
	return s.root
}

func (s *Store) PutClaim(in ClaimInput) (PutResult, error) {
	var out PutResult
	err := s.withWriteLock(func() error {
		var err error
		out, err = s.putClaimLocked(in)
		return err
	})
	return out, err
}

func (s *Store) putClaimLocked(in ClaimInput) (PutResult, error) {
	if err := validateClaim(in); err != nil {
		return PutResult{}, err
	}
	idx, err := s.rebuildLocked(true)
	if err != nil {
		return PutResult{}, err
	}
	if in.BaseHash != "" && in.BaseHash != idx.Hash {
		return PutResult{}, fmt.Errorf("%w: have %s want %s", ErrCASMismatch, idx.Hash, in.BaseHash)
	}

	now := s.now()
	claim := newClaim(in, now)
	slot := slotKey(claim.CanonicalEntity, claim.CanonicalKey)

	if existingID := idx.Current[slot]; existingID != "" {
		existing := idx.Claims[existingID]
		if valuesEqual(existing.Value, claim.Value) {
			claim.DuplicateOf = existing.ID
			claim.DuplicateReason = "same_entity_key_equal_value"
			if err := s.appendEvent(Event{Type: EventClaim, ID: claim.ID, TS: now, Claim: &claim}); err != nil {
				return PutResult{}, err
			}
			idx, err = s.rebuildLocked(true)
			if err != nil {
				return PutResult{}, err
			}
			return PutResult{Status: ResultCollapsed, Claim: claim, Existing: &existing, RedirectTo: existing.ID, IndexHash: idx.Hash}, nil
		}
		conflict := conflictFor(existing, claim, now)
		if err := s.appendEvent(Event{Type: EventConflict, ID: conflict.ID, TS: now, Conflict: &conflict}); err != nil {
			return PutResult{}, err
		}
		idx, err = s.rebuildLocked(true)
		if err != nil {
			return PutResult{}, err
		}
		return PutResult{Status: ResultConflicted, Claim: claim, Existing: &existing, Conflict: &conflict, IndexHash: idx.Hash}, nil
	}

	if err := s.appendEvent(Event{Type: EventClaim, ID: claim.ID, TS: now, Claim: &claim}); err != nil {
		return PutResult{}, err
	}
	idx, err = s.rebuildLocked(true)
	if err != nil {
		return PutResult{}, err
	}
	return PutResult{Status: ResultInserted, Claim: claim, IndexHash: idx.Hash}, nil
}

func (s *Store) ResolveConflict(conflictID, chosenClaimID, supersedingValue, provenance string) (Resolution, error) {
	var out Resolution
	err := s.withWriteLock(func() error {
		var err error
		out, err = s.resolveConflictLocked(conflictID, chosenClaimID, supersedingValue, provenance)
		return err
	})
	return out, err
}

func (s *Store) resolveConflictLocked(conflictID, chosenClaimID, supersedingValue, provenance string) (Resolution, error) {
	idx, err := s.rebuildLocked(true)
	if err != nil {
		return Resolution{}, err
	}
	conflict, ok := idx.Conflicts[conflictID]
	if !ok {
		return Resolution{}, ErrConflictNotFound
	}
	if conflict.Resolved {
		return idx.Resolutions[conflict.ResolutionID], nil
	}
	if chosenClaimID == "" && strings.TrimSpace(supersedingValue) == "" {
		return Resolution{}, fmt.Errorf("chosen claim or superseding value is required")
	}
	now := s.now()
	res := Resolution{
		ID:               idFor("resolution", conflictID, chosenClaimID, supersedingValue, provenance),
		ConflictID:       conflictID,
		ChosenClaimID:    chosenClaimID,
		SupersedingValue: strings.TrimSpace(supersedingValue),
		Provenance:       strings.TrimSpace(provenance),
		TS:               now,
	}
	res.Hash = hashJSON(res)
	if err := s.appendEvent(Event{Type: EventResolution, ID: res.ID, TS: now, Resolution: &res}); err != nil {
		return Resolution{}, err
	}
	if _, err := s.rebuildLocked(true); err != nil {
		return Resolution{}, err
	}
	return res, nil
}

func (s *Store) RetractClaim(claimID, provenance string) (Retraction, error) {
	var out Retraction
	err := s.withWriteLock(func() error {
		var err error
		out, err = s.retractClaimLocked(claimID, provenance)
		return err
	})
	return out, err
}

func (s *Store) retractClaimLocked(claimID, provenance string) (Retraction, error) {
	idx, err := s.rebuildLocked(true)
	if err != nil {
		return Retraction{}, err
	}
	if _, ok := idx.Claims[claimID]; !ok {
		return Retraction{}, ErrClaimNotFound
	}
	now := s.now()
	ret := Retraction{
		ID:         idFor("retraction", claimID, provenance, now.Format(time.RFC3339Nano)),
		ClaimID:    claimID,
		Provenance: strings.TrimSpace(provenance),
		TS:         now,
	}
	if err := s.appendEvent(Event{Type: EventRetraction, ID: ret.ID, TS: now, Retraction: &ret}); err != nil {
		return Retraction{}, err
	}
	if _, err := s.rebuildLocked(true); err != nil {
		return Retraction{}, err
	}
	return ret, nil
}

func (s *Store) Search(query string) ([]Claim, error) {
	var out []Claim
	err := s.withWriteLock(func() error {
		idx, err := s.rebuildLocked(true)
		if err != nil {
			return err
		}
		out = searchIndex(idx, query)
		return nil
	})
	return out, err
}

func searchIndex(idx Index, query string) []Claim {
	q := normalizeText(query)
	out := []Claim{}
	for _, slot := range sortedKeys(idx.Current) {
		c := idx.Claims[idx.Current[slot]]
		if c.Retracted {
			continue
		}
		if q == "" || strings.Contains(c.CanonicalEntity, q) || strings.Contains(c.CanonicalKey, q) || strings.Contains(normalizeText(c.Value), q) {
			out = append(out, c)
		}
	}
	return out
}

func (s *Store) ScanExternalChanges(provenance string) (ScanResult, error) {
	var out ScanResult
	err := s.withWriteLock(func() error {
		var err error
		out, err = s.scanExternalChangesLocked(provenance)
		return err
	})
	return out, err
}

func (s *Store) scanExternalChangesLocked(provenance string) (ScanResult, error) {
	if strings.TrimSpace(provenance) == "" {
		provenance = "scan_external_changes"
	}
	idx, err := s.rebuildLocked(false)
	if err != nil {
		return ScanResult{}, err
	}
	files, err := filepath.Glob(filepath.Join(s.root, "*.md"))
	if err != nil {
		return ScanResult{}, err
	}
	sort.Strings(files)
	type snapshot struct {
		path      string
		claims    []parsedProjectionClaim
		ambiguous bool
	}
	var snapshots []snapshot
	for _, path := range files {
		claims, ambiguous, err := parseProjectionFile(path)
		if err != nil {
			return ScanResult{}, err
		}
		snapshots = append(snapshots, snapshot{path: path, claims: claims, ambiguous: ambiguous})
	}
	var result ScanResult
	result.FilesScanned = len(snapshots)
	for _, snap := range snapshots {
		path := snap.path
		if snap.ambiguous {
			result.Ambiguous = append(result.Ambiguous, filepath.Base(path))
		}
		for _, c := range snap.claims {
			current, ok := idx.Claims[c.ID]
			if ok && current.Hash == c.Hash && valuesEqual(current.Value, c.Value) {
				result.Skipped++
				continue
			}
			_, err := s.putClaimLocked(ClaimInput{
				Entity:     c.Entity,
				Key:        c.Key,
				Value:      c.Value,
				Provenance: provenance + ":" + filepath.Base(path),
			})
			if err != nil {
				return ScanResult{}, err
			}
			result.Imported++
			idx, err = s.rebuildLocked(true)
			if err != nil {
				return ScanResult{}, err
			}
		}
	}
	if len(result.Ambiguous) > 0 {
		if err := s.writeImportInbox(result.Ambiguous); err != nil {
			return ScanResult{}, err
		}
	}
	if _, err := s.rebuildLocked(true); err != nil {
		return ScanResult{}, err
	}
	return result, nil
}

func (s *Store) Rebuild() (Index, error) {
	var idx Index
	err := s.withWriteLock(func() error {
		var err error
		idx, err = s.rebuildLocked(true)
		return err
	})
	return idx, err
}

func (s *Store) rebuildLocked(render bool) (Index, error) {
	events, err := s.readEvents()
	if err != nil {
		return Index{}, err
	}
	idx := newIndex()
	for _, ev := range events {
		switch ev.Type {
		case EventClaim:
			if ev.Claim == nil {
				return Index{}, fmt.Errorf("claim event %q missing claim", ev.ID)
			}
			c := *ev.Claim
			idx.Claims[c.ID] = c
			idx.EntityClaims[c.CanonicalEntity] = appendUnique(idx.EntityClaims[c.CanonicalEntity], c.ID)
			if c.DuplicateOf != "" {
				idx.Redirects[c.ID] = c.DuplicateOf
				idx.Tombstones[c.ID] = c.DuplicateReason
				continue
			}
			slot := slotKey(c.CanonicalEntity, c.CanonicalKey)
			if existingID := idx.Current[slot]; existingID != "" {
				existing := idx.Claims[existingID]
				if valuesEqual(existing.Value, c.Value) {
					c.DuplicateOf = existing.ID
					c.DuplicateReason = "replay_same_entity_key_equal_value"
					idx.Claims[c.ID] = c
					idx.Redirects[c.ID] = existing.ID
					idx.Tombstones[c.ID] = c.DuplicateReason
					continue
				}
				conflict := conflictFor(existing, c, c.TS)
				idx.Conflicts[conflict.ID] = conflict
				idx.Tombstones[c.ID] = "conflicted"
				continue
			}
			idx.Current[slot] = c.ID
		case EventConflict:
			if ev.Conflict == nil {
				return Index{}, fmt.Errorf("conflict event %q missing conflict", ev.ID)
			}
			conflict := *ev.Conflict
			idx.Conflicts[conflict.ID] = conflict
		case EventResolution:
			if ev.Resolution == nil {
				return Index{}, fmt.Errorf("resolution event %q missing resolution", ev.ID)
			}
			res := *ev.Resolution
			idx.Resolutions[res.ID] = res
			conflict, ok := idx.Conflicts[res.ConflictID]
			if !ok {
				return Index{}, fmt.Errorf("resolution %q references unknown conflict %q", res.ID, res.ConflictID)
			}
			conflict.Resolved = true
			conflict.ResolutionID = res.ID
			idx.Conflicts[conflict.ID] = conflict
			chosen := conflict.Existing
			loserID := conflict.Incoming.ID
			switch {
			case res.SupersedingValue != "":
				chosen = newClaim(ClaimInput{
					Entity:     conflict.Entity,
					Key:        conflict.CanonicalKey,
					Value:      res.SupersedingValue,
					Provenance: res.Provenance,
				}, res.TS)
				chosen.ResolvedConflictID = conflict.ID
				idx.Claims[chosen.ID] = chosen
				idx.EntityClaims[chosen.CanonicalEntity] = appendUnique(idx.EntityClaims[chosen.CanonicalEntity], chosen.ID)
				idx.Tombstones[conflict.Existing.ID] = "conflict_resolved"
				idx.Tombstones[conflict.Incoming.ID] = "conflict_resolved"
				for _, id := range []string{conflict.Existing.ID, conflict.Incoming.ID} {
					if claim, ok := idx.Claims[id]; ok {
						claim.Retracted = true
						claim.RetractedBy = res.ID
						idx.Claims[id] = claim
					}
				}
			case res.ChosenClaimID == conflict.Incoming.ID:
				chosen = conflict.Incoming
				idx.Claims[chosen.ID] = chosen
				idx.EntityClaims[chosen.CanonicalEntity] = appendUnique(idx.EntityClaims[chosen.CanonicalEntity], chosen.ID)
				loserID = conflict.Existing.ID
			case res.ChosenClaimID != "" && res.ChosenClaimID != conflict.Existing.ID:
				return Index{}, fmt.Errorf("resolution %q chose claim %q outside conflict %q", res.ID, res.ChosenClaimID, conflict.ID)
			}
			if loser, ok := idx.Claims[loserID]; ok {
				loser.Retracted = true
				loser.RetractedBy = res.ID
				idx.Claims[loser.ID] = loser
			}
			idx.Tombstones[loserID] = "conflict_resolved"
			idx.Redirects[loserID] = chosen.ID
			idx.Current[slotKey(chosen.CanonicalEntity, chosen.CanonicalKey)] = chosen.ID
		case EventRetraction:
			if ev.Retraction == nil {
				return Index{}, fmt.Errorf("retraction event %q missing retraction", ev.ID)
			}
			ret := *ev.Retraction
			c, ok := idx.Claims[ret.ClaimID]
			if !ok {
				return Index{}, fmt.Errorf("retraction %q references unknown claim %q", ret.ID, ret.ClaimID)
			}
			c.Retracted = true
			c.RetractedBy = ret.ID
			idx.Claims[c.ID] = c
			delete(idx.Current, slotKey(c.CanonicalEntity, c.CanonicalKey))
			idx.Tombstones[c.ID] = "retracted"
		default:
			return Index{}, fmt.Errorf("unknown event type %q", ev.Type)
		}
	}
	idx.Hash = hashIndex(idx)
	idx.Candidates = idx.deriveCandidates()
	idx.Hash = hashIndex(idx)
	if err := s.writeIndex(idx); err != nil {
		return Index{}, err
	}
	if render {
		if err := s.renderProjections(idx); err != nil {
			return Index{}, err
		}
	}
	return idx, nil
}

func (idx Index) deriveCandidates() []Candidate {
	current := idx.currentClaims()
	var out []Candidate
	for _, entity := range sortedKeys(current) {
		claims := current[entity]
		for i := 0; i < len(claims); i++ {
			left := claims[i]
			for j := i + 1; j < len(claims); j++ {
				right := claims[j]
				if left.CanonicalKey == right.CanonicalKey {
					continue
				}
				if left.ValueHash != right.ValueHash {
					continue
				}
				out = append(out, Candidate{
					Entity: entity,
					Left:   left,
					Right:  right,
					Reason: "same_entity_equal_value_different_key",
				})
			}
		}
	}
	return out
}

func (idx Index) currentClaims() map[string][]Claim {
	out := map[string][]Claim{}
	for _, slot := range sortedKeys(idx.Current) {
		c := idx.Claims[idx.Current[slot]]
		if c.Retracted || c.DuplicateOf != "" {
			continue
		}
		out[c.CanonicalEntity] = append(out[c.CanonicalEntity], c)
	}
	for entity := range out {
		sort.Slice(out[entity], func(i, j int) bool {
			return out[entity][i].CanonicalKey < out[entity][j].CanonicalKey
		})
	}
	return out
}

func (s *Store) canonPath(parts ...string) string {
	all := append([]string{s.root, canonDir}, parts...)
	return filepath.Join(all...)
}

func (s *Store) appendEvent(ev Event) error {
	if err := os.MkdirAll(s.canonPath(), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.canonPath(eventLogFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (s *Store) withWriteLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.canonPath(), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.canonPath(lockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *Store) readEvents() ([]Event, error) {
	path := s.canonPath(eventLogFile)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []Event
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return nil, fmt.Errorf("decode event log: %w", err)
		}
		out = append(out, ev)
	}
	return out, sc.Err()
}

func (s *Store) writeIndex(idx Index) error {
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.canonPath(indexFile), append(data, '\n'), 0o644)
}

func (s *Store) renderProjections(idx Index) error {
	current := idx.currentClaims()
	for _, entity := range sortedKeys(idx.EntityClaims) {
		active := current[entity]
		if len(active) == 0 {
			if err := os.Remove(filepath.Join(s.root, entity+".md")); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		var b strings.Builder
		b.WriteString("---\n")
		b.WriteString("canon_entity: " + entity + "\n")
		b.WriteString("claim_count: " + fmt.Sprint(len(active)) + "\n")
		b.WriteString("---\n\n")
		b.WriteString("# " + displayTitle(active[0].Entity) + "\n\n")
		for _, c := range active {
			b.WriteString("<!-- canon-claim id=\"" + c.ID + "\" key=\"" + c.CanonicalKey + "\" hash=\"" + c.Hash + "\" -->\n")
			b.WriteString("- **" + c.CanonicalKey + "**: " + c.Value + "\n")
			b.WriteString("  - provenance: " + c.Provenance + "\n")
		}
		if err := os.WriteFile(filepath.Join(s.root, entity+".md"), []byte(b.String()), 0o644); err != nil {
			return err
		}
	}
	if len(idx.Conflicts) > 0 {
		if err := os.MkdirAll(filepath.Join(s.root, "conflicts"), 0o755); err != nil {
			return err
		}
	}
	for _, id := range sortedConflictIDs(idx.Conflicts) {
		c := idx.Conflicts[id]
		var b strings.Builder
		b.WriteString("# Conflict " + c.ID + "\n\n")
		b.WriteString("- entity: " + c.Entity + "\n")
		b.WriteString("- key: " + c.CanonicalKey + "\n")
		b.WriteString("- resolved: " + fmt.Sprint(c.Resolved) + "\n\n")
		b.WriteString("## Existing\n\n")
		b.WriteString("- claim: " + c.Existing.ID + "\n")
		b.WriteString("- value: " + c.Existing.Value + "\n")
		b.WriteString("- provenance: " + c.Existing.Provenance + "\n\n")
		b.WriteString("## Incoming\n\n")
		b.WriteString("- claim: " + c.Incoming.ID + "\n")
		b.WriteString("- value: " + c.Incoming.Value + "\n")
		b.WriteString("- provenance: " + c.Incoming.Provenance + "\n")
		if err := os.WriteFile(filepath.Join(s.root, "conflicts", c.ID+".md"), []byte(b.String()), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func validateClaim(in ClaimInput) error {
	if strings.TrimSpace(in.Entity) == "" {
		return fmt.Errorf("entity is required")
	}
	if strings.TrimSpace(in.Key) == "" {
		return fmt.Errorf("key is required")
	}
	if strings.TrimSpace(in.Value) == "" {
		return fmt.Errorf("value is required")
	}
	if strings.TrimSpace(in.Provenance) == "" {
		return fmt.Errorf("provenance is required")
	}
	return nil
}

func newClaim(in ClaimInput, ts time.Time) Claim {
	entity := canonicalEntity(in.Entity)
	key := canonicalKey(in.Key)
	value := strings.TrimSpace(in.Value)
	c := Claim{
		Entity:          strings.TrimSpace(in.Entity),
		CanonicalEntity: entity,
		Key:             strings.TrimSpace(in.Key),
		CanonicalKey:    key,
		Value:           value,
		Provenance:      strings.TrimSpace(in.Provenance),
		TS:              ts.UTC(),
		ValueHash:       hashString(normalizeValue(value)),
	}
	c.ID = idFor("claim", entity, key, c.ValueHash, c.Provenance, ts.UTC().Format(time.RFC3339Nano))
	c.Hash = hashJSON(c)
	return c
}

func newIndex() Index {
	return Index{
		Version:      1,
		Claims:       map[string]Claim{},
		Conflicts:    map[string]Conflict{},
		Resolutions:  map[string]Resolution{},
		EntityClaims: map[string][]string{},
		Current:      map[string]string{},
		Redirects:    map[string]string{},
		Tombstones:   map[string]string{},
	}
}

func slotKey(entity, key string) string {
	return entity + "\x00" + key
}

func canonicalEntity(s string) string {
	return slug(normalizeText(s))
}

func canonicalKey(s string) string {
	slugged := slug(normalizeText(s))
	if slugged == "" {
		return "unknown"
	}
	return slugged
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

func normalizeText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = nonAlnum.ReplaceAllString(s, " ")
	return strings.Join(strings.Fields(s), " ")
}

func normalizeValue(s string) string {
	return normalizeText(s)
}

func valuesEqual(a, b string) bool {
	return normalizeValue(a) == normalizeValue(b)
}

func slug(s string) string {
	s = strings.TrimSpace(nonAlnum.ReplaceAllString(strings.ToLower(s), "-"))
	s = strings.Trim(s, "-")
	if s == "" {
		return "unknown"
	}
	return s
}

func dedupe(in []string) []string {
	out := in[:0]
	var last string
	for i, v := range in {
		if i == 0 || v != last {
			out = append(out, v)
			last = v
		}
	}
	return out
}

func appendUnique(in []string, v string) []string {
	for _, existing := range in {
		if existing == v {
			return in
		}
	}
	return append(in, v)
}

func conflictFor(existing, incoming Claim, ts time.Time) Conflict {
	return Conflict{
		ID:           idFor("conflict", incoming.CanonicalEntity, incoming.CanonicalKey, existing.ValueHash, incoming.ValueHash, incoming.ID),
		Entity:       existing.CanonicalEntity,
		CanonicalKey: incoming.CanonicalKey,
		Existing:     existing,
		Incoming:     incoming,
		TS:           ts,
	}
}

func idFor(parts ...string) string {
	return hashString(strings.Join(parts, "\x00"))[:16]
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hashJSON(v any) string {
	data, _ := json.Marshal(v)
	return hashString(string(data))
}

func hashIndex(idx Index) string {
	idx.Hash = ""
	return hashJSON(idx)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedClaimIDs(m map[string]Claim) []string       { return sortedKeys(m) }
func sortedConflictIDs(m map[string]Conflict) []string { return sortedKeys(m) }

func displayTitle(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "Unknown"
	}
	return s
}

type parsedProjectionClaim struct {
	ID     string
	Entity string
	Key    string
	Value  string
	Hash   string
}

var claimCommentRE = regexp.MustCompile(`<!--\s*canon-claim\s+id="([^"]+)"\s+key="([^"]+)"\s+hash="([^"]+)"\s*-->`)

func parseProjectionFile(path string) ([]parsedProjectionClaim, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	lines := strings.Split(string(data), "\n")
	entity := ""
	for _, line := range lines {
		if v, ok := strings.CutPrefix(line, "canon_entity: "); ok {
			entity = strings.TrimSpace(v)
			break
		}
	}
	if entity == "" {
		entity = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	var claims []parsedProjectionClaim
	for i, line := range lines {
		m := claimCommentRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if i+1 >= len(lines) {
			continue
		}
		valueLine := strings.TrimSpace(lines[i+1])
		prefix := "- **" + m[2] + "**: "
		if !strings.HasPrefix(valueLine, prefix) {
			continue
		}
		claims = append(claims, parsedProjectionClaim{
			ID:     m[1],
			Entity: entity,
			Key:    m[2],
			Value:  strings.TrimSpace(strings.TrimPrefix(valueLine, prefix)),
			Hash:   m[3],
		})
	}
	ambiguous := len(claims) == 0 && strings.TrimSpace(stripFrontmatterAndHeading(lines)) != ""
	return claims, ambiguous, nil
}

func stripFrontmatterAndHeading(lines []string) string {
	out := make([]string, 0, len(lines))
	inFrontmatter := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if i == 0 && trimmed == "---" {
			inFrontmatter = true
			continue
		}
		if inFrontmatter {
			if trimmed == "---" {
				inFrontmatter = false
			}
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func (s *Store) writeImportInbox(files []string) error {
	if len(files) == 0 {
		return nil
	}
	sort.Strings(files)
	var b strings.Builder
	b.WriteString("# Import inbox\n\n")
	b.WriteString("These Markdown files contain prose that canon could not safely import as claims.\n\n")
	for _, file := range files {
		b.WriteString("- " + file + "\n")
	}
	return os.WriteFile(s.canonPath("import-inbox.md"), []byte(b.String()), 0o644)
}
