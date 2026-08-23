# canon v1 — build spec

Ratified 2026-08-23 (pleasantville #wedge-release deliberation; tracker: iss-f7ffea).
This is the build brief. Scope is deliberately narrow — ship the deterministic core,
defer everything that needs an LLM or a graph.

## The promise (do not drift from this)

A folder does not lose truth under concurrent agent writes: **one canonical fact per
`(entity, key)`; contradictions become first-class objects, never silent loss.**

Framing discipline (learned the hard way in the debate):
- Not "memory" (that's Basic Memory's category), not "engineered coherence" (academic).
- The public claim is the promise above. The word on the box is *canon*, not a second brain.
- The substrate on-ramp is **latent**: the `.canon/` event model is designed to map onto a
  message-bus substrate later, but v1 has **no NATS/JetStream on the default path** and the
  README must not smell like one. The wedge is complete without pleasantville.

## Data model

- **Claim** — the only unit of truth: `{ entity, key, value, provenance, ts, hash }`.
  `value` is a scalar or short Markdown. `provenance` records who/what asserted it.
- **Conflict object** — emitted when a `put_claim` targets an existing `(entity, key)`
  with a *different* value. Preserves **both** claims + provenance; never merges them.
- **Resolution** — the recorded outcome of an explicit `resolve_conflict` (chosen value
  or a superseding value). The engine never adjudicates on its own.
- **Note / projection** — the Markdown rendering of the claims sharing a canonical entity.
  A projection, never a write target. Files are readable output + hand-editable input.

## Storage (`.canon/`)

- Append-only **event log**: `claim` / `conflict` / `resolution` / `retraction` events.
  This is the source of authority.
- A **derived index**: entity/alias → canonical id, `(entity,key)` → current claim(s),
  content fingerprints. Rebuildable from the log at any time.
- **Markdown projections** on disk: regenerated from the index; hand edits are reconciled
  back into events only on an explicit `scan_external_changes`.

## MCP tool surface (v1)

Mutating (truth):
- `put_claim { entity, key, value, provenance, base_hash? }` — CAS on `base_hash` when
  supplied; collapse-or-conflict semantics above.
- `resolve_conflict { conflict_id, chosen | superseding_value, provenance }`
- `retract_claim { claim_id, provenance }`

Read / layout (never create facts):
- `search { query }` — returns canonical claims/notes + redirects; no dupes.
- `render_entity { entity }`, `move_projection`, `merge_projection` (layout only).
- `scan_external_changes` — **batch** import of hand edits (on MCP start + explicit call).
  Parses known frontmatter/claim blocks into claims; ambiguous prose lands as an
  import-conflict / unstructured inbox, never silently as canonical truth.

## Identity resolution — the make-or-break

`put_claim` collapses/conflicts only on exact canonical `(entity, key)` slots unless the
caller explicitly declares an alias in a later tool. v1 must not guess semantic key
equivalence — not on the write path, and not as an after-the-fact report either: equal
values under different keys can be unrelated (`employees=100`, `offices=100`), and
different values under semantically similar keys (`founding date`, `first day`) need
MELD-style embedding/NLI or explicit aliasing.

**Semantic same-fact-different-key detection is MELD roadmap, not v1.** v1 ships no
heuristic for it — no equal-value "candidate" report, no thesaurus, no scoring. Claims
under different keys simply stay separate claims, which is the honest outcome; the demo
shows that divergence rather than papering over it. When it lands it will be embedding/NLI
plus explicit aliasing, and it will emit real claims/conflicts, not advisory hints.

The v1 engine is conservative: exact slot conflict, explicit redirect/tombstone, and no
silent overwrite.

## Release gate — the 3-agent demo (nothing ships until it passes clean)

Scripted fixture, no narration allowed:
- Three agents, one folder, overlapping writes on the same entity.
- Includes at least one genuine contradiction.
- Required result: **one canonical note; redirects/tombstones for the duplicates; one
  conflict object preserving both claims with provenance; zero silent clobbers.**

If explaining why the demo "won" takes a paragraph, it failed.

## Requirements source

Extract the concrete failure cases from **our own pv-memory** last-write-wins-on-modtime
clobbers (the pleasantville `MEMORY.md` store) — we are already the customer. Do **not**
dogfood Basic Memory to derive requirements (it trains us on their ontology). Basic Memory
is only the demo **foil**: reproduce its silent clobber, show canon refusing to.

## v1 non-goals (explicitly out)

- No graph / relations (Zep's game).
- No LLM anywhere on the write path.
- No live `fsnotify` watcher (scan is batch).
- No hosted/cloud/Teams story, no co-editing/CRDT.
- No NATS/JetStream on the default path.

## Academic anchor

MELD (arXiv:2608.16357, 2026-08-17) defines a five-outcome reconciliation for distributed
agentic memories — insert / merge / relate / conflict / reject — and holds that "a detected
contradiction is preserved for later adjudication, never silently resolved." **canon v1 is
the deterministic subset** of that procedure (insert / conflict / reject on claim-key
identity + hash); merge/relate (MELD's embedding+NLI outcomes) are the roadmap — that is
where semantic same-fact-different-key identity belongs, and v1 does not approximate it.
MELD being *federated* is the validation of the eventual substrate on-ramp.
