# canon

**A folder that doesn't lose truth under concurrent agent writes.**

When more than one agent — or more than one session of the same agent — maintains a
shared knowledge base, it rots: the same fact gets written twice under different
wording, edits silently clobber each other, and genuine contradictions get merged
into mush. Most "agent memory" tools either paper over this with last-write-wins or
dissolve it with CRDTs that make disagreement *disappear*. Agents don't need merged
paragraphs. They need **non-lossy truth**.

`canon` is a small MCP server over a plain folder that guarantees:

> **one canonical fact per `(entity, key)`; contradictions become first-class
> objects you can adjudicate — never silent loss.**

Two agents writing the same fact collapse to one canonical note. Two agents writing
*different* values for the same fact produce an explicit **conflict object** that
preserves both claims with provenance, for a human or a later agent to resolve. The
engine never adjudicates on its own, and never silently drops a write.

## How it works (v1)

- **One write primitive:** `put_claim {entity, key, value, provenance}`. Same
  `(entity, key)` + equal value → collapses. Same `(entity, key)` + different value →
  a conflict object. `resolve_conflict` and `retract_claim` are explicit and recorded.
- **Legible by construction.** Truth lives in an append-only event log under
  `.canon/`; the Markdown files are a *projection* — readable output and hand-editable
  input, imported back into claims on an explicit scan. You can open, diff, and repair
  everything.
- **No black box.** No LLM on the write path, no vector database, no graph, no
  background file watcher. v1 identity is deterministic and conservative: exact
  canonical `(entity, key)` slots collapse or conflict. Semantic key equivalence is
  never guessed — claims under different keys stay separate claims.
- **Reachable from anywhere.** It's an MCP server — Claude Code, claude.ai, Cursor, or
  any MCP client shares the same source of truth.

## Status

v1, in development. The release gate is a single demo that has to speak for itself:

> three agents, one folder, overlapping writes → **one canonical note, tombstones for
> the duplicates, one conflict object preserving both claims, zero silent clobbers**.

If that demo needs narration to explain why it won, it isn't done.

### Not in v1

Semantic **same-fact-different-key** identity — recognizing that `founding date` and
`first day` are the same fact — is deliberately out. It needs embedding/NLI plus explicit
aliasing (the MELD merge/relate outcomes), and v1 ships no heuristic stand-in for it: no
equal-value guessing, no toy thesaurus. Two agents writing the same fact under different
keys get two claims, and the demo shows exactly that divergence rather than hiding it.
That work is roadmap.

## MCP server

Run the stdio MCP server against a folder:

```bash
go run ./cmd/canon-mcp --root /path/to/knowledge-folder
```

Tools exposed:

- `put_claim`
- `resolve_conflict`
- `retract_claim`
- `search`
- `scan_external_changes`

## Demo gate

Run the release-gate fixture:

```bash
go run ./cmd/canon-demo
```

It races writers against one `(entity, key)`, verifies the conflict preserves both
claims with provenance, checks duplicate redirects/tombstones, and probes that claims
written under different keys stay visibly separate instead of being silently merged.

## License

Apache-2.0.
