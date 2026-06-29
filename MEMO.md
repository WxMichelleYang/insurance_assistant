# Design Memo — AI-Assisted Insurance Policy Wording Review

**For:** UK Tech PI Underwriting · **Date:** 29 June 2026

---

## 1. Problem & approach

For every broker clause an underwriter must answer four questions: does it diverge from our standard wording, does the divergence breach a red-line or create exposure, what exact text caused the issue, and has a similar position been approved before — and is that precedent actually transferable. We propose a decision-support system structured as **two pipelines sharing one knowledge base**:

- **Part A — Knowledge Base build.** Offline / event-triggered ingestion of insurer-side documents (standard wording, red-lines, prior approvals). Slow-changing inputs; runs when those documents are added or updated.
- **Part B — Submission review.** Online, per-submission pipeline. Parses the broker pack, queries the KB, produces evidenced findings, captures reviewer feedback.

The system never decides — it surfaces, explains, and routes. The underwriter accepts, edits, rejects, or escalates each finding, and those decisions become training data for the next iteration.

## 2. Architecture at a glance

Both pipelines run on AKS with services communicating over Kafka. Shared substrate:

- **Blob Storage** — raw documents, immutable, content-hashed.
- **MongoDB** — typed records (clauses, red-lines, prior approvals, findings, partial pipeline state).
- **Azure AI Search** — three logical indexes (`standard_clauses`, `red_lines`, `prior_approvals`), each with vector + BM25 and rich filter fields.
- **Azure SQL** — document lifecycle registry, audit trail of every LLM call, reviewer feedback, evaluation results.

```
                         ┌──── Part A: KB build (event-driven, idempotent) ────┐
   Standard wording  ─┐  │                                                     │
   Red-lines         ─┼─▶│ ingest → parse → upsert Mongo + Search → register   │─┐
   Prior approvals   ─┘  │                                                     │ │
                         └─────────────────────────────────────────────────────┘ │
                                       ▲                                         ▼
                                       │                              ┌──────────────────┐
                          promote: signed-off                         │   Knowledge Base │
                          submission → prior approval                 │ (Mongo + Search) │
                                       │                              └──────────────────┘
                                       │                                         ▲
                         ┌──── Part B: Submission review ─────────┐              │
   Broker pack       ─▶ │ parse → pin kb_snapshot_id →            │              │
                        │   retrieve(3 queries/clause) →          │──────────────┘
                        │   judge → finding → UI                  │
                        │                       ↑                 │
                        │       underwriter feedback (incl. sign-off → promote)
                        └─────────────────────────────────────────┘
```

**MVP** runs Part B synchronously per submission with one strong LLM judge prompt. **Target state** routes by clause type — cheap classifier filters obvious clauses, specialist judges run per red-line category, active learning over reviewer disagreements.

**The supplied pack, mapped to the architecture.** The case-study inputs split cleanly across the two pipelines:

| File | Authored by | Sits in | Notes |
|---|---|---|---|
| `01_Submission_Schedule.docx` | Broker (Alder Finch) | Part B input | Placement slip for this client (BrightWave) |
| `02_Broker_Base_Wording.docx` | Broker | Part B input | Proposed base policy wording |
| `03_Endorsement_01_AcquiredEntities.docx` | Broker | Part B input | Endorsement to the base wording |
| `04_Endorsement_04_CyberMedia.docx` | Broker | Part B input | Endorsement to the base wording |
| `05_Standard_Wording_UK_TechPI.docx` | Insurer | Part A KB (`standard_clauses`) | Canonical position, versioned |
| `06_Red_Lines_UK_TechPI.xlsx` | Insurer | Part A KB (`red_lines`) | Narrative absolute positions |
| `07_Prior_Approval_Orion.docx` | Broker wording, **insurer artefact** | Part A KB (`prior_approvals`) | Insurer's approval record — incl. underwriting note |
| `08_Prior_Approval_Helix.docx` | Broker wording, **insurer artefact** | Part A KB (`prior_approvals`) | As above; D&O — the LoB trap |

The distinction on 07/08 matters: the underlying wording was authored by another broker (Orion's, Helix's), but the documents themselves are **insurer-confidential** — header marks "STRICTLY CONFIDENTIAL — INSURER INTERNAL USE ONLY", approval signed by EMEA Underwriting, with an insurer-authored note attached. They never sit in a broker's pack; they live in the insurer's KB and are produced by the promotion loop (sign-off → prior approval). The broker submitting BrightWave never sees Orion or Helix.

---

## Part A — Knowledge Base build

The KB is **continuously updated**, not built once. New prior approvals arrive frequently (every underwriting sign-off can produce one), red-lines are tightened or added as governance evolves, and standard wordings are revised periodically. The design treats this as a first-class property: ingestion is event-driven, idempotent, versioned, and never leaves partial state visible to readers (see §6).

Three document classes; each has a typed representation, a Mongo collection, and an AI Search index. They are kept logically separate because their filter schemas and consequences differ.

### 3. Standard wording

- **Source:** insurer's canonical position per LoB × region × version (`05_Standard_Wording_UK_TechPI.docx`).
- **Ingest:** OOXML parse → segment by numbered headings → produce `Clause` records (full schema in §7). Defined terms (capitalised in the OOXML) are extracted into a per-document glossary so downstream comparison knows that the broker's *Insured* and our *Insured* are not the same object.
- **Store:** Mongo `standard_clauses`; AI Search index over text (vector + BM25) with `lob`, `region`, `version` as filterable facets.
- **Versioning:** every standard wording is `(lob, region, version)`. Old versions are retained — past submissions must be reviewable against the version current at submission time, not against today's.

### 4. Red-lines

- **Source:** narrative absolute positions, one per row (`06_Red_Lines_UK_TechPI.xlsx`).
- **Ingest:** parse spreadsheet → one record per row `{red_line_id, lob, region, category, narrative}`.
- **Store:** Mongo `red_lines`; AI Search index over `narrative` with `lob`, `region`, `category` as filters.
- **Design choice — narrative stays narrative.** We deliberately do **not** translate red-lines into deterministic rules. "*No catch-all language to automatically allow cover for either: (i) companies which are not legally defined…*" carries judgement that disappears in a regex. Categories (Insured Definition, Defence Costs, Cyber/Privacy/Media, Territory, Contractual Liability, Prior Acts/Acquisitions) become retrieval facets, not rule keys.

### 5. Prior approvals

This is the highest-risk index — wrong retrieval here produces fabricated precedent. Designed conservatively.

- **Source:** previously approved broker wordings per insured (`07_Prior_Approval_Orion.docx`, `08_Prior_Approval_Helix.docx`).
- **Ingest:** parse the structured header into typed metadata (`reference`, `lob`, `region`, `insured`, `approval_date`, `approved_by`, `status`, `underwriting_note`); parse approved clauses as sub-records `{clause_id, approval_id, heading_path, text, span}`.
- **Store:** Mongo `prior_approvals` with parent record + sub-collection of clauses; AI Search index over approved-clause text with filters `lob`, `region`, `approval_date`, `insured_size_band` (where known), `status`.
- **Underwriting note as typed field.** The note ("*This approval relates to a D&O policy and is not transferable to a Tech PI risk without separate underwriting review*") is captured as structured metadata, not just embedded prose. The ranker uses it directly to demote and the UI surfaces it verbatim.

### 6. Updates and re-indexing

The three document classes update on independent cadences — the system handles them through one pathway and does not need to know the cadence in advance.

| Update | Frequency in practice | Source | Risk profile |
|---|---|---|---|
| New prior approval | High (per sign-off) | Auto-promotion from Part B, or manual upload | Low — additive; past findings unaffected |
| Red-line modified / added | Medium (governance) | Underwriting governance | High — may invalidate in-flight submissions |
| Standard wording revised | Low (annual+) | Insurer policy team | Medium — versioned; past submissions pinned to old version |

**One ingestion path, idempotent.** Every update — whether a freshly authored red-line, a revised standard wording, or a signed-off submission being promoted — flows through the same path: Blob upload (content-hashed key, so re-uploads dedupe) → Kafka `kb.doc.received` → classify → parse → **upsert** Mongo and AI Search → SQL doc-registry transitions `received → parsed → indexed → live` → emit `kb.doc.indexed`. The `live` flip is the visibility boundary; partial state is never readable. A re-upload of unchanged content is a no-op.

**Promotion loop — Part B feeds Part A.** When an underwriter signs off a broker wording (all findings resolved, submission bound), the system emits the approved version to the KB ingestor with structured metadata: `insured`, `lob`, `region`, `approval_date`, `approved_by`, and the underwriting note authored during review. This is the closed loop that grows the precedent corpus without a separate export process. Crucially, the metadata is captured at the moment of sign-off — no post-hoc reconstruction.

**Snapshot pinning for in-flight submissions.** Every submission binds at parse time to a `kb_snapshot_id` — a digest over every live `(doc_id, version)` in the KB at that instant. Findings are computed against that snapshot. If a red-line is tightened or a new prior approval lands while a submission is mid-review, the reviewer's findings do **not** silently change underneath them. The UI offers a "re-run against latest KB" action that produces a *diff* of findings (added / removed / changed), not a replacement. Audit stays clean: every finding is reproducible against the KB version it was computed against.

**Proactive notification on red-line changes.** When a red-line is added or tightened, we compute which in-flight submissions *would* gain findings under the new red-line and notify the relevant underwriters via Teams/email with the diff. Waiting for someone to click "re-run" would miss the change.

**Re-indexing without downtime.** Per-document upserts are the default. For breaking changes (embedding model upgrade, schema migration) we build the new AI Search index alongside the live one, replay the SQL doc registry into it, then atomically swap the index alias. Readers never see a partially-built index. The doc registry is the source of truth — the AI Search index is fully rebuildable from it.

**Parsing failures fail loud.** PDFs below 0.9 word-confidence (common for scanned prior approvals) route to manual triage rather than being parsed best-effort; the registry status `parse_failed` makes them visible at a glance. Garbage text produces confident-looking wrong findings — failing at ingest is much cheaper than catching it at review.

### 7. Parsing and normalisation — implementation

Part A and Part B share one Python parser library. It is organised as a deterministic stack of stages with one LLM-backed step (LoB inference) used only at submission time. Per-document output is the canonical `Clause` record at the end of this section.

**Per-format readers.**

- **DOCX** (`01–05`, `07–08`): `python-docx` for paragraphs, runs, headings, tables, headers/footers. For the few things the high-level API hides — list-numbering definitions in `word/numbering.xml`, custom styles, tracked-change markers — the reader falls back to direct OOXML parsing with `lxml`. Both endorsements and the schedule are DOCX; the reader sees them as paragraph streams plus table rows.
- **XLSX** (`06`): `openpyxl`. The red-lines spreadsheet is row-per-record — each non-empty row becomes one `{red_line_id, lob, region, category, narrative}`.
- **PDF / image** (production, not in this pack): Azure Document Intelligence `prebuilt-layout` returns paragraphs with bounding boxes, tables, and per-word confidence. Pages with mean confidence < 0.9 fail the stage with `parse_failed` and go to manual triage; we never feed best-effort OCR into retrieval.

**Deterministic normalisation chain** (applied uniformly across formats):

1. NFKC Unicode normalisation.
2. Smart-quote → straight-quote; em-dash / en-dash collapsed to standard glyphs.
3. Soft-hyphen and non-breaking space removed.
4. Whitespace collapsed (paragraph breaks preserved; intra-paragraph runs squashed).
5. Headers, footers, watermarks ("STRICTLY CONFIDENTIAL — INSURER INTERNAL USE ONLY"), and signature blocks ("Signed on behalf of…") stripped before segmentation.
6. Each emitted clause carries both `text` (normalised, used by embeddings and judges) and `text_raw` (the unmodified substring of the source). Spans index into `text_raw`, so evidence quotes are re-extracted **verbatim** from the document the underwriter is reading.

**Segmentation — two-pass.**

1. *Heading-driven split.* A paragraph is a clause boundary if any **two** of: (a) the OOXML `w:pStyle` is `Heading 1/2/3`; (b) the prefix matches `^\d+\.` or `^\d+\.\d+`; (c) the first run is bold and all-caps. Requiring two signals avoids spurious splits on `(a)` / `(b)` sub-items inside definitions.
2. *Body split.* Paragraphs longer than ~2000 characters are sentence-split with 200-char overlap so embedding windows are bounded. The unit of comparison is the smallest "rule-bearing unit" — typically a level-2 numbered paragraph (`§2.1`, `§3`, `§6`).

`heading_path` is carried on every clause as the full ancestor chain (e.g. `["2. DEFINITIONS", "2.1 Insured"]`). This is the topic anchor for the standard-analogue retriever's first channel — heading-text similarity is robust even when the body has been heavily reworded.

**Defined-term extraction.** A structured pass over each document's "DEFINITIONS" section: sub-items where the heading word matches the body's first defined word (`2.1 Insured … "Insured means: …"`) produce glossary entries `{term, definition, clause_id, span}`. Parenthetical shorthand introductions (`("the Insurer")`) are captured the same way. The glossary is attached to the document; every clause carries the list of terms it actually uses (`defined_terms_used`). The divergence judge receives both source and target glossaries, so it does not compare the broker's `Insured` against the standard's `Insured` as if they had the same scope — they don't.

**Tables.** The submission schedule is mostly a two-column `(label, value)` table. The reader walks it via `python-docx` and maps known labels through an alias dictionary (`"Insured" | "Named Insured" → named_insured`; `"Class / Line of Business" → line_of_business`); unrecognised labels land in `extra_fields`. Tables embedded inside policy text (limit schedules, sub-limit grids) are linearised back into clause text with a marker so they remain searchable.

**Endorsement linking.** Endorsements detected by header pattern (`ENDORSEMENT`, `ENDT-…`, "*Attached to and forming part of…*"). The parent base-wording reference is parsed out of the "*Attached to and forming part of the [Insured] [Policy Name]*" phrasing and stored as a Mongo relation `{endorsement_doc_id, base_doc_id, effective_order}` — so the broker's own order-of-precedence clause (broker §10 in this pack) drives evaluation rather than a hard-coded rule.

**LoB inference (submission-time, low-confidence only).** When the schedule's `Class / Line of Business` is blank (as in the supplied pack), a constrained-output Azure OpenAI call (JSON mode, schema-validated) reads `{business_description, requested_features, top heading_paths, Insured definition}` and returns `{lob: enum, confidence, evidence: [spans]}`. Confidence < 0.85 pauses the pipeline for one-click underwriter confirmation before any KB query runs — the whole downstream is hard-filtered on LoB, so a wrong inference here invalidates every finding.

**Canonical `Clause` record** — the parser's output, identical across all document classes:

```json
{
  "clause_id": "BRG-WORD-§2.1",
  "doc_id": "02_Broker_Base_Wording.docx#sha256:…",
  "doc_type": "broker_base | standard_wording | red_line | prior_approval_clause | broker_endorsement",
  "lob": "UK Tech PI", "region": "UK",
  "version": "v3.0",
  "heading_path": ["2. DEFINITIONS", "2.1 Insured"],
  "text": "Insured means: (a) the Named Insured; and (b) …",
  "text_raw": "Insured means:\n(a) the Named Insured; and\n(b) …",
  "span": {"page": 1, "start_char": 1042, "end_char": 1387},
  "defined_terms_used": ["Named Insured", "Business"],
  "checksum": "sha256:…"
}
```

---

## Part B — Submission review

### 8. Submission-time additions

The broker pack runs through the §7 parser the same way as KB documents; the submission-specific additions are:

- **Schedule → typed `Submission` record** (`named_insured`, `period`, `limit`, `retention`, `territory`, `jurisdiction`, `retroactive_date`, `requested_features`, `extra_fields`).
- **LoB inference** fires only when the schedule's `Class / Line of Business` field is blank (it is, in this pack); see §7.
- **Endorsement → base relations** are resolved against the broker's order-of-precedence clause (broker §10) at parse time, not at retrieval time, so judges always see the effective wording.
- **KB snapshot pin.** The submission records its `kb_snapshot_id` before any retrieval call (see §6). All findings will be reproducible against this exact KB state.

### 9. Retrieval — three queries per clause

For each broker clause we fire three retrieval calls against the KB, all hard-filtered on broker `lob` and `region`:

| Query | Index | Top-k | Filters |
|---|---|---|---|
| Closest standard analogue | `standard_clauses` | 1–3 | `lob`, `region`, `version=current` |
| Candidate red-lines | `red_lines` | ≤5 | `lob`, `region` |
| Candidate prior approvals | `prior_approvals` | ≤5 | `lob`, `region`, `status=active` |

All three are hybrid (dense vector + BM25). The standard-analogue query runs in two channels in parallel — **topic anchor** (heading match: "Defence Costs" → "Defence Costs") for the common case, and **semantic span** for when the broker has reshuffled structure (e.g. embedded a defence-costs rule inside §2 Definitions).

In **addition** to the hard-filtered prior-approval query, we run a parallel **cross-LoB lookup** with the LoB filter relaxed. Results are surfaced separately in the UI as "similar wording in other lines of business — not treated as precedent" so the underwriter sees that Helix-style near-matches were considered and rejected, rather than the system silently dropping them.

### 10. Clause-level comparison

The comparison pipeline sits between retrieval (§9) and finding production (§12). Four stages in order; output is one structured comparison record per broker × standard pair.

**Stage 1 — Bipartite matching.** Build a similarity matrix over broker × standard candidates from the two-channel retrieval (heading-path anchor + body semantic), combining as `0.6·topic + 0.4·body`. Greedy assignment with a confidence threshold (≈ 0.5 in MVP, tuned against the gold set) produces three outcomes:

- *Matched pair* → goes to stages 2–4.
- *Broker-only clause* (no standard analogue above threshold) → finding **"broker-introduced clause not in standard"** (e.g. broker §10 *Order of Precedence* — standard has no equivalent; broker §7 *Territory and Jurisdiction (Worldwide)* arguably also).
- *Standard-only clause* (no broker analogue) → finding **"broker omitted standard coverage"**. Often the highest-exposure category — what the broker *didn't say* (e.g. standard §6 *Network Security ... unless a separate module is specifically underwritten* has no broker analogue restricting cyber; broker has affirmatively granted it).

**Stage 2 — Defined-term reconciliation (deterministic, no LLM).** For each matched pair, look up the glossary entries for the terms each clause uses (`defined_terms_used` from §7). Diff scopes structurally:

- Single-sentence definitions → token-level diff.
- Compound definitions with sub-items → set diff over sub-clauses ((a), (b), (c)…) plus phrase-level diff within each sub-item.

Output: a `defined_term_drift` record `{term, broker_scope, standard_scope, delta}`. Computed *before* the judge runs so the judge is told the underlying terms differ — preventing the common failure mode of comparing surface clause text while ignoring that *Insured* means different things on each side.

**Stage 3 — Divergence judge (one LLM call, constrained JSON output).** Inputs:

- Both clauses' `text` (normalised) and `heading_path`.
- Glossary entries for the terms each clause uses.
- The `defined_term_drift` record from stage 2.
- The submission's `Submission` record (limit, territory, requested features) for context.

Output schema:

```json
{
  "verdict": "material_divergence | immaterial_drafting | equivalent",
  "rationale": "...",
  "broker_quote": "...",      // must exact-match broker.text_raw
  "standard_quote": "...",    // must exact-match standard.text_raw
  "divergence_axes": ["scope_of_insured", "automatic_extension"],
  "self_confidence": 0.94
}
```

Both quotes pass through the span-grounding gate — re-extract from `text_raw` by exact match. One retry, then `judgement_failed` → manual review.

**Stage 4 — Endorsement layering.** After base comparison, the broker's own order-of-precedence clause (broker §10 in this pack) drives application of endorsements. For every base finding, if an endorsement touches the same `heading_path` topic, re-run the judge with the **effective wording** (base + endorsement overlay) against standard. The base finding is either preserved with updated evidence spans, downgraded to `equivalent` (if the endorsement narrowed the broker side to match standard — rare), or its rationale upgraded (if the endorsement widened the gap). An "applied endorsement X" trace is persisted regardless, so nothing silently disappears.

#### Worked example — §2.1 *Insured* definition

(The Defence Costs example in §13 covers a body-only divergence with cross-LoB precedent. This one exercises defined-term reconciliation and endorsement layering, which Defence Costs doesn't.)

**Stage 1 — matching.** Broker `BRG-§2.1` `heading_path = ["2. DEFINITIONS", "2.1 Insured"]`. Topic-anchor channel returns `STD-§2.1` at cosine 0.98 on heading; body-semantic channel returns the same clause at cosine 0.71 (bodies diverge — that's the signal). Combined 0.87 → matched pair.

**Stage 2 — defined-term reconciliation:**

| | Broker `Insured` | Standard `Insured` |
|---|---|---|
| (a) | Named Insured | Named Insured |
| (b) | "*any past, present or future subsidiary, associated company, joint venture, or other entity over which the Named Insured exercises management control*" | "*any subsidiary entity wholly owned by the Named Insured at inception and specifically declared to the Insurer*" |
| (c) | "*any director, officer, partner, member or employee of any entity falling within paragraphs (a) or (b)*" | — |

`defined_term_drift`: broker adds *associated company* (not a legally defined form), *joint venture*, *over which exercises management control* (catch-all), *past/present/future* temporal extension, plus individual officers/employees; broker removes *wholly owned* and *specifically declared* constraints.

**Stage 3 — judge output:**

```json
{
  "verdict": "material_divergence",
  "rationale": "Broker introduces unbounded catch-all language ('associated company', 'over which exercises management control') and temporal extension ('past, present or future') absent from the standard, and removes the 'wholly owned at inception' and 'specifically declared' constraints. Standard limits Insured to declared wholly-owned subsidiaries; broker captures effectively any related entity automatically.",
  "broker_quote": "any past, present or future subsidiary, associated company, joint venture, or other entity over which the Named Insured exercises management control",
  "standard_quote": "any subsidiary entity wholly owned by the Named Insured at inception and specifically declared to the Insurer",
  "divergence_axes": ["scope_of_insured", "automatic_extension"],
  "self_confidence": 0.94
}
```

Both quotes exact-match their `text_raw`. Span gate passes.

**Stage 4 — endorsement layering.** Endorsement 01 (Acquired Entities) further modifies `Insured` by adding "*any entity acquired or formed during the Period of Insurance where the annual turnover of such entity does not exceed 35% of consolidated group turnover*". Broker §10 makes the endorsement override the base. The judge re-runs against the effective wording (base + Endorsement 01) and the verdict stays `material_divergence`; the rationale and evidence spans now reference both the base catch-all *and* the 35%-turnover sub-rule.

**Downstream — one clause, two findings.** This comparison feeds the divergence finding in §12. In parallel (§11), the red-line query against the same clause retrieves **RL-01** ("*No catch-all language to automatically allow cover for either: (i) companies which are not legally defined (e.g. 'associated companies')…*"). The red-line judge fires on the phrases *"associated company"* and *"over which exercises management control"* — direct match. Two findings on the same `broker_quote`: one divergence, one red-line breach.

### 11. Red-line and precedent judges

Two further judges run alongside the divergence judge (§10), on the other two retrieval channels from §9. Same constrained-JSON shape, same span-grounding gate (re-extract from `text_raw` by exact match, retry once, then `judgement_failed`):

- *Red-line judge* → `{violates | does_not_violate}` + which red-line + quoted phrase from the broker clause + quoted phrase from the red-line narrative.
- *Precedent relevance judge* → `{transferable | partial | not_transferable}` + what's narrower + what's broader + quoted phrases from both clauses.

Two hard rules on prior approvals: (a) nothing can be cited unless its `approval_id` was returned by the retriever — post-hoc lookup verifies; (b) different LoB never qualifies as transferable precedent (LoB filter), but is shown via the cross-LoB lookup with the mismatch named.

### 12. Findings: evidence, source, confidence

Every finding persists before render, in the brief's schema:

```
finding_id · clause_id · issue_type · why_it_matters ·
comparison_source (standard_clause_id | red_line_id | prior_approval_id) ·
quoted_evidence · prior_approval (id + transferability + delta) ·
recommended_action · confidence · model_version · prompt_hash
```

Confidence combines retrieval score, judge self-reported confidence, and — most importantly — a **span-grounding check**: the quoted phrase is re-extracted from `text_raw` by exact match, and the finding is rejected if not found verbatim. This single gate removes most hallucinations cheaply.

### 13. Worked example — Defence Costs

| Field | Value |
|---|---|
| Broker clause | §3 *"Defence Costs shall be payable by the Insurer **in addition to the Limit of Indemnity** and shall be advanced on an ongoing basis."* |
| Standard analogue | §3 *"Defence Costs form part of and not in addition to the Limit of Indemnity."* |
| Issue type | Material divergence + **Red-line breach (RL-03)** |
| Why it matters | Defence costs outside the limit can materially exceed the priced exposure; explicit insurer red-line for UK Tech PI. |
| Prior approval | **Helix-UK-DO-2023-041 — not transferable.** Returned by the cross-LoB lookup (surface-similar — advancement in addition to limit) but LoB = D&O. Shown as "similar wording in other lines of business", not as precedent. |
| Recommended action | Revert to standard wording or escalate; do not rely on Helix. |
| Confidence | 0.92 — verbatim quote, RL-03 directly applicable, retrieval clean. |

A second finding fires on §6 + Endorsement 01 (prior acts for acquired entities). Here **Orion** is genuinely Tech PI and on-topic — but Orion *excludes* prior acts and caps at 15% turnover / 60-day notification, where the broker grants prior acts at 35% / 120 days. Orion is therefore returned as a **partial precedent** with deltas itemised, not a green light.

---

## 14. UX for the underwriter

Three-pane review screen: submission schedule and finding list on the left; broker document with each finding highlighting a span in the centre; the comparison source — standard clause, red-line, or prior approval — pinned to the right when a finding is selected. Four actions per finding: *accept*, *edit*, *reject (one of five fixed reasons)*, *escalate*. Filters by issue type, red-line, severity, and "has prior approval". A separate panel shows cross-LoB near-precedents that the system **didn't** treat as binding, with the LoB mismatch named.

Feedback *shape* matters more than volume. "Wrong precedent" trains a different model than "rationale incorrect"; both differ from "this is fine actually". Five explicit reject reasons plus free-text; edits to the recommended-action field are first-class training data.

## 15. Evaluation

**Offline.** Curated gold set of ~150 broker clauses across the eight document families, each labelled with expected findings. Metrics: clause-level **recall**, **precision**, **evidence accuracy** (quoted span exact-matches the source), and **precedent F1** split by transferable/partial/not-transferable. Evidence accuracy is the dominant metric — recall and precision are tunable, but one hallucinated precedent breaks underwriter trust irrecoverably.

**Online.** Reviewer accept-rate per finding type, time-to-decision, post-bind incident rate, false-negative discovery rate. Shadow-run every new prompt or model against recent submissions before promotion.

**Adversarial corpus baked into CI:** structural reshuffles (rule hidden inside Definitions), defined-term shadowing, endorsements that fully override base wording (Endorsement 04 cyber), near-precedents from other LoBs (the Helix trap), and prior approvals that are narrower than the request (the Orion trap). Promotion gated on no-regression in evidence accuracy and red-line recall.

## 16. Security, audit, monitoring

Blob with per-tenant CMK encryption. Every LLM call persists `{prompt_hash, prompt_template_version, model, model_version, retrieved_doc_ids, raw_response, parsed_response, latency, cost}` to SQL — audit trail *and* evaluation substrate. Findings carry their `prompt_hash` so any past decision is exactly reproducible. PII (insured names, addresses) is masked before LLM calls where the comparator doesn't need it. RBAC enforced at AI Search query time — prior approvals are insurer-confidential and tenant + role filters cannot be bypassed at render.

Monitoring: cost per submission, p95 latency per stage, retrieval-empty rate, span-grounding-failure rate, judge-disagreement rate vs historical reviewer decisions. Alerts on regression of evidence accuracy in shadow runs.

## 17. Risks, trade-offs, MVP → target

- **Hallucinated precedents** — mitigated by hard-grounded retrieval (no citation without retriever-returned ID), verbatim-quote requirement, LoB filter, adversarial Helix-style tests in CI.
- **Endorsement override missed** — mitigated by parsing order-of-precedence and replaying base findings under endorsements; "suppressed by endorsement" trace rather than silent drop.
- **Variable broker formats break the parser** — mitigated by OCR confidence gate at ingest.
- **Reviewer over-trust** — mitigated by no auto-accept, surfacing retrieval evidence and confidence components separately rather than a single score, and reporting what the model *didn't* find that the gold set says it should have during eval.
- **Cost** — MVP runs one strong judge per clause; target state routes by clause type so a cheap classifier handles obviously-equivalent clauses (the majority), strong judge only on retrieved-divergence candidates.

**MVP → target deltas.** Specialist judges per red-line category; clause-classifier trained on accumulated reviewer labels; precedent-graph view showing *why* an approval is closer than another; active learning over reviewer disagreements; LoB inference surfaced for one-click confirmation rather than treated as silent metadata.
