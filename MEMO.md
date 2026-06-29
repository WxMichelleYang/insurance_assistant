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

---

## Part A — Knowledge Base build

The KB is **continuously updated**, not built once. New prior approvals arrive frequently (every underwriting sign-off can produce one), red-lines are tightened or added as governance evolves, and standard wordings are revised periodically. The design treats this as a first-class property: ingestion is event-driven, idempotent, versioned, and never leaves partial state visible to readers (see §6).

Three document classes; each has a typed representation, a Mongo collection, and an AI Search index. They are kept logically separate because their filter schemas and consequences differ.

### 3. Standard wording

- **Source:** insurer's canonical position per LoB × region × version (`05_Standard_Wording_UK_TechPI.docx`).
- **Ingest:** OOXML parse → segment by numbered headings → produce `Clause` records `{clause_id, doc_id, lob, region, version, heading_path, text, span, defined_terms}`. Defined terms (capitalised in the OOXML) are extracted into a per-document glossary so downstream comparison knows that the broker's *Insured* and our *Insured* are not the same object.
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

---

## Part B — Submission review

### 7. Parsing the broker pack

Same `Clause` normalisation as the KB, plus:

- **Schedule** → typed record (Limit, Territory, Retroactive Date, requested features).
- **LoB inference** when the Class field is blank (as in the supplied pack): constrained-output LLM call against an allow-list using business description + requested features. Low confidence prompts the underwriter before the pipeline fans out — this is load-bearing because every KB query is hard-filtered on LoB.
- **Endorsements** become separate `Clause` records linked to the base wording with their order-of-precedence parsed from the broker's own wording (broker §10 in the pack), not hard-coded.

### 8. Retrieval — three queries per clause

For each broker clause we fire three retrieval calls against the KB, all hard-filtered on broker `lob` and `region`:

| Query | Index | Top-k | Filters |
|---|---|---|---|
| Closest standard analogue | `standard_clauses` | 1–3 | `lob`, `region`, `version=current` |
| Candidate red-lines | `red_lines` | ≤5 | `lob`, `region` |
| Candidate prior approvals | `prior_approvals` | ≤5 | `lob`, `region`, `status=active` |

All three are hybrid (dense vector + BM25). The standard-analogue query runs in two channels in parallel — **topic anchor** (heading match: "Defence Costs" → "Defence Costs") for the common case, and **semantic span** for when the broker has reshuffled structure (e.g. embedded a defence-costs rule inside §2 Definitions).

In **addition** to the hard-filtered prior-approval query, we run a parallel **cross-LoB lookup** with the LoB filter relaxed. Results are surfaced separately in the UI as "similar wording in other lines of business — not treated as precedent" so the underwriter sees that Helix-style near-matches were considered and rejected, rather than the system silently dropping them.

### 9. LLM judgement

Three judges, each receiving the broker clause plus the retrieved candidate verbatim, each returning a structured response with a **quoted phrase from the source** as evidence:

- *Divergence judge* → `{material_divergence | immaterial_drafting | equivalent}` + rationale + quoted phrase.
- *Red-line judge* → `{violates | does_not_violate}` + which red-line + quoted phrase.
- *Precedent relevance judge* → `{transferable | partial | not_transferable}` + what's narrower + what's broader.

Endorsements are applied **after** base comparison: if Endorsement 04 affirms cyber cover, the §8 base finding is re-evaluated under the endorsed position rather than reported alongside. A "suppressed by endorsement" trace is kept so nothing silently disappears.

Two hard rules on prior approvals: (a) nothing can be cited unless its `approval_id` was returned by the retriever — post-hoc lookup verifies; (b) different LoB never qualifies as transferable precedent (LoB filter), but is shown via the cross-LoB lookup with the mismatch named.

### 10. Findings: evidence, source, confidence

Every finding persists before render, in the brief's schema:

```
finding_id · clause_id · issue_type · why_it_matters ·
comparison_source (standard_clause_id | red_line_id | prior_approval_id) ·
quoted_evidence · prior_approval (id + transferability + delta) ·
recommended_action · confidence · model_version · prompt_hash
```

Confidence combines retrieval score, judge self-reported confidence, and — most importantly — a **span-grounding check**: the quoted phrase is re-extracted from the source by exact match, and the finding is rejected if not found verbatim. This single gate removes most hallucinations cheaply.

### 11. Worked example — Defence Costs

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

## 12. UX for the underwriter

Three-pane review screen: submission schedule and finding list on the left; broker document with each finding highlighting a span in the centre; the comparison source — standard clause, red-line, or prior approval — pinned to the right when a finding is selected. Four actions per finding: *accept*, *edit*, *reject (one of five fixed reasons)*, *escalate*. Filters by issue type, red-line, severity, and "has prior approval". A separate panel shows cross-LoB near-precedents that the system **didn't** treat as binding, with the LoB mismatch named.

Feedback *shape* matters more than volume. "Wrong precedent" trains a different model than "rationale incorrect"; both differ from "this is fine actually". Five explicit reject reasons plus free-text; edits to the recommended-action field are first-class training data.

## 13. Evaluation

**Offline.** Curated gold set of ~150 broker clauses across the eight document families, each labelled with expected findings. Metrics: clause-level **recall**, **precision**, **evidence accuracy** (quoted span exact-matches the source), and **precedent F1** split by transferable/partial/not-transferable. Evidence accuracy is the dominant metric — recall and precision are tunable, but one hallucinated precedent breaks underwriter trust irrecoverably.

**Online.** Reviewer accept-rate per finding type, time-to-decision, post-bind incident rate, false-negative discovery rate. Shadow-run every new prompt or model against recent submissions before promotion.

**Adversarial corpus baked into CI:** structural reshuffles (rule hidden inside Definitions), defined-term shadowing, endorsements that fully override base wording (Endorsement 04 cyber), near-precedents from other LoBs (the Helix trap), and prior approvals that are narrower than the request (the Orion trap). Promotion gated on no-regression in evidence accuracy and red-line recall.

## 14. Security, audit, monitoring

Blob with per-tenant CMK encryption. Every LLM call persists `{prompt_hash, prompt_template_version, model, model_version, retrieved_doc_ids, raw_response, parsed_response, latency, cost}` to SQL — audit trail *and* evaluation substrate. Findings carry their `prompt_hash` so any past decision is exactly reproducible. PII (insured names, addresses) is masked before LLM calls where the comparator doesn't need it. RBAC enforced at AI Search query time — prior approvals are insurer-confidential and tenant + role filters cannot be bypassed at render.

Monitoring: cost per submission, p95 latency per stage, retrieval-empty rate, span-grounding-failure rate, judge-disagreement rate vs historical reviewer decisions. Alerts on regression of evidence accuracy in shadow runs.

## 15. Risks, trade-offs, MVP → target

- **Hallucinated precedents** — mitigated by hard-grounded retrieval (no citation without retriever-returned ID), verbatim-quote requirement, LoB filter, adversarial Helix-style tests in CI.
- **Endorsement override missed** — mitigated by parsing order-of-precedence and replaying base findings under endorsements; "suppressed by endorsement" trace rather than silent drop.
- **Variable broker formats break the parser** — mitigated by OCR confidence gate at ingest.
- **Reviewer over-trust** — mitigated by no auto-accept, surfacing retrieval evidence and confidence components separately rather than a single score, and reporting what the model *didn't* find that the gold set says it should have during eval.
- **Cost** — MVP runs one strong judge per clause; target state routes by clause type so a cheap classifier handles obviously-equivalent clauses (the majority), strong judge only on retrieved-divergence candidates.

**MVP → target deltas.** Specialist judges per red-line category; clause-classifier trained on accumulated reviewer labels; precedent-graph view showing *why* an approval is closer than another; active learning over reviewer disagreements; LoB inference surfaced for one-click confirmation rather than treated as silent metadata.
