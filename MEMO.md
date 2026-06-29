# Design Memo — AI-Assisted Insurance Policy Wording Review

**For:** UK Tech PI Underwriting · **Date:** 29 June 2026

---

## 1. Problem & approach

For every broker clause an underwriter must answer four questions: does it diverge from our standard wording, does the divergence breach a red-line or create exposure, what exact text caused the issue, and has a similar position been approved before — and is that precedent actually transferable. We propose a decision-support pipeline that returns a structured **finding set**, each finding evidenced by a verbatim text span from the source. The system never decides; it surfaces, explains, and routes. The underwriter accepts, edits, rejects, or escalates each finding, and those decisions become training data.

## 2. End-to-end architecture

Event-driven pipeline on AKS, services communicating over Kafka. State persisted to MongoDB (clauses, findings, partial pipeline state) and Azure SQL (audit, feedback, evaluation). Raw files in Blob Storage.

```
Broker pack ─► [ingest]   classify, OCR if needed             → doc.ingested
              [parse]    segment into clauses, glossary       → doc.parsed
              [index]    embed clauses to Azure AI Search     → doc.indexed
              [compare]  for each clause: retrieve standard /
                         red-lines / prior approvals, judge,
                         persist finding                      → finding.created
              [review]   underwriter accepts/edits/rejects    → review.captured
              [learn]    feedback → SQL → eval + rubric updates
```

Kafka gives us retry, replay, and back-pressure for free, and lets the UI stream partial findings as they arrive instead of waiting for the whole pack. **MVP** runs the stages synchronously per submission (Kafka topics in place but with one consumer each) and uses a single LLM judge prompt. **Target state** adds specialist judges per red-line category, a cheap clause-classifier that filters obvious clauses out before the strong judge, and active learning over reviewer disagreements.

## 3. Document parsing, segmentation, normalisation

Every input — broker base wording, endorsements, schedule, standard wording, red-line spreadsheet, prior approvals — is normalised into a single `Clause` record: `{clause_id, doc_id, doc_type, heading_path, text, span (page + char offsets), checksum}`. Span offsets are mandatory: every downstream citation re-derives the quote from the source so we can verify it survived round-tripping.

- **DOCX:** parse OOXML directly rather than via OCR. Heading levels (`w:pStyle`) drive primary segmentation; run-level styling marks defined terms which feed a per-document glossary. Red-line spreadsheet rows are ingested as `{category, narrative}` records keyed by region + LoB.
- **PDF / scanned:** Azure Document Intelligence layout model; pages below 0.9 word confidence go to manual triage rather than being fed to retrieval — a noisy clause produces a confident-looking wrong finding.
- **Defined-term scoping:** the broker's `Insured` is not the standard's `Insured`. We attach the source document's glossary to each clause so the comparator never compares definitions across scopes.
- **Schedule parsing:** the submission schedule becomes a typed record (Limit, Territory, Retroactive Date, requested features). **Line of business is inferred**, not parsed — the brief deliberately blanks the field. A constrained-output LLM call against an allow-list (`UK Tech PI`, `D&O`, …) reads business description + requested features and returns LoB plus confidence. Low confidence → underwriter prompt before the rest of the pipeline runs.

## 4. Clause-level comparison

For each broker clause we retrieve its closest standard-wording analogue and ask a judge LLM whether they materially diverge. Retrieval is hybrid (BM25 + dense embeddings) in Azure AI Search, with two channels run in parallel:

- **Topic anchor:** heading/topic match (Defence Costs, Insured, Notification). Catches cases where text differs heavily but topic is obvious.
- **Semantic span:** clause-body match against the standard corpus. Catches structural reshuffles — e.g. a defence-costs rule embedded inside a definitions clause.

The judge receives both clauses verbatim and returns `{material_divergence | immaterial_drafting | equivalent}`, a one-sentence rationale, and the **quoted phrase** responsible. Endorsements are applied **after** base comparison: if Endorsement 04 affirms cyber cover, the §8 base finding is re-evaluated against the endorsed position rather than reported alongside. Order-of-precedence is itself parsed from the broker wording rather than hard-coded, because brokers vary on this.

## 5. Red-line detection

Red-lines are narrative ("No catch-all language to automatically allow cover for either: (i) companies which are not legally defined…"). We deliberately do **not** translate them to deterministic rules — the narrative carries judgement that disappears in a regex. Each red-line is indexed as an embedded statement with its category tag. For each broker clause:

1. Retrieve top-k red-lines by hybrid search, hard-filtered to the same region + LoB.
2. Pass clause + candidate red-line to a judge that must answer *did this clause violate this red-line, and what exact phrase shows it*.
3. Surface the finding only with verbatim evidence; never without.

Red-line judgements live alongside, not inside, standard-wording divergences. They have different consequences — a red-line breach is an escalation, a drafting divergence is a discussion — and both can fire on the same clause.

## 6. Prior-approval retrieval and ranking

The highest-risk surface for hallucination. Two hard rules: (a) nothing is cited unless it came back from the search index — the LLM cannot invent an approval ID, validated by post-hoc lookup; (b) the ranking signal is **transferability**, not surface similarity. We score candidates on:

- **LoB match** (hard filter — different LoB never qualifies as precedent, but is surfaced separately as "similar text — different line of business" so the underwriter sees we considered and rejected it).
- **Region / jurisdiction match.**
- **Clause-text semantic similarity.**
- **Narrowness:** does the approved clause grant *less or equal* scope than the broker is requesting? A precedent narrower than the request is partial, not green-light.
- **Insured-size band and recency** when known.

The judge then returns relevance score, why, what's narrower, what's broader — quoting the differing phrases. This is what catches the Helix-D&O precedent being misapplied to Tech PI: LoB filter demotes it, and the rationale names the mismatch explicitly.

## 7. Findings: evidence, source, confidence

Every finding is persisted before render, matching the brief's schema:

```
finding_id · clause_id · issue_type · why_it_matters ·
comparison_source (standard_clause_id | red_line_id | prior_approval_id) ·
quoted_evidence · prior_approval (id + transferability + delta) ·
recommended_action · confidence · model_version · prompt_hash
```

Confidence combines retrieval score, judge self-reported confidence, and — most importantly — a **span-grounding check**: we re-extract the quoted phrase from the source by exact match and reject the finding if not found verbatim. This single gate removes most hallucinations cheaply.

## 8. Worked example — Defence Costs

The pipeline produces this finding from the supplied pack:

| Field | Value |
|---|---|
| Broker clause | §3 *"Defence Costs shall be payable by the Insurer **in addition to the Limit of Indemnity** and shall be advanced on an ongoing basis."* |
| Standard analogue | §3 *"Defence Costs form part of and not in addition to the Limit of Indemnity."* |
| Issue type | Material divergence + **Red-line breach (RL-03)** |
| Why it matters | Defence costs outside the limit can materially exceed the priced exposure; explicit insurer red-line for UK Tech PI. |
| Prior approval | **Helix-UK-DO-2023-041 — not transferable.** Surface-similar (advancement in addition to limit) but LoB = D&O, not Tech PI. Flagged "similar text, different LoB" rather than dropped silently. |
| Recommended action | Revert to standard wording, or escalate; do not rely on Helix. |
| Confidence | 0.92 — verbatim quote, RL-03 directly applicable, retrieval clean. |

A second finding fires on §6 + Endorsement 01 (prior acts for acquired entities). Here **Orion** is genuinely Tech PI and genuinely on-topic — but Orion *excludes* prior acts and caps at 15% turnover / 60-day notification, where the broker grants prior acts at 35% / 120 days. Orion is returned as a **narrower precedent**, with the deltas itemised — not as a green light. This is the precedent-relevance behaviour the brief is testing.

## 9. UX for the underwriter

A three-pane review screen: submission schedule and finding list on the left; the broker document (with each finding highlighting a span) in the centre; the comparison source — standard clause, red-line, or prior approval — pinned to the right when a finding is selected. Four actions per finding: *accept*, *edit*, *reject (one of five fixed reasons)*, *escalate*. Filters by issue type, red-line, severity, and "has prior approval". The underwriter can pivot a finding from standard-divergence to red-line-breach (or vice versa) — this is a strong feedback signal we record verbatim.

Feedback *shape* matters more than volume. A reject reason of "wrong precedent" trains a different model than "rationale incorrect"; both are different from "this is fine actually". Five explicit reject reasons plus free-text, and edits to the recommended-action field are first-class training data.

## 10. Evaluation

**Offline.** Curated gold set of ~150 broker clauses spanning the eight document families, each labelled with expected findings. Metrics: clause-level **recall** (did we surface every real issue?), **precision**, **evidence accuracy** (quoted span exact-matches the source?), and **precedent F1** split by transferable/non-transferable. Evidence accuracy is the dominant metric — precision/recall are tunable, but one hallucinated precedent breaks underwriter trust irrecoverably.

**Online.** Reviewer accept-rate per finding type, time-to-decision, post-bind incident rate, false-negative discovery rate (issues the model missed that surfaced at claims). Shadow-run every new prompt or model against the last N submissions before promotion.

**Test design.** Adversarial corpus baked in: structural reshuffles (defence-costs rule hidden inside §2 Definitions), defined-term shadowing (broker redefining `Insured`), endorsements that fully override base wording (Endorsement 04 affirming cyber), near-precedents from other LoBs (the Helix trap), and prior approvals that are *narrower* than the request (the Orion trap). CI gates promotion on no-regression in evidence accuracy and recall on red-line breaches.

## 11. Security, audit, monitoring

Blob Storage with per-tenant CMK encryption. Every LLM call persists `{prompt_hash, prompt_template_version, model, model_version, retrieved_doc_ids, raw_response, parsed_response, latency, cost}` to Azure SQL — this is the audit trail *and* the evaluation substrate. Findings carry their `prompt_hash` so any past decision is exactly reproducible. PII in submissions (insured names, addresses) is masked before LLM calls where the comparator doesn't need it. RBAC: prior approvals are insurer-confidential — the index enforces tenant + role at query time, not at render time.

Monitoring: cost per submission, p95 latency per stage, retrieval-empty rate, span-grounding-failure rate, judge-disagreement rate against historical reviewer decisions. Alerts on regression of evidence accuracy in shadow runs.

## 12. Risks, trade-offs, MVP→target

- **Hallucinated precedents** — highest-impact failure mode. Mitigated by hard-grounded retrieval, verbatim-quote requirement, LoB filter, and adversarial Helix-style tests in CI.
- **Endorsement override missed** — Mitigated by parsing order-of-precedence and replaying base findings under endorsements; surface a "suppressed by endorsement" trace rather than silently dropping a finding.
- **Variable broker formats break the parser** — Mitigated by OCR confidence gate that routes low-confidence pages to manual triage rather than feeding garbage to retrieval.
- **Reviewer over-trust** — Mitigated by no-auto-accept, surfacing retrieval evidence and confidence components separately rather than a single score, and showing what the model *didn't* find that the gold set says it should have during eval.
- **Cost** — MVP runs one strong judge per clause. Target state routes by clause type: a cheap classifier handles obviously-equivalent clauses (the majority), the strong judge runs only on retrieved-divergence candidates.

**MVP → target deltas.** Replace the single judge with specialist judges per red-line category; train a clause-classifier on accumulated reviewer labels so cheap clauses skip the LLM entirely; add a precedent-graph view so underwriters see *why* one approval is closer than another rather than a relevance number; introduce active learning over reviewer disagreements to surface clause types where the model and humans drift.

**Things I'd do differently if I could.** The line-of-business inference at intake is structurally fragile — a single bad inference propagates to every downstream filter. In target state I'd surface LoB inference to the underwriter for one-click confirmation before the pipeline fans out, rather than treating it as silent metadata.
