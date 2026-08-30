# Event Impact Triage

Event Impact Triage is the Harness-owned boundary between prospective receipt and checkpoint or
impact analysis. It prevents two opposite errors: treating every headline as a trade trigger, and
treating every item outside one registered checkpoint rule as financially irrelevant.

## Correct state interpretation

A Prospective Checkpoint Readiness Report may show post-admission candidate versions. That state
means only that real, actual-receipt source content exists and still needs classification. Operator
or Codex inspection outside the Harness can postpone promotion conservatively, but it cannot create
an `eligible` or `ineligible` record, measure classifier quality, or authorize a model or order.

The previously inspected news versions therefore remain **unclassified**, not formally ineligible.
An `ineligible` triage result is always relative to one frozen checkpoint eligibility rule. It is
never a claim that the event has zero market, industry, issuer, or portfolio impact.

## Layered flow

```text
actual-receipt Observation Versions
  -> freeze every unclassified version in receipt order
  -> factual and checkpoint-rule classification (portfolio-independent)
  -> Event Impact Triage Proposal from a bounded Agent run
  -> Harness validates full coverage, citations, run authority, cost and receipt order
      -> eligible: first-eligible checkpoint candidate
      -> needs_review: block selection and create/extend an Attention Watch
      -> ineligible for this checkpoint:
           -> EventAssessment when a plausible transmission path exists
           -> Attention Watch when a material fact is unresolved
           -> archive when the registered rule is not met and no further route is justified
  -> only the selected checkpoint candidate may continue to PDI-30 and Query Gate
```

Checkpoint eligibility must remain independent of current holdings. Otherwise the same fact could
change classification merely because the portfolio changed. Holdings enter the next impact and
priority layer, where they should receive higher urgency than searching for new opportunities.

## Context for impact and portfolio priority

The impact layer may use only content-bound inputs available at its cutoff:

- a Position Snapshot containing instrument, side, quantity, account/environment pseudonym,
  observed-at time, and Provider/reconciliation identity without broker credentials;
- prospective event facts and revisions from the Candidate Set;
- market, universe, tradability, expectation, macro and positioning Decision Inputs when present;
- a Historical Analogy Pack whose cases retain `strict`, `modeled_pit`, or `outcome_opened_review`
  labels and cannot upgrade one another;
- registered Method Skills for evidence quality, event structure, exposure mapping, historical
  analogy, market state, reflexivity and countercases.

Position-aware analysis may change attention priority, invalidation conditions, risk-reduction
recommendations, or the need for a Watch. It cannot change source facts, historical authority,
checkpoint eligibility, Trading Mandate, approval, or execution state.

## Model adapters and bounded specialist Agents

The semantic judgment belongs inside the project runtime, not to an external Codex operator. The
existing Model Provider Profile remains the only adapter entry. A triage execution plan may select
one coordinator and only the specialists justified by available inputs:

| Role | Allowed output | Must not own |
| --- | --- | --- |
| fact verifier | changed facts, revisions, source conflicts, citations | checkpoint or trade state |
| transmission mapper | first- through fourth-order paths and affected entities | portfolio truth |
| portfolio impact | intersection with a frozen Position Snapshot and urgency | account credentials or orders |
| historical analogy | comparable structures, differences and evidence-lane labels | causal or PIT promotion |
| countercase reviewer | disconfirming evidence, missing links and invalidation conditions | final disposition |
| coordinator | one canonical Triage Proposal covering every candidate version | Harness admission |

Specialists are optional, persona-free tasks rather than an open debate. The Harness freezes each
role's model profile, prompt, Skill/tool/MCP surface, input identities, token/cost ceiling and child
count before execution. Every child produces a typed artifact and Usage Ledger record; the
coordinator receives those immutable artifacts but must still produce an independently validated
complete Proposal. The Harness reopens the entire run bundle before it admits a Triage Decision. No
Agent may recursively expand roles, discover arbitrary tools, or call execution Providers.

The v1 runtime now executes either a frozen coordinator-only baseline or a treatment with exactly
three bounded specialists—fact verification, transmission mapping and countercase review—before the
coordinator. Every role uses the registered Model Provider Profile, exact Skill manifest hashes,
closed JSON output, no tools or MCP servers, at most three correction turns, and a precomputed token
and cost ceiling. Outputs, raw responses, validation events and usage are sealed; completed runs are
reopened exactly. A process interruption with an uncertain Provider outcome becomes
`human_input_required` and is never retried automatically.

The first real 121-version run showed that one full-set output per role is not an acceptable work
unit. The coordinator-only arm completed two Provider turns before a later turn timed out; the
treatment completed two turns before its first specialist exceeded the frozen output budget. The
Harness produced no Comparison Report or triage selection. This is preserved as negative runtime
evidence, including the known Usage records and the unresolved timed-out Provider outcome. It is
not a semantic classifier result and cannot be repaired by raising limits or replaying the uncertain
request.

PDI-29E therefore introduces a pre-model Work Manifest. It binds the same Candidate Set to stable,
receipt-ordered work units under both a candidate-count ceiling and an estimated serialized-prompt
ceiling. Exact normalized-content duplicates may share one atom, but every Observation Version
identity remains covered. The manifest contains hashes, sizes, ordering and policy only—never paid
news payloads or labels—and grants no PIT, Judgment or execution authority. Baseline and treatment
must use the same manifest. Any missing, duplicate, reordered or oversized unit fails closed.

The later runtime recovery is intentionally separate: bounded map units produce compact candidate
digests, a partition unit proposes cross-unit event clusters from those digests, and bounded
classification units reopen only each proposed cluster's raw frozen content. The Harness alone
assembles and validates the unchanged full Proposal. Every unit needs its own authoritative Run and
Usage record; one failed or ambiguous unit blocks the arm rather than allowing partial conclusions.

The v1 Manifest contract has passed both a synthetic 121-version test and the exact private real
Candidate Set preflight. The real Manifest contains 121 distinct atoms and eleven work units: ten
units of 12 versions and one of one. Their conservative canonical-JSON UTF-8 size bounds are all
below the frozen 32,768 ceiling. No payload or label is present in the Manifest. This proves only
that the current input can be partitioned deterministically; runtime phase binding, per-unit Usage
authority and completed comparison arms remain unimplemented.

Position and historical IDs are deliberately not accepted yet. The Harness must first define and
reopen their typed payloads so the model receives actual cutoff-bound content rather than an opaque
identifier. Their absence does not block portfolio-independent triage. Until those contracts pass,
portfolio-impact and historical-analogy specialist roles remain outside executable plans.

## Skill accumulation and evaluation

Triage quality and trading quality are separate claims. A reusable discovery/impact Skill advances
only through a pre-reveal evaluation:

1. freeze a broad discovery prompt, source/time coverage and case-exclusion manifest;
2. keep exact events or structures as hidden holdouts;
3. score exact-event recall, event-family recall, time-authority accuracy, exposure mapping,
   `needs_review` calibration, unsupported-path rate and resource cost;
4. run an independent countercase pass and repeat on another time or event-family block;
5. promote the Skill only for the demonstrated domain; record misses as ontology, source, mapping,
   timing or instruction failures rather than adding example-specific keywords.

The recent hidden-case miss supports three general additions—forecast probability changes,
clinical milestones, and recall/channel/regulatory escalation—but does not establish their trading
direction or alpha. Historical outcome-opened cases may develop or audit a Skill; they cannot enter
strict historical backtest inputs unless their version and authority were provable at cutoff.

## Dual-track acceptance

The shared engine-neutral EventAssessment, Candidate Impact, Signal Intent, Order Intent and Policy
contracts feed two separate authority lanes:

- **Historical/backtest:** strict PIT admits only content and authority known by cutoff. Late-found
  history belongs to Modeled-PIT or outcome-opened review and is excluded from strategy promotion.
- **Prospective/paper/live:** actual receipt -> triage -> Snapshot/Query Gate -> Judgment -> Decision
  Admission. Missing optional information stays visible and does not globally block experiments.

Prospective process diagnostics and `manual_each` mock paper can advance without repairing all old
PIT gaps. Strategy-labeled paper/live promotion still requires registered multi-case calibration,
tradability, mandate, risk, approval, reconciliation and execution acceptance. Prospective evidence
does not retroactively qualify history, and retrospective evidence does not authorize a live action.

## Acceptance status

Implemented contract evidence:

- complete Candidate Set freezing against a persisted prospective Data Snapshot and Readiness
  Report;
- exact partition of every candidate version into cited event clusters;
- checkpoint-relative `eligible`, `ineligible`, and `needs_review` outputs with separate impact
  routes and an explicit prohibition on zero-impact claims;
- first-eligible ordering that blocks on an earlier unresolved candidate;
- authoritative coordinator/specialist run-bundle and Usage Ledger binding before a Harness
  Decision;
- content identities and JSON Schemas for Candidate Set, Proposal, Decision, Execution Plan,
  Specialist Artifact, Label Set, Comparison Registration and Comparison Report;
- a no-tool Model Provider runtime for the coordinator-only baseline and bounded three-specialist
  treatment, including exact restart reopening and fail-closed ambiguous-interruption behavior;
- registered comparison mechanics for complete pre-reveal labels, false-positive/false-negative,
  `needs_review`, route, unsupported-path and cost scoring. A single or operator-exposed batch cannot
  authorize promotion; an append-only SQLite store records the complete label/plan identity under
  the Harness clock, each arm must start afterward, and Provider-reported token/cost usage must stay
  inside the frozen ceilings and reopen from the authoritative Usage Ledger.

Still required for real acceptance:

- typed, secret-free Position Snapshot and evidence-lane-preserving Historical Analogy Pack payload
  bindings before their optional specialists can be enabled;
- complete PDI-29E Work Manifest and bounded work-unit recovery of the sealed 121-version batch;
  the existing monolithic attempt failed before a Comparison Report and remains negative evidence;
- replay through both arms with complete per-unit Run Records and costs, followed by a later pristine
  blind batch and an explicit cross-batch promotion disposition;
- passing semantic results for false positives, must-catch misses, `needs_review`, routing,
  unsupported transmission paths and resource use;
- a real selected event passed through PDI-30 and Query Gate.

None of this grants historical PIT, Judgment-run, Strategy Admission, paper/live execution, account,
credential, mandate, approval, or broker authority.
