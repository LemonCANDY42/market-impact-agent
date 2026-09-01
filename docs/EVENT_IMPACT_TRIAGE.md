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
  -> freeze the earliest bounded unclassified receipt-order prefix
     (the Batch Selection also binds the complete unclassified set)
  -> factual and checkpoint-rule classification (portfolio-independent)
  -> Event Impact Triage Proposal from a bounded Agent run
  -> Harness validates full coverage, citations, run authority, cost and receipt order
      -> eligible: first-eligible checkpoint candidate
      -> needs_review: block selection and create/extend an Attention Watch
      -> ineligible for this checkpoint:
           -> EventAssessment when a plausible transmission path exists
           -> Attention Watch when a material fact is unresolved
           -> archive when the registered rule is not met and no further route is justified
  -> Trigger Admission
       -> checkpoint_eligible: exact selected Triage Decision cluster
       -> material_event: canonical EventAssessment projection + passing Materiality Gate
  -> only the exact Trigger Admission may continue to PDI-30 and Query Gate
```

The bounded prefix is a capacity boundary, not a relevance filter. Sources continue to append while
Triage runs, so freezing every outstanding candidate can exceed the Work Manifest's accepted
128-version ceiling and permanently prevent formal classification. Batch Selection instead binds
the full unclassified population and deterministically selects only its earliest prefix. A later
batch cannot overtake an earlier unresolved version, and no model chooses which candidates enter
the batch. The selected versions are then reopened from the append-only Journal in actual receipt
order and frozen into one cross-source Data Snapshot before any model call.

The Harness then installs one durable Active Batch head for the exact registration, checkpoint,
route plan and route-admission epoch. Re-running `agent prospective-triage-run`, changing its
requested batch size, or racing another caller reopens the same Snapshot, Candidate Set, Manifest,
Execution Plan, Run Journal, Usage Ledger and Provider-health state. It cannot create an overlapping
Plan while that head exists. Only the concrete append-only Decision Store reopening the exact
Candidate Set Decision releases the head. That transaction also advances the route-epoch revision;
a stale preparation that lost the original race cannot install after the winning Decision. A failed
or ambiguous member remains attached to the original Plan and retains its original replacement and
cost authority.

Checkpoint eligibility must remain independent of current holdings. Otherwise the same fact could
change classification merely because the portfolio changed. Holdings enter the next impact and
priority layer, where they should receive higher urgency than searching for new opportunities.

## Context for impact and portfolio priority

The impact layer may use only content-bound inputs available at its cutoff:

- the currently implemented minimum Position Snapshot containing only target, venue, instrument
  class and `as_of`, sufficient to detect a held-target intersection but not portfolio size, side,
  concentration or account state;
- prospective event facts and revisions from the Candidate Set;
- market, universe, tradability, expectation, macro and positioning Decision Inputs when present;
- a Historical Analogy Pack whose cases retain `strict`, `modeled_pit`, or `outcome_opened_review`
  labels and cannot upgrade one another;
- registered Method Skills for evidence quality, event structure, exposure mapping, historical
  analogy, market state, reflexivity and countercases.

Position-aware analysis may change attention priority, invalidation conditions, risk-reduction
recommendations, or the need for a Watch. It cannot change source facts, historical authority,
checkpoint eligibility, Trading Mandate, approval, or execution state.

## Formal impact-to-decision bridge

Triage and decision-input freezing now meet through one Harness-owned `Prospective Trigger
Admission`; there is no operator/Codex shortcut. A policy, earnings, or macro checkpoint-rule
selection uses the `checkpoint_eligible` kind. A financially material event uses `material_event`
only when the frozen checkpoint itself is the registered material-event mechanism and Triage routed
the cluster to `EventAssessment`; a completed-assessment authority must then produce the canonical
engine-neutral projection, and the registration-bound deterministic Materiality Gate must admit at
least one cited target/path/horizon combination. Material candidates preserve ready-time order
across all completed Triage Decisions in the same admitted route epoch: a later passing cluster must
bind every earlier routed cluster's non-admitted result. The durable authority also blocks an earlier
unresolved review or direct eligible candidate from another batch. A non-material checkpoint cannot
reuse this route, and a material-event checkpoint cannot use the direct-selection shortcut.

The prospective EventAssessment artifact is a binding/projection, not a second causal authority. It
retains the canonical EventAssessment artifact hash, exact Triage Decision/cluster, cited
Observation Versions, transmission paths, counterevidence, invalidation conditions, and optional
Position Snapshot/Historical Analogy Pack. Missing optional context is recorded as degradation.
The Materiality Gate verifies scope and evidence reconciliation; it does not infer alpha or declare
tradability. Snapshot Set v5 and Query Gate v5 both require the exact Trigger Admission, and a
material-event Query Gate rejects target inputs outside the admitted Materiality result.
The durable Trigger Admission reopens the authoritative Triage Candidate Set/Proposal/Decision, the
ready-time-ordered history for the entire route epoch and the completed EventAssessment authority.
Snapshot Set and Query Gate must in turn reopen that durable admission; self-consistent caller-created
IDs are insufficient.

The concrete PDI-29G runtime now performs one bounded no-tool model call per ready-time-ordered
EventAssessment cluster. Before dispatch, the Harness reopens exposure observations actually received
by the Triage Decision cutoff and freezes an `ExposureCandidateView`. An exact issuer/ETF reference
narrows the mapping; otherwise a bounded catalog lets the model discover a supported target without
inventing one. The prompt receives compact labels while the terminal artifact retains the full private
Observation Version provenance. Output targets must exactly match the frozen view. A valid no-path
response is a completed `watch` business outcome and remains non-admitting; transport ambiguity,
invalid JSON/semantics and authority mismatch remain run failures. Only a path-bearing completed run
can create the canonical EventAssessment projection and enter deterministic Materiality.
Provider construction is lazy: terminal replay and Usage reconciliation do not require credentials or
touch the Provider. A preparation failure before dispatch leaves the same Run safely resumable; an
OPEN circuit can recover only through a successful safe availability probe. Any durable dispatch with
no terminal response is instead recorded as an unknown Provider outcome with nonzero attempt usage and
cannot be retried automatically. The orchestration reserves a complete bounded EventAssessment unit
against the registration's aggregate cost before dispatch, so an over-cap run cannot become completed
assessment authority or Trigger Admission.

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
below the frozen 32,768 ceiling. No payload or label is present in the Manifest.

The v2 Work Execution Plan and runtime now bind that Manifest before any model request. The Plan is
content identified over the exact Candidate Set and Manifest hashes, comparison arm, Model Provider
Profile, Skill and prompt/output contracts, stable map graph, maximum classify fan-out and
per-phase/aggregate run, token and cost ceilings. Baseline maps each Work Unit through one
coordinator; treatment fixes fact verification, transmission mapping and countercase review before
the Work Unit coordinator. One partition coordinator consumes only the complete Digest set, and
one classify coordinator per Cluster Seed reopens only that seed's frozen raw content. The Harness
then assembles the unchanged full Proposal and validates exact Candidate Set coverage.

The v3 Work Execution Plan is a second explicit dialect of this same Harness runtime, not another
orchestration owner and not another Work Manifest. V2 Plan JSON, prompt bytes, output and correction
contracts, execution bindings, transcripts, terminal artifacts and replay remain unchanged. V3 has
its own Plan schema, runtime reference, prompt-template IDs and Run artifact schema IDs. In v3, each
specialist returns one finding object per input atom without `atom_id`, and the coordinator returns
one Digest draft per atom without `atom_id`; exact array length and order are the binding, and the
Harness injects the Work Unit's authoritative atom IDs before sealing the accepted output and
building canonical Candidate Digest v1 artifacts. The partition coordinator receives global
zero-based `atom_ordinal` values and returns `atom_ordinals`; the Harness rejects booleans,
non-integers, negative or out-of-range values, non-increasing order within a cluster, duplicates and
missing coverage before translating ordinals into canonical Cluster Partition v1 atom IDs. Classify
and final Proposal assembly retain their existing semantic authority.

V3 corrections carry the v3 output contract and stable validation categories without echoing atom
IDs. Map contracts describe each narrative field as a bounded array of trimmed strings and include
the forbidden label/route/authority control vocabulary; this supplies the model with the type,
length and safety constraints that v2's field-name-only contract did not express. Parsing remains
strict: there is no tolerant coercion, guessed identity or relaxed reserved-token guard. A
comparison may register v2/v2 or v3/v3 Plans, never a mixed pair; the existing Work Comparison
Registration and Report remain the authority for v3/v3 because their semantic inputs and evidence
requirements did not change.

The v4 Work Execution Plan keeps v3 map and partition byte semantics but closes the remaining
classify-contract gap exposed by a real nine-version batch. Its classify prompt specifies each
field's scalar/array type, every eligibility/route/archetype/stage/channel enum, bounded narrative
arrays and `triage_confidence` in `[0, 1]`. The model no longer returns
`candidate_version_ids`; the Harness injects the exact Cluster Seed identities before building the
canonical Proposal. V4 has distinct Plan, prompt, runtime and terminal artifact revisions. It does
not relax parsing, coverage, budget or authority checks, and v2/v3 evidence remains immutable.

The v5 Work Execution Plan removes the remaining unnecessary long-ID echo found by the second real
v4 batch. Classify now returns strictly increasing `evidence_ordinals` into the frozen Cluster Seed
instead of copying `evidence_version_ids`; the Harness resolves each ordinal to the exact supplied
Observation Version before building the same canonical Proposal. Empty, duplicate, non-integer or
out-of-range ordinals remain invalid. Corrections expose a bounded validation category such as
`invalid_evidence_ordinal_coverage_or_order`, not evidence content. This simplifies the model seam
without weakening citation ownership, frozen coverage or replay authority.

The first same-Candidate-Set v5 infrastructure revalidation proved 22 real classify outputs could
use ordinal citation, but the 23rd classify member selected EventAssessment with no transmission
channel three times. V5 exposed only a generic correction and therefore stopped after 39 completed
members and 42 Provider attempts, with no Proposal or Decision. Its 306,299 input / 164,488 output
Token result is terminal infrastructure evidence, not semantic quality or alpha evidence.

The v6 Work Execution Plan retains v5 identity binding and adds the five route conditions already
enforced by `TriageClusterProposal`: eligible checkpoint routing and evidence, ineligible route
exclusion, `needs_review` route/uncertainty, EventAssessment fact/archetype/transmission, and Attention
Watch fact/question requirements. `rule_reasons` is explicitly non-empty. Corrections return bounded
categories such as `event_assessment_requires_fact_archetype_and_transmission`; raw evidence stays
out of the error category. These are model-visible descriptions of existing Harness invariants, not
new policy authority or stricter output symmetry.

The same-Candidate-Set v6 infrastructure revalidation completed all 47 logical members after one
explicit replacement of a legacy-misclassified HTTP 408 Run. Its authoritative Ledger includes the
old ambiguous dispatch and the replacement: 48 Usage records / physical attempts, 396,709 input and
172,508 output Tokens. The admitted Decision classified all 39 versions into 30 clusters: 28 archive
and two EventAssessment routes. The EventAssessment clusters were a reported tanker attack while
transiting the Strait of Hormuz and a reported Shanghai Telecom network outage; both were ineligible
for the frozen capital-market-policy checkpoint but retained explicit risk/cost or demand/attention
transmission channels. No eligible checkpoint, Query Gate, Judgment or execution authority exists.
Because v4, v5 and v6 reuse the same exposed Candidate Set, this is contract/runtime evidence only,
not a pristine blind semantic promotion result.

The v7 Work Execution Plan leaves the v6 prompt, identities and semantic output contract unchanged.
It uses pinned `json-repair==0.63.4` as the direct parser; the library internally keeps its standard-
JSON fast path. Harness admission remains deliberately narrower: a non-strict response is accepted
only when repair is exactly one structural punctuation insertion or deletion and the complete
sequence of strings, field identities, number text and literals is unchanged. The Run persists the
raw-content hash, parsed-output hash, parser/policy identity and structural edit as a content-
identified parse-evidence artifact. Multiple edits, quote/literal/number changes, schema-guided
coercion and untrimmed wrappers still fail closed.

The v8 Work Execution Plan keeps the v7 parser and immutable older dialects but removes one
redundant material-event output. A material-event stage-one classifier cannot know final checkpoint
eligibility before EventAssessment and the deterministic Materiality Gate, so it returns only
`archive`, `attention_watch`, or `event_assessment`. The Harness injects the provisional canonical
eligibility value for artifact compatibility. Direct policy, Earnings and macro checkpoints retain
model-authored checkpoint-relative eligibility because their frozen trigger rule is decidable at
classify time.

The pristine v8 comparison rejected the remaining architecture. Baseline completed 27 logical
members at 106,755 input / 113,878 output Tokens and routed all five consensus must-catch labels.
Treatment completed 31 logical members at 257,800 input / 159,428 output Tokens, missed all five,
and produced 14 unsupported material routes. Treatment had loaded news verification, equity exposure
and adversarial-risk instructions before a frozen universe, exposure graph or portfolio existed; it
over-aggregated evidence and routed every resulting cluster to Attention Watch. This is a structural
counterexample to doing full downstream analysis at ingress. Baseline still had only 13/29 exact
routes, so neither arm is accepted.

V9 applies the narrower first-principles boundary:

- Harness owns actual-receipt admission, exact-content deduplication, Work Unit bounds, stable order,
  model profile, cost, replay and durable authority.
- One coordinator call per Work Unit returns one positional item per atom: route, changed fact,
  explicit typed transmission or unresolved observable. It does not echo IDs.
- Archive requires no supported plausible material transmission. Attention Watch requires a named
  future observable. EventAssessment requires a concrete changed fact and one explicit plausible
  transmission. The model does not decide final materiality.
- No model phase clusters events. Each exact-content atom is a provisional one-atom cluster; later
  EventAssessment/Watch may link revisions and evolving event stages with additional evidence.
- Harness derives the compatibility Digest, Partition and Proposal. `triage_confidence` is fixed to
  zero because v9 does not ask for an uncalibrated probability. Complete target mapping, direction,
  magnitude, portfolio effects, historical analogies, countercases and actions remain downstream.

For the current 29-atom shape this reduces one arm from roughly 27-31 model members to three bounded
Work Unit calls. Scripted acceptance covers mixed archive/Watch/EventAssessment output, positional
coverage without model-visible IDs, deterministic downstream expansion, Usage reopening and restart
with no additional Provider call. Real semantic acceptance still requires a new pristine batch.

This boundary follows proven component patterns, not a claim that an LLM trading strategy is proven:
[NautilusTrader](https://nautilustrader.io/docs/latest/concepts/) and
[LEAN](https://www.quantconnect.com/docs/v2/writing-algorithms/key-concepts/algorithm-engine)
keep deterministic event/execution state around replaceable strategy logic;
[Qlib](https://github.com/microsoft/qlib) separates research workflows from execution authority; and
weak-supervision cascades such as [Snorkel](https://snorkelproject.org/get-started/) motivate a cheap
high-recall entrance followed by richer downstream analysis. The Harness adopts those ownership and
cascade ideas while retaining its own PIT, approval and execution gates.

A failed comparison no longer permanently occupies the active head. The state authority first stores
an append-only terminal batch bound by content hash to the Candidate Set, Comparison Registration,
Comparison Report, blockers and versions;
only then may the run authority release the head. Readiness excludes those versions from later
selection, but Trigger Admission cannot see them as a Decision. This preserves the failure without
manufacturing archive, eligibility or financial-impact claims.

The rejected pristine v8 batch has completed that path. A no-call Provider replay reopened the old
27/32 historical attempts and their Usage records, reproduced the failed gate, terminalized all 29
candidate versions and released the active head. The attempt counts are historical Ledger evidence;
the terminalization sent zero new generation requests. The next semantic evidence must come from a
new v9 pristine batch.

Every Provider call has a durable request-dispatched event first. A timeout or process interruption
after dispatch without a completed response becomes `human_input_required`; restart never sends
that request again. Completed units reopen without a Provider call. Run identity includes phase,
Work Unit or Cluster Seed, and role, so the treatment may repeat the same role across bounded units
without weakening identity. Authority recomputes the expected graph and reopens every prompt,
response, transcript, terminal artifact, metric and exact per-unit Usage record. One missing,
ambiguous, over-budget or tampered unit blocks all downstream phases and produces no Proposal.
An explicit replacement retains the original ambiguous Usage beside the new Run and consumes the
same turn, input, output, cost, phase and aggregate ceilings rather than receiving a fresh budget.
Its Journal start must not precede the Grant time.
An old pre-v7 member that already exhausted its received-response budget may instead use one explicit
Format Recovery Grant, but only when the final immutable response passes the bounded v7 punctuation
policy and the closed semantic contract. This is not a Provider retry: a separate recovery Run makes
zero Provider calls and creates no Usage, while authority keeps the source failed Usage charged and
reopens the original prompt/response chain. Recovery-of-recovery and silent old-plan reinterpretation
are forbidden.
An exclusive crash-safe claim rooted beside the Run Journal gives exactly one process ownership of a
Run while it may dispatch; a concurrent same-plan caller returns fail-closed without calling the
Provider. Every correction turn binds its own dispatch prompt to the preceding invalid assistant
response and deterministic Harness correction. Terminal output, transcript and raw response must
match the last completed response and validation event exactly. Returned Provider usage is journaled
before model/tool/secret validation; secret-bearing response bodies are never persisted. Each
completed predecessor passes this full reopening gate before its output may enter a later phase.
Scripted-provider acceptance covers the frozen v2 behavior, v3 positional behavior, v4/v5 typed
classify behavior over 121
candidates, a cluster spanning the first and last Work
Units, exact final coverage, both arms, restart without repeated calls, ambiguous dispatch,
Provider-reported budget excess, concurrent ownership, multi-turn correction, invalid Provider
identity/tool/secret responses, cross-Manifest input and artifact/event/pointer/Usage tamper or
predecessor corruption rejection. V3 also covers every specialist role, positional coordinator
binding, the twelve-item substituted-atom-ID failure shape, typed correction/Usage evidence, every
ordinal validation class, Harness-bound v5 evidence citation, schema packaging and equal-revision
reporting. This is mechanics evidence only;
private real-run terminal evidence and semantic quality remain separate acceptance layers. A later
actual-receipt v4 treatment completed all 13 expected members over nine Digests and eight Cluster
Seeds, then fully reopened its Proposal and Usage Ledger; that is one real process result, not a
semantic promotion result.

The first real v2/v2 Work Comparison is terminal negative evidence. It registered the exact sealed
121-version Candidate Set, operator-exposed Label Set and eleven-unit Manifest as comparison
`event-impact-triage-work-comparison-e45b1d3cb71a5949a3418b82d06fbb32aab46eec062cde3e35cc61419ae2ff97`
before either arm started. Baseline completed four members, then its fifth map/coordinator member
used three full responses and exceeded the 32,768 aggregate unit-output ceiling at 34,552 after
successive array-type, string-item-type and reserved-control-token violations. Its terminal arm
totals are 158,332 input / 121,558 output Tokens and ten Provider attempts. Treatment completed five
members, then its sixth map/transmission-mapper member failed the closed contract after three
responses each returned twelve items but replaced one required atom ID with one nonexistent ID. Its
terminal totals are 134,069 input / 42,707 output Tokens and eight attempts. Private summary hash
`99c87ad303d2241792e698e08ed2616db388531c49d0b40f6ec51aa0b763f1e5` binds the outcome. Neither arm
completed, so no Comparison Report, score, promotion or downstream authority exists; this evidence
cannot be retried, repaired, or converted into v3 evidence.

The v2 work path has its own append-only Work Comparison Registration and Report; it does not reuse
or mutate the failed v1 comparison identity. Before either arm may start, the Harness-clock
registration binds the exact Candidate Set ID/hash, sealed Label Set, Work Manifest ID/hash, both
Work Execution Plan IDs, prospective registration, checkpoint contract, Model Provider Profile,
frozen v1 semantic metrics/gates and the sum of the two plan cost ceilings. Labels remain outside
both plans and every runtime prompt. Evaluation first performs the complete per-unit artifact,
journal and Usage reopening, then derives arm start from the earliest authoritative Run creation,
finish from the latest terminal Run update and cost from exactly the completed Usage records.
Terminal artifact timestamps must reconcile to those Run records. A pre-registration start,
failed, ambiguous or incomplete unit, identity drift, cross-Manifest input, missing or extra Usage,
or timestamp/artifact tamper fails before scoring.

The Report binds both Work Plan IDs, content hashes of both complete Arm Outcomes, hashes of both
authority receipts and the registered aggregate cost cap. Its closed blocker taxonomy determines
the batch gate; promotion-only blockers do not fail that gate, while unknown or score-contradictory
blockers cannot be rehashed into a valid Report. Downstream use must invoke the Harness report
authority with the registration, frozen inputs, both outcomes and both runtime authorities. That
authority fully re-evaluates and reopens the comparison at the recorded evaluation time and requires
byte-identical Report output; a structurally valid caller-authored Report is not acceptance evidence.

The bounded 121-candidate fixture freezes eleven Work Units, 133 baseline and 166 treatment maximum
Runs, and a 299 microusd aggregate plan cap. It completes and scores both arms, validates the two
new schemas, proves label absence from Manifest/plans, and reopens both arms after restart without
another Provider call. This remains scripted mechanics evidence only. The original v1 real attempts
remain immutable negative capacity evidence and are not eligible inputs to a v2 Report.

The map/partition artifact layer is also closed. One Candidate Digest binds one exact Manifest atom
and may record bounded supported facts, conflicts, transmission hypotheses, countercases,
uncertainty and checkpoint-rule evidence. Every field may remain empty when extraction is not
supported; the contract never forces the model to invent a fact. A Cluster Partition then consumes
every Digest and atom exactly once. It may join atoms from different Work Units, requires evidence
for a definite multi-atom merge, and represents an unresolved grouping as `needs_review`. Neither
artifact contains labels, eligibility, impact route, PIT, Judgment or execution authority. Their
schemas and deterministic assembly tests pass. The nine-version v4 run produced nine real Digests,
one exhaustive Partition with eight Cluster Seeds and a complete Proposal. The batch, atom,
work-unit, Digest and Cluster surfaces all cap retained candidate
identities at 128. Digest and merge narratives reject reserved label, route and authority control
tokens; arbitrary semantic paraphrases still require the later typed classifier and Harness
validation rather than being treated as structurally impossible.

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

The durable accumulation path is now owned by `docs/SKILL_GOVERNANCE.md`. A triage miss, exposure
path, event-family structure, or countercase may seed one Outcome-Opened Full-Information Review,
but it remains a research conclusion. It needs two additional independent Event Case/time-family
validations, bounded divergence, resolved material counterexamples, and a complete current
Skill/catalog conflict review before it can become a non-executable Skill candidate. The triage
coordinator and specialists working on one Candidate Set are one analysis unit, not independent
validation evidence. No candidate can route itself back into triage or Judgment.

## Dual-track acceptance

The shared engine-neutral EventAssessment, Candidate Impact, Signal Intent, Order Intent and Policy
contracts feed two separate authority lanes:

- **Historical/backtest:** strict PIT admits only content and authority known by cutoff. Late-found
  history belongs to Modeled-PIT or outcome-opened review and is excluded from strategy promotion.
- **Prospective/paper/live:** actual receipt -> triage -> Trigger Admission -> Snapshot/Query Gate ->
  Judgment -> Decision Admission. Missing optional information stays visible and does not globally
  block experiments.

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
- first-eligible route-epoch ordering across completed Decisions that blocks on an earlier unresolved
  candidate;
- authoritative coordinator/specialist run-bundle and Usage Ledger binding before a Harness
  Decision;
- typed Position Snapshot and evidence-lane-preserving Historical Analogy Pack contracts plus
  durable prospective EventAssessment/Materiality/Trigger Admission storage and replay;
- Snapshot Set/Query Gate identity binding for either an exact checkpoint-eligible selection or a
  formally material EventAssessment path, without granting Judgment or execution authority;
- content identities and JSON Schemas for Candidate Set, Proposal, Decision, Execution Plan,
  Specialist Artifact, Label Set, Comparison Registration and Comparison Report;
- a no-tool Model Provider runtime for the coordinator-only baseline and bounded three-specialist
  treatment, including exact restart reopening and fail-closed ambiguous-interruption behavior;
- registered comparison mechanics for complete pre-reveal labels, false-positive/false-negative,
  `needs_review`, route, unsupported-path and cost scoring. A single or operator-exposed batch cannot
  authorize promotion; an append-only SQLite store records the complete label/plan identity under
  the Harness clock, each arm must start afterward, and Provider-reported token/cost usage must stay
  inside the frozen ceilings and reopen from the authoritative Usage Ledger.
- append-only Decision authority for direct-run v1 evidence, immutable legacy Work v2 evidence and
  current multi-member Work v3 evidence. V3 additionally binds the authority-derived Work finish
  time and requires it to equal `decided_at`; an equivalent retry may verify and reopen an existing
  v2 Decision but never rewrite it. The real nine-version legacy v2 Decision classified every
  candidate exactly once, producing
  five archive, two EventAssessment and one Attention Watch cluster with no eligible checkpoint;
  the subsequent readiness audit excluded all nine and exposed 26 genuinely new candidates.
- a second real v4 treatment froze 39 later actual-receipt versions into four work units, 39
  Digests and 29 Cluster Seeds. The full treatment graph contains 46 logical members: sixteen map
  role/work-unit members, one partition and 29 classify. Before operator resolution, 29 completed,
  one classify member was sealed `_AmbiguousRun` / `human_input_required`, and sixteen logical
  members had not started; thirty were attempted across 31 physical Provider attempts, consuming
  343,211 input and 151,024 output Tokens. The incident
  path was an upstream TLS `bad record MAC`, followed by an unsafe second project generation POST
  and gateway `auth_unavailable`, not quota or HTTP 429 evidence. The batch therefore has no
  Proposal, authority receipt or Decision. It is negative runtime evidence, not a partial semantic
  result; restart cannot resend the ambiguous request, and the completed classifications cannot be
  assembled by an operator outside Harness authority. The later explicit one-time Replacement Grant
  preserved that Run and Usage, created one distinct replacement identity and reused every completed
  member. The replacement succeeded and the graph advanced to 39 completed logical members, then a
  different classify member failed after three structurally consistent responses each copied its
  sole evidence Version ID with one missing or extra character. Six logical members never started.
  The authoritative Ledger contains 41 Run Usage records, 44 physical attempts, 393,440 input and
  205,023 output Tokens. No Proposal or Decision exists. That failure motivated v5 ordinal evidence
  citation; it does not authorize replay of the terminal v4 member. Prospective
  failure/retry/circuit semantics are owned by
  [MODEL_PROVIDER_RELIABILITY.md](MODEL_PROVIDER_RELIABILITY.md).
- the same-Candidate-Set v5 infrastructure revalidation proved 22 classify members could cite exact
  evidence through Harness-resolved ordinals, then stopped on an undeclared EventAssessment
  transmission requirement. V6 made the existing conditional route invariants model-visible. Its
  full 47-member run was admitted as Decision
  `event-impact-triage-decision-05598ca394786a82538c78794d82d65b9c130c4ec725da795aba95317c37a3dd`:
  39 versions, 30 clusters, 28 archive, two EventAssessment, no eligible checkpoint. The exact
  authoritative v6 Ledger contains 48 physical attempts, 396,709 input and 172,508 output Tokens;
  the extra attempt is the immutable old HTTP 408 dispatch retained beside its one replacement.
  This proves the replacement and v6 contract can complete and reopen, but not blind classifier
  quality or alpha.
- the first batch claimed through the bounded current-route ingress froze the earliest 32 of 293
  unclassified actual-receipt versions into three Work Units. It produced all 32 Digests and a
  25-cluster Partition, then stopped after 36 completed logical members when one classify member
  exhausted three fully received malformed JSON responses. The final response had one extra closing
  bracket and otherwise identical semantic tokens. One explicit Format Recovery Grant preserved the
  failed terminal, Journal and Usage, then created a zero-Provider recovery Run. The one remaining
  never-started classify member made one normal Provider request. The graph completed all 38 logical
  members with 41 physical attempts, 389,749 input / 200,654 output Tokens and 318,755 microusd
  CPA-equivalent cost. Decision
  `event-impact-triage-decision-2c01a7f0347b9463bdd43fa8abc5b06f86de227928a7bd62ff01219d7038b14d`
  contains two archive clusters and 23 `needs_review` Attention Watch clusters; no eligible
  checkpoint, EventAssessment, Query Gate, Judgment or execution authority exists. The completed
  Decision released the Active Batch head. This proves recovery/reopening mechanics, not semantic
  quality, alpha or a pristine blind promotion.
  A post-run structural audit found that 14 of the 23 Watch clusters had no explicit transmission
  channel and 10 had no affected entity reference. Their median `triage_confidence` was 0.92; this
  means confidence in the conservative Watch route, not probability of market impact or profit. The
  exposed batch is a development diagnostic only. It motivated v8's removal of premature
  material-event eligibility echoing; it does not supply sealed labels or a promotion result.

The first pristine v9 comparison is now immutable negative evidence. It froze 11 new actual-receipt
versions under sealed labels and completed one same-contract coordinator call per arm. Both arms
caught 2/2 must-catch events and produced strict JSON without repair. Baseline scored 6/11 exact
routes with five unsupported EventAssessment routes; treatment scored 4/11 with four unsupported
EventAssessment routes. The treatment was therefore both non-zero on the registered unsupported
route gate and worse than baseline. Total authoritative usage was 15,176 input / 13,220 output
Tokens and 18,900 microusd. The failed report terminalized all 11 versions and released the active
head without creating a Decision.

V10 preserves v9's one-coordinator-call graph and four-field positional result. It changes only the
versioned semantic rule: EventAssessment now requires the supplied item itself to support a realized
or committed new causal fact and a concrete transmission variable already changed or committed to
change. Generic risk appetite, sentiment, discount-rate and possible-future-opportunity narratives
are explicitly insufficient on their own. A plausible event missing one named observable goes to
Attention Watch; routine closes, auctions, calendars, scheduled statistics, requests, meetings or
plans without a supplied surprise, enacted term, named project, procurement, financing, production
commitment or other realized change are archived. Its completed pristine comparison was terminal:
12 versions; baseline had 7 unsupported routes, 5/12 exact routes and 8,494 microusd, while treatment
had 6 unsupported routes, 6/12 exact routes and 8,363 microusd. Total authoritative usage was 14,391
input / 11,648 output Tokens and 16,857 microusd. The gate failed and created no Decision.

V11 is the smallest scope-binding follow-up. It keeps the one-coordinator graph and unchanged
four-field positional route output, and retains v10's evidence-bounded fact/transmission and
Watch/archive rules. Its sole input addition is a closed projection of the registered checkpoint's
`eligibility_rule`, `exclusion_rules`, `target_venues`, and `allowed_instrument_classes`. Those
fields constrain routing; the coordinator cannot invent cross-market target links. It creates no
target map, new authority, state or entity. V2-v10 remain immutable replay dialects. Its new sealed
pristine comparison completed on 12 actual-receipt versions. Baseline produced zero unsupported
EventAssessment routes but over-routed foreign and routine context to Watch; treatment promoted all
five out-of-scope Hong Kong issuer earnings to EventAssessment. Both arms scored 6/12 exact routes
and routed all four sealed Watch items to Watch. Total authoritative usage was 13,626 input / 19,447
output Tokens and 26,062 microusd. The failed Report terminalized all versions and released the
active head without creating a Decision.

The v9-v11 failures localize the remaining ingress problem: semantic scope instructions do not make
an LLM a stable scope authority. It may still invent a generic path from an out-of-scope issuer or
foreign-policy fact to risk appetite, discount rates or future policy, or move that uncertainty into
an over-broad Watch. The fix does not narrow discovery or add another target authority. V11 is
operational for new, non-comparison-bound batches and may create a Triage Decision, but its routes
remain cost/quality dispositions. EventAssessment must cite concrete target paths and the existing
deterministic Materiality Gate alone enforces registered venue/class/horizon scope before Trigger
Admission. A plausible event missing that target may request one bounded target-resolution Watch;
it cannot claim materiality or silently expand the trading universe. Routine context remains
archived. Watch and downstream EventAssessment may
propose additional anchored Monitoring Scopes for newly discovered issuers, industries, ETFs,
frozen sets or information aspects, but those scopes need separate Harness admission, budget, PIT
lineage and a fresh Run. They cannot silently widen the original decision or trading universe.

An unresolved `watch` or `needs_review` result no longer prevents the runtime from evaluating later
ready-time EventAssessment candidates. It still blocks Trigger Admission for the route epoch until
the earlier authority is resolved. This separates analysis throughput from admission order: later
events can acquire durable, replayable assessments, but none can leapfrog an unresolved predecessor
into Query Gate or trading authority.

That controlled expansion now has a concrete proposal/admission seam. A parent Agent sees only the
named `WatchDelegateProfile` records permitted for its Agent type and lineage depth, similar to Skill
selection by description. It may choose one profile and propose a subject, question,
evidence and registered matcher; it cannot define the Provider, URL, cadence, budget, callback model,
tools or execution. The Harness persists the decision, applies branch/depth/global caps, deduplicates
equivalent active collection scopes, and preserves a callback subscription for each accepted parent.
This keeps discovery open-ended enough to follow a newly supported transmission while preventing a
speculative association from silently becoming a trade-universe or account action.

The proposal/admission boundary is deliberately still fail-closed. A first implementation froze a
caller-supplied parent projection, but append-only self-signing proves only immutability, not that the
evidence, subjects or matcher terms came from a real parent Run. That minting path was removed. The
current `AgentDelegationContextStore` cannot issue a context and rejects even a content-addressed
caller artifact; profile offers, admission, lookup, callback and restart activation therefore remain
closed. PDI-40 must next name one concrete parent Run/Decision owner whose durable artifact can derive
the complete projection without a second generic decision-view state machine. Admission-before-Watch
activation recovery has isolated mechanics tests, but cannot become operational acceptance until the
parent authority is real.

Still required for real acceptance:

- preserve the closed material-ingress authority boundary: the ordinary path rejects
  comparison-governed v9/v10 before any Provider call; v11 ordinary operation accepts only new,
  non-comparison-bound batches; and failed-batch terminalization may exclude actual-receipt versions only after
  reopening the append-only Comparison Registration and Report plus both exact Run/Usage authorities.
  Recovery after Report or terminal commit is local-only, uses the first durable identity and time,
  does not construct or probe a Provider, and rejects authority subclasses or reinstallation of an
  already terminalized batch;
- portfolio-impact and historical-analogy analysis inside EventAssessment; they are intentionally not
  ingress roles; the complete Account State/Position Snapshot contract is read-only infrastructure
  and still needs a real accepted account Provider plus binding into the Authorized Decision View;
- one real path-bearing PDI-29G EventAssessment that passes deterministic Materiality; the concrete
  runtime/authority and a real completed no-path Watch are accepted, while v8-v11 terminal comparison
  versions remain negative evidence and cannot be recycled;
- ongoing observation of false positives, misses, `needs_review`, routing, unsupported transmission
  paths and resource use as operational quality evidence;
- a real selected event passed through PDI-30 and Query Gate.

The second-batch v4 interruption is resolved only at the runtime-authority layer: its one permitted
replacement is durably consumed, the original dispatch remains immutable, and no late response can
be merged. The later v4 semantic-contract failure remains terminal. V5 and v6 Plans using the same
frozen Candidate Set are explicitly labelled infrastructure revalidations; neither rewrites earlier
evidence nor counts as a pristine blind semantic batch.

None of this grants historical PIT, Judgment-run, Strategy Admission, paper/live execution, account,
credential, mandate, approval, or broker authority.
