# Agent Effectiveness Acceptance

This document defines the only promotion path for a strategy or Judgment Skill. The canonical
evaluator is the decision authority; neither a Registration, case binding, JSON document nor Schema
validation independently promotes a strategy. Existing Phase 2, Method Quality, regime, Triage and
prospective studies retain their frozen registrations and reports, but none may independently label
an Agent, strategy or Skill effective. Engineering acceptance, investment effectiveness,
execution-Provider acceptance and execution authorization remain separate claims.

## Promotion authority v2

Promotion is rooted in one concrete `LocalDataSnapshotStore`. The root persists a stable
`harness_authority_id`, one SQLite index, and one content-addressed artifact directory. The
authoritative Run Journal, Trigger Admission strategy window, and
`StrategyValidationAuthorityStore` must share this exact root and use `BEGIN IMMEDIATE` for
dependency-closed mutations. Caller paths, `:memory:` stores, fresh roots, copied artifacts, and
legacy path-created journals cannot promote.

Before execution, `StrategyValidationAuthorityStore` derives `StrategyCaseRunPlan v2` from the
Registration and same-root evidence indexes. The caller supplies only Registration ID, Run ID and
case ID. The plan binds the epoch; case, root event, regime and role; the derived evidence lane and
owner ID/hash; input, Data Snapshot, lineage, qualification and admission hashes;
model, prompt, Skill, tool, universe, cost and fill hashes; and the primary baseline identity,
definition, configuration and development-selection evidence. Actual Agent completion is the sole
writer of `StrategyCaseTerminal v2` for every terminal disposition. It binds the actual Judgment
when present, a completion-written Run Manifest, and exact candidate and baseline measurement CAS
paths when present. Those measurements are projections of exact candidate and baseline
`StrategyBacktestOutcomeReceipt` IDs, never caller-supplied scalars. Each receipt binds the frozen
strategy-variant content hash, exact strategy and target-selection references, complete simulation
configuration, and the engine configuration in its actual Run Manifest. The Harness derives the
arm from the Registration's candidate variant or exact named primary-baseline variant and rechecks
that binding both when writing measurements and when evaluating them. Missing actual outcomes,
advanced metrics or stress evidence are inconclusive and cannot be replaced by returned objects or
caller values. A measurement with an invalid arm or case, a receipt owned by a different frozen
plan, or values diverging from its receipt is likewise typed inconclusive evidence; CAS identity,
path and authority-root corruption still fail closed.

A promotion-bound Run Journal rejects generic event writers. Each authority root owns a persistent
private event-authentication identity. The Harness composition root lexically binds a private
signed-event sink while constructing AgentEngine and returns only the Engine run interface; neither
the Run Journal nor the data store exposes a signer, sink, key accessor, or privileged append API.
Every privileged event authenticates the root, Run, sequence, previous hash and canonical event content.
Each model-turn event commits its Provider/model identity, pre-turn context, assistant and
raw-response artifacts, usage, attempts and estimated cost. Terminal reopen verifies every event
signature, reconstructs the final transcript and aggregates Run Metrics from the canonical ordered
events; standalone transcript or metrics blobs cannot establish completion, and a completed
Judgment requires at least one positive-usage completed model turn. This boundary assumes a trusted
host and process plus the root's private filesystem permissions; Python reflection/private-state
extraction and host compromise are outside the threat model.

`RunMetrics.tool_calls` counts model-requested calls from signed model-turn events. Tool completion
events separately own executed results and `result_bytes`. A failure before execution may therefore
have requested calls with zero completed calls; a completed Judgment requires every requested call
to have a matching completion event.

Every promotion-bound attempt begins with a signed `run.started` event binding the Run config,
Provider/model identity, and frozen strategy plan. Every non-completed disposition ends with a
signed `run.failed` event binding its exact status, timestamp, error, and reconstructed metrics.
An eventless Run row or caller-authored error JSON is recoverable incomplete state, not a terminal
denominator member, and blocks run-set sealing.

The prospective Trigger Admission authority opens an epoch/window with an explicit Registration to
case/root/regime mapping, automatically appends every matching admission, and maintains one
sequence/hash head. Only the first authoritative receipt may append to windows open at that time;
an idempotent replay cannot backfill a window opened later. Sealing is atomic. A matching admission
received later whose authoritative `admitted_at` is at or before cutoff marks the earlier seal
stale; it may not be silently omitted.

`StrategyValidationAuthorityStore.evaluate(registration_id)` accepts no outcome, metric, selector,
store, path, denominator row, or evidence-lane override. A v2 report binds the
`harness_authority_id`, run-set seal and prospective window seal. Existing v1 Registration,
denominator, binding, and report artifacts remain replayable, but the v1 caller-shaped evaluation
API is promotion-ineligible.

The promotion-capable historical outcome producer is the actual `NautilusBacktestBridge` configured
with the canonical `LocalDataSnapshotStore` and its same-root `ArtifactStore`. It persists the
`BacktestResult`, source Snapshot, normal capital path, two actual fills and costs, deterministic
return/P&L/risk/turnover/adverse-excursion metrics, and a second actual doubled-fee stress run in a
content-addressed receipt plus the root SQLite index. A standalone or legacy `BacktestResult` has no
receipt authority and remains replayable but promotion-ineligible.

The bridge also executes the frozen `cash-no-action.v1` baseline as a flat starting-capital path
with no fills, no costs and a flat doubled-fee stress path. Broad-ETF, fixed-map and other named
baselines remain typed missing until their exact strategy is implemented. `target_selection_ref` is
provenance metadata, not executable distinctness: changing only that label cannot turn the candidate
into a baseline. Each variant instead freezes its exact `strategy_ref`, market, input instruments,
holding-horizon request template and simulation configuration.

## One decision path, separate evidence lanes

Historical replay, mock paper and broker paper reuse the same frozen evidence/tool view, Agent
runtime, Judgment validation, Signal, portfolio policy, deterministic sizing, Order Intent, hard
policy and mandate semantics. They differ only where the environment owns different facts:

- a backtest owns cutoff-bound simulated account state and deterministic fills;
- paper owns reconciled paper-account state and broker events; and
- live, which remains disabled, would require a separately authorized account and Provider.

No new Decision Episode store is introduced. A validation case reopens the existing Snapshot,
Evidence Lineage, Run Manifest, Account State, Portfolio Decision, Sizing Decision, Intent,
execution/reconciliation and Outcome artifacts. A missing owning artifact is typed missingness, not
a caller-supplied substitute.

Evidence lanes never upgrade one another:

| Lane | Permitted claim |
| --- | --- |
| outcome-opened retrospective | discovery and counterexample search |
| Modeled-PIT | input/process readiness and chronological diagnostic evidence |
| Strict-PIT | historical strategy/Skill promotion evidence when the frozen holdout passes |
| prospective actual receipt | process evidence immediately; later promotion evidence after the registered denominator accrues |

Prospective plans reopen the exact case's sealed Trigger Admission ID and artifact hash. Modeled-PIT
plans reopen the exact readiness-checkpoint authority row frozen by that case. The current root has
no historical qualification/lineage authority capable of proving Strict-PIT, so historical plans
are explicitly retrospective and inconclusive even when their backtest economics are favorable.
Repeated-digit placeholders, caller-selected lanes and copied admission artifacts establish no lane.

## Frozen strategy epoch

`StrategyValidationRegistration v1` remains the frozen registration payload; model Profile, prompt,
Skill catalog, tool manifest, universe, cost model and fill model hashes; and the exact definition
and executable configuration of the candidate and every named baseline. Candidate and baseline
configurations must be distinct; changing only the arm label cannot create comparison evidence.
An unsupported baseline produces a typed missing outcome and therefore an inconclusive case rather
than reusing the candidate run. The Registration also binds the development-selection evidence hash
that justified the primary baseline. A baseline name without those three hashes is not frozen. The
Registration also freezes `earliest_complete_run_per_case_v1`: for each Registration and Event Case,
the Harness-owned SQLite/CAS `StrategyCaseRunAuthorityStore` selects the complete run with the
earliest registered start time, breaking an exact timestamp tie by Run Manifest hash and then the
content-derived binding ID. Completed bindings are immutable and the selection commits the complete
eligible binding set; the promotion boundary does not accept a caller selector or protocol substitute.
The v2 run-set seal is unavailable until every registered evaluation case has at least one fully
bound typed terminal; retries remain in the complete eligible set and use the frozen selector.

Every registered case freezes `case_id`, `root_event_id`, regime and role, plus optional source
Snapshot and modeled-readiness binding references that must already resolve in the same root. One root event may appear
only once across development and evaluation, so a second target, horizon or label cannot cross from
development into holdout or increase the denominator. `historical_strict` freezes eight development
cases and exactly 24 historical holdout cases across at least six regimes.

`prospective_confirmation` additionally binds one cohort derived by the Harness-owned SQLite/CAS
`ProspectiveDenominatorStore`. The store registers the strategy epoch, qualification policy, opening
and cutoff, then appends each qualifying actual-receipt event with its exact Trigger Admission ID and
hash. Sealing consumes every stored row at or before cutoff and commits the seal time, append-only
Journal hash, qualification digest and every eligible `case_id`/`root_event_id`; it is append-closed
afterward. The registration cannot predate the seal and its prospective cases must equal the complete
cohort reopened by registration. A caller cohort object or favorable subset is not promotion
evidence. The cohort must contain at least 30 clusters across at least four regimes; every qualifying
abstention, failure, missing input and no-fill remains in it. The registration grants no execution
capability. Any material change creates a new epoch; opened or failed cases remain development
evidence and may never become unseen holdout evidence again.

The Event Case is the independent unit. Multiple targets, horizons, exposures, Agent replicas or
model Profiles derived from the same root event do not increase the denominator. The 24-case
holdout must cover at least six registered market regimes. Missing, failed, abstained and unfilled
cases stay in the all-case denominator.

The strongest deployable primary baseline is selected on development data and frozen before the
holdout. Reports also disclose cash/no-action, same-cashflow scheduled investment, tradable broad
and sector ETF hold, fixed-event mapping, simple lagged trend/volatility and the first valid
single-Agent decision where applicable. A research index may not satisfy a tradability claim.

## Balanced acceptance gate

`StrategyValidationReport v2` returns `accepted`, `rejected` or `inconclusive` and grants no
execution authority. Historical acceptance requires the complete 24-case Strict-PIT holdout;
prospective confirmation requires at least 30 actual-receipt clusters, at least 20 non-empty
executions and four regimes. These are distinct registered programs, not interchangeable evidence
lanes. Modeled-PIT and an incomplete cohort are inconclusive even if their descriptive economics
look favorable. Economic gates run only after every frozen case has one valid candidate arm, one
valid primary-baseline arm and every required outcome/path observation; a larger-than-minimum cohort
with any missing case remains inconclusive and cannot be rejected from its partial economics.
Legacy v1 replay applies the same denominator rule: duplicate or unexpected case IDs and frozen
root-event or regime mismatches make the evidence incomplete, so it does not compute economics or
turn invalid evidence into a rejection.

Registration freezes `equal_weight_active_cases_v1` portfolio aggregation, fixed starting capital
of `1000000`, a maximum of 32 simultaneous positions and deterministic
`root_event_id_then_case_id_v1` ordering. Evaluation reopens every selected candidate and baseline
receipt, derives period returns from each sealed capital path, and compounds the two arms separately
on the union timestamp path. It recomputes portfolio return, drawdown, CVaR95, Sharpe, Sortino,
turnover, liquidity utilization, adverse excursion and the doubled-fee stress result from that
common-capital policy. Turnover is the sum of absolute fill notional after equal-active-case scaling
divided by frozen common starting capital. Liquidity utilization is scaled executed notional divided
by scaled executable-side available-liquidity notional. Adverse excursion is the worst
timestamp-level equal-active-case aggregate of each position's marked adverse move relative to
entry, not a drawdown alias. Every open position must have an exact adverse-excursion mark at every
common observation timestamp in its entry-through-exit interval; pre-entry zeroes and post-exit or
interior carry-forward are not substitutes. The actual stress path must carry the same complete
fill-liquidity and marked-position evidence. The evaluator never substitutes a first case, averages
receipt-level metrics, or requires heterogeneous case paths to carry identical metrics. Missing
normal or stress observations, or overlap beyond the frozen cap, are typed inconclusive.

The caller never declares the evidence lane, measured return or authority binding. For every case,
the evaluator asks the concrete `StrategyCaseRunAuthorityStore` for the full eligible set using only
the exact Registration and case ID. The evaluator independently verifies the set hash and frozen
earliest-complete selection; a later favorable retry or replay cannot replace it. The selected
binding commits the exact Data Snapshot, Evidence Lineage,
qualification report, Run Manifest and admission hashes; Registration ID and hash; strategy epoch;
frozen model, prompt, Skill, tool, universe, cost and fill hashes; primary baseline
definition/configuration and development-selection evidence; exact case returns and absolute P&L;
and the complete portfolio-metrics hash. The evaluator rejects a binding from another epoch or run
and rejects caller-chosen outcome or portfolio values. It derives Strict-PIT or
prospective-actual-receipt eligibility, qualification, admission and non-empty execution from the
reopened authority. A missing, substituted, mixed-lane, unqualified or unadmitted binding fails
closed.

The JSON Schema is a structural interchange check, not promotion authority: every consumer must
reopen the exact Registration, authority bindings, ordered-independent Event Case outcomes and
portfolio metrics, then recompute the report with `revalidate_strategy_validation_report`.
Cross-field economic ratios cannot be delegated to schema validation or caller-asserted gate
booleans. The v1 critical value and ratios are exact constants: `1.714`, `0.80`, `0.85`, `0.50` and
`0.20`; construction rejects altered values. Decimal fields use fixed-point strings and never
exponent notation.

Acceptance is conjunctive:

- mean and common-capital-path after-cost return are positive;
- the one-sided 95% clustered paired lower bound versus the frozen primary baseline is positive;
- maximum drawdown is at most 80% of the primary baseline and 95% CVaR at most 85%;
- Sharpe and Sortino both exceed the primary baseline;
- the frozen stressed-cost path remains positive;
- in a losing baseline case, candidate loss is no more than half the baseline loss;
- no Event Case contributes more than 20% of absolute outcome; and
- leave-one-event and leave-one-regime excess-return conclusions remain positive.

Return, drawdown, tail risk, adverse excursion, downside capture, turnover, liquidity and the
opportunity cost of avoided exposure are always reported together. Cash abstention cannot be called
risk skill merely because its drawdown is zero. A narrower result may be reported when only a
narrower gate passes, but it cannot promote the complete strategy.

## Skill lifecycle

Outcome-opened research may propose a general or family/horizon-specific Skill candidate. It needs
two further independent validation blocks, complete duplicate/conflict/subsumption and material
counterexample review, and then a pristine paired with/without-Skill ablation on identical inputs.
The Agent's stochastic replicas are averaged within each Event Case.

`JudgmentSkillTrace` records `offered`, `loaded`, `agent_reported_use` and
`influenced_proposal_paths`; it proves visibility and claimed use, not causal value. Active promotion
requires a later Strict-PIT or prospective paired incremental-effect result with no registered
risk/cost harm, and is limited to the demonstrated market, family and horizon. PIT or safety harm
quarantines immediately; replicated ineffectiveness or incremental harm demotes the Skill without
rewriting prior traces.

## Prospective and Paper progression

All qualifying actual-receipt events, abstentions, failures, missing inputs and no-fills enter the
sealed prospective denominator established by its qualification window, cutoff, Journal and digest.
Ten and 20 independent clusters are diagnostic looks only and cannot be submitted to the
confirmation evaluator. The explicit `prospective_confirmation` program requires at least 30
independent clusters, 20 non-empty executions and four market regimes under one unchanged strategy
epoch. A missing sealed case makes the confirmation incomplete; events are never omitted, replaced
or backfilled. An accepted confirmation report still has `execution_capability: none` and cannot
satisfy mandate, Provider, credential, kill-switch or reconciliation gates.

Experimental paper operation may begin before strategy promotion when the prospective Query Gate,
account view, mandate, policy, execution Provider and reconciliation gates pass. It must be labeled
experimental. Mock acceptance cannot satisfy IBKR acceptance. Live remains fail-closed behind its
own authorization, mandate, credential isolation, kill switch and reconciliation evidence.

## Stop rules

- Stop Agent spending when the same input-contract omission still causes the readiness pilot to
  abstain; fix the frozen input rather than adding replicas or Prompt variants.
- Stop an epoch immediately for PIT/authority, mandate, identity, kill-switch or reconciliation
  violations.
- Mark harm or futility under the frozen registration; do not add favorable cases, horizons,
  baselines, models or Skills after opening outcomes.
- Report historical Strict-PIT, Modeled-PIT, retrospective, prospective, mock and broker-paper
  evidence separately. No aggregate `alpha` label may merge them.
