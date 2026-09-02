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

An approved legacy checkpoint retirement before any Trigger or Agent Run is separate non-run
accounting in the existing Trigger store. Its `missed_window` disposition preserves original
Candidate/Decision references and closes that old slot; `legacy_session_unanchored` explicitly has
no proven deadline. It is not an Agent terminal, eligible admission, completed strategy case or
positive promotion evidence. It neither rewrites old experimental denominators nor inserts a
fabricated member into a sealed promotion window. A later current-time reassessment needs its own
registration and retains the original receipt times of accumulated subject evidence.

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

### Opened historical readiness pilot — September 2026

The small historical-readiness runner is a diagnostic entry to the existing `AgentEngine`, not a
second strategy evaluator. It reuses content-verified frozen research documents, the Pattern Pack,
registered Method Skill routing, read-only tools, Run Journal and Usage Ledger. Its new question
permits up/down/abstain over a declared horizon instead of inheriting the old long-only question.
The original input and any mechanically derived question-only Evidence Pack are both bound; no
source evidence, availability timestamp or prior result is edited. Prior expectation may be
explicitly unknown. The model must cite its event-to-variable-to-target reasoning rather than be
given an operator-authored financial conclusion.

Before the first call, the initial opened-development cohort is fixed to these existing inputs:

| Frozen checkpoint (UTC cutoff) | Operator-only coverage purpose | Treatment addition |
| --- | --- | --- |
| 2024-09-30 01:25 | Policy-stimulus opportunity assessment | `narrative-diffusion-assessment` |
| 2020-02-03 01:25 | Epidemic-shock risk assessment | `expectations-base-rates` |
| 2021-07-01 01:25 | No clear fresh catalyst / abstention assessment | `second-level-cycle-context` |

These purposes are not answer labels and are withheld from the Agent. Every case uses the same
five-trading-session research horizon and existing masked `broad-market-a` research target. Two
complete control/treatment pairs run first; either-arm target/direction/abstain disagreement opens
one third complete pair. Confidence is recorded, never used as a cutoff or quantity. A failed pair
stops further pairs while started peers finish and are accounted for. Source, model, prompt, Skill,
tool and budget bindings are frozen before dispatch, with at most two active requests and a maximum
of six bounded Agent runs per case. The three cases are not run as overlapping Plans.

This cohort cannot produce an executable Intent: the target is a research index alias, not a
tradable instrument, and the old documents do not establish current order rules, raw executable
quotes, simulated holdings or strict historical authority. It can measure whether explicit
horizon, source-grounded event reasoning and typed expectation absence improve the previous
mechanical-abstention pattern. Even a completed non-empty Judgment does not demonstrate avoided
loss, after-cost return, Skill incremental value or historical/paper parity. Report this narrower
readiness evidence separately and do not substitute it for the executable pilots or holdout.

The fixed cohort completed on 2026-09-02 under its original Luna xhigh Profile:

| Checkpoint | Control decisions | Treatment decisions | Physical requests | CPA estimate (USD) |
| --- | --- | --- | --- | --- |
| 2024-09-30 | abstain, abstain | abstain, abstain | 9 | 0.055391 |
| 2020-02-03 | abstain, abstain, abstain | down, abstain, abstain | 12 | 0.073308 |
| 2021-07-01 | abstain, abstain | abstain, abstain | 8 | 0.051702 |

All 14 logical runs completed, read the six Evidence Items and Pattern Pack, and reconciled to
their three Usage Ledgers: 29 physical requests, 406,162 input / 82,630 output Tokens and
180,401 microusd. The adaptive rule correctly used a third complete pair only for the risk case.
There were 13 abstentions and one five-session down proposal, not a non-empty majority. The latter
had decision confidence 0.55 and candidate confidence 0.56; neither changed admission or sizing.
This is a completed diagnostic with no positive effectiveness acceptance or Skill promotion.
It does not establish the executable-pilot gate or justify expanding the holdout.

Private report identities are the registrations
`ebc8c7ea2f914e970557c5a81d6bbf430030a737524b15852d5114c6405136b9`,
`410902270ceea42edc4eb2038ac5779d8a6e19f8c661d700d090c80e5e198c1a`, and
`dfd9aecfc2c78c964650b631da1dee78f94f7300627f0d05f4cc68404d62803f`, respectively.
The corresponding Usage Ledger hashes are
`ef8f52e7046baaf69ffaf124746ff734dac44bedd147878622d540569ca6c4f7`,
`a27c253c95247a64d346504039b833470e142deb67d7f57f38fe2d6dd8007918`, and
`edb1dbb8f000d47c59aeef6d0eae0ce3d3e5d8f6a6af1d1af2e573d4115d62eb`.
No licensed source payload is included here.

The next input repair must supply the alias's permitted market/benchmark semantics, distinguish
claim-only Strict-PIT and execution limitations from economic decision blockers, and check that a
selected method's prerequisites match the actual content rather than merely a source category.
Generic critical-gap instructions and expectation-oriented methods create abstention pressure, but
the one down proposal demonstrates this is not a deterministic prohibition. Metadata-only official
context and genuine uncertainty about horizon persistence remain separate economic limitations.
Do not weaken live/PIT authority or force a directional answer to improve a completion metric.

### Historical readiness v2: two analysts with conditional adjudication

The opt-in v2 runner implements two independent analyses per arm, with one Judge invocation
per arm on decision disagreement instead of a third vote. Control uses the same reducer as treatment,
so a Skill comparison does not confound extra adjudication with the Skill. V1 and the completed
cohort above are unchanged. The Judge receives the same frozen evidence and
auditable conclusions, citations, counterarguments and uncertainties from both analyses, not future
outcomes or private model reasoning. Adjudication is evidence-led, never majority voting: inspect
the original sources and compare each analysis's factual claims, assumptions, transmission path,
counterevidence and horizon. A minority view may prevail; both analyses may be rejected; a different
supported conclusion within the original research scope is permitted. Vote count, model tier and
self-reported confidence confer no decision authority. It may select or synthesize a supported
conclusion, abstain, or request additional evidence for a separately frozen later decision. Its Profile is independently
configurable through an existing accepted Model Provider Profile without changing analyst identities.
No new model adapter or automatic model upgrade is implied.

Register the exact disagreement rule, Judge Profile/prompt, total budget and final-decision rule
before dispatch; use at most one Judge per disagreeing arm, with no recursive debate or repeated
adjudication until agreement.
Analyst agreement is not evidence of correctness. The Judge is another bounded Judgment producer,
not a source of facts, Trading Mandate authority or a replacement for Query/Policy/Execution gates.
Account for the whole treatment pipeline when comparing cost and effect against the frozen control;
the Judge does not create another independent Event Case.

The frozen disagreement key is decision/target/direction/horizon, not confidence or prose equality.
Different rationales with the same terminal decision do not automatically trigger adjudication in
v2; this is not semantic agreement detection. On agreement the first analyst terminal represents
that arm, without combining confidence or claiming agreement is true. On disagreement the single
Judge's validated Judgment represents the arm, even if it rejects both analysts. Failure or
incomplete required reads aborts the case; there is no majority fallback or additional Judge.

Registration freezes analyst and Judge Profiles/pricing, both analyst bindings, the Judge template
binding, research scope and six-run worst-case budget (four analysts plus two conditional Judges).
After both analysts complete, Harness reopens their exact terminal artifacts against their Journals,
removes self-reported confidence and model metadata from the displayed proposals, and derives the
Judge's task from those proposals plus the unchanged Evidence Pack. Before Judge dispatch, source
terminal hashes, input hash and actual execution binding are saved privately. The Judge must reread
the original sources; analyst opinions do not become new Evidence Items. All calls use AgentEngine,
the same two read-only tools and Usage Ledger; at most two requests run concurrently. Cancellation
drains started peers and denies later work. Crashed/ambiguous batches retain single-use reservation;
ordinary bounded transport retry remains the existing Adapter's responsibility.

V2 requires an explicit research target description and provenance reference, frozen as operator
research-scope metadata, not newly established financial evidence or retroactive historical authority.
For the existing A-share dataset, the source materializer maps `broad-market-a` to the case's primary
index; all 15 registered cases use `000300.SH`, `index_daily`, price returns. The Agent-facing
description can retain the identity mask while stating mainland A-share broad-index exposure,
prior-session close-to-close features and a non-executable research target. The actual event-to-target
transmission still needs citations from the frozen evidence. Original source documents and
Evidence Pack remain unchanged; only the task gains the new registered definition.

The task distinguishes Strict-PIT/Intent claim limitations from economic reasons to abstain. It does
not permit future information or invent expectations. A missing content-bound `prior_expectation`
rejects `expectations-base-rates` and a `second-level-cycle-context` consensus-gap application before
dispatch; other cycle applications retain the existing route. `--treatment-skill none` is explicitly
allowed in v2 to diagnose the same no-addition pipeline twice without forcing an unsuitable Skill.
That mode has no Skill-effect claim. Data-category presence alone still does not prove semantic
prerequisites; substantive adequacy remains an evidence audit, not a fabricated completeness flag.

The first real v2 acceptance run is limited in advance to the already-opened 2020-02-03 risk
checkpoint, keeping its original six evidence items, Pattern Pack and five-session horizon.
Both analyst arms and conditional Judges use the separate Luna max CPA Profile; the complete
case cap is USD 3.00. Both arms use the common controls with `treatment-skill none`, so this is
repeatability/input-flow evidence, not a Skill ablation or max-versus-xhigh comparison. The target
description is derived from the original A-share dataset/materializer mapping above; no original
source document, price row, availability time or authority time is rewritten. All terminal outcomes
are retained. Agreement skips Judge dispatch; a Judge must not be forced by resampling. Failure,
economic abstention or remaining mechanical abstention does not permit a replacement case or
additional replica. The earlier v1 cohort remains failed development evidence, not a baseline
whose causal difference can be attributed to this multi-change diagnostic.

That single v2 run terminated on 2026-09-02 at 06:16:25 UTC without a valid case result:

| Member | Control | Treatment (same controls, no added Skill) |
| --- | --- | --- |
| First analyst | Five-session down proposal | Economic abstention |
| Second analyst | CPA HTTP 408; no terminal Judgment | CPA HTTP 408; no terminal Judgment |

All four members read the six frozen Evidence Items and Pattern Pack. The first two completed
Judgments differed; the abstention concerned repricing, exposure and horizon uncertainty rather
than treating Strict-PIT or non-executability as the sole blocker. These are observations from an
opened case, not an effectiveness comparison. Both second-round generation requests failed with
HTTP 408 after dispatch; this does not establish whether upstream generation had begun or completed.
The Adapter classifies that POST failure as generation-unknown and forbids automatic retry.
Started peers drained, no additional analyst or Judge was dispatched, and `final_decisions` is empty.
The real evidence therefore accepts failure containment and record retention, not successful
end-to-end Judge adjudication. It is an incomplete experiment, not a negative strategy outcome.

Four terminal Usage Records reconcile to eight physical requests and known lower bounds of
55,506 input / 38,279 output Tokens and USD 0.057039. The two failed requests returned no token
usage; the report correctly sets `accounting_complete=false` and
`recorded_totals_are_lower_bounds=true`. Failed-request elapsed time is likewise not included in
the terminal usage latency, so those latency metrics must not be read as complete elapsed time.
Read-only Keeper metadata for the same window contains eight matching Luna max requests; the
two failed entries report 478,184 / 187,458 ms latency and 1,904 / 2,933 ms time to first token.
This corroborates a failure after an initial upstream response rather than proving generation
never started. Timestamp/usage correlation is diagnostic evidence, not a replacement for an exact
request-ID reconciliation. Keeper reports zero tokens on those failures; that does not establish
zero generation or zero billed usage. Neither telemetry set identifies the exact internal timeout
cause, so no quota, network or model-capacity root cause is claimed.
Private registration, report and Usage Ledger identities are respectively
`a837bbcf2f2dd80ed4044c0881a9bf706bcb560485fc444dddc12ff6a995e9b6`,
`d7f8cfeb7f78f645a53d245cbd169f784604644cdbefab80c97507b3011b1020`, and
`ace5f49f455d17bd116ebed7749a1814e8609e098c70a823f6f175c2ae466c79`.
The original USD 3 cap and single-use registration remain unchanged. Before another separately
registered model experiment, diagnose the CPA timeout boundary and obtain bounded transport
acceptance; do not resample this case to force a Judge. Strict-PIT remains 0/18, and this run grants
no Skill promotion, Signal, Intent, Mock, IBKR Paper or Live authority.

Subsequent read-only gateway diagnosis resolved the error boundary to an upstream stream closing
before `response.completed`, not a Harness deadline or proven quota failure. See
`MODEL_PROVIDER_RELIABILITY.md` for the exact pinned implementation and the requested-versus-effective
parameter limitation. The generic-run diagnostic/latency and interrupted-dispatch repair applies
only to new evidence; it does not fill in this experiment's missing Usage or permit another attempt
under this registration. A separate synthetic wiring canary is not a valid historical case or a
successful conditional-Judge experiment.

Implementation acceptance passed Ruff, format, Pyright and 1,628 tests. Independent read-only
review found an overly conservative mixed-model preflight: six hypothetical Judge runs were
being checked before the actual four-analyst/two-Judge mix. The correction budgets that actual mix
directly, preserves the v1 six-run estimator and freezes both cost estimates; regression tests cover
an affordable higher-priced Judge and rejection when the mixed cost really exceeds the cap.

### Remaining opened v2 diagnostics after received-408 acceptance

After the bounded transport repair and successful synthetic canary, the continuation is fixed
before dispatch to the original 2024-09-30 and 2021-07-01 checkpoints, in that order. The failed
2020-02-03 v2 registration remains incomplete and is never redispatched or replaced. The private
continuation manifest is `82b6304acd0861077c2baeb37f1b7fcaad1d03ffb63003404804e4bb0d4e7b07`.
Both cases keep their original six evidence items, Pattern Pack and five-session horizon; v2 adds
the same registered broad-index meaning and lane/economic uncertainty separation used by the risk
diagnostic. Both arms use common controls with no extra Skill. Analyst and conditional-Judge roles
select the new Luna max CPA received-408 Profile; each case reserves USD 3.00 estimated cost,
at most two concurrent model requests and six logical runs. Cases do not overlap.

A transport, output or required-read failure stops later queued cases. If the opportunity case
still abstains solely because of research-lane, target-identity or claim limitations, stop model
calls and repair inputs instead. An evidence-grounded economic abstention remains a legitimate
result, not permission to replace the case or force a Judge. These are previously opened input
diagnostics, not a new holdout, Skill ablation, executable-pilot gate, strategy promotion or proof of
avoided losses. The original incomplete risk member remains visible in the wider program report.

The opportunity case completed five Runs: control abstained twice; treatment abstained once and
proposed up for five sessions once. Only treatment invoked its registered Judge. The Judge reread
the original evidence and resolved to abstain: an already substantial rally and broad diffusion did
not establish incremental continuation rather than reversal through the requested horizon. Missing
implementation detail and the unknown checkpoint open remained economic uncertainties, not an
automatic research-lane or target-alias veto. This meets the continuation rule for running the
remaining fixed case; it does not prove that abstention was the correct financial decision.

All five terminals reopened without dispatch or Journal changes and matched their frozen
prompt/config/Skill bindings and Usage records. The Judge input binds both exact analyst terminals;
the agreeing control arm made no Judge call. Ten physical requests recorded 137,215 input / 61,397
output Tokens and 101,122 microusd estimated cost. There were no unknown-generation failures or
408 regenerations in this case. Registration
`f91d19aded80abe661df9e5bb56948fb92f2b2bbd849d21d02467ddea4b6a65a`, report
`e1f61bf21b4f92bca6620f925654c5b03a52eb6f81f1a068d41ae5a619f9638d`, Usage
`22822987bed11ba3413df9c0e9a4aca77a75ef051ca170a20da6bf356137ecf9`, and the initial
continuation audit `4cb3414a2d73089c2e8d46c2c74f0d3484031bfbd50214a9559a232f62153005`
remain private immutable evidence. This demonstrates conditional adjudication mechanics on a real
historical input, not a Judge quality gain, positive return, avoided loss or Skill increment.

The remaining abstention case then completed all four analyst Runs. Both arms agreed on abstention,
so neither invoked a Judge. The mixed news record did not isolate a new economic change with a
supported broad-index transmission and five-session horizon. Unknown expectations remained visible;
the result must not be relabeled a correct avoided trade without subsequent outcome evaluation.
One analyst needed its existing bounded contract-correction turn because the returned `event_id`
did not match the frozen Evidence Pack. That was a completed semantic correction, not a network
retry or another analyst sample. Nine physical requests recorded 184,289 input / 29,232 output
Tokens and 71,939 microusd; no 408 or unknown-generation attempt occurred. Registration
`f7f725023481fac852ab48549027a6670418aef8b925b3a92c02d114e7bd7b4a`, report
`20d16467a2a32fa6f6b44dbe76017e735f724385adebf9c593879f988eb30eb1`, and Usage
`db6028b3670f93010e65aab6c245d3e04f8d5c53a078ae78645ec5464a32c1e8` remain immutable.

Final continuation audit `283d2c5eac51b1d72b2fd0fd2596ad1b362f79cb07f4b5160451190eee6486f8`
reopened all nine completed Runs without dispatch and reconciled 19 requests, 321,504 input /
90,629 output Tokens and 173,061 microusd across the two cases. It also verified the actual
conditional-Judge branch and its two source terminals. Confidence remains uncalibrated observation:
opportunity analysts reported 87%/86% (control), 84% abstain/54% up (treatment), and its Judge 91%
abstain; abstention-case analysts reported 94–98%. These values concern their stated decisions,
not measured probabilities of future price moves; the Judge input omits analyst confidence.
The bounded continuation is complete. Do not repeat it to obtain non-empty decisions. Next work is
executable, event-centered inputs and outcome/baseline evaluation, not a claim that two final
abstentions satisfy the original three-case readiness/effectiveness gate.

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
