# Market Impact

This context names the evidence-to-execution concepts that the harness must keep
distinct so that research, approval, and broker state cannot become competing
authorities.

## Evidence and events

**Evidence Item**:
A source-backed observation with separate occurrence, publication, visibility,
and retrieval times.
_Avoid_: News blob, context

**Source Observation**:
An immutable raw and normalized Provider record captured before it is eligible to become
Evidence. It retains original upstream identity, aggregator identity when present, source
publication/update times, strategy availability, local retrieval, and completeness gaps.
_Avoid_: API response, trusted fact

**Data Query**:
A content-identified request for one read-only Observation capability at one UTC cutoff, binding
immutable parameters, a versioned source policy, an exact ordered Provider/version/upstream-source
set, each Source Route Configuration hash, and explicit coverage requirements. Agent arguments may
refine domain parameters but cannot change its cutoff, sources, credentials, or cache policy.
_Avoid_: Search prompt, Provider fallback, live model request

**Source Route Configuration**:
A secret-free, content-identified description of one Provider's exact upstream route, source
identity, expected redirect identity, publisher identity, content scope, and license scope. A Data
Query binds its hash; credentials remain outside it.
_Avoid_: Provider name, URL string, credential file

**Source Route Acceptance Report**:
A content-identified result of one bounded route trial over rights and identity, transport,
completeness, time and revision semantics, market semantics, deterministic replay and storage, and
Agent isolation. It binds the exact source configuration, Provider manifest, captured rights notice,
and Data Snapshot. Passing accepts that prospective route only; it does not establish historical PIT,
promote Evidence, or grant execution authority.
_Avoid_: Provider enabled flag, license assumption, historical-data approval

**News Observation Batch**:
A content-identified result of one historical or masked-replay news query over an exact ordered
Provider/source chain. It preserves typed fetch outcomes, raw hashes, publication/update/availability
times, version lineage, filtering rejections, and accepted observations under one UTC half-open
window. It is normalized source material, not automatically admitted Evidence or sentiment.
_Avoid_: News digest, fallback search, Evidence Pack

**Availability Time**:
The earliest instant a strategy may use one exact source version, measured from a real-time
receipt or derived from source publication plus a frozen, source-specific latency model.
It is not the time a later historical backfill happened to run.
_Avoid_: Event time, retrieval time

**Retrieval Time**:
When the Harness fetched and stored its local copy. It supports audit, identity, and
diagnostics but does not reconstruct historical source availability.
_Avoid_: Publication time, visibility time

**Event Envelope**:
The immutable point-in-time boundary containing an event and the Evidence Items
available as of a stated instant.
_Avoid_: Prompt, news dump

**Evidence Pack**:
An immutable, content-identified set of point-in-time Evidence Items, source artifacts,
Pattern Pack references, and research scope made available to one Judgment Run.
_Avoid_: Model context, live search results

**Historical Evidence Manifest**:
A content-identified provenance companion that binds one benchmark case's Evidence Pack to
exact source versions, occurrence/publication/availability/retrieval times, latency basis,
revision lineage, and Source Version Receipts. A synthetic or untrusted receipt proves only that
the contract is internally chronological. v1 has no source-authentication authority and therefore
cannot admit any retrospective holdout. It is not additional Agent evidence.
_Avoid_: Historical data dump, outcome label, Evidence Pack

**Source Version Receipt**:
A content-identified binding from one Evidence Reference `source_ref` to a Provider/archive and
immutable archive version, source version identity, raw and extracted hashes, source times,
retrieval, availability basis, and trust status. Invented metadata may validate the contract but
is not authenticated point-in-time proof; a receipt binds assertions but does not authenticate
them.
_Avoid_: Ordered timestamps, source URL, provenance claim

**Latency Calibration**:
A content-identified source-class calibration binding a sample, Provider/archive, version,
observation count, calibrated time, and modeled availability offset. Modeled availability may use
it only when both artifacts carry the required trust status.
_Avoid_: Assumed delay, arbitrary backoff, receipt time

**Pattern Pack**:
A versioned pre-cutoff research asset containing reusable event mechanisms, transmission
scales, analogues, applicability conditions, and counterexamples.
_Avoid_: Agent memory, learned truth

**Research Method Skill**:
A versioned research procedure that states when and how to examine evidence without adopting
a persona, predicting an answer, or granting new authority. General methods and event-family
methods remain distinct so their incremental value can be measured.
_Avoid_: Agent role, expert opinion, prompt template

**Skill Route**:
A content-identified selection of applicable Research Method Skills from a frozen catalog and
point-in-time market, narrative, analysis-need, and evidence context. It records selected and
evidence-rejected methods, never uses realized outcomes for selection, and never lets the model
silently add its own method or capability.
_Avoid_: Prompt routing, model-selected tools

**Event Cluster**:
One market-relevant development represented by one or more related disclosures
or catalysts rather than by individual headlines.
_Avoid_: Article group, duplicate news

**Event Archetype**:
A reusable class describing the root cause of new market-relevant information,
such as an issuer action, geopolitical-security event, or physical disruption.
One Event Cluster has one primary archetype; linked causes and consequences are
represented as separate clusters and Transmission Paths.
_Avoid_: Topic, sector label, transmission mechanism

**Market State Descriptor**:
A research-only, point-in-time description of market direction/speed plus independent volatility,
drawdown, recovery, breadth/dispersion, narrative-salience, and causal-complexity axes. Its primary
price state is `up_fast`, `up_mild`, `down_fast`, `down_mild`, or `unclassified`; it is not an Event
Archetype or a claim that an Agent could trade a retrospectively selected period.
_Avoid_: Bull/bear story bucket, smoothed hindsight regime, Event Archetype

**Regime Study Registration**:
A content-identified research plan binding every representative Market State case to candidate
Research Method Skills, checkpoint cadence, case-specific search terms, minimum source diversity,
evidence-type coverage, and one frozen long-horizon baseline protocol. It distinguishes an
implemented adapter from authenticated historical availability and keeps discovery archives from
silently satisfying an evidence requirement. An outcome-opened registration can support
descriptive diagnostics only.
_Avoid_: Backtest result, completed evidence corpus, Agent prompt

**Regime Evidence Record**:
A content-identified, case-scoped source version that retains source/provider/publisher identity,
claim and revision lineage, occurrence/publication/update/availability times, the explicit
availability basis, immutable authority identity/time/hash, and content hash. Licensed payloads
remain outside the record. A source-reported time is distinct from an actual receipt or modeled
latency and does not inherit authority from local retrieval.
_Avoid_: Article metadata, current web page, inferred historical visibility

**Regime Evidence Manifest**:
A private, content-identified binding from one Regime Study Registration and exact market panel to
the complete set of candidate Regime Evidence Records. It validates source/category/provider
registration, revision lineage, and artifact identity but does not by itself declare any checkpoint
complete or authorize an Agent run.
_Avoid_: News corpus, Evidence Pack, source-readiness claim

**Regime Evidence Qualification Report**:
A content-identified per-case, per-checkpoint evaluation of registered record minima, independent
source diversity, lookback freshness, and point-in-time authority. It reports content completeness
and authenticated availability separately. The first checkpoint of an event-timed case additionally
requires a verified event-revelation record published between the registered event observation and
decision cutoff; older background documents cannot satisfy that semantic gate. This is the strict
PIT admission report: only a fully passing report may support a historically authenticated Agent
comparison. An outcome-opened registration can never support an effectiveness claim.
_Avoid_: Model evaluation, source plan, successful backtest

**Modeled-PIT Policy**:
A content-identified, category-specific exploratory visibility policy. It freezes whether a source
uses the previous-session panel snapshot or `available_at` plus an explicit safety delay, while
preserving every unresolved `authority_at` gap. It cannot rewrite a Regime Evidence Record, satisfy
strict PIT, calibrate its own latency assumptions, or authorize inference or execution.
_Avoid_: Historical receipt, strict qualification, timestamp correction

**Modeled-PIT Qualification Report**:
A separate, content-identified process-diagnostic admission report that applies one Modeled-PIT
Policy to the same dataset, panel, Manifest, and strict qualification lineage. It identifies which
opened checkpoints can be replayed under frozen visibility assumptions and reports strict authority
failures alongside them. Strict and modeled qualification reports are not interchangeable.
_Avoid_: Relaxed strict report, alpha evidence, execution gate

**Retrospective Data Lane**:
A content-identified archive lane for historical material first obtained after the decision time or
whose historical availability/authority cannot be proven. It preserves the source's old occurrence,
publication, and update times while recording the real later receipt and unresolved authority gap.
It may support postmortem and decision-process analysis, but it is excluded from strict backtest
inputs, strategy promotion, prospective Judgment inputs, and order generation. Modeled-PIT is one
explicit reconstruction method; it is not a synonym for every retrospective record.
_Avoid_: Relaxed PIT, backdated receipt, alpha evidence, trading input

**Prospective Actual Receipt**:
A future Source Observation whose immutable local retrieval is the strategy's first proven receipt,
so `available_at` and `authority_at` both equal `retrieved_at`. The existing source/provider/version,
raw hash, and revision lineage are adapted into the canonical Regime Evidence Record; no second
receipt authority is introduced. Future receipts may calibrate future latency treatment but never
backdate historical evidence.
_Avoid_: Current-page backfill, inferred historical availability, modeled delay

**Prospective Receipt Journal**:
An append-only, policy-bound record of prospective collection attempts, first actual receipts,
immutable content versions, and later sightings. It may freeze a cadence-qualified selection into a
Data Snapshot and emit a compressed analytical projection, but it is not a Source Observation,
Evidence authority, feature store, scheduler, or execution ledger.
_Avoid_: Latest-value cache, historical archive, data warehouse, broker journal

**Prospective Collection Job**:
A content-identified Harness schedule binding exactly one accepted Source Route report, Source
Configuration, Prospective Collection Policy, adapter kind, first due time, jitter bound, misfire
grace, and Provider timeout. The durable runtime creates one uniquely identified Collection
Opportunity per logical due time, uses expiring leases for concurrency and crash recovery, and gives
every opportunity a typed terminal or recoverable state. An OS supervisor may invoke the one-shot
worker, but it does not own cadence or Provider selection.
_Avoid_: Cron-owned business schedule, daemon authority, Agent-selected URL, execution job

**Prospective Collection Tracer Report**:
A private, content-identified acceptance report for one bounded CSRC official-event Job and one
Tushare market-context Job. It binds their accepted route reports, Collection Policies, logical
opportunities, actual-receipt Data Snapshots, interval health, and isolation gates. Passing proves
the repository's smallest real scheduled collection path; it does not install a host supervisor,
authenticate historical PIT, authorize a model call, promote Evidence, or open paper/live trading.
_Avoid_: Historical qualification, service-install receipt, Query Gate, execution acceptance

**Prospective Diagnostic Registration**:
A content-identified requirements freeze for two or three first-eligible future checkpoints with
different event mechanisms. It fixes each checkpoint's end-of-day cutoff construction, explicit
capability applicability, route-kind and source-diversity minima, cadence/gap/freshness limits,
eligible venues and instruments, candidate horizons, paired Agent arms, three replicates, aggregate
model budget, hidden-outcome rule, and stop/go conditions before new acquisition or inference. It
selects requirements, not Providers, observations, outcomes, or trades. Schema v1 preserves the
original all-required diagnostic. Schema v2 requires an actual-receipt event trigger while allowing
the other declared information capabilities to be optional observed context whose absence must stay
visible to the Agent and evaluator.
_Avoid_: Source configuration, experiment result, Provider allowlist, trading mandate

**Prospective Checkpoint Route Plan**:
A content-identified, no-authority binding from registered checkpoint capability/route kinds to
already accepted Harness Collection Jobs. A separate durable Harness-clock admission record is the
lower bound for trigger candidate receipt, so the checked-in plan cannot self-authorize a backdated
route. It selects neither an Event nor a conclusion and grants no model, historical-PIT, or
execution authority.
_Avoid_: Provider fallback, Event selection, Watch, model registration

**Prospective Checkpoint Readiness Report**:
A content-identified read-only audit of one route plan against durable Job health, accepted source
identity, and post-admission observation-version identities. Runtime health evidence is bounded by
the report's evaluation time; if later mutable Job state prevents historical reconstruction, the
audit fails closed instead of applying current health to the past. It distinguishes an operational
route waiting for an external event, an unconfigured trigger route, and an observed but still
unclassified candidate. It does not perform semantic eligibility selection, calculate a
trading-session barrier, freeze a Snapshot Set, or authorize a model call.
_Avoid_: Query Gate pass, Event Envelope, trigger decision, execution readiness

**Prospective Checkpoint Snapshot Set**:
A content-identified reconciliation of one registered checkpoint's immutable barrier with the
accepted route reports, Collection Policies, internally complete Journal-frozen Data Snapshots, raw
response hashes, exact selected Observation identities, and capability-specific read-only Agent
tools. It authorizes the exact selected Snapshot set through `FrozenDataSnapshotInput` without
becoming a composite evidence or data authority. `Complete` means every selected Snapshot and
Observation binding is internally verifiable; it never means every optional information capability
is present. Schema v2 preserves the original capability-complete contract. Schema v3 may retain
registered coverage gaps: structurally valid present Snapshots remain usable, missing optional
information remains visible, and no absent or unaccepted source is fabricated. Schema v4 also binds
each accepted route to exact Source Observation IDs; Agent tools expose only the Query Gate-authorized
Checkpoint Decision Input IDs projected from those observations, never every row in a shared Snapshot.
_Avoid_: Composite Data Snapshot, Evidence Pack, Provider fallback, execution approval

**Prospective Evidence Lineage**:
The exact binding from one Evidence Reference to one selected Data Snapshot, Source Observation
version, and deterministic Checkpoint Decision Input. A prospective Query Gate accepts the reference
only when source identity, availability, raw content, and all three content identities reconcile with
the authorized checkpoint input set.
_Avoid_: Matching URL, inferred provenance, same-cutoff Evidence Pack

**Prospective Query Gate Result**:
A content-identified preflight binding one v2 registration, checkpoint Snapshot Set, Evidence Pack,
Prospective Evidence Lineage, Prospective Execution Plan, model profile, cost ceiling, and exact
authorized Snapshot and Checkpoint Decision Input IDs. Missing required trigger or structural input
is blocking; missing optional information and unmet corroboration targets are nonblocking gaps passed
into the Judgment Run. It grants one bounded process-diagnostic model run only, never historical PIT,
strategy promotion, paper/live execution, or an alpha claim.
It reconstructs every supplied Decision Input from the Harness-owned frozen Snapshot Store before
granting authority; a caller cannot make fabricated data canonical by recomputing its content ID.
Schema v4 binds a single content-addressed evaluation material containing the exact registration,
Snapshot Set, Decision Inputs, and underlying Data Snapshots so downstream paper admission can
re-evaluate the Gate and restart recovery can reopen its authority evidence.
_Avoid_: Information-completeness score, strategy admission, order approval

**Prospective Execution Plan**:
A content-identified pre-model plan binding the registered model-profile alias to the exact Provider
Profile identity, provider/model, and two distinct frozen execution surfaces for the registered
control and treatment arms. It prevents callers from relabeling runs after execution and makes the
model and per-arm prompt/Skill/tool/MCP/runtime surface independently recoverable.
The alias resolves only through the Harness-bundled profile registry. The treatment surface must
preserve the complete control runtime/tool/MCP/Skill prefix and add at least one routed-method Skill;
two arbitrary hashes cannot be self-labeled as control and treatment.
_Avoid_: Mutable run configuration, arm label, model alias alone

**Checkpoint Decision Input**:
A deterministic, content-identified, Provider-neutral read-only projection of one Source Observation
already bound to a Prospective Checkpoint Snapshot Set. It preserves the observation and Snapshot
identities, source and lineage, occurred/published/source-updated/available/authority times, explicit
price basis, and unresolved completeness gaps while normalizing capability-specific field names. It
does not create another Snapshot, infer consensus or causality, promote Evidence, prove historical
PIT, or grant execution authority.
_Avoid_: Provider row, normalized conclusion, composite Snapshot, fill price

**Checkpoint Market Universe View**:
A content-identified, non-authoritative deterministic join of market and exposure Checkpoint
Decision Inputs from one Prospective Checkpoint Snapshot Set with one versioned exchange-instrument
rule set. It binds research price bases, effective instrument candidates, lot/tick rules, and
observed industry-to-tradable mappings while retaining every unresolved availability, taxonomy,
rebalance, suspension, and corporate-action gap; it is not a Snapshot, Evidence, Query Gate pass, or
execution admission.
_Avoid_: Composite Data Snapshot, executable universe, historical industry map, broker instrument

**Attention Watch**:
A Harness-approved, content-identified, expiring read-only policy that periodically reuses registered
Data Queries and Providers for one event or entity, evaluates deterministic new-information
triggers, and may enqueue one idempotent Agent wake-up bound to a newly frozen Data Snapshot. The
Agent may propose a Watch; it cannot choose arbitrary network routes, keep itself resident, bypass
budgets, mutate prior Judgments, notify arbitrary destinations, or submit an order from the Watch.
_Avoid_: Long-running Agent, cron prompt, market-data stream, order trigger

**Regime Agent Experiment Report**:
A private, content-identified development diagnostic binding qualified checkpoint Evidence Packs,
three paired runs per Method Arm, the exact Provider Profile, majority decisions, registered-open
path metrics, market/industry comparators, and the complete model-cost ledger. It may describe a
Skill's incremental decisions inside the opened case, but one retrospective case is not a general
effectiveness, alpha, paper, or live-execution claim.
_Avoid_: Strategy acceptance, holdout result, deployable signal

**Modeled-PIT Agent Validation Report**:
A private, content-identified aggregation of the exact Modeled-PIT registration, paired checkpoint
reports, majority decisions, realized opened-case paths, baselines, and the union model-cost ledger.
It may expose input or orchestration defects and compare decision behavior under frozen assumptions,
but it is always strict-PIT-ineligible, inference-ineligible, broker-unreachable, and execution-free.
_Avoid_: Strict PIT result, method promotion, alpha report, paper/live acceptance

**Regime Study Baseline Report**:
A private, panel-bound evaluator result comparing cash, the primary market index, an equal-sector
buy-and-hold proxy, and lagged monthly sector momentum using daily paths, modeled costs, turnover,
drawdown, CVaR, Sharpe, information ratio, and upside/downside participation. Industry indices are
non-executable research proxies; the report measures case difficulty and comparator strength, not
Agent skill or alpha.
_Avoid_: Strategy acceptance, execution simulation, method-Skill effect

**Revelation Mode**:
How evidence becomes observable: scheduled, unscheduled, continuously updated,
or retrospectively revised.
_Avoid_: Event type, source type

**Event Stage**:
The point reached in an event's evidence and market-diffusion lifecycle, from
pre-event through first observation, corroboration, realization, resolution, or
invalidation.
_Avoid_: Order status, approval status

**Expectation Delta**:
The point-in-time difference between an observed outcome and a cited prior
baseline. It may be explicitly unknown when no defensible baseline exists.
_Avoid_: Sentiment, surprise score without a baseline

**Transmission Channel**:
The causal mechanism carried by one step of a Transmission Path, such as demand,
capacity and cost, policy access, funding, uncertainty, attention, or forced
market flow.
_Avoid_: Event Archetype, industry label

**Transmission Directness**:
The number of causal hand-offs between the Event Cluster and an affected
exposure: direct, second-order, third-order, or fourth-order.
_Avoid_: Confidence, market overlay

**Transmission Path**:
An ordered, evidence-linked sequence of Transmission Channels from an Event
Cluster to an affected security or market exposure. Each step states its
directness, affected variable, counterevidence, blockers, and invalidation.
_Avoid_: Correlation edge, causal score

**Listing Snapshot**:
An immutable capture of a Provider's reported instrument listing lifecycles at
one retrieval time. It does not assert that the captured fields were known or
unchanged at an earlier date.
_Avoid_: Security master, historical truth

**Data Snapshot**:
An immutable, content-identified bundle binding one Data Query to ordered Provider attempts,
accepted Source Observations, cutoff rejections, degradation, and provenance that a Backtest
Request or read-only Agent tool can cite exactly. It is replay input, not proof of historical
completeness, executable liquidity, source infallibility, or Evidence admission.
_Avoid_: Cache, market-data dump

**Pre-event Universe**:
A fixed set of instruments eligible at an event cutoff, reconstructed from and
bound to one Listing Snapshot. It is not proof against source revision,
omission, or survivorship bias.
_Avoid_: Current constituents, dynamic universe

**Exposure Registry**:
A versioned, pre-outcome mapping from eligible instruments to economically distinct target
roles, supporting sources, and transmission directness. It constrains target selection but
does not predict direction or authorize a trade.
_Avoid_: Stock pick list, sector constituents

**Prospective Cohort Registration**:
A content-identified commitment that freezes an event-family hypothesis, future event
accrual rule, Exposure Registry, Judgment replicate protocol, baselines, missingness policy,
and acceptance gate before holdout outcomes exist.
_Avoid_: Backtest plan, selected event list

**Method Ablation Registration**:
A content-identified pre-outcome comparison that holds evidence, model, action space, and
evaluation constant while varying only named layers of research guidance. Each registered
Method Arm remains in the all-event comparison even when it abstains or fails.
_Avoid_: Prompt experiment, best-of prompt search

**Method Quality Benchmark Registration**:
A content-identified commitment that freezes historical contamination controls, case strata,
method suites and exact Skill-route content, repeated-run protocol, and the content identity of a
separate schema-validated Evaluation Specification before holdout outcomes are opened.
_Avoid_: Prompt leaderboard, backtest report, Prospective Cohort Registration

**Independent Evaluation Unit**:
One Event Case, not one stochastic Agent replicate. Repeated Agent runs within the same case
measure method instability and are averaged before cross-case inference; they do not create new
market observations or additional degrees of freedom.
_Avoid_: Case-replicate pair, model sample, repeated market event

**Clustered Paired Estimate**:
A comparison that first averages every registered replicate within each Method Arm and Event Case,
then computes paired differences and uncertainty across independent Event Cases. Missing cells
make the estimate inconclusive; no case or replicate may be deleted after outcomes are known.
_Avoid_: Replicate-level t-test, best-run score, filtered pair set

**Archive Capture Locator**:
A content-bound pointer to one historical archive record: immutable collection, target URL, capture
timestamp, object path, byte offset and length, upstream payload digest, and HTTP status. The
locator identifies what must be fetched and verified; it does not assert the publisher's original
publication time.
_Avoid_: Current URL, search result, mutable archive query

**Verified Archive Record**:
An exact byte-range archive response whose compressed member, WARC framing, capture metadata,
target, status, payload digest, and optional block digest have been checked. It proves that the
specific payload was present no later than the archive capture time. Historical Evidence still
requires source-specific publication-time extraction and a frozen availability-latency rule.
_Avoid_: Source Version Receipt, publication timestamp, Evidence-ready news

**Method Quality Market Snapshot**:
A content-identified pre-run commitment to the exact evaluation calendar, source vintage, case
`as_of` and cutoff, corporate actions, adjusted price rows, benchmark rows, non-empty fee schedule, and
non-empty venue rules capable of revealing a benchmark outcome. It grants no execution capability.
_Avoid_: Mutable price cache, post-run data pull, live market feed

**Method Quality Outcome Seal**:
A content-identified pre-run record binding one registered case, its Market Snapshot, evaluation
specification, and the complete expected case-replicate-arm run matrix while containing no outcome
payload.
_Avoid_: Outcome file, unenforced promise, Agent run manifest

**Method Quality Outcome Opening**:
The append-only sequence-one artifact that repeats the seal bindings, binds every expected
Judgment Artifact and deterministic result, and retains every registered judgment in the all-event
denominator. Results are non-executable directional research scores and round-trip cost proxies,
not cash-portfolio PnL, short positions, or investable returns. Validator enforcement is limited
to the supplied artifact boundary; it is not a transactional store lock.
_Avoid_: Mutable seal, partial result table, selected winner

**Masked Agent Input Manifest**:
A content-identified control artifact that binds original and masked Evidence Packs, evidence
documents, and Pattern Pack tool documents through one alias map and a forbidden-token scan over
the complete prompt and frozen-tool surfaces.
_Avoid_: Renamed original Evidence Pack, prompt-only alias list, hidden outcome label

**Method Arm**:
One frozen guidance treatment in a Method Ablation Registration, such as neutral evidence,
general equity methods, reusable patterns, or an event-family method.
_Avoid_: Agent persona, model variant, strategy baseline

**Source Coverage Registration**:
A content-identified, pre-accrual commitment to the observable source universe, mandatory
discovery and confirmation Providers, polling cadence, freshness limits, and failure action.
It bounds a first-eligible claim but does not prove exhaustive world coverage.
_Avoid_: Feed list, scraper config, complete news universe

**Coverage Receipt**:
An immutable record of which required Observation Providers succeeded or failed during one
polling interval under a Source Coverage Registration. It may block accrual, but it cannot
establish occurrence facts or make an event eligible.
_Avoid_: Health check, source truth, Accrual Decision

**Candidate Event Observation**:
An immutable, point-in-time assertion that a possible Event Cluster may satisfy one
Prospective Cohort Registration, including its occurrence facts and supporting Source
Observation. It is not an Accrued Event or a trading judgment.
_Avoid_: Event candidate, alert, qualifying event

**Accrual Decision**:
A deterministic, content-identified admission or non-admission result for one Candidate
Event Observation under one Prospective Cohort Registration. It never predicts market
direction and cannot remove an earlier Accrued Event.
_Avoid_: Agent decision, event label, trade decision

**Accrued Event**:
The next Event Cluster admitted by a Prospective Cohort Registration's outcome-independent
eligibility and separation rules. Missing later evidence or market data cannot remove or
replace it.
_Avoid_: Chosen example, usable event

**Common-Support View**:
A secondary comparison limited to Accrued Events on which the candidate and named baselines
all had their pre-registered inputs. It supplements, and never replaces, the all-event
denominator where missing inputs become explicit abstentions.
_Avoid_: Clean sample, filtered cohort

**Backtest Request**:
An immutable, engine-neutral request embedding the exact Signal Intent content and binding
it to a compatible point-in-time evaluation window, target-containing universe, data
snapshot, strategy, and Simulation Specification.
_Avoid_: Engine config, Order Intent

**Simulation Specification**:
The versioned market-data, fill, fee, venue-rule, capital, and randomness assumptions
under which a Backtest Request is evaluated.
_Avoid_: Trading Mandate, live venue configuration

**Backtest Run Manifest**:
An immutable record binding a Backtest Request to the exact engine, bridge, configuration,
input hashes, and execution time used for one run.
_Avoid_: Provider Manifest, backtest report

**Backtest Result**:
The normalized completed or failed outcome of one Backtest Run Manifest, including
deterministic result identity, metrics, artifacts, or explicit failure reasons.
_Avoid_: Broker receipt, live performance claim

**Calibration Evidence**:
A frozen set of independently repeated Backtest Results labeled by Event Cluster,
chronological partition, and pre-registered candidate or baseline variant. It is input to a
calibration gate, not permission to tune after observing the test partition.
_Avoid_: Backtest leaderboard, selected wins

**Calibration Cell**:
One pre-registered Event Cluster evaluation unit binding its visibility cutoff,
chronological partition, target mapping, evaluation window, Data Snapshot, horizons, and
Simulation Specification before outcomes are opened.
_Avoid_: Backtest row, tunable sample

**Variant Decision**:
One pre-registered candidate or baseline action for a Calibration Cell. A long-only action
is either `buy`, with an exact Backtest Request, or `abstain`, with no fabricated Signal
Intent or Result and zero exposure in the fixed calibration denominator.
_Avoid_: Failed trade, synthetic zero-PnL result

**Calibration Gate Result**:
A content-identified, engine-neutral acceptance or rejection report that applies one
versioned cross-event protocol to Calibration Evidence. Rejection is evidence and blocks
phase promotion; acceptance grants backtest calibration only.
_Avoid_: Alpha claim, paper approval, live mandate

## Decisions

**Judgment Run**:
A bounded, auditable Agent evaluation of one Evidence Pack under exact model, Skill, tool,
MCP, budget, and context configuration.
_Avoid_: Backtest run, chat session

**Judgment Artifact**:
The immutable result of a Judgment Run, including cited evidence, transmission reasoning,
candidate impacts, blockers, abstention state, and the complete configuration identity.
It is a proposal awaiting deterministic admission, not a Signal Intent.
_Avoid_: Model response, trade recommendation

**Judgment Replicate Set**:
A pre-sized collection of independent Judgment Artifacts produced from the same Evidence
Pack and runtime surface without cross-replicate memory. It measures decision stability; it
is not a multi-Agent debate.
_Avoid_: Agent team, repeated chat

**Decision Run Manifest**:
A content-identified record of one eligible Prospective Query Gate, exact Evidence Pack and Agent
Execution Plan, and the complete registered paired Judgment Replicate Set. It records every terminal
run, Judgment, execution-surface, and content-identified metrics artifact, verifies exact
provider/model and per-arm provenance, derives treatment-arm stability by the pre-registered rule,
and retains the control arm only for comparison. It is run lineage, not a new policy or execution
authority.
Each run also binds the exact final `judgment.validated` Journal event. That event binds the Judgment
proposal, transcript, content-identified metrics payload, and cost, so a caller cannot lower reported
cost by replacing a standalone metrics object.
Every run must begin at or after the bound Query Gate evaluation; a later Gate cannot retroactively
authorize an earlier model run.
_Avoid_: Experiment folder, model summary, execution admission

**Ensemble Decision**:
A deterministic candidate-or-abstain result derived from one Judgment Replicate Set by a
pre-registered agreement rule. It cannot add targets, evidence, direction, or confidence
that no qualifying Judgment Artifact proposed. It binds the exact runtime, prompt, Skill,
tool, MCP, context-estimator, and compactor surface frozen before replicate one; a mismatch
or reused Judgment Artifact forces abstention.
_Avoid_: Model vote, committee recommendation

**Candidate Impact**:
One Judgment Artifact's evidence-linked directional hypothesis for a target exposure at a
stated horizon, directness, confidence, and invalidation boundary.
_Avoid_: Order, portfolio weight

**Event Assessment**:
A versioned fast or deep judgment about an Event Cluster, its Transmission
Paths, counterevidence, expected persistence, and invalidation conditions.
_Avoid_: Judgment Run, Agent opinion, prediction

**Signal Intent**:
A time-bounded, evidence-linked expression of directional interest in a
security. It is not an instruction to trade.
_Avoid_: Trade, recommendation

**Order Intent**:
An idempotently identified request to evaluate a specific order against policy
and approval. It is not proof of broker acceptance.
_Avoid_: Order, execution

**Decision Admission**:
A content-identified deterministic admission of one exact Decision Run Manifest. An abstaining
manifest produces an archived abstention with no Signal or Order. A treatment arm with the registered
two-of-three target-and-direction agreement may produce one exact Signal Intent and paper Order
Intent. The Signal is deterministically reconstructed from the exact agreeing treatment Judgments,
must remain valid for the Order's entire lifetime, and cannot substitute another allowed target or
direction. Neither Signal nor Order may predate the consensus Manifest; the control arm is
comparison-only and need not agree. The Manifest's content hashes are not occurrence authority:
paper admission additionally requires the trusted Harness composition root to bind each planned
execution surface to an Agent runtime authority that reopens the completed Run Record, full Journal
chain, terminal/transcript/raw/tool-result artifacts, validation event, and Journal-recomputed
metrics. The order caller cannot self-attest or inject that authority. The admission remains
`execution_diagnostic_only`, carries no Strategy Admission, alpha claim, live capability, credential
access, or authority to bypass Trading Mandate, tradability, price, policy, approval, Provider, and
reconciliation gates.
_Avoid_: Experimental Paper Admission, strategy promotion, broker acceptance, Agent execution tool

**Trading Mandate**:
A versioned, expiring grant that defines the accounts, environments,
instruments, directions, and risk envelope in which Order Intents may proceed.
_Avoid_: Permission flag, auto-trade switch

**Price Basis**:
A source-identified, time-bounded price observation used to evaluate one Order
Intent. It binds instrument, units, source version, observation time, and expiry;
it is neither an adjusted research price nor proof of an executable fill price.
_Avoid_: Current price, model price

**Hard Policy Evaluation**:
An immutable result of applying one evaluator version to an exact Order Intent,
Trading Mandate, Price Basis, and evaluation time. It may deny, require manual
approval, or establish eligibility; it cannot establish broker acceptance.
_Avoid_: Risk score, Agent approval

**Approval Decision**:
An auditable deny, manual-review, approve, or reject result tied to the exact
Order Intent, Trading Mandate, Price Basis, Hard Policy Evaluation, actor, and
decision time.
_Avoid_: Confirmation, model confidence

## Providers and execution

**Provider**:
An external or in-process capability owner for market data, backtesting,
account state, paper execution, or live execution.
_Avoid_: Tool, broker

**Observation Provider**:
A read-only Provider that discovers or acquires source records under an Observation
manifest. It cannot own Evidence promotion, research, approval, or execution. Aggregated
and direct sources retain distinct identities.
_Avoid_: Evidence authority, execution Provider

**Capability Snapshot**:
A point-in-time record of what a Provider declares and what the harness has
independently verified it can do.
_Avoid_: Integration config, feature list

**Provider Acceptance**:
A Harness-owned, content-identified conformance decision binding one Provider implementation,
dependency and configuration version, environment, account scope, advertised capability subset, and
the exact restart, ambiguity, reconciliation, and fault evidence that passed. Provider declarations
are inputs to this decision and cannot mint verified capability themselves.
_Avoid_: Provider manifest, successful connection, mock acceptance

**Model Provider Profile**:
A versioned declaration binding one model adapter to its exact Provider identity, endpoint,
model, capabilities, context limits, credential reference, and pricing basis.
_Avoid_: Environment variables, fallback chain, model alias

**Usage Ledger**:
An append-only account of model and tool consumption for completed and incomplete Judgment
Runs, linked to their Provider Profile, experiment, Method Arm, and execution identity.
_Avoid_: Billing invoice, transient metrics, usage dashboard

**Usage Ledger Union**:
A content-identified reconciliation over one or more Usage Ledgers. It deduplicates exact Run IDs,
fails on conflicting payloads, and reports terminal status counts plus total estimated cost. An
experiment aggregate binds this union instead of accepting an unverified prior-cost scalar.
_Avoid_: Summed dashboard number, caller-supplied historical cost, billing invoice

**Execution Event**:
A provider-observed state change for an order, fill, position, balance, or
reconciliation result.
_Avoid_: Callback, tool result

**External Order**:
A broker order not created by a known Order Intent in the harness ledger.
_Avoid_: Unknown order, manual order

**Reconciliation**:
The fail-closed process of comparing provider truth with the harness ledger
before accepting new Order Intents. A complete reconciliation snapshot must state
its cutoff/completeness and gaps; the absence of a record is authoritative only
inside such a complete snapshot.
_Avoid_: Sync, refresh
