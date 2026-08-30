# Market Impact Agent

Market Impact Agent is an auditable, event-driven trading agent harness. It is
designed to turn point-in-time evidence into layered market-impact reasoning,
versioned signal intents, and policy-gated actions executed by replaceable
backtest engines and paper-trading or broker providers.

The eventual product goal is Agent-directed automated trading in both paper and
live environments. The repository is currently building and validating the
evidence, policy, model-runtime, reconciliation, and execution infrastructure
required to reach that goal safely.

> [!WARNING]
> This repository is an early executable skeleton. It has no verified broker
> connection, cannot submit live orders, and must not be used with real capital.

## Why this project exists

Markets react to surprise, expectations, positioning, transmission mechanisms,
and attention—not to headline sentiment alone. A useful event-trading system
must therefore distinguish when information became visible, trace direct and
secondary effects, compare historical analogues, test the result without
look-ahead bias, and stop or exit when the thesis decays.

The project deliberately does **not** build another exchange simulator or
broker API. Mature engines own fills, fees, slippage, portfolio state, and
reconciliation. This harness owns evidence, event reasoning, provider
capabilities, approval policy, and an auditable intent boundary.

## Current status

The bootstrap implements:

- canonical domain contracts for signals, order intents, mandates, and approval;
- a Harness-owned durable paper-execution contract slice that freezes exact intent/mandate/price/
  policy/approval identities, atomically queues approved mock intents, treats lost acknowledgements
  and expired leases as `unknown`, and requires complete reconciliation before continuing; a
  Decision Admission can now persist an eligible prospective Query Gate→exact Evidence lineage→frozen
  Harness-profile/control-plus-routed-method Execution Plan→paired Decision Run Manifest with sealed
  Judgment/Journal-event/metrics evidence→stable
  treatment consensus→deterministic Signal→paper Order provenance hash without claiming
  strategy promotion or execution authority. Paper admission does not trust those caller-supplied
  hashes alone: the trusted composition root must bind the exact execution-surface hash to an Agent
  runtime authority that reopens each completed Run Record, full hash-chained Journal, terminal
  Judgment, transcript/raw response, tool-result, validation-event, and recomputed metrics artifact.
  There is still
  no Agent execution tool, account capability, broker connection, or live path;
- immutable point-in-time Evidence Items, Event Envelopes, deterministic
  fast/deep/combined routing, and evidence-linked Transmission Paths;
- a separate read-only Observation Provider contract plus current public Polymarket and
  Kalshi snapshot adapters, an authenticated World Monitor discovery adapter, and private
  content-addressed raw/normalized JSON bundles with explicit occurrence, publication,
  update, availability, aggregator-fetch, and retrieval semantics;
- a Provider-neutral [Data Input Harness](docs/DATA_INPUT_HARNESS.md) with content-identified
  semantic queries, strict/modeled/prospective/retrospective lanes, frozen Provider-manifest/source routes,
  concurrent reads, typed degradation, raw-response retention, cutoff filtering, immutable
  Data Snapshots, persistent complete-snapshot caching, and bound read-only Agent tools whose
  checkpoint results are content-identified Provider-neutral decision inputs with explicit source,
  time, authority, price-basis, and completeness-gap fields; a separate content-identified
  Checkpoint Market Universe View can deterministically join one frozen checkpoint's market,
  instrument, taxonomy, membership, and ETF-mapping inputs—including exact daily exchange-PCF
  constituent links—with versioned SSE/SZSE lot/tick rules while keeping suspension,
  corporate-action, taxonomy-interval, PCF-weight, and rebalance gaps explicit; the v3 prospective
  registration and Query Gate block missing trigger/structural inputs while allowing the Agent to
  reason or abstain with optional information missing. A content-identified route plan now binds
  accepted Collection Jobs before any new trigger, and a read-only readiness report distinguishes
  expected waiting from missing trigger infrastructure without starting a model. The formal
  [Event Impact Triage](docs/EVENT_IMPACT_TRIAGE.md) contract freezes every unclassified candidate,
  requires a cited complete Agent partition, separates checkpoint eligibility from broader market
  or held-position impact routing, and leaves first-eligible selection to the Harness after
  authoritative coordinator/specialist Run and Usage Ledger verification. A fixture-accepted,
  no-tool Model Provider runtime now freezes coordinator-only and bounded-specialist comparison
  arms, budgets, restart behavior and pre-reveal evaluation mechanics. The first sealed real
  121-version comparison attempt stopped fail-closed on timeout/budget capacity before producing a
  report; an arm-neutral Work Manifest partitions the exact input into eleven bounded units, and a
  Manifest-bound runtime now preserves the byte-exact v2 dialect and adds a v3 positional-identity
  dialect: specialist findings and coordinator Digest drafts bind by exact array position, while
  partition drafts use strictly validated zero-based global atom ordinals. V4 retains those
  positional bindings and adds full typed classify contracts while the Harness injects Cluster Seed
  candidate identities. The Harness translates every dialect into the same canonical Digest,
  Partition and Proposal contracts and retains
  fail-closed map, partition, per-cluster classification, restart and Usage reopening. A distinct Work Comparison
  Registration/Report pre-binds the Candidate Set, Manifest, plans and frozen gates, then scores
  only two fully reopened completed arms using Journal-derived timing and exact Usage cost. Its
  Report binds Outcome/receipt hashes and requires byte-identical Harness replay before downstream
  use. The real v2/v2 replay was safely terminal but incomplete: baseline stopped
  `budget_exhausted` after four completed members and treatment stopped `failed` after five, so no
  Report or semantic authority exists. Its v3 121-candidate positional proof is scripted mechanics
  evidence that addresses those identity/type-contract failures without rewriting v2. A later
  nine-version real v4 treatment completed all 13 members and produced a native Work Decision: no
  eligible policy checkpoint, five archive clusters, two EventAssessment routes and one Attention
  Watch route. That Decision classified each version once and granted no Query Gate, Judgment or
  execution authority. Typed Position Snapshot/historical-context
  payloads, completed real comparison arms and passing blind semantic results remain open acceptance
  work. Order-time
  tradability and risk remain
  independently fail-closed; the
  framework also has a prospective RSS/Atom Provider that freezes a secret-free source
  configuration, validates redirect identity, rejects full-content feed elements before raw-byte
  retention, and records actual receipt without claiming historical PIT or article-body rights;
- the first accepted A-share official-event route: a bounded CSRC publication Provider plus a
  reusable seven-gate Source Route Acceptance Report that binds captured rights evidence, exact raw
  JSON records, publication time, actual receipt, append-only revisions, and deterministic replay;
  the accepted route is private-research only and grants neither historical PIT nor execution;
- a prospective `tushare-observation` Provider with twenty-three separately content-identified
  routes for news, index/ETF/calendar context, instrument/industry/ETF-PCF exposure, margin
  positioning, macro schedule, and analyst forecasts; the accepted routes have passed real private
  capture, stored-bundle replay, and the seven route gates without exposing the purchased token or
  licensed rows. The purchased news entitlement now has seven active short-news source Jobs and one
  `major_news` Job at 2-, 5-, or 15-minute cadence. Two documented short-news sources remain
- an append-only Prospective Receipt Journal that preserves every source attempt and content
  revision, deduplicates repeat sightings, freezes cadence-qualified Data Snapshots, and writes
  private Parquet/ZSTD analytical projections without becoming another evidence authority;
- a Harness-owned one-shot collection worker with content-identified Jobs, unique logical due
  opportunities, expiring leases, bounded jitter/backoff, misfire and cancellation classification,
  restart-safe staged Snapshot recovery, machine-readable health, rolling windows derived from the
  Harness logical due time, healthy typed `no_data`, and append-only Collection Usage Records with
  proved request/page/byte/row/latency totals and averages; a real CSRC-plus-Tushare
  market tracer and the authorized host process-supervisor acceptance have passed, while complete
  multi-policy operations, retention, and restore acceptance remains a separate gate. A 30 August
  runtime audit found route-plan drift: one serial worker could spend 291 seconds across
  thirteen due Jobs while two-minute news Jobs have a 90-second grace window, producing post-
  admission misses and zero currently operational checkpoints. The repository repair now orders
  automatically selected work by absolute deadline, samples the Harness clock at each actual claim,
  and runs a plan-bound maximum of four opportunities concurrently. Route-plan v2 adds an explicit
  predecessor, one atomic current head, immutable effective intervals, and a freeze-time authority
  recheck. The host now runs supervisor v4 under the admitted v2 route epoch; the first accepted
  ten-opportunity window recorded zero misses and failures, with maximum all-Job claim latency of
  74.54 seconds and trigger-Job latency of 45.304 seconds. Longer multi-policy soak, retention and
  restore acceptance remain open;
  the append-only collector state is not reset or discarded;
- a minimal Harness-owned Attention Watch runtime with immutable event/query scope, TTL and byte/
  poll/wake budgets, a Journal-frozen baseline, atomic expiring due leases, durable
  due/backoff/cooldown/cancellation state, new-version detection, restart-safe duplicate suppression,
  and an idempotent local wake-up outbox that always binds a complete frozen Snapshot;
- strict JSON/schema codecs for the engine-neutral Backtest Request and Result, with Run
  Manifest adapter identity and named input hashes;
- a pinned optional NautilusTrader `1.231.0` bridge with one deterministic synthetic
  A-share replay covering a suspension, opening limit lock, lot size, fees, and slippage;
- a language-neutral Tushare HTTPS adapter for bounded stock-listing, exchange-calendar, and
  unadjusted daily-bar, adjustment-factor, and source daily-price-limit reads plus
  deterministic pre-event A-share universe construction, with a disabled, unverified
  Provider manifest;
- a private, content-addressed local Tushare Data Snapshot bundle with Parquet tables,
  hash/permission validation, and a stable `data_snapshot_id` for later replay requests;
- narrowly versioned modeled-open adapters and a token-free backtest CLI that consume validated
  private `600028.XSHG` integration/cohort and `601857.XSHG` opened-development bundles in memory,
  with repeated-result local acceptance and no derived licensed fixture committed;
- independent 1/3/10-session Nautilus runs from one Backtest Request, normalized cost-aware
  `net_return`, and fail-closed v1/v2 Phase 2 calibration gates over pre-registered repeated
  event/baseline Results and honest long-only abstentions;
- a bounded local Agent Harness with content-addressed Evidence/Pattern/Judgment contracts,
  append-only crash-safe runs, automatic context compaction, on-demand hashed Skills, typed
  permissioned tools, official MCP lifecycle handling, budgets, cancellation, redaction,
  closed-output correction, a pinned MiniMax M3 China-endpoint Provider, and a pinned local
  CLIProxyAPI `gpt-5.6-luna` xhigh Provider;
- persona-free, automatically routed Research Method Skills adapted from stable event,
  fundamental, market-context, and adversarial-risk workflows; a frozen four-arm ablation
  holds evidence/model/action space constant while varying only those method layers;
- a separate [Skill discovery and governance](docs/SKILL_GOVERNANCE.md) lane for registered
  outcome-opened full-information research. It requires one discovery plus at least two additional
  independent Event Case/time-family validations, bounded divergence, resolved material
  counterexamples, and exhaustive comparison against every runtime Skill, both research catalogs,
  and open candidate before creating a non-executable candidate. A Judgment v2 sidecar can retain
  offered/routed/loaded/reported-use/influence trace, and a pre-outcome evaluation band can measure
  broadly correct direction, horizon, return, volatility and adverse excursion. Neither contract
  changes PIT, consensus, Signal/Order, approval, paper, or live authority;
- a content-identified Model Provider Profile/Factory plus a hash-chained Usage Ledger for
  successful and failed runs, including per-run Token/time/result budgets and a cost ceiling when
  the Provider has a defensible USD/token price;
- an adaptive paired process diagnostic that runs two complete control/treatment pairs first and a
  third complete pair only if either arm disagrees, plus observational per-Judgment
  `decision_confidence` that is accumulated per model but never changes sizing, approval, or policy;
- a Provider-neutral historical news batch contract with exact ordered source registrations,
  typed missingness/failures, strict UTC half-open filtering before limits, lineage deduplication,
  and a read-only news-evidence assessment Skill that cannot mint Evidence or signal weight;
- a synthetic physical-energy supply-shock vertical slice with both one generated Judgment
  Artifact and a generated three-of-five Ensemble Decision deterministically admitted into
  the existing Signal Intent and Backtest Request, then replayed by the unchanged Nautilus
  bridge without re-running a model;
- a real MiniMax M3 five-replicate synthetic-bundle acceptance in which all five runs
  completed under one pre-frozen execution binding and exact three-of-five agreement selected
  `600938.XSHG/up/1 session`; this is runtime evidence only and has no matching replay snapshot;
- a content-identified prospective Agent Phase 2 registration that freezes first-eligible
  physical-shock accrual, an upstream A-share Exposure Registry, five independent Judgment
  replicates, five baselines, missingness handling, and a stricter all-event gate before any
  holdout event or outcome is opened;
- a private append-only Accrual Ledger that validates actual-receipt Candidate Event
  Observations, replays deterministic admission against that registration, retains explicit
  non-admission reasons, enforces revision/separation/cohort rules, and detects stored-row or
  hash-chain tampering;
- a frozen Source Coverage Registration and one-shot physical-energy monitor that privately
  retains exact GDELT, EIA, and ENTSOG response bytes, records mandatory-source failures in
  content-identified Coverage Receipts, derives revision-aware ENTSOG gas candidates, and
  blocks accrual whenever the registered coverage cycle is incomplete;
- an idempotent cutoff scheduler that freezes each admitted event's Evidence Pack, exact
  Pattern Packs, Exposure Registry, source receipts, and provenance manifest only after the
  registered 60-minute evidence cutoff;
- a provider capability manifest and registry;
- a deterministic hard-policy evaluator;
- a hard-policy-gated paper execution gateway and idempotent mock provider;
- JSON Schemas for event, signal, order, and provider artifacts;
- machine-readable status, provider-manifest validation, and event-chronology commands.
- prediction-market capture and offline bundle-validation commands; all three Observation
  Providers remain disabled and unverified.

Planned integrations are documented but **not claimed as working**:

- a separately validated Nautilus-to-IBKR paper Provider;
- an external-process VeighNa bridge for future A-share gateways;
- HTTP/gRPC execution-provider transports.

NautilusTrader is the selected default engine foundation and behavioral reference. The
Harness uses it through an engine-neutral backtest bridge; later execution Providers still
pass through the same policy, approval, and audit boundary. Version `1.231.0` is an
optional, exact dependency and grants backtest capability only.

The first official token-backed local Tushare capture and validation completed on 2026-08-25
for `600028.SH`, using 2019-09-18 as-of metadata and a 2019-09-19..2019-10-10 daily window.
It proves the language-neutral HTTPS adapter/capture/validation path and that the local
credential worked for that account, target, and window. Tushare is fully usable as an upstream
source under the owner's purchased, private, non-redistributed deployment. This authorizes its
integration; it does not bypass Harness admission. The current `tushare-http` market-data Provider
Manifest remains `disabled`/`unverified` until its claimed route and capabilities pass their gates,
and that one capture does not prove every optional API is enabled, historical PIT authority,
full-universe completeness, actual liquidity or fillability, alpha, or paper/live readiness. The
modeled-open replay is a separate deterministic simulation gate, not a Provider or source-truth
claim. See
[docs/TUSHARE_DATA.md](docs/TUSHARE_DATA.md).

The first pre-registered real cohort completed on 2026-08-26: two train and five later test
Event Clusters, 35 registered variant decisions, and two deterministic runs for each of 25
buys. The v2 gate rejected it with exactly `candidate_net_return_not_positive`. This is a
valid negative research result; the opened cohort cannot be retuned and reused as unseen
evidence. No baseline-superiority, alpha, Phase 3, paper, or live claim has passed. See
[docs/PHASE2_CALIBRATION.md](docs/PHASE2_CALIBRATION.md).

The bounded research-only Agent runtime and its exact non-claims are recorded in
[docs/AGENT_RUNTIME.md](docs/AGENT_RUNTIME.md). Deterministic hardening, base-wheel isolation,
the private MiniMax M3 v4 single run, the private five-replicate v2 ensemble run, and a full
CLIProxyAPI Luna xhigh single run pass the current local runtime gates. This proves bounded
Provider substitution through the same auditable Agent-to-frozen-decision pipeline, not model
quality, repeated behavioral equivalence, event-family calibration, alpha, or execution readiness.
The real ensemble selected `600938.XSHG`, while
the committed deterministic replay fixture uses `600028.XSHG`; those results are deliberately
not combined. None of this overrides the failed trading-calibration result.

The first real MiniMax four-arm method ablation also completed 20 of 20 runs. It showed that
persona-free general and family-specific method layers can be deterministically routed,
costed, replayed, and can change the selected horizon. Because the single synthetic bundle
contains one eligible long-only target and no market outcome, it establishes process behavior
only—not a winning Skill, better forecast, backtest result, or alpha claim.

The first real outcome-opened development case uses the 2019 Abqaiq–Khurais attack and recovery as
two information states of one Event Case. Agent-visible inputs now replace event-specific magnitude,
facility, restoration, shipment, and calendar-date fingerprints with coarse mechanism categories
and shifted dates. The relative evidence sequence and lag are preserved, so residual narrative
linkage and memorization risk remain and this is not an authenticated holdout. The replacement case
completed all 40 required Agent runs and passed joint report/request preflight before four
deterministic Nautilus replays. Every ensemble abstained; the registered fixed-long control was net
negative in both states. The one-case result supports implementation and evidence-update diagnosis
only: it does not rank the four methods or support alpha, prospective, or execution claims. All
artifacts from earlier case identities remain invalid. See
[docs/ABQAIQ_DEVELOPMENT_BENCHMARK.md](docs/ABQAIQ_DEVELOPMENT_BENCHMARK.md).

The method-quality protocol is now frozen separately. It adds a persona-free research-bias
Skill, strict Historical Evidence Manifest and Source Version Receipt contracts, consistent
historical identity aliases, train-only outcome memory, 8 development and 24 holdout case targets,
cross-mechanism and family-specific suites, content-bound Skill instructions, machine-validated
Market Snapshot/Outcome Seal/Outcome Opening contracts, deterministic directional-score and
aggregation equations, cost/risk metrics, and registered future promotion gates. The method selector computes
a non-executable directional research score and round-trip cost proxy, not cash-portfolio PnL,
short exposure, or an investable return; Nautilus remains a later trading/backtest-engine boundary.
Receipts bind assertions but do not authenticate them, so v2 cannot yet admit a retrospective
holdout. v1 was retired before any outcome opening because it incorrectly counted five Agent
replicates of one event as five independent observations. v2 averages replicates within each Event
Case, performs inference across 24 cases, freezes one primary promotion contrast, and treats the
family/Pattern-Pack contrasts as diagnostics requiring new prospective confirmation. The clustered
estimator is executable, but cannot support a claim until a future content-identified case/run
matrix is bound into the pre-run seals/openings. No self-asserted pair artifact is accepted. The
overall promotion evaluator is future work and no promotion claim is made. Style attribution is
deferred. One of eight development cases has been built and run as an explicitly opened,
non-inferential diagnostic; the remaining seven development cases and all holdout cases remain
unbuilt. See
[docs/METHOD_QUALITY_BENCHMARK.md](docs/METHOD_QUALITY_BENCHMARK.md).

Pinned TradingAgents `0.3.1` is deployed privately as an external native-capability baseline. The
current Luna xhigh runner gives it the real Abqaiq event and `601857.SH` target, registered
cutoff-bound news and Tushare market data, and its unchanged analyst, debate, risk, and model-prior
methods. Only cross-run memory, outcome reflection, post-cutoff/live data, and broker reachability
are disabled. The earlier strongly masked MiniMax smoke remains a negative input-isolation
diagnostic; it is not the current native comparison. The native 5-by-2 run compares behavior,
resource use, and stability with the Harness, but different model-visible identity and architecture
prevent a causal same-method ranking from this one opened Event Case. Deployment and news-pipeline
findings are in [docs/TRADINGAGENTS_EXTERNAL_BASELINE.md](docs/TRADINGAGENTS_EXTERNAL_BASELINE.md).

The native Luna xhigh 5-by-2 completed all ten runs with zero structured-output degradation. Its
ratings were seven `Hold`, two `Underweight`, and one `Sell`; all map to abstention in the Harness's
one-sided long action space. The native graph used 174 model calls, 903,651 input and 376,799
output Tokens, and 7,531.109 cumulative seconds. Its reports were broad and actionable but drifted
beyond the requested one-session horizon and generated precise levels and sentiment values from
sparse inputs. This is behavior, stability, and resource-use evidence, not a method ranking or an
alpha result.

The first isolated Luna xhigh diagnostic for the optional news-assessment Skill completed 20 of
20 paired runs. Both the general-method control and the otherwise identical Skill treatment
abstained in every attack/recovery replicate. The Skill increased input Tokens by 12.2% and output
Tokens by 5.7% without a visible process or decision improvement on this sparse opened case, so it
remains opt-in rather than part of every Agent run. This does not evaluate richer multi-source news
batches, where the Skill's declared precondition actually holds.

The attempted A-share regime validation made 108 paired Luna xhigh Agent calls over six opened
cases under one shared $20 cap, but an independent audit found the qualification gate did not
require archive/provider authority to predate each checkpoint. Correct replay qualifies 0/18
selected checkpoints, so every run is retained only as invalidated descriptive behavior and cost
evidence. Both arms always abstained; same-window primary-index, equal-sector, and lagged
sector-momentum baselines expose missed rallies and rotations. Exact lineage and cost reconciliation
now fail closed. This is not Agent effectiveness, alpha, paper trading, or live authority. See
[docs/MARKET_REGIME_RESEARCH.md](docs/MARKET_REGIME_RESEARCH.md).

The later full-access publisher-archive pass verified 115 of 120 unique archive candidates and
replaced 98 current snapshots with 100 canonical historical versions. Strict requalification
improves established-news readiness to 2/18 frozen validation checkpoints, but the complete gate
remains 0/18 because market, industry, positioning, macro, additional-news, and event-revelation
authority gaps remain. A second replay recovered exact article bodies for 100 of the 115 accepted
versions; the rebuilt Agent inputs contain every exact news payload at 13/18 checkpoints and six of
eight at the other five.

A separate opened-outcome Modeled-PIT diagnostic admitted the frozen 18-checkpoint selection under
content-identified safety delays without changing strict PIT. It completed 108 Luna xhigh calls:
all runs completed, both arms abstained at all 18 checkpoints, and the routed Skills changed no
majority decision. Horizon persistence was a blocker in 108/108 runs, event identity or attribution
in 106/108, and expectation delta in 105/108. The result points to three input-contract gaps before
another model run: expose the observed event fact without future outcome leakage, bind a defensible
prior expectation, and register mechanism-appropriate horizons plus executable index/ETF targets.
It does not establish Agent effectiveness or alpha. The report is
`regime-modeled-pit-agent-validation-report-317f79ea1602e7d381eba01f9522123116033bdbbc179180dfa71f46f895f380`.
The aggregate rehashes every paired registration and report, then recomputes each
`common_input_hash` from the frozen Evidence Pack, Method Evidence Declaration, and registered
horizon. It also reconstructs both arm execution bindings from that instruction and matches all 36
binding hashes against the report artifacts and six-record local Usage Ledgers; all 18 formal
checkpoints passed that execution audit. The audit also rebuilds every run summary and coverage row
from the terminal Judgment Artifact, Run Journal, and ledger metrics, so report decisions cannot be
rewritten independently of the 108 recorded runs. Terminal replay additionally reparses the final
model-turn assistant payload and matches the Judgment proposal, raw response, transcript, and
metrics to the hash-chained `judgment.validated` event.

The report also reconciles every current append-only Usage Ledger by Run ID. The 70-ledger union has
528 unique terminal runs, zero duplicate or conflicting records, and $4.928324 total estimated
model cost under the shared $20 cap. This corrects the earlier incomplete $2.436518 total; the new
Modeled-PIT experiment contributed $0.991336 and one invalid-horizon diagnostic contributed
$0.053516.

The first archive-capture adapter is now reproducible through `market-impact archive
common-crawl-verify`. It verifies a fixed Common Crawl byte range, one complete gzip/WARC member,
capture metadata, target/status, and payload/block digests; redirects, widened origins, changed
locators, truncated records, and digest mismatches fail closed. A passing archive capture proves
only that exact content existed by the archive capture time. It does not supply or authenticate the
publisher's original `published_at`, so it cannot yet admit a method-quality historical case. See
[docs/ARCHIVE_AUTHORITY.md](docs/ARCHIVE_AUTHORITY.md).
The historical recovery ladder, publisher archive audit/materialization commands, remaining PIT
gaps, and stock-versus-index price basis are in
[docs/PIT_EVIDENCE_RECOVERY.md](docs/PIT_EVIDENCE_RECOVERY.md).
The current and future data-plane layers, source gates, adopted or deferred infrastructure,
continuous collection flow, and scale triggers are in
[docs/DATA_PLATFORM_PLAN.md](docs/DATA_PLATFORM_PLAN.md).

The research reset is now frozen in
[docs/PHASE2_AGENT_PREREGISTRATION.md](docs/PHASE2_AGENT_PREREGISTRATION.md). Accrual starts
after 2026-08-27T00:00:00Z and targets the first five qualifying future physical energy
supply shocks. No event has accrued and no holdout outcome has been opened, so this is a
preregistration and operational-ledger milestone only—not a successful backtest or
permission to enter Phase 3. The first monitor covers global news discovery plus direct
European gas confirmation; its registered blind spots explicitly exclude a claim of global
exhaustiveness, and oil/non-European direct confirmation still needs additional adapters.

## Architecture at a glance

```text
Prospective Snapshot Set -> exact Decision Inputs + Evidence lineage -> Query Gate
       -> frozen Provider Profile + two-arm Execution Plan -> 3 Judgment Runs per arm
       -> runtime-authority-verified Judgment/Journal/metrics evidence -> Decision Run Manifest
       -> treatment 2/3 agreement or archived abstention -> Decision Admission -> SignalIntent
       -> backtest request -> Nautilus backtest bridge
       -> deterministic policy -> optional semantic approval
       -> durable OrderIntent -> sealed submission capability -> execution provider
       <- order/fill/account events <- reconciliation <- provider
```

An LLM may propose an `OrderIntent`; only the harness gateway can convert an
eligible, active intent into the capability accepted by a provider. The LLM
never receives raw, unrestricted broker authority. Hard policy is fail-closed
and cannot be overridden by a semantic approval agent.

See [GOAL.md](GOAL.md), [ARCHITECTURE.md](ARCHITECTURE.md), and
[ROADMAP.md](ROADMAP.md) for the accepted product boundary and delivery gates.

## Local development

Requirements:

- Python 3.13 or 3.14
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync --all-extras --python 3.13
uv run market-impact status
uv run market-impact event validate examples/events/synthetic-energy-supply-shock.json
uv run market-impact prediction capture --provider polymarket --limit 20
uv run market-impact prediction capture --provider kalshi --limit 20
uv run market-impact prediction validate BUNDLE.json
uv run market-impact agent validate \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json
uv run market-impact agent method-ablation-run \
  --ablation-registration examples/calibration/agent-method-ablation-v1.json \
  --parent-registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --method-catalog examples/research/research-method-catalog-v1.json \
  --provider-profile examples/providers/minimax-m3-research-v1.json \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json \
  --experiment-id YOUR_UNIQUE_METHOD_ABLATION_ID
uv run market-impact agent study-validate \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --source-coverage-registration examples/research/physical-energy-source-coverage-v1.json
uv run market-impact agent study-source-poll \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --source-coverage-registration examples/research/physical-energy-source-coverage-v1.json \
  --ledger LEDGER.sqlite3
uv run market-impact agent study-observe \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --source-coverage-registration examples/research/physical-energy-source-coverage-v1.json \
  --coverage-receipt COVERAGE_RECEIPT.json \
  --observation CANDIDATE_OBSERVATION.json \
  --raw-source RAW_SOURCE_FILE \
  --ledger LEDGER.sqlite3
uv run market-impact agent study-ledger-validate \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --source-coverage-registration examples/research/physical-energy-source-coverage-v1.json \
  --ledger LEDGER.sqlite3
uv run market-impact agent study-freeze-due \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --source-coverage-registration examples/research/physical-energy-source-coverage-v1.json \
  --ledger LEDGER.sqlite3 \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json
uv run market-impact backtest run --request REQUEST.json --data-snapshot BUNDLE_DIRECTORY
uv run market-impact backtest phase2-register --cohort COHORT.json \
  --data-snapshot-root .market-impact/tushare --output PRIVATE_REGISTRATION.json
uv run market-impact backtest phase2-run --registration PRIVATE_REGISTRATION.json \
  --data-snapshot-root .market-impact/tushare --output-dir PRIVATE_OUTPUT_DIRECTORY
uv run market-impact backtest phase2-gate --evidence PRIVATE_EVIDENCE.json
uv run market-impact data validate-prospective-diagnostic \
  --registration examples/research/prospective-diagnostic-registration-v3.json
uv run market-impact data accept-tushare-observation \
  --source-config examples/providers/tushare-observation-index-daily-v1.json \
  --parameters-json '{"ts_code":"000300.SH","start_date":"20260827","end_date":"20260827"}' \
  --window-start 2026-08-28T11:17:00Z \
  --poll-interval-seconds 300 \
  --maximum-gap-seconds 1800
uv run market-impact data collection-run-due --maximum-state-bytes 10000000000
uv run market-impact data collection-health
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

The immutable v1 prospective registration remains as the original all-slots-required diagnostic,
and v2 preserves the first partial-observation contract. V3 is current for new checkpoints: it
retains the v2 information boundary but runs two complete pairs before conditionally requiring a
third. Validating it does not itself authorize a model call or paper order.

Token-backed Tushare capture reads `TUSHARE_TOKEN` only from the process environment. Historical
bundles remain under ignored `.market-impact/tushare/`; prospective observations, Journals, route
reports, and raw artifacts remain under ignored `.market-impact/data-inputs/`. See the data-boundary
document for exact semantics and non-claims.

World Monitor capture reads `WORLD_MONITOR_API_KEY` only from the process environment.
Prediction-market responses are stored under the ignored `.market-impact/observations/`
directory by default. Direct public capture needs no credential. See
[docs/OBSERVATION_DATA.md](docs/OBSERVATION_DATA.md) for time semantics, licensing, and
non-claims.

GitHub Actions, when enabled, only repeats these commands on standard public
runners. Local commands remain the source of truth; development and releases do
not depend on GitHub-hosted compute.

## Safety and licensing

- Live trading is disabled by default and not implemented in this bootstrap.
- Secrets, account identifiers, paid news, and licensed market data must never
  be committed.
- Nothing in this project is financial advice or a promise of profitability.
- First-party code is licensed under Apache-2.0. Third-party code and financial, news,
  model, and API data remain subject to their own terms. See [LICENSE](LICENSE).

Read [SECURITY.md](SECURITY.md) and [RISK_DISCLOSURE.md](RISK_DISCLOSURE.md)
before testing any external provider.
