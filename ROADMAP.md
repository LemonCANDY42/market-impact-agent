# Roadmap

Roadmap items are evidence gates, not a feature inventory. A later phase does
not begin because an earlier API exists; its acceptance evidence must exist.

## Phase 0 — Auditable skeleton

- [x] Freeze vocabulary and authority boundaries.
- [x] Publish provider, signal, order, mandate, and policy contracts.
- [x] Add a fail-closed mock execution provider and local verification.
- [ ] Complete independent review and confirm local and remote evidence agree.
- [ ] Tag the accepted bootstrap.

## Phase 1 — Event research vertical slice

- [x] Define a separate read-only Observation Provider contract with source occurrence,
  publication/update, aggregator fetch, strategy availability, retrieval, upstream identity,
  degradation, and content-addressed raw-capture semantics.
- [x] Capture and validate current public Polymarket and Kalshi snapshots through real
  endpoints; add a disabled World Monitor discovery adapter with explicit unavailable-cache
  behavior and no false empty-data claim.
- [x] Add the Provider-neutral Data Input Harness skeleton: content-identified semantic queries,
  fixed Provider/version/source bindings, concurrent reads, typed degradation, cutoff filtering,
  immutable Data Snapshots, persistent complete-snapshot cache, and bound read-only Agent tools.
  This is framework acceptance with fixture Providers, not acceptance of a historical or live
  vendor adapter.
- [x] Add the local prospective receipt plane: content-identified collection policies, append-only
  source receipts and version sightings, cadence/gap qualification, immutable Snapshot freeze,
  Parquet/ZSTD projection, and reuse of the run-authorized frozen Snapshot Agent tool. Keep
  SQLite/CAS authoritative, DuckDB/Polars optional, and distributed infrastructure deferred until
  scale evidence exists.
- [ ] Accept the first A-share prospective source set for official event facts, market/industry
  context, positioning, macro vintages, and effective-dated tradable-universe mappings. Each route
  must pass rights, transport, completeness, timestamp/revision, market-semantics, and deterministic
  replay gates before it can support checkpoint experiments or paper operation.
  - [x] Accept the first official-event route: the CSRC publication endpoint passed the reusable
    seven-gate contract with three prospective actual-receipt observations and deterministic replay
    in `source-route-acceptance-report-0671f5669de1cd78741350d8cb373a5fbd8d4535cb5efafcb1b5a5714a8d7216`.
    The private-research route does not satisfy the remaining market, industry, positioning, macro,
    universe, or historical-PIT gates.
- [ ] Add Harness-owned Attention Watches after the receipt plane is accepted: content-identified
  event/query scope, TTL and budgets, fixed/adaptive cadence, deterministic change/corroboration
  triggers, durable restart state, duplicate suppression, and an idempotent wake-up outbox that
  freezes a new Data Snapshot before starting a fresh Agent run. Watches remain read-only and cannot
  submit paper or live orders.
  - [x] Add the minimum fixed-cadence/new-version slice: immutable Watch Policy and Wake contracts,
    Journal-frozen aggregate baseline, shared SQLite/CAS state, atomic expiring due lease, TTL and
    poll/byte/wake budgets, non-terminal source/collector backoff, cancellation, wake-only cooldown,
    restart-safe seen-version state, complete-Snapshot gating, and a pending/delivered idempotent
    outbox. The bound Collection Policy remains the sole cadence authority. Adaptive cadence,
    corroboration/materiality triggers, an external process supervisor, and fresh Agent-run dispatch
    remain open.

### Prospective decision-input delivery program

Detailed requirements, boundaries, dependencies, and acceptance logic are canonical in
`docs/DATA_PLATFORM_PLAN.md`. These checkboxes record accepted Task status, not code presence.

Stage 0 — resolve high-impact uncertainty:

- [x] `PDI-00` Freeze source, framework, storage, supervision, and multi-Snapshot dispatch
  decisions from official contracts and bounded real probes. Tushare is fully usable as an upstream
  source for the owner's private deployment, while every Agent-visible route remains fail-closed
  until its Harness acceptance passes. Private report
  `pdi00-source-probe-report-dde5120eca6b259116e72ab7d15a8a80352635c6467c3f7f4e7b00733df18864`
  records the secret-free query identities, statuses, fields, row counts, and response hashes: its
  EOD/index, margin, effective industry membership, ETF mapping, news, macro-schedule, and
  analyst-forecast interfaces responded successfully under the current token, while the probed
  real-time minute interfaces require additional entitlement. Keep
  SQLite/CAS/Parquet and a Harness-owned due state, use `launchd` only as a later authorized process
  supervisor, and do not add OpenBB core, APScheduler, Kafka, a lakehouse, or a feature store without
  the documented evolution evidence. The non-production barrier prototype is retained on local
  branch `prototype/pdi-00-snapshot-barrier-20260828` at `e00f3fa`; it is not merged.

Stage 1 — freeze requirements:

- [x] `PDI-01` Freeze the three-checkpoint prospective diagnostic registration before new
  acquisition, model calls, or outcome opening. Registration
  `prospective-diagnostic-registration-fc975cf2ca4280837f64528720b447615de74b445f21fdc9045465c36d9e9dfd`
  fixes first-eligible policy, earnings-expectation, and macro mechanisms; EOD cutoffs; all six
  required capability slots and their route/cadence/freshness minima; target venues/classes;
  horizons; paired arms; three replicates; the USD 20 aggregate ceiling; hidden outcomes; and exact
  stop/go rules. It grants no model, historical-PIT, or execution authority.

Stage 2 — accept capability-complete data slices:

- [ ] `PDI-10` Accept checkpoint-relevant official-event and established-news routes.
  - [x] Accept the CSRC official-event route and the Tushare/Sina prospective aggregator route.
  - [ ] Accept the direct publisher coverage required by each registered checkpoint; aggregator
    receipt cannot substitute for publisher authority.
- [ ] `PDI-11` Accept the registered A-share market/index/ETF context route.
  - [x] Accept route-level Tushare `index_daily`, `fund_daily`, and `trade_cal` captures and replay.
  - [x] Bind the three price/calendar record kinds into a content-identified checkpoint market-
    universe view without converting index or adjusted research prices into executable prices.
  - [ ] Prove registered breadth/volatility/liquidity, sequence completeness, and corporate-action
    semantics at the future checkpoint barrier.
- [ ] `PDI-12` Accept an effective-dated tradable instrument and universe route.
  - [x] Accept route-level `etf_basic`, `stock_basic`, and `stk_limit` captures and replay.
  - [x] Bind content-identified SSE/SZSE lot/tick rules effective from 2026-07-06 into the checkpoint
    market-universe view, scoped to registered venues/classes and explicit exchange-rule exceptions.
  - [x] Reject the currently available false substitutes: `suspend_d` is stock-only, the purchased
    account lacks `rt_etf_k`, a recent bar proves activity rather than future order acceptance, and
    positive exchange suspension notices do not prove a complete two-venue absence/status view.
  - [ ] Add decision-time suspension/status evidence; listing lifecycle plus a daily bar is research-
    eligible but cannot prove the instrument is presently tradable.
- [ ] `PDI-13` Accept an effective-dated industry taxonomy, membership, and exposure route.
  - [x] Accept route-level SW2021 `index_classify` and `index_member_all` captures and replay.
  - [x] Implement an exact-code, source-identity-preserving join from current-as-received taxonomy,
    effective membership, and ETF `index_code` observations; an absent join remains an explicit gap.
  - [x] Accept `etf_sh_cons` and `etf_sz_cons` exchange-PCF routes and a non-empty exact-code join from
    ETF → PCF constituent → current effective SW member → SW2021 taxonomy. The real 2026-08-29
    probes matched 211/300 SSE and 98/99 SZSE PCF rows without using names.
  - [ ] Prove the taxonomy effective interval and rebalance/classification-revision lineage; the
    observed current relation cannot be back-applied as a historical mapping.
- [ ] `PDI-14` Accept the registered positioning route.
  - [x] Accept the route-level Tushare exchange-margin capture and replay.
  - [ ] Freeze units, publication cadence, revision behavior, and checkpoint-aligned no-data rules.
- [ ] `PDI-15` Accept the registered macro release-and-revision route.
  - [x] Accept the Tushare schedule-observation route and freeze the direct NBS
    calendar→RSS→article/XLSX prospective design.
  - [ ] Implement and accept the direct original-release route; keep revision lineage blocked until
    an official correction/revision relationship can be established.
- [ ] `PDI-16` Accept the registered prior-expectation route.
  - [x] Accept the route-level Tushare `report_rc` forecast-observation capture and replay.
  - [ ] Freeze and verify the registered as-of population, source-diversity, unit, and consensus
    derivation contract at the checkpoint barrier.
- [ ] `PDI-17` Freeze and reconcile the complete multi-Snapshot checkpoint input sets.
  - [x] Implement the non-authoritative reconciliation contract and capability-specific read-only
    tools: accepted routes, Collection Policies, Journal-frozen Snapshots, raw hashes, a shared
    barrier, freshness/coverage, and `FrozenDataSnapshotInput` must reconcile. Tool manifest v2
    projects each bound Source Observation into a content-identified Provider-neutral Checkpoint
    Decision Input while preserving source/time/authority identity, price basis, and fail-closed
    completeness gaps. The evidence gate remains open until the registered future checkpoint
    receipts exist.

Stage 3 — operate continuous collection:

- [x] `PDI-20` Pass one supervised CSRC-plus-market collection tracer bullet. Durable Jobs,
  opportunities, leases, jitter/backoff, misfires, cancellation, health, concurrent-worker safety,
  and staged-Snapshot recovery are covered by deterministic tests. A real isolated CSRC plus
  Tushare `index_daily` run captured 2 plus 20 observations with no miss/failure and passed all six
  gates in report
  `prospective-collection-tracer-report-4859c478c9d778bf912f038cc37a9d068db23e3cbe2098e0dac3663c73b59454`.
  This is repository/runtime acceptance only; no host supervisor is installed.
- [x] `PDI-21` Install and accept the authorized host process supervisor.
  - [x] Freeze a content-identified, secret-free launchd pre-install plan and a private
    environment-file boundary, then install and explicitly enable the authorized v3 plan on the
    registered host.
  - [x] Add a fail-closed clean-process environment and a content-identified supervisor acceptance
    receipt binding the plan, source commit, runtime evidence, service definition, and machine
    registry.
  - [x] Accept private receipt
    `prospective-supervisor-receipt-84aaffced904893571f51a7a680c453ee4bc71c2718ba8589f06e68e061a2e70`.
    It records 16 successful one-shot runs, lifecycle/reload and failure-recovery tests, zero secret
    matches across 5,246 scanned files, complete rollback/reinstallation, and no trading authority.
    A full Mac reboot was intentionally not performed; the tested lifecycle was launchd
    bootout/bootstrap/reload.
- [ ] `PDI-22` Pass multi-policy health, retention, compression, backup, and restore acceptance.
  - [x] Add content-identified operations thresholds, state metrics and disk-budget enforcement,
    SQLite online backup, CAS/Parquet hash inventory, corruption rejection, and clean-root restore.
  - [x] Back up and clean-root restore the current 105 MiB host state, including the PDI-21 receipt:
    5,133 manifested files passed hash, SQLite, foreign-key, identity, and row-count verification in
    `prospective-backup-manifest-b9eb5131246d9a7cdc5ed1875faa11dee2789c14f13b9065d44d5c07e5e725db`.
  - [x] Start bounded pre-registration accrual with active CSRC and Tushare `index_daily` Jobs. The
    first two CSRC opportunities produced complete two-observation actual-receipt Snapshots with no
    miss or failure; the repeated unchanged capture added sightings without duplicating content
    versions. The 18:00 Asia/Shanghai Tushare opportunity remains scheduled. A second backup retains
    both Jobs and the first successful opportunity in
    `prospective-backup-manifest-172375b9a0b045f0300dc26e323e15fac48a5d6b0ac5eeee5bafec3b63395e14`.
    Its backup verification passed; an unpersisted clean-root restore reproduced the state but is
    runtime evidence only. These finite-window Jobs are not the registered PDI-22 soak and do not
    prove an indefinite rolling-date contract.
  - [ ] Complete PDI-17 checkpoint Snapshot sets, bind the final registration to them and the
    accepted supervisor receipt, then run the registered multi-policy soak and fault matrix.

Stage 4 — prove complete Judgment inputs before automatic dispatch:

- [ ] `PDI-30` Assemble one complete prospective Event Envelope/Evidence Pack and frozen tool set.
- [ ] `PDI-31` Pass Query Gate preflight for two or three registered checkpoints.
- [ ] `PDI-32` Run three paired replicates per checkpoint under the frozen stop and cost rules.

Stage 5 — automate bounded follow-up and open registered outcomes:

- [ ] `PDI-40` Admit bounded Agent-proposed Watches without arbitrary network or execution access.
- [ ] `PDI-41` Dispatch a claimed Wake idempotently into one fresh bounded Judgment Run.
- [ ] `PDI-42` Open outcomes after the registered horizon and issue the next-research go/no-go.

### Remaining Phase 1 research work

- [ ] Extend source-specific historical publication/vintage and revision adapters plus frozen
  latency models calibrated from prospective real-time receipts. The CSRC HTML/archive path proves
  one official source class; current snapshots and unadapted sources remain non-authoritative.
- [x] Build immutable Evidence Item and Event Envelope materialization with occurrence,
  publication, visibility, retrieval, revision, and duplicate-claim semantics.
- Implement event-archetype, transmission-channel, directness, revelation-mode, and
  lifecycle contracts without a universal topic ontology.
- Implement fast/deep/combined routing with source-tier and depth/branch caps.
- Produce `event_transmission.json` for one synthetic and one real energy
  supply-shock event.
- Add negative cases: future visibility, missing evidence, contradiction,
  unrelated target, excessive depth, and duplicate reporting.

Exit gate: every path is evidence-linked and independently inspectable; no
broker or backtest mutation is reachable from the research skill.

## Phase 2 — Historical replay and calibration

Status: blocked by a valid negative result. Deterministic replay works, but the first
pre-registered real calibration cohort failed its exit gate. Phase 3 strategy promotion and
Agent-directed paper dispatch remain closed; the opened cohort cannot be retuned and relabeled
as unseen evidence. Provider-neutral mock execution infrastructure may proceed independently,
but it grants neither strategy admission nor paper-provider authority.

- [x] Compare stable Nautilus `1.231.0` with `2.0.0rc3` on Python 3.13/3.14;
  select `1.231.0` as the first implementation candidate and keep the RC comparison-only.
- [x] Define the narrow engine-neutral Backtest Request, Run Manifest, Result, and bridge
  protocol without importing Nautilus types.
- [x] Implement the `NautilusBacktestBridge` against pinned optional dependency `1.231.0`
  and pass the first deterministic synthetic A-share replay twice with identical results.
- [x] Add a disabled Tushare HTTP contract adapter and deterministic pre-event A-share
  universe builder.
- [x] Add private, content-addressed local Parquet bundles whose validated ID can bind a
  Backtest Request without committing licensed data.
- [x] Pass the first token-backed Tushare acceptance and retain its licensed Data Snapshot
  privately and locally; keep the Provider disabled/unverified and make no replay claim.
- [x] Implement the strict validated-bundle to modeled-open Nautilus gate, request/result
  codecs, input identity binding, and generated-bundle acceptance without licensed fixtures.
- [x] Record the local repeated-result acceptance for the named private Tushare bundle:
  two token-free 1/3/10-session runs on 2026-08-25 produced result identity
  `a974181a4e65ec91e6203876647c52211be00f234be5ec6e10df602e8a75a726`;
  licensed observations and metrics remain private and ignored.
- [x] Record the data granularity, book type, fill model, fee model, venue rules, engine
  version, adapter version, and configuration in every replay manifest.
- [x] Execute every horizon in a fresh Nautilus engine, normalize cost-aware `net_return`,
  and implement the versioned Phase 2 gate with generated pass/fail cohorts.
- [x] Apply the gate to the current real repeated evidence and record its expected rejection:
  one manual event cannot clear cohort, baseline, positive-return, or dominance requirements.
- [x] Add v2 Calibration Cell and Variant Decision registration so long-only rules may
  honestly buy or abstain without fabricated signals or Results.
- [x] Compare event reasoning with sentiment, momentum, fixed mapping, and simple hold
  baselines over two training and five later test Event Clusters.
- [x] Capture seven source-hardened private snapshots and execute all 25 registered buys
  twice with source adjustment factors, source price limits, T+1, and modeled costs.
- [x] Apply the frozen v2 gate and record its single rejection reason:
  `candidate_net_return_not_positive`.
- [x] Freeze a materially new prospective Agent study before future outcomes: first-eligible
  physical-shock accrual, pre-outcome upstream Exposure Registry, five independent Judgment
  replicates, five baselines, all-event missingness, and a 40% dominance bound.
- [x] Implement the five-isolated-run orchestrator, pre-replicate execution binding,
  content-identified three-of-five Ensemble Decision, invalid/reuse/mismatch abstention, and
  deterministic Ensemble Decision-to-Nautilus request gate.
- [x] Pass a real MiniMax M3 synthetic-bundle normal run: five of five completed under one
  binding and three selected `600938.XSHG/up/1 session`. Retain the earlier failed/abstaining
  run as negative evidence; do not claim a market replay because no matching snapshot exists.
- [x] Freeze persona-free general Research Method Skills, deterministic routing, one
  Model Provider Profile/Factory, success/failure Usage Ledger, hard per-run estimated-cost
  cap, and a four-arm same-input ablation runner. Treat synthetic comparison as process
  evidence only; do not use it to reopen an outcome-frozen cohort.
- [x] Add the first evidence-gated public-investor method catalog: owner value, second-level cycle
  context, expectations/base rates, reflexive feedback, and narrative diffusion. Freeze a
  three-pair Luna xhigh diagnostic with CPA Usage Keeper pricing and a hard $10 aggregate cap.
  The opened Abqaiq recovery result completed 6/6 Agent runs with full evidence/Pattern coverage;
  both arms abstained 3/3, so retain the method as optional process evidence rather than claiming
  incremental quality. Correct the original six-`model_call_count` label to six Agent runs and
  retain the observed 12 Provider requests in a schema-validated, content-identified correction
  backed by a redacted CPA event artifact. Replace caller-reported evidence labels with a
  content-identified declaration binding each method-evidence type to exact Evidence/Pattern refs.
- [x] Freeze a separate method-quality protocol with historical identity masking, strict Source
  Version Receipt and evaluation artifact contracts, train-only outcome memory, eight development
  and 24 holdout case targets, content-bound general/family Skill routes, deterministic directional
  research-score and paired-estimator rules, repeated-run noise, cost proxies, and registered future
  promotion gates. Validate the first
  synthetic contract-only Historical Evidence Manifest and separate masked Agent input; do not
  claim source-authenticated no-lookahead or that the corpus exists yet. Style attribution remains
  deferred and is not a promotion metric.
- [x] Retire the first method-quality statistical specification before any outcome opening after
  review identified case-replicate pseudoreplication. Freeze v2 with Event Case as the independent
  unit, five runs as within-case noise measurements, one primary promotion contrast, diagnostic-only
  secondary contrasts, and an executable clustered paired estimator. Preserve v1 as negative audit
  evidence; it can never be used for a claim.
- [x] Implement the first immutable archive-capture authority adapter. The Common Crawl path binds
  collection, target, capture time, object path, byte range, status, and payload digest; it verifies
  the exact gzip/WARC member, target/status, payload digest, optional block digest, and truncation.
  A complete official record passes the live transport path and a truncated record is rejected for
  archive-capture acceptance. This is archive authentication, not publisher-time authentication.
- [x] Build the first real outcome-opened method-development case. The 2019 Abqaiq–Khurais attack
  and recovery are two strongly masked information states of one Event Case. Exact event
  fingerprints were replaced with coarse mechanism categories; residual memorization/linkage risk
  remains, so this is not an authenticated holdout.
- [x] Rerun all four arms times five MiniMax M3 replicates for both strongly masked states and open
  outcomes only after both reports and both Backtest Requests pass joint preflight. All 40 runs
  completed, every ensemble abstained, and both one-session replays were deterministic. The
  fixed-long control was net negative in both states. This accepts the implementation diagnostic,
  not a method ranking or alpha claim; artifacts from earlier case identities remain invalid.
- [x] Deploy pinned TradingAgents `0.3.1` outside the Harness. Preserve its native roles, prompts,
  debate, risk graph, and model prior knowledge while binding retrieved news and market data to the
  real Abqaiq event/target and each historical cutoff. Disable only outcome reflection, cross-run
  memory, post-cutoff/live data, and broker reachability. The earlier masked MiniMax smoke returned
  `Hold`; the first real-identity CLIProxyAPI Luna attack run also returned `Hold` with 19 model
  calls and no structured-output degradation. These remain distinct experiments. This is an
  external baseline, not Harness authority or execution.
- [x] Add a second content-identified model Provider Profile and adapter for the exact local
  CLIProxyAPI loopback origin, dedicated project credential, `gpt-5.6-luna`, and `xhigh`. Text,
  function-tool, model-availability, identity, origin, environment-proxy bypass, and existing
  MiniMax regression checks pass.
  Included Codex OAuth usage records Token counts but has no asserted USD/token price.
- [x] Complete the native TradingAgents five-replicate comparison for attack and recovery on Luna
  xhigh. All 10 runs completed with zero structured-output degradation: seven `Hold`, two
  `Underweight`, and one `Sell`, mapping to 10 abstentions in the Harness's one-sided long action
  space. The native graph used 174 model calls, 903,651 input and 376,799 output Tokens, and
  7,531.109 cumulative seconds. This accepts an external behavior/stability/resource baseline,
  not a method-quality result from one opened Event Case.
- [x] Implement the Provider-neutral historical news batch contract: exact ordered source chain,
  typed data/no-data/not-configured/rate-limit/error outcomes, UTC half-open filtering before
  limits, no host-clock treatment of undated records, version-lineage deduplication, reconciled
  rejection counts, and canonical/schema validation. Add a read-only `news-evidence-assessment`
  Skill that describes sample independence and disagreement but mints no Evidence or signal weight.
- [x] Run the content-bound Luna xhigh paired development diagnostic for `general_methods` versus
  the same route plus `news-evidence-assessment`. All 20 attack/recovery runs completed and both
  arms abstained in every replicate. The Skill added 12.2% input Tokens and 5.7% output Tokens but
  did not create a visible decision or news-quality improvement on this sparse three/five-item
  case. Keep it opt-in for genuinely multi-source news batches; this is not a negative universal
  result or a method-quality claim.
- [ ] Implement and accept source-specific established-news publication-time extraction plus a
  frozen latency calibration and build the first complete historical case. The accepted CSRC
  official-page extractor does not satisfy this news gate. Until then v2 admits no retrospective
  holdout, even when archive capture and internal receipt hashes are valid.
- [ ] Build the remaining seven opened development cases across positive, offsetting, missing,
  ambiguous, revision, and cross-mechanism paths. Separately, after publisher-time authority exists,
  freeze all 24 outcome-independent masked historical holdout cases with authenticated evidence and matching
  market snapshots and seals, and implement the overall promotion evaluator. That future evaluator
  must combine time, abstention, baselines, strata, concentration, drawdown, CVaR, and cost gates;
  the clustered paired interval alone is not an overall promotion decision; the implemented
  estimator must remain unusable for a claim until content-identified case and pair bindings exist
  in the pre-run seals/openings.
- [x] Implement the private append-only Accrual Ledger with actual-receipt source identity,
  deterministic admission/non-admission, revision lineage, first-eligible separation,
  cohort limits, idempotency, and tamper detection.
- [x] Freeze the first Source Coverage Registration; implement private exact-response
  capture, mandatory-source failure receipts, direct ENTSOG gas revision normalization, and
  idempotent T0+60 Evidence Pack freezing with no broker reachability.
- [ ] Extend registered direct confirmation beyond European gas to oil and non-European
  infrastructure, run prospective coverage/latency acceptance, and admit the first five
  qualifying future events without replacement or outcome-based selection.
- Pass the frozen real-data gate without a single event dominating the outcome.

Exit gate: reproducible results beat at least one meaningful baseline without a
single event dominating the outcome. This gate verifies backtesting only; it grants no
paper or live capability. Failure stops expansion.

## Phase 3 — Event-family discovery

Blocked until a new Phase 2 hypothesis passes on a later unseen holdout. The separate
engine-neutral runtime prerequisite in `docs/AGENT_RUNTIME.md` has a deterministic hardened
runtime covering compaction, on-demand Skills, MCP lifecycle, permissions, recovery,
observability, and injection/secret negative cases while exposing no broker or account
capability. A fresh MiniMax M3 China-endpoint run passed the same hardened surface. This work
does not establish model quality or reopen the failed trading-calibration gate.

Research-only market-state case registration, private source qualification, and descriptive
benchmark/sector diagnostics may proceed without promotion. They must remain outside Agent-visible
inputs and accepted Method Quality scoring until this gate opens.

- Add market-state/style rotation and probabilistic climate/agriculture cases.
- [x] Freeze the first 15-case A-share Regime Study Registration. It assigns candidate method
  Skills by case, requires official/macro/positioning/market/industry evidence and at least eight
  established-news items from two publishers per checkpoint, and keeps Bloomberg/Reuters
  entitlement routes distinct from GDELT/Common Crawl discovery. The readiness audit correctly
  rejects all current retrospective cases for Agent effectiveness because authenticated historical
  availability is not yet established.
- [x] Run the first no-model long-horizon comparator over the private five-index/31-industry panel.
  All 15 cases were covered; 12 support annualized risk statistics. Cash, primary-index,
  equal-sector, and lagged monthly top-three momentum now report costs, turnover, drawdown, CVaR,
  Sharpe, information ratio, and upside/downside participation. These opened, selected windows and
  non-executable industry indices are difficulty diagnostics only.
- [x] Implement the content-identified Regime Evidence Manifest and per-checkpoint qualification
  report, including frozen 09:25 Asia/Shanghai cutoffs, category freshness windows, revision
  lineage, publisher diversity, and distinct actual-receipt/source-reported/modeled-latency bases.
  Add fixed-collection Common Crawl lookup, digest-verified Internet Archive replay, and
  source-specific CSRC, State Council, and NBS HTML extractors. Add the semantic event-revelation
  gate and a CSRC live-transcript segment extractor: old official background cannot qualify the
  first event checkpoint. Later captures are never backfilled into earlier checkpoints.
- [x] Audit and invalidate the attempted six-case Agent validation. All 108 Luna xhigh calls
  completed structurally, but the qualification gate checked authority kind without requiring
  `authority_at <= cutoff`. Correct replay qualifies 0/18 selected checkpoints, so the runs are
  descriptive invalid/superseded diagnostics, not formal experiments. They show both arms always
  abstaining and missing positive broad/sector baselines, but cannot support a pipeline or Skill
  claim. Attempted calls cost $1.028187. Aggregate lineage and costs bind exact
  Provider/panel/Manifest/qualification IDs and reconcile from case reports rather than caller
  assertions. The later Usage Ledger Union below supersedes the incomplete $2.436518 all-diagnostic
  total that was recorded at this point. No paper/live authority follows.
- [x] Add exact-URL publisher archive recovery without weakening the PIT gate. The audit separates
  found-but-unverified captures, genuine no-capture responses, and source errors; materialization
  requires digest replay plus publisher publication/update and cutoff checks before minting a
  `verified_archive` record. The first 2021-sector audit retained 32 archive-index failures as
  `source_error`, proving failure semantics only. A later full-access six-case audit completed 230
  lookups with 162 found, 68 not found, and zero source errors; 115 of 120 unique candidates replayed
  successfully. The replacement Manifest preserves two real revision chains. Strict requalification
  raises established-news readiness to 2/18 frozen validation checkpoints but leaves the complete
  gate at 0/18. Licensed market/industry/positioning versions, missing macro authorities, 16 news
  windows, and the first 2024 policy event revelation remain open.
- [x] Split historical evidence into strict PIT, opened-outcome Modeled-PIT, and prospective actual
  receipt without creating another evidence-record or orchestration authority. The frozen Modeled-
  PIT policy applies prior-session price snapshots or source availability plus content-identified
  safety delays, reports every historical authority gap, and cannot enter strict qualification,
  inference, broker, paper, or live paths. Prospective receipts map into the same evidence record
  with `available_at == authority_at == retrieved_at` and never backdate historical cases.
- [x] Run the frozen six-case, 18-checkpoint Modeled-PIT process diagnostic after recovering article
  bodies. All 108 Luna xhigh calls completed; both arms abstained at every checkpoint and routed
  Skills changed 0/18 decisions. Thirteen checkpoints had every exact registered news payload and
  five had six of eight, so missing article bodies are not the sole active blocker. Across runs,
  horizon persistence was unresolved in 108/108, event identity or attribution in 106/108, and
  expectation delta in 105/108. The content-identified report is
  `regime-modeled-pit-agent-validation-report-317f79ea1602e7d381eba01f9522123116033bdbbc179180dfa71f46f895f380`.
  The aggregate rehashes each paired registration/report and recomputes the frozen-input/horizon
  binding, reconstructs both arm execution bindings, and matches all 36 hashes to their artifacts
  and local Usage Ledgers. All 18 formal checkpoints match, while the earlier one-session default
  run remains an excluded invalid diagnostic. All 108 run summaries, decisions, metrics, and
  coverage rows are also rebuilt from terminal artifacts and Run Journals before aggregation;
  terminal replay reparses the final assistant payload and matches proposal, raw response,
  transcript, and metrics to the hash-chained validation event.
  Its Usage Ledger Union covers 70 ledgers and 528 unique runs with zero duplicates or conflicts:
  $3.883472 preexisting, $0.053516 invalid-horizon diagnostic, and $0.991336 formal Modeled-PIT,
  totaling $4.928324 under the shared $20 cap. This corrects the earlier incomplete total.
- [x] Add the first concrete Data Input Provider for prospective RSS/Atom receipts. The route binds
  a secret-free Source Route Configuration hash, rejects redirect/configuration drift, retains the
  exact response and selected XML item bytes, and exposes a Snapshot-bound read-only tool primitive.
  A real Federal Reserve press-feed capture completed with one accepted actual-receipt observation.
  Bloomberg feed URLs remain discovery-only pending license review and provide no historical PIT.
  No Agent experiment or execution capability follows from this connector acceptance.
- [ ] Register the next small process diagnostic around the observed event fact, cited prior
  expectation, falsifiable transmission path, mechanism-appropriate horizon set, and executable
  index/ETF target universe. Acquire each input through a fixed Data Query and expose only its
  frozen Data Snapshot to the Agent. Keep future outcomes hidden. Run only two or three
  representative checkpoints first; stop if the same blockers remain before spending across all
  18 again.
- [ ] Complete source-specific historical capture for exchange official material, additional macro
  vintages, positioning, filings, Bloomberg, and Reuters. Add a defensible original-vintage
  or explicitly bounded price-history treatment. Bind every checkpoint to accepted source versions
  and latency authority before any masked method-Skill model run. Qualify additional quiet,
  slow-trend, revision, and black-swan cases without outcome-based replacement. Keep three paired
  replicates and the shared $20 CPA cap; do not spend model budget on a case whose source or
  event-revelation gate fails, and do not rerun the unchanged always-abstain input contract.
- Promote the prospective physical-energy family only if its frozen Agent Phase 2 holdout
  passes; the registration and committed synthetic slice are not acceptance evidence.
- Research broader single-event and cumulative-narrative families, including
  CPO/AI infrastructure, policy themes, scheduled surprises, and uncertainty
  resolution.
- Promote only repeatable, falsifiable families to reference packs.

Exit gate: event families have pre-registered universes, analogues, negative
cases, and regime tags. Examples alone are not taxonomy evidence.

## Phase 4 — Paper execution

- [x] Add the provider-neutral durable intent/approval/outbox contract slice. Exact Order Intent,
  Trading Mandate, Price Basis, hard-policy, and approval hashes survive restart; atomic leases,
  ambiguous-submit `unknown`, provider acknowledgement without fill inference, mock-provider
  durability, global reconciliation blocking, duplicate identity rejection, and complete-snapshot
  recovery pass locally. This is mock contract acceptance only: it exposes no Agent execution tool,
  account capability, IBKR route, or paper admission.
- Add an independently registered `ibkr-nautilus-paper` Provider over the pinned Nautilus
  engine and official IB adapter; create a direct IBKR Provider only if that path cannot
  pass lifecycle and reconciliation acceptance.
- Add CLI, MCP approval tools, generic webhook, and macOS notifications.
- Pass crash/restart/reconciliation and duplicate-order acceptance.

Exit gate: the prospective Query/strategy gates admit Agent-directed paper operation and paper
state reconciles with IBKR after every injected failure. Mock contract acceptance does not satisfy
this gate.

IBKR Paper acceptance covers Harness-to-Nautilus-to-IB order identity, ambiguous submit
outcomes, partial and duplicate fills, disconnects, gateway and process restart, external
orders, and complete order/fill/position/account reconciliation. VeighNa remains a sibling
Provider bridge, not a NautilusTrader plugin, and must pass a separate acceptance program
on a gateway-supported host before any A-share execution claim is made.

## Phase 5 — Controlled live research

- Add expiring Trading Mandates, hard portfolio limits, notification escalation,
  and a tested kill switch.
- Run `manual_each`, then timeboxed/policy-auto modes with deliberately tiny risk.
- Keep VeighNa A-share live as a separate vendor/host acceptance program.

Exit gate: explicit user authorization plus independent operational review.
There is no scheduled date and no implied entitlement to advance.
