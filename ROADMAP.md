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
- [ ] Add source-specific historical publication/vintage and revision adapters plus frozen
  latency models calibrated from prospective real-time receipts. Current snapshots alone are
  not historical point-in-time evidence.
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
pre-registered real calibration cohort failed its exit gate. Phase 3 and paper execution
remain closed; the opened cohort cannot be retuned and relabeled as unseen evidence.

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
- [x] Freeze a separate method-quality protocol with historical identity masking, strict Source
  Version Receipt and evaluation artifact contracts, train-only outcome memory, eight development
  and 24 holdout case targets, content-bound general/family Skill routes, deterministic directional
  research-score and paired-estimator rules, repeated-run noise, cost proxies, and registered future
  promotion gates. Validate the first
  synthetic contract-only Historical Evidence Manifest and separate masked Agent input; do not
  claim source-authenticated no-lookahead or that the corpus exists yet. Style attribution remains
  deferred and is not a promotion metric.
- [ ] Implement and accept an immutable provider/archive authority plus adapter and calibration
  verification. Until then v1 admits no retrospective holdout, even if receipt fields and hashes
  are internally consistent.
- [ ] After that authority exists, build and validate the eight-case development corpus, freeze all
  24 outcome-independent masked historical holdout cases with authenticated evidence and matching
  market snapshots and seals, and implement the overall promotion evaluator. That future evaluator
  must combine time, abstention, baselines, strata, concentration, drawdown, CVaR, and cost gates;
  the future paired interval alone is not an overall promotion decision; paired computation remains
  unavailable and fail-closed until content-identified case and pair bindings exist in the
  pre-run seals/openings.
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

- Add market-state/style rotation and probabilistic climate/agriculture cases.
- Promote the prospective physical-energy family only if its frozen Agent Phase 2 holdout
  passes; the registration and committed synthetic slice are not acceptance evidence.
- Research broader single-event and cumulative-narrative families, including
  CPO/AI infrastructure, policy themes, scheduled surprises, and uncertainty
  resolution.
- Promote only repeatable, falsifiable families to reference packs.

Exit gate: event families have pre-registered universes, analogues, negative
cases, and regime tags. Examples alone are not taxonomy evidence.

## Phase 4 — Paper execution

- Add durable intent/outbox and approval records.
- Add an independently registered `ibkr-nautilus-paper` Provider over the pinned Nautilus
  engine and official IB adapter; create a direct IBKR Provider only if that path cannot
  pass lifecycle and reconciliation acceptance.
- Add CLI, MCP approval tools, generic webhook, and macOS notifications.
- Pass crash/restart/reconciliation and duplicate-order acceptance.

Exit gate: paper state reconciles with IBKR after every injected failure.

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
