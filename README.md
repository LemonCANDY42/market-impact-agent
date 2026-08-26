# Market Impact Agent

Market Impact Agent is an auditable, event-driven trading agent harness. It is
designed to turn point-in-time evidence into layered market-impact reasoning,
versioned signal intents, and policy-gated actions executed by replaceable
backtest engines and paper-trading or broker providers.

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
- immutable point-in-time Evidence Items, Event Envelopes, deterministic
  fast/deep/combined routing, and evidence-linked Transmission Paths;
- a separate read-only Observation Provider contract plus current public Polymarket and
  Kalshi snapshot adapters, an authenticated World Monitor discovery adapter, and private
  content-addressed raw/normalized JSON bundles with explicit occurrence, publication,
  update, availability, aggregator-fetch, and retrieval semantics;
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
- a narrowly versioned `600028.XSHG` modeled-open adapter and token-free backtest CLI that
  consume a validated private bundle in memory, with repeated-result local acceptance and
  no derived licensed fixture committed;
- independent 1/3/10-session Nautilus runs from one Backtest Request, normalized cost-aware
  `net_return`, and fail-closed v1/v2 Phase 2 calibration gates over pre-registered repeated
  event/baseline Results and honest long-only abstentions;
- a bounded local Agent Harness with content-addressed Evidence/Pattern/Judgment contracts,
  append-only crash-safe runs, automatic context compaction, on-demand hashed Skills, typed
  permissioned tools, official MCP lifecycle handling, budgets, cancellation, redaction,
  closed-output correction, and a pinned MiniMax M3 China-endpoint Provider;
- a synthetic physical-energy supply-shock vertical slice whose frozen Agent judgment can be
  deterministically admitted into the existing Signal Intent and Backtest Request and then
  replayed by the unchanged Nautilus bridge without re-running the model;
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
- a second model Provider adapter; the accepted MiniMax v4 evidence covers one Provider only
  and does not establish runtime portability.

NautilusTrader is the selected default engine foundation and behavioral reference. The
Harness uses it through an engine-neutral backtest bridge; later execution Providers still
pass through the same policy, approval, and audit boundary. Version `1.231.0` is an
optional, exact dependency and grants backtest capability only.

The first official token-backed local Tushare capture and validation completed on 2026-08-25
for `600028.SH`, using 2019-09-18 as-of metadata and a 2019-09-19..2019-10-10 daily window.
It proves the language-neutral HTTPS adapter/capture/validation path and that the local
credential worked for that account, target, and window. It does not prove general
quota/permissions, completeness, historical truth, full-universe prices, actual liquidity or
fillability, alpha, or paper/live readiness. The Tushare Provider therefore remains disabled
and unverified. The modeled-open replay implementation is a separate deterministic
simulation gate, not a Provider or source-truth claim. See
[docs/TUSHARE_DATA.md](docs/TUSHARE_DATA.md).

The first pre-registered real cohort completed on 2026-08-26: two train and five later test
Event Clusters, 35 registered variant decisions, and two deterministic runs for each of 25
buys. The v2 gate rejected it with exactly `candidate_net_return_not_positive`. This is a
valid negative research result; the opened cohort cannot be retuned and reused as unseen
evidence. No baseline-superiority, alpha, Phase 3, paper, or live claim has passed. See
[docs/PHASE2_CALIBRATION.md](docs/PHASE2_CALIBRATION.md).

The bounded research-only Agent runtime and its exact non-claims are recorded in
[docs/AGENT_RUNTIME.md](docs/AGENT_RUNTIME.md). Deterministic hardening, base-wheel isolation,
and the private MiniMax M3 v4 run pass the current local runtime gate. This proves one pinned
Provider and the auditable Agent-to-frozen-judgment pipeline, not model quality, event-family
calibration, alpha, Provider portability, or execution readiness. None of it overrides the
failed trading-calibration result.

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
Evidence Pack + pre-cutoff Pattern Pack -> Agent Harness -> sealed Judgment Artifact
       -> deterministic admission -> SignalIntent
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
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Token-backed Tushare capture reads `TUSHARE_TOKEN` only from the process environment and
writes licensed observations under the ignored `.market-impact/tushare/` directory. See the
data-boundary document for the exact command and non-claims.

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
