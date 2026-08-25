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
- a provider capability manifest and registry;
- a deterministic hard-policy evaluator;
- a hard-policy-gated paper execution gateway and idempotent mock provider;
- JSON Schemas for event, signal, order, and provider artifacts;
- machine-readable status, provider-manifest validation, and event-chronology commands.

Planned integrations are documented but **not claimed as working**:

- a separately validated Nautilus-to-IBKR paper Provider;
- an external-process VeighNa bridge for future A-share gateways;
- MCP, HTTP/gRPC, and native Python provider transports.
- an Agent runtime with durable runs, automatic context compaction, on-demand Skills, MCP
  lifecycle, permissions, recovery, and evaluation; MiniMax M3 on the China endpoint is the
  first planned local model fixture, not an execution Provider.

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

The future Agent runtime boundary is frozen separately in
[docs/AGENT_RUNTIME.md](docs/AGENT_RUNTIME.md). A successful MiniMax response will not by
itself satisfy that gate or override the failed trading-calibration result.

## Architecture at a glance

```text
Evidence -> fast/deep event assessment -> SignalIntent
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

GitHub Actions, when enabled, only repeats these commands on standard public
runners. Local commands remain the source of truth; development and releases do
not depend on GitHub-hosted compute.

## Safety and licensing

- Live trading is disabled by default and not implemented in this bootstrap.
- Secrets, account identifiers, paid news, and licensed market data must never
  be committed.
- Nothing in this project is financial advice or a promise of profitability.
- The project is licensed under AGPL-3.0-or-later. See [LICENSE](LICENSE).

Read [SECURITY.md](SECURITY.md) and [RISK_DISCLOSURE.md](RISK_DISCLOSURE.md)
before testing any external provider.
