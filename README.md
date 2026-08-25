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
- engine-neutral Backtest Request, Simulation Specification, Run Manifest, and Result;
- a pinned optional NautilusTrader `1.231.0` bridge with one deterministic synthetic
  A-share replay covering a suspension, opening limit lock, lot size, fees, and slippage;
- a disabled, unverified Tushare HTTPS adapter for bounded stock-listing, exchange-calendar,
  and unadjusted daily-bar reads plus deterministic pre-event A-share universe construction;
- a provider capability manifest and registry;
- a deterministic hard-policy evaluator;
- a hard-policy-gated paper execution gateway and idempotent mock provider;
- JSON Schemas for event, signal, order, and provider artifacts;
- machine-readable status, provider-manifest validation, and event-chronology commands.

Planned integrations are documented but **not claimed as working**:

- a separately validated Nautilus-to-IBKR paper Provider;
- token-backed Tushare data acceptance and locally retained historical snapshots;
- an external-process VeighNa bridge for future A-share gateways;
- MCP, HTTP/gRPC, and native Python provider transports.

NautilusTrader is the selected default engine foundation and behavioral reference. The
Harness uses it through an engine-neutral backtest bridge; later execution Providers still
pass through the same policy, approval, and audit boundary. Version `1.231.0` is an
optional, exact dependency and grants backtest capability only.

The Tushare adapter has deterministic contract tests but no token-backed success evidence
on this machine. It remains disabled and unverified, and its current listing data cannot by
itself prove a revision-free historical universe. See
[docs/TUSHARE_DATA.md](docs/TUSHARE_DATA.md).

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
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

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
