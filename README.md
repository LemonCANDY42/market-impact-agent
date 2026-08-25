# Market Impact Agent

Market Impact Agent is an auditable, event-driven trading agent harness. It is
designed to turn point-in-time evidence into layered market-impact reasoning,
versioned signal intents, and policy-gated actions executed by replaceable
backtest, paper-trading, or broker providers.

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
- a provider capability manifest and registry;
- a deterministic hard-policy evaluator;
- a hard-policy-gated paper execution gateway and idempotent mock provider;
- JSON Schemas for event, signal, order, and provider artifacts;
- machine-readable status, provider-manifest validation, and event-chronology commands.

Planned integrations are documented but **not claimed as working**:

- NautilusTrader as the default/reference engine for backtesting and IBKR paper trading;
- Tushare HTTP market data;
- an external-process VeighNa bridge for future A-share gateways;
- MCP, HTTP/gRPC, and native Python provider transports.

NautilusTrader is the selected default engine and reference implementation for execution
semantics. It still passes through the same harness adapter, policy, approval, and audit
boundary as every other engine. The bootstrap does not yet depend on or initialize it.

## Architecture at a glance

```text
Evidence -> fast/deep event assessment -> SignalIntent
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
uv sync --python 3.13
uv run market-impact status
uv run market-impact provider validate examples/providers/nautilus-planned.json
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
