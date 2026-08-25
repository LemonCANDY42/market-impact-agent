# Architecture

## Ownership

The harness is the sole orchestration and approval authority. Research agents
produce artifacts; deterministic policy establishes eligibility; semantic
approval operates only inside the hard envelope; providers own external
capabilities and observed execution facts.

```text
Source adapters
    -> Evidence Items -> Event Envelope
    -> fast/deep router -> Event Assessment
    -> Signal Intent
         -> Backtest Request -> engine-neutral backtest port
              -> Nautilus backtest bridge -> deterministic replay and results
         -> Order Intent -> Hard Policy: DENY | REQUIRE_MANUAL | ELIGIBLE
              -> Semantic Auto Approver (optional; cannot override hard policy)
              -> Approval Decision -> durable outbox
              -> paper execution gateway -> sealed submission capability
              -> Provider Registry -> execution provider
              <- Execution Events <- private stream + bounded polling
              <- Reconciliation <- order/fill/position/account truth
```

## Provider boundary

Providers may be native Python components or external MCP, HTTP, or gRPC
services. Transport is not trust. Every provider supplies a versioned manifest
that separates declared from verified capabilities.

Live execution is invisible to agents until conformance proves:

- deterministic local validation;
- stable client and venue order identities;
- asynchronous order/fill state with deduplication;
- bounded query fallback when streams miss events;
- startup and continuous reconciliation;
- explicit `UNKNOWN` outcomes after ambiguous transport failures;
- paper/live account binding and a kill switch.

MCP is the preferred agent-facing control surface, not the execution ledger.
One-shot tool success never means broker acceptance or fill.

The bootstrap exposes one composed `PaperExecutionGateway`. It re-evaluates hard
policy and provider capability before issuing the sealed input accepted by an
execution provider. Providers do not accept a raw `OrderIntent`, and no
equivalent live gateway exists.

## Default engine foundation

NautilusTrader is the selected default foundational trading and backtest engine and the
behavioral reference for engine integration. Its common event-driven kernel, typed
commands and events, clocks, cache, portfolio, risk, simulation, and execution lifecycle
provide the first concrete path through which the Harness will replay frozen research.

The Harness still owns Evidence Items, Event Assessments, Signal and Order Intents,
Trading Mandates, approval, outbox identity, and normalized audit history. Nautilus owns
engine-local strategy, clock, cache, portfolio, risk, simulated venue, and execution state
for one configured run or node. A broker remains the external truth for orders, fills,
positions, and cash.

The Harness does not import NautilusTrader types into its public contracts. Phase 2 adds a
small engine-neutral backtest request/result port and a `NautilusBacktestBridge`. Later
Provider integrations bind an exact engine, adapter, version, configuration, market, and
environment; using Nautilus never grants capability or conformance by itself.

The 2026-08-25 compatibility spike selected stable NautilusTrader `1.231.0`; the first
synthetic A-share replay then passed repeated-result acceptance and enabled it as an exact
optional dependency. `2.0.0rc3` remains a migration comparison only until a final v2
release passes the same replay acceptance.

```text
Harness domain / policy / canonical artifacts
    -> engine-neutral backtest and execution ports
         -> Nautilus engine foundation (default/reference)
              -> NautilusBacktestBridge                    [Phase 2 accepted first slice]
              -> ibkr-nautilus-paper Provider              [Phase 4]
         -> VeighNa external Provider bridge               [later]
         -> future direct IBKR, LEAN, or other adapters
```

Nautilus reduces the semantic gap between backtest and live by sharing core components
and strategy/command/event models. It does not make data arrival, fills, venue rules, or
recovery behavior identical. A-share T+1, price limits, fees, executable-price timing,
and every paper/live recovery property require explicit Harness acceptance.

## Approval model

Hard policy has three outcomes:

- `DENY`: a non-overridable rule failed;
- `REQUIRE_MANUAL`: information or policy requires human action;
- `ELIGIBLE`: the request may proceed to its configured approval mode.

Approval modes are `disabled`, `manual_each`, `timeboxed`, `policy_auto`, and
`autonomous`. Autonomous means no per-order confirmation; it never means no
hard limits.

## State

The initial state model is intentionally local:

- JSON for evidence, assessment, signal, mandate, approval, and order artifacts;
- Parquet for market and backtest series;
- SQLite for run indexes, provider snapshots, intent outbox, and projected state.

The bootstrap contains schemas and in-memory mock behavior only. Persistence is
implemented when the first replay slice requires it—not before.

## Runtime boundary

First-party code supports Python `>=3.13,<3.15`.

- NautilusTrader is the default engine foundation. Stable `1.231.0` is an exact optional
  dependency for the accepted first replay slice; paper Provider integration remains
  unimplemented and disabled.
- Tushare is accessed through a language-neutral HTTPS adapter; licensed data remains
  local. The adapter is disabled and unverified until token-backed acceptance succeeds.
  A universe reconstructed from current listing metadata stays bound to that retrieval
  snapshot and is not treated as proof against source revision or survivorship bias.
- VeighNa is an external-process bridge. VeighNa 4.4 and current A-share vendor
  gateways do not provide a verified same-process Python 3.14/macOS path.
- LEAN remains a comparison candidate in its own Docker/Python runtime and is
  not a bootstrap dependency.

See [docs/PROVIDER_CONTRACT.md](docs/PROVIDER_CONTRACT.md) for conformance details.
