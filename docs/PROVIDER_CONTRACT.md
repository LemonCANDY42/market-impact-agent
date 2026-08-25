# Provider Contract

Providers expose external market-data, account, or execution capabilities to the Harness.
A provider may use a native Python adapter, MCP, HTTP, or gRPC. Transport is an
integration choice, not a trust signal. Backtest engines implement the separate
engine-neutral `BacktestBridge` request/result port; doing so does not register a Provider
or grant any market-data, paper, or live capability.

## Registration

Every provider supplies a versioned manifest that declares:

- provider identity and implementation version;
- supported environments and markets;
- declared and independently verified capabilities;
- order types;
- streaming and reconciliation support;
- enabled state and trust tier.

Verification evidence is scoped to the exact capability, market, environment,
implementation version, dependency version, and relevant configuration. Passing a
backtest check never verifies a paper Provider, and a paper check never verifies live.

The JSON contract is defined in
[`schemas/provider-manifest.schema.json`](../schemas/provider-manifest.schema.json).
Unknown, disabled, or unverified providers are never eligible for live execution.

## Execution boundary

Agents and ordinary callers may propose a typed `OrderIntent`; they cannot submit it to a
provider. The composition root binds a trusted Trading Mandate, clock, and reference-price
source into the paper execution gateway; callers supply none of that policy context. The
gateway evaluates the exact intent, checks the provider's enabled and verified paper
capability, and issues a sealed submission capability only when hard policy is `ELIGIBLE`
and no separate approval remains pending. The provider contract accepts that capability
rather than a raw intent. No live gateway exists in the bootstrap.

The provider owns broker-specific translation and execution facts. The harness owns
policy, approval, evidence, and the immutable decision trail.

A production-grade execution provider must preserve the caller's `client_order_id`,
return a stable provider order identifier, stream order-state changes, and reconcile
open orders, fills, positions, and cash after reconnects. Submission must be idempotent.
An MCP tool can be the invocation surface, but it does not replace these requirements.

## Capability trust

`declared_capabilities` describe what an adapter claims. `verified_capabilities` describe
what the harness is allowed to route to after contract tests. The latter must be a subset
of the former.

Trust tiers are deliberately explicit:

| Tier | Meaning |
| --- | --- |
| `unverified` | Manifest exists; no execution claim is trusted. |
| `mock` | Deterministic local test double only. |
| `paper_validated` | Paper behavior and recovery contract were tested. |
| `live_validated` | Live-specific certification was completed separately. |

Paper validation never upgrades a provider to live validation.

## Initial adapters

- `mock-execution` is the only enabled implementation in the bootstrap.
- `NautilusBacktestBridge` is the accepted reference implementation of the engine-neutral
  backtest port for the bounded synthetic XSHG cash-equity fixture. It is not a Provider
  and grants no market-data, paper, live, IBKR, or VeighNa capability.
- `tushare-http` implements a bounded read-only HTTPS contract for SSE/SZSE listing
  metadata, exchange calendars, and unadjusted daily bars. Its manifest remains disabled
  and unverified after the first token-backed local acceptance: one account, target, and
  window do not establish general permission, quota, completeness, or source correctness.
  No licensed response is committed. Successful responses can be materialized only into
  private local Data Snapshot bundles; this does not promote the Provider's capability or
  grant the data source orchestration authority.
- `ibkr-nautilus-paper` is the first planned US/HK paper Provider identity. It binds a
  pinned Nautilus version, the official IB adapter, Harness translation, configuration,
  market, and environment. A direct IBKR Provider remains possible if that stack cannot
  satisfy recovery or reconciliation acceptance. No account is connected.
- VeighNa is represented as a disabled external-process bridge. Its core may be usable on
  Python 3.13, but common A-share gateways depend on vendor runtimes and are not claimed
  to work on macOS or Python 3.14.

## Reference semantics

The Harness owns its safety semantics. They are informed and exercised by
NautilusTrader's public execution model—command/event separation, stable identities,
definitive-versus-unknown outcomes, risk before execution, duplicate-fill protection,
stream and query recovery, external-order classification, and reconciliation—but using
Nautilus does not automatically satisfy them. Public schemas and normalized domain events
never expose NautilusTrader types.

The bootstrap execution runtime remains separate from the narrow engine-neutral backtest
port; replay requests never pass through `submit/cancel/reconcile`. Other engines can
implement `BacktestBridge` through their own adapters and need not reproduce
Nautilus-specific strategy APIs, OMS modes, execution algorithms, or unsupported order
types.

## Failure behavior

Invalid or missing price references, inactive or expired order intents, expired mandates,
mismatched accounts or environments, unknown order state, provider disconnects, and
reconciliation gaps fail closed. A semantic agent cannot turn an infrastructure
uncertainty into approval.
