# Provider Contract

Providers expose external market-data, backtest, account, and execution capabilities to
the harness. A provider may use a native Python adapter, MCP, HTTP, or gRPC. Transport is
an integration choice, not a trust signal.

## Registration

Every provider supplies a versioned manifest that declares:

- provider identity and implementation version;
- supported environments and markets;
- declared and independently verified capabilities;
- order types;
- streaming and reconciliation support;
- enabled state and trust tier.

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
- NautilusTrader is the selected default engine and first real conformance/reference
  Provider for backtest and paper execution. It receives no policy bypass and is not yet
  installed or enabled.
- Tushare is a planned read-only A-share data provider.
- IBKR is the first planned US/HK paper destination, initially through the NautilusTrader
  adapter. A direct IBKR Provider remains possible if that path cannot satisfy recovery or
  reconciliation acceptance. No account is connected by this repository.
- VeighNa is represented as a disabled external-process bridge. Its core may be usable on
  Python 3.13, but common A-share gateways depend on vendor runtimes and are not claimed
  to work on macOS or Python 3.14.

## Reference semantics

The common contract adopts the smallest useful subset of NautilusTrader's public
execution constraints: command/event separation, stable client/venue/trade identities,
definitive-versus-unknown outcomes, risk before execution, duplicate-fill protection,
stream and query recovery, external-order classification, and reconciliation. These are
behavioral acceptance requirements, not copied implementation or a re-export of
NautilusTrader's types.

Other engines implement this harness contract through their own adapters. They do not
need to implement Nautilus-specific strategy APIs, OMS modes, execution algorithms, or
unsupported order types.

## Failure behavior

Invalid or missing price references, inactive or expired order intents, expired mandates,
mismatched accounts or environments, unknown order state, provider disconnects, and
reconciliation gaps fail closed. A semantic agent cannot turn an infrastructure
uncertainty into approval.
