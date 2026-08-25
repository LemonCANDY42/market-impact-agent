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
    -> Hard Policy: DENY | REQUIRE_MANUAL | ELIGIBLE
    -> Semantic Auto Approver (optional; cannot override hard policy)
    -> Approval Decision
    -> durable Order Intent/outbox
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

## Reference engine

NautilusTrader is the selected default trading engine and reference Provider
implementation. Its public execution architecture informs the harness conformance
baseline: typed commands and events, risk before execution, stable client/venue/trade
identities, explicit unknown outcomes, private-stream plus query recovery, startup and
continuous reconciliation, external-order handling, and consistent simulation/live
semantics.

The harness does not import NautilusTrader types into its public contract. It defines the
small low-frequency subset needed by event-driven trading, and `NautilusProviderAdapter`
will translate that contract into NautilusTrader. This keeps the dependency direction
stable and avoids forcing sibling engines to reproduce Nautilus-specific OMS, execution
algorithms, or every supported order type.

```text
Harness Provider Contract
    -> NautilusProviderAdapter (default/reference)
         -> Nautilus backtest
         -> IBKR Paper
    -> VeighNa external bridge -> supported A-share gateway
    -> future direct IBKR, LEAN, or other adapters
```

All branches must pass the same capability, policy, idempotency, recovery, and
reconciliation conformance suite. The default adapter has no privileged bypass.

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

- NautilusTrader is the default/reference backtest and paper engine. Its adapter is
  planned and disabled in the bootstrap.
- Tushare is accessed through a language-neutral HTTP adapter; licensed data
  remains local.
- VeighNa is an external-process bridge. VeighNa 4.4 and current A-share vendor
  gateways do not provide a verified same-process Python 3.14/macOS path.
- LEAN remains a comparison candidate in its own Docker/Python runtime and is
  not a bootstrap dependency.

See [docs/PROVIDER_CONTRACT.md](docs/PROVIDER_CONTRACT.md) for conformance details.
