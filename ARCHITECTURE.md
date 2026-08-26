# Architecture

## Ownership

The harness is the sole orchestration and approval authority. Research agents
produce artifacts; deterministic policy establishes eligibility; semantic
approval operates only inside the hard envelope; providers own external
capabilities and observed execution facts.

```text
Observation adapters (aggregated discovery + direct sources)
    -> immutable raw/normalized observation bundle
    -> availability and latency gate -> Evidence Items -> Event Envelope
    -> Evidence Pack + pre-cutoff Pattern Pack
    -> Agent Harness -> sealed Judgment Artifact
    -> deterministic admission -> Event Assessment -> Signal Intent
         -> Backtest Request -> engine-neutral backtest port
              -> Nautilus backtest bridge -> independent horizon replays and results
              -> calibration gate -> baseline/out-of-sample accept or reject
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

Observation Providers use a separate, read-only manifest from execution Providers. The
execution registry requires environments, order types, streaming, and reconciliation;
forcing news, macro, or prediction-market sources into it would mix acquisition with order
lifecycle authority. Observation manifests instead declare upstream sources, temporal
fields, history/revision support, authentication, licensing notes, and verified data
capabilities. They remain disabled until their exact source and capability pass acceptance.

Aggregators are discovery accelerators, not canonical authorities. The Harness retains both
the aggregator identity and original upstream identity, then uses direct adapters where
rules, revision history, market identifiers, or historical series matter. Copies of one
upstream record share a claim identity and do not become independent corroboration.

An Observation records occurrence, source publication/update, optional aggregator fetch,
strategy availability, and local retrieval separately. Historical replay uses source
availability or a frozen delivery-latency model; local backfill retrieval time is audit
metadata and never substitutes for historical visibility. See
[`docs/OBSERVATION_DATA.md`](docs/OBSERVATION_DATA.md).

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

For a multi-horizon Backtest Request, the bridge constructs a fresh low-level Nautilus
BacktestEngine per horizon and merges only normalized, horizon-prefixed metrics into the
engine-neutral Result. Phase 2 calibration remains a Harness authority: it verifies repeated
result identity, pre-registered Calibration Cells and Variant Decisions, Event Cluster
walk-forward partitions, comparable candidate/baseline windows and runtime manifests,
ratio-unit cost-aware return, honest long-only abstentions, baseline superiority, and
single-event dominance. Nautilus does not decide whether research may be promoted. The
first real v2 cohort failed because candidate mean net return was not positive; all later
capability phases remain closed.

## Agent runtime boundary

The Agent runtime is another Harness adapter, not a second orchestration authority.
It normalizes model messages and tool calls while the Harness owns durable run state,
context compaction, Skill selection, MCP lifecycle, permissions, budgets, audit, recovery,
and human/policy gates. The exact China origin is pinned; deterministic hardening tests and
a fresh MiniMax M3 run pass the bounded local research-runtime gate. The runtime receives
research-only tools and no broker/account capability. This is runtime evidence, not model
quality, event-family calibration, alpha, or execution acceptance.

Historical evaluation exposes only one immutable Evidence Pack and pre-cutoff Pattern Pack
to a Judgment Run. Prospective tools must first capture and time-bind new source material
before the Agent may cite it. The sealed Judgment Artifact records the exact Provider,
model, prompt, Skill, MCP, tool, compaction, evidence, and output identities. Replaying a
trade consumes that artifact; it never re-runs the model inside Nautilus.

Context summaries, Skill instructions, MCP output, and model responses are evidence-bearing
artifacts with identities and provenance; none may silently become policy. Model success is
separate from trading-research calibration and Provider execution conformance. The complete
accepted v1 boundary and remaining non-claims are in `docs/AGENT_RUNTIME.md`.

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

The first market-data persistence slice is a private, content-addressed local Tushare Data Snapshot:
normalized listing, universe, calendar, and daily tables in Parquet plus a JSON manifest.
The Agent runtime now uses private content-addressed artifacts plus a hash-chained append-only
SQLite run index for recovery; it introduces no database service. Current prediction-market
snapshots use the same local-first principle: one private,
content-addressed JSON bundle contains the complete decoded JSON response, normalized
observations, temporal provenance, and Provider manifest. No service or database was
introduced.

## Runtime boundary

First-party code supports Python `>=3.13,<3.15`.

- NautilusTrader is the default engine foundation. Stable `1.231.0` is an exact optional
  dependency for the accepted first replay slice; paper Provider integration remains
  unimplemented and disabled.
- Tushare is accessed through a language-neutral HTTPS adapter; licensed data remains
  local. One official token-backed local capture and validation completed on 2026-08-25
  for `600028.SH`, using 2019-09-18 as-of metadata and a 2019-09-19..2019-10-10 daily
  window; the local credential worked for that account, target, and window. A separately
  versioned modeled-open adapter validated and consumed that exact private bundle twice
  with identical 1/3/10-session replay identity for the narrow `600028.XSHG` Nautilus gate.
  A hardened v2 adapter additionally binds adjustment factors and source daily limits,
  separates adjusted pre-cutoff observations from the unadjusted evaluation window, and
  replayed seven pre-registered private snapshots. The v2 gate rejected the real cohort
  because candidate mean net return was not positive. These
  acceptances prove neither general quota/permissions, completeness, historical truth,
  full-universe prices, observed liquidity/fillability, alpha, nor paper/live readiness.
  The Tushare Provider remains disabled and unverified. A universe reconstructed from current
  listing metadata stays bound to that retrieval snapshot and is not treated as proof
  against source revision or survivorship bias.
- VeighNa is an external-process bridge. VeighNa 4.4 and current A-share vendor
  gateways do not provide a verified same-process Python 3.14/macOS path.
- LEAN remains a comparison candidate in its own Docker/Python runtime and is
  not a bootstrap dependency.

See [docs/PROVIDER_CONTRACT.md](docs/PROVIDER_CONTRACT.md) for conformance details.
