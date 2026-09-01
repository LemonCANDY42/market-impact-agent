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
provider. The composition root binds a trusted Trading Mandate, clock, Price Basis source,
durable execution store, provider, and any Agent run authorities into the paper execution service;
callers supply none
of that policy context. The service freezes the exact intent, mandate, price, policy
evaluation, and approval, then atomically queues an eligible intent. Only its durable dispatch
path can issue a sealed submission capability containing those hashes and a unique submission
attempt identity. The provider contract accepts that capability rather than a raw intent. No
live gateway exists in the bootstrap.

For Agent paper flow, the Harness may first persist a `DecisionAdmission` binding an eligible
prospective Query Gate, exact Evidence lineage, complete paired Decision Run Manifest, stable
treatment Judgment identities plus their Journal-bound metrics, a Harness-bundled Provider Profile,
control-plus-routed-method Prospective Execution Plan, deterministically
reconstructed Signal, and exact paper Order. Providers never see or validate that research
provenance, and it grants no submission capability. The unchanged mandate, hard policy,
approval, durable outbox, sealed capability, and reconciliation path still decides whether an order
can reach a provider.

The Decision Run Manifest cannot authenticate its own run occurrence. At Agent paper admission,
each planned execution-binding hash must resolve to a composition-root-bound runtime authority that
reopens the actual completed Run Record, verifies the Journal chain and source artifacts, and
recomputes metrics. Missing authority or a mismatched result fails closed before the immutable paper
copy is written. This is a Harness boundary and does not add research-validation duties to the
execution Provider.

The provider owns broker-specific translation and execution facts. The harness owns
policy, approval, evidence, and the immutable decision trail.

A production-grade execution provider must preserve the caller's `client_order_id`,
return a stable provider order identifier, stream order-state changes, and reconcile
open orders, fills, positions, and cash after reconnects. Submission must be idempotent.
Reconciliation returns an identified snapshot with observation time, completeness, receipts,
and explicit gaps; a bare receipt list cannot prove that a missing order is absent. An MCP tool
can be the invocation surface, but it does not replace these requirements.

## Capability trust

`declared_capabilities` describe what an adapter claims. `verified_capabilities` describe
what the harness is allowed to route to after contract tests. The latter must be a subset of the
former and, for any external execution Provider, must be backed by a Harness-owned Provider
Acceptance binding the exact implementation, dependency, configuration, environment, account scope,
and accepted fault/reconciliation evidence. A Provider cannot verify or replace its own acceptance.

Trust tiers are deliberately explicit:

| Tier | Meaning |
| --- | --- |
| `unverified` | Manifest exists; no execution claim is trusted. |
| `mock` | Deterministic local test double only. |
| `paper_validated` | Paper behavior and recovery contract were tested. |
| `live_validated` | Live-specific certification was completed separately. |

Paper validation never upgrades a provider to live validation.

## Initial adapters

- `mock-execution` is the only enabled implementation in the bootstrap. It is paper-only,
  declares no account capability, and can retain its order truth in a separate SQLite file so
  crash/ambiguous-submit/reconciliation tests cross a real restart boundary. This is a local
  contract test double, not an accepted paper venue.
- `NautilusBacktestBridge` is the accepted reference implementation of the engine-neutral
  backtest port for the bounded synthetic XSHG cash-equity fixture. A separate versioned
  modeled-open adapter can feed it from any fully validated `600028.SH`/SSE bundle matching
  its snapshot, request window, and versioned rules; generated bundles cover this path in
  tests, while the named private bundle/request is the first local acceptance evidence.
  Modeled liquidity is not a Provider observation. Neither path grants market-data, paper,
  live, IBKR, or VeighNa capability.
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
  satisfy recovery or reconciliation acceptance. Separately, the narrower
  `ibkr-paper-account-read` adapter has completed one real local IB Gateway Paper account read: it
  exposes no mutation method, waits for independent account, summary, API-open-order and execution
  barriers, and normalizes only credential-free content-identified account state. A nonzero client
  cannot prove the absence of manually submitted TWS orders without binding them, so that exact gap
  remains explicit and blocks exposure increase. This accepts bounded `ACCOUNT` reads only; it does
  not verify submit/cancel/replace, external-order recovery, restart reconciliation, market data, or
  `ibkr-nautilus-paper` execution.
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

The provider-neutral account-to-order seam is now concrete rather than engine-specific:
`AuthorizedDecisionView -> PortfolioDecision -> OrderSizingDecision -> OrderIntent ->
DecisionAdmission v2`. The v2 admission binds the trusted parent account state, derived position,
mandate, raw price, Instrument Master identity, side-applicable venue/class rule and deterministic
quantity artifacts before the execution Provider is called.
Admission, approval and dispatch each compare that parent with the trusted current Account State
source and the composition-root maximum age. The admitted Position Snapshot cannot select or extend
that policy. Dispatch checks the exact account state and order/mandate/price expiries both after the
durable claim and inside the sealed Provider-capability validator. A changed or stale snapshot
expires the pending operation before the Provider call; the Harness requires a fresh decision
instead of carrying an old manual approval forward. Legacy Decision Admission v1 remains readable
but is never executable.
Nautilus and future sibling engines consume only the resulting engine-neutral operation; they do
not recalculate Harness sizing or become a portfolio-policy authority. The accepted durable mock
path proves this contract and restart validation, but does not satisfy `ibkr-nautilus-paper`.

The bootstrap execution runtime remains separate from the narrow engine-neutral backtest
port; replay requests never pass through `submit/cancel/reconcile`. Other engines can
implement `BacktestBridge` through their own adapters and need not reproduce
Nautilus-specific strategy APIs, OMS modes, execution algorithms, or unsupported order
types.

## Failure behavior

Invalid, missing, future-dated, or stale Price Bases; inactive or expired order intents;
expired mandates; mismatched accounts or environments; unknown order state; provider
disconnects; external orders; and reconciliation gaps fail closed. An expired submission
lease becomes `unknown` and is never returned to the queue. A semantic agent cannot turn an
infrastructure uncertainty into approval.

A Provider must reject a failed sealed-capability validation before any external mutation and
report the typed `SubmissionCapabilityRejected` outcome. The Harness can then expire the durable
claim because non-submission is known. Transport or Provider exceptions without that guarantee stay
`unknown` and require reconciliation.
