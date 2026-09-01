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

Cancel is an independently accepted optional operation. The Harness issues a sealed cancellation
capability only after binding the exact reconciled provider order, content-identified request,
manual approval, Provider identity/version and one durable attempt lease. Submission authority is
bound to the same Provider identity/version; changing the configured adapter cannot inherit an old
order merely because client/provider order strings collide. A Provider that rejects cancellation
authority before any mutation may return `CancellationCapabilityRejected`; any transport or
Provider exception without that proof is `unknown` and is never retried automatically. A
cancellation command receipt cannot establish terminal state. The next globally complete Provider
reconciliation must explicitly identify the same provider order as canceled. Replace is
deliberately absent from the Provider port: the Harness models it as an atomic durable cancellation
link followed, after cancellation reconciliation, by a newly admitted Order Intent with a new
client identity.

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
  crash/ambiguous-submit/cancel/reconciliation tests cross a real restart boundary. The Harness
  now accepts its provider-neutral submit, cancel, replace-as-cancel-plus-new-intent and kill-switch
  lifecycle, including ambiguous cancellation resolution. This is a local contract test double,
  not an accepted paper venue.
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
  satisfy recovery or reconciliation acceptance. Its first mutation-free candidate probe now
  completes a real local Paper Gateway connection through pinned NautilusTrader `1.231.0`, waits
  for Nautilus execution reconciliation and portfolio initialization, and cross-checks the
  resulting account/open-order/open-position counts against the independently completed direct
  account read. The probe loads no Strategy and emits a content-identified private readiness
  report. The remaining manual-TWS-order coverage gap makes the report read-only accepted but not
  exposure-increase-ready; this is configuration/lifecycle evidence, not execution acceptance.
  The execution-side candidate now exists behind a provider-neutral
  `NautilusPaperExecutionRuntime` port, with concrete runtime version
  `0.2.0-candidate` pinned to NautilusTrader `1.231.0` and its exact installed
  `nautilus_ibapi` version. One long-lived `TradingNode` and one bounded Harness Strategy
  own submit, cancel, event delivery, and broker reconciliation;
  the Harness adapter does not implement another OMS or infer broker lifecycle state.
  It validates the exact sealed Harness capability,
  translates only registered Instrument Master routes carrying an explicit market, assigns a deterministic bounded
  Nautilus client order ID and commits its identity before dispatch. A lost response leaves that
  identity ambiguous and reconciliation-only; neither submit nor cancel is silently sent again
  after restart. The candidate is disabled by default. A complete content-identified Harness
  Provider Acceptance must match the runtime configuration hash, opaque account scope and complete
  Instrument route-set hash before the Provider can recover or reduce risk. New-order admission is
  separately open only during that acceptance's explicit validity interval, and every new mutation
  is also checked against the accepted market and order-type sets. Durable
  order and cancellation bindings retain those scope hashes, so a reused state database cannot send
  an old identity to another account or runtime. The concrete runtime uses `DAY` only; the exact
  time-in-force, Provider/runtime/dependency versions, anonymous account scope, market set,
  Instrument route-set, order types, validity interval, and per-scenario fault evidence are all
  part of Provider Acceptance v3. The anonymous account reference is an HMAC-SHA-256
  pseudonym derived from the raw Paper account reference with a Harness-held key; callers cannot
  supply an arbitrary account hash. The configuration hash also binds every accepted Nautilus
  instrument ID, Gateway route/client setting, and startup/command timeout that can change runtime
  behavior. After submit admission expires, cancellation may
  remain available only as exact-scope risk reduction for an already bound order; new exposure stays
  disabled.

  The runtime requires IB client ID `0` and the official adapter's
  `fetch_all_open_orders=True` path. That establishes an all-open-orders discovery request, not
  ownership, continuous subscription, or cancellation control. Acceptance additionally requires a
  positively observed exclusive API-client scope and positive evidence for the Gateway/TWS
  manual-order auto-bind path. These are observed results, not trusted configuration switches:
  the effective adapter client ID must remain `0`, and any client-ID collision, fallback, or
  missing auto-bind observation fails closed. Every discovered external/manual order remains classified as
  external, is reconciled, and creates a fail-closed Provider gap; it is never adopted into a
  Harness order binding. Broker account, order, permanent-order, and execution identifiers remain
  inside private Provider evidence; public acceptance binds only opaque account and content hashes.
  Runtime reconciliation covers cash, positions, orders, and executions as separate completeness
  facets. Orders, executions, and positions are normalized from the exact
  `generate_mass_status()` return rather than a later asynchronous cache view. Each successful
  query barrier is stamped with the current connection generation, so a
  prior startup or pre-disconnect result cannot satisfy post-reconnect readiness. Because a full
  disconnect/reconnect can occur between two high-level connected-state observations, each
  reconciliation and canonical mutation also binds the official IB adapter's exact
  `_last_disconnection_ns` marker. Native submit/cancel rereads that marker immediately before the
  Strategy mutation and requires exact equality, including `None`; a changed marker requires a
  fresh full reconciliation. This private adapter dependency is guarded by the exact pinned
  NautilusTrader `1.231.0` version and fails closed if the field is absent or has an invalid type.
  It deduplicates
  identical fills by broker execution identity and fails closed on conflicting duplicates, missing
  or stale facets, disconnect, any local gap, or any external order. Process and Gateway restart recovery
  reuses the deterministic Harness/Nautilus client identity and official startup/continuous
  reconciliation; an ambiguous acknowledgement is never redispatched. Native modify and native
  replace are not accepted capabilities. Replace semantics are a Harness-owned cancel followed by
  a fresh submit: persist and dispatch cancel, reconcile the exact original broker identity to
  `canceled`, then separately admit, persist, and submit a new intent with a new Harness
  order/submission identity. An ambiguous cancel,
  incomplete reconciliation, external order, or any other gap prevents the replacement submission.
  Exact canceled reconciliation advances a prepared/dispatched cancellation row to terminal
  `canceled` idempotently, so a process restarted after a lost cancel response can resume replace
  without redispatching cancel. The native driver also rechecks the command's `created_at`,
  `expires_at`, and activation authorization against its clock immediately before
  `Strategy.submit_order`; admission before a queue delay cannot authorize an expired mutation.
  No real
  Paper mutation or Provider Acceptance has been performed yet.

  Acceptance cannot be minted from caller-provided scenario hashes or pass booleans. A secret-bearing
  Harness acceptance-runner capability HMAC-seals each exact JSON artifact/result pair. The durable
  evidence authority verifies that runner provenance before storing the bytes, sealed observation,
  and observation identities used by an acceptance. Evidence acceptance alone cannot construct an
  enabled Provider. Production activation remains closed because no external canonical provisioner
  yet registers the exact acceptance-runner authority in `LocalDataSnapshotStore`. Once that
  external prerequisite exists, acceptance creation can use one Harness authority transaction to
  bind the sealed acceptance content ID, evidence store, runtime registration, Instrument-route
  registration, mutation outbox and activation validity into one current head under the store's
  persisted `harness_authority_id`. The sole issuance entry accepts only that concrete store and
  accepted evidence content ID; it accepts no caller runtime, routes, verifier, signing key, clock,
  raw command or acceptance object. Both Provider status and runtime mutation reopen and fully
  verify the sealed acceptance payload/evidence and registrations from the same root. Mutation also
  requires the exact active runtime, current connection generation, and a fresh bounded
  reconciliation scope. Execution-state mechanics are tested through an explicitly test-only
  adapter and do not establish activation authority. The runtime's
  evidence-authority pin remains an evidence-scope check, not activation authority. A caller-owned
  evidence database, runner key, verifier, runtime, matching self-pin, imported constructor seal, or
  fresh state root without the registered runner authority therefore remains disabled. Loading or
  enabling an acceptance re-resolves every stored
  artifact/result, verifies its runner HMAC, exact scope, authority identity, and derived result. A
  missing, edited, fabricated, mismatched, or unauthenticated artifact leaves the Provider unverified.
  Arbitrary local filesystem or SQLite mutation is outside the in-process capability threat model;
  content identity, current-head checks and evidence reopening detect such drift where possible.
  Public artifacts expose only anonymous content hashes. Raw broker
  account, order, permanent-order, and execution identifiers stay inside the private evidence store.
  Separately, the narrower
  `ibkr-paper-account-read` adapter has completed one real local IB Gateway Paper account read: it
  exposes no mutation method, waits for independent account, summary, API-open-order and execution
  barriers, and normalizes only credential-free content-identified account state. A nonzero client
  cannot prove the absence of manually submitted TWS orders without binding them, so that exact gap
  remains explicit and blocks exposure increase. This accepts bounded `ACCOUNT` reads only; it does
  does not verify submit/cancel/replace, external-order recovery, restart reconciliation, market
  data, or `ibkr-nautilus-paper` execution.
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

Provider reconciliation v2 distinguishes acknowledgement from lifecycle state. An order may be
`accepted`, `pending_cancel`, `canceled`, `partially_filled`, `filled`, `rejected`, `expired`, or
`unknown`; cumulative filled quantity and unique Provider fill identities accompany the state.
The Harness canonicalizes their order before hashing and rejects duplicate fill identity across
orders, overfill, a full-fill quantity
that differs from the immutable Order Intent, a partial-fill quantity that is not partial, any
cross-snapshot decrease in cumulative quantity, disappearance of an earlier fill identity, stale
order observation or terminal-state regression. Reconciliation changes canonical order state only
when the whole normalized snapshot is complete and gap-free. A partially filled open order remains
eligible for cancel of its unfilled remainder.
Submission receipts remain accepted-without-fill acknowledgements. A fast fill before that
acknowledgement is therefore ambiguous until reconciliation rather than being mislabeled accepted.

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

The same rule applies to cancel. The kill switch prevents new submission claims but deliberately
keeps exact cancel and reconciliation paths available; clearing requires a new complete
reconciliation whose v2 artifact and durable run bind the kill generation sampled before the
Provider call. It never implies that the Provider canceled all open orders. Every complete run also
rechecks all durable accepted open orders; a later missing or contradictory order blocks the gate.
Restart with any such order fails closed until a fresh reconciliation, and an unbound legacy order
is never silently assigned to the currently configured Provider.
