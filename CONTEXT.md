# Market Impact

This context names the evidence-to-execution concepts that the harness must keep
distinct so that research, approval, and broker state cannot become competing
authorities.

## Evidence and events

**Evidence Item**:
A source-backed observation with separate occurrence, publication, visibility,
and retrieval times.
_Avoid_: News blob, context

**Source Observation**:
An immutable raw and normalized Provider record captured before it is eligible to become
Evidence. It retains original upstream identity, aggregator identity when present, source
publication/update times, strategy availability, local retrieval, and completeness gaps.
_Avoid_: API response, trusted fact

**Availability Time**:
The earliest instant a strategy may use one exact source version, measured from a real-time
receipt or derived from source publication plus a frozen, source-specific latency model.
It is not the time a later historical backfill happened to run.
_Avoid_: Event time, retrieval time

**Retrieval Time**:
When the Harness fetched and stored its local copy. It supports audit, identity, and
diagnostics but does not reconstruct historical source availability.
_Avoid_: Publication time, visibility time

**Event Envelope**:
The immutable point-in-time boundary containing an event and the Evidence Items
available as of a stated instant.
_Avoid_: Prompt, news dump

**Evidence Pack**:
An immutable, content-identified set of point-in-time Evidence Items, source artifacts,
Pattern Pack references, and research scope made available to one Judgment Run.
_Avoid_: Model context, live search results

**Pattern Pack**:
A versioned pre-cutoff research asset containing reusable event mechanisms, transmission
scales, analogues, applicability conditions, and counterexamples.
_Avoid_: Agent memory, learned truth

**Event Cluster**:
One market-relevant development represented by one or more related disclosures
or catalysts rather than by individual headlines.
_Avoid_: Article group, duplicate news

**Event Archetype**:
A reusable class describing the root cause of new market-relevant information,
such as an issuer action, geopolitical-security event, or physical disruption.
One Event Cluster has one primary archetype; linked causes and consequences are
represented as separate clusters and Transmission Paths.
_Avoid_: Topic, sector label, transmission mechanism

**Revelation Mode**:
How evidence becomes observable: scheduled, unscheduled, continuously updated,
or retrospectively revised.
_Avoid_: Event type, source type

**Event Stage**:
The point reached in an event's evidence and market-diffusion lifecycle, from
pre-event through first observation, corroboration, realization, resolution, or
invalidation.
_Avoid_: Order status, approval status

**Expectation Delta**:
The point-in-time difference between an observed outcome and a cited prior
baseline. It may be explicitly unknown when no defensible baseline exists.
_Avoid_: Sentiment, surprise score without a baseline

**Transmission Channel**:
The causal mechanism carried by one step of a Transmission Path, such as demand,
capacity and cost, policy access, funding, uncertainty, attention, or forced
market flow.
_Avoid_: Event Archetype, industry label

**Transmission Directness**:
The number of causal hand-offs between the Event Cluster and an affected
exposure: direct, second-order, third-order, or fourth-order.
_Avoid_: Confidence, market overlay

**Transmission Path**:
An ordered, evidence-linked sequence of Transmission Channels from an Event
Cluster to an affected security or market exposure. Each step states its
directness, affected variable, counterevidence, blockers, and invalidation.
_Avoid_: Correlation edge, causal score

**Listing Snapshot**:
An immutable capture of a Provider's reported instrument listing lifecycles at
one retrieval time. It does not assert that the captured fields were known or
unchanged at an earlier date.
_Avoid_: Security master, historical truth

**Data Snapshot**:
An immutable, content-identified bundle of Provider observations and provenance
that a Backtest Request can cite exactly. It is replay input, not proof of
historical completeness, executable liquidity, or source infallibility.
_Avoid_: Cache, market-data dump

**Pre-event Universe**:
A fixed set of instruments eligible at an event cutoff, reconstructed from and
bound to one Listing Snapshot. It is not proof against source revision,
omission, or survivorship bias.
_Avoid_: Current constituents, dynamic universe

**Backtest Request**:
An immutable, engine-neutral request embedding the exact Signal Intent content and binding
it to a compatible point-in-time evaluation window, target-containing universe, data
snapshot, strategy, and Simulation Specification.
_Avoid_: Engine config, Order Intent

**Simulation Specification**:
The versioned market-data, fill, fee, venue-rule, capital, and randomness assumptions
under which a Backtest Request is evaluated.
_Avoid_: Trading Mandate, live venue configuration

**Backtest Run Manifest**:
An immutable record binding a Backtest Request to the exact engine, bridge, configuration,
input hashes, and execution time used for one run.
_Avoid_: Provider Manifest, backtest report

**Backtest Result**:
The normalized completed or failed outcome of one Backtest Run Manifest, including
deterministic result identity, metrics, artifacts, or explicit failure reasons.
_Avoid_: Broker receipt, live performance claim

**Calibration Evidence**:
A frozen set of independently repeated Backtest Results labeled by Event Cluster,
chronological partition, and pre-registered candidate or baseline variant. It is input to a
calibration gate, not permission to tune after observing the test partition.
_Avoid_: Backtest leaderboard, selected wins

**Calibration Cell**:
One pre-registered Event Cluster evaluation unit binding its visibility cutoff,
chronological partition, target mapping, evaluation window, Data Snapshot, horizons, and
Simulation Specification before outcomes are opened.
_Avoid_: Backtest row, tunable sample

**Variant Decision**:
One pre-registered candidate or baseline action for a Calibration Cell. A long-only action
is either `buy`, with an exact Backtest Request, or `abstain`, with no fabricated Signal
Intent or Result and zero exposure in the fixed calibration denominator.
_Avoid_: Failed trade, synthetic zero-PnL result

**Calibration Gate Result**:
A content-identified, engine-neutral acceptance or rejection report that applies one
versioned cross-event protocol to Calibration Evidence. Rejection is evidence and blocks
phase promotion; acceptance grants backtest calibration only.
_Avoid_: Alpha claim, paper approval, live mandate

## Decisions

**Judgment Run**:
A bounded, auditable Agent evaluation of one Evidence Pack under exact model, Skill, tool,
MCP, budget, and context configuration.
_Avoid_: Backtest run, chat session

**Judgment Artifact**:
The immutable result of a Judgment Run, including cited evidence, transmission reasoning,
candidate impacts, blockers, abstention state, and the complete configuration identity.
It is a proposal awaiting deterministic admission, not a Signal Intent.
_Avoid_: Model response, trade recommendation

**Candidate Impact**:
One Judgment Artifact's evidence-linked directional hypothesis for a target exposure at a
stated horizon, directness, confidence, and invalidation boundary.
_Avoid_: Order, portfolio weight

**Event Assessment**:
A versioned fast or deep judgment about an Event Cluster, its Transmission
Paths, counterevidence, expected persistence, and invalidation conditions.
_Avoid_: Judgment Run, Agent opinion, prediction

**Signal Intent**:
A time-bounded, evidence-linked expression of directional interest in a
security. It is not an instruction to trade.
_Avoid_: Trade, recommendation

**Order Intent**:
An idempotently identified request to evaluate a specific order against policy
and approval. It is not proof of broker acceptance.
_Avoid_: Order, execution

**Trading Mandate**:
A versioned, expiring grant that defines the accounts, environments,
instruments, directions, and risk envelope in which Order Intents may proceed.
_Avoid_: Permission flag, auto-trade switch

**Approval Decision**:
An auditable deny, manual-review, approve, or reject result tied to one Order
Intent and one Trading Mandate version.
_Avoid_: Confirmation, model confidence

## Providers and execution

**Provider**:
An external or in-process capability owner for market data, backtesting,
account state, paper execution, or live execution.
_Avoid_: Tool, broker

**Observation Provider**:
A read-only Provider that discovers or acquires source records under an Observation
manifest. It cannot own Evidence promotion, research, approval, or execution. Aggregated
and direct sources retain distinct identities.
_Avoid_: Evidence authority, execution Provider

**Capability Snapshot**:
A point-in-time record of what a Provider declares and what the harness has
independently verified it can do.
_Avoid_: Integration config, feature list

**Execution Event**:
A provider-observed state change for an order, fill, position, balance, or
reconciliation result.
_Avoid_: Callback, tool result

**External Order**:
A broker order not created by a known Order Intent in the harness ledger.
_Avoid_: Unknown order, manual order

**Reconciliation**:
The fail-closed process of comparing provider truth with the harness ledger
before accepting new Order Intents.
_Avoid_: Sync, refresh
