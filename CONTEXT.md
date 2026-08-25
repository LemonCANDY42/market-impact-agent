# Market Impact

This context names the evidence-to-execution concepts that the harness must keep
distinct so that research, approval, and broker state cannot become competing
authorities.

## Evidence and events

**Evidence Item**:
A source-backed observation with separate occurrence, publication, visibility,
and retrieval times.
_Avoid_: News blob, context

**Event Envelope**:
The immutable point-in-time boundary containing an event and the Evidence Items
available as of a stated instant.
_Avoid_: Prompt, news dump

**Event Cluster**:
One market-relevant development represented by one or more related disclosures
or catalysts rather than by individual headlines.
_Avoid_: Article group, duplicate news

**Event Archetype**:
A reusable class of events that share a transmission logic and validation
method, such as a physical supply shock or cumulative industry narrative.
_Avoid_: Topic, sector label

**Transmission Path**:
An evidence-linked sequence from an Event Cluster through direct or indirect
economic, market, or attention channels to an affected security.
_Avoid_: Correlation edge, causal score

## Decisions

**Event Assessment**:
A versioned fast or deep judgment about an Event Cluster, its Transmission
Paths, counterevidence, expected persistence, and invalidation conditions.
_Avoid_: Agent opinion, prediction

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
