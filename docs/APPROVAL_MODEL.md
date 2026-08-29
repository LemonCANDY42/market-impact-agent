# Approval Model

Approval is a policy state machine, not a conversational promise.

## Modes

| Mode | Intended behavior |
| --- | --- |
| `disabled` | Deny every order. This is the default for live trading. |
| `manual_each` | Require a human decision for every otherwise-valid order. |
| `timeboxed` | Permit only within an explicit instrument, account, size, side, and time mandate. |
| `policy_auto` | Allow an approver agent to decide only inside the hard-policy envelope. |
| `autonomous` | No per-order confirmation, but the same hard policy and kill switch still apply. |

## Decision order

1. For an Agent-originated paper order, validate and persist the exact eligible Prospective Query
   Gate→Prospective Evidence Lineage→Prospective Execution Plan→complete Decision Run
   Manifest→Decision Admission→Signal→Order binding. The six Judgment, final validation-event, and
   metrics artifacts, Harness-bundled Provider Profile, and control-plus-routed-method surfaces must
   reconcile before admission. Content hashes alone are insufficient: the composition root must
   bind each execution-surface hash to a trusted Agent runtime authority that reopens the actual Run
   Record, complete Journal chain, terminal/transcript/raw/tool-result artifacts, and
   Journal-recomputed metrics. This provenance
   grants no execution authority. Gate evaluation precedes every run; Manifest creation precedes
   Signal validity and Order creation.
2. Validate the `OrderIntent`, including its explicit creation-to-expiry window, and its
   parent signal/evidence references.
3. Evaluate non-overridable hard rules.
4. Return `DENY`, `REQUIRE_MANUAL`, or `ELIGIBLE`.
5. Only an `ELIGIBLE` intent may reach a configured semantic auto approver.
6. Atomically queue an approved intent in the durable paper outbox.
7. Record every submission attempt, provider acknowledgement, execution event, and
   reconciliation result without inferring a fill from acknowledgement.

Hard rules include order-intent and mandate time bounds, account and environment matching,
instrument and side allowlists, positive finite price references, order-notional limits,
provider capability/trust, stale-data thresholds, and kill switches. The semantic approver
can choose a stricter result; it cannot override `DENY` or `REQUIRE_MANUAL`.

Only the Harness-owned `PaperExecutionService` can turn an approved durable outbox row into
the sealed capability accepted by a provider. Its Trading Mandate, clock, Price Basis source,
provider, and Agent run authorities are bound by the trusted composition root, not supplied by the
order caller.
The capability binds exact hashes for the intent, mandate, price, policy evaluation, and
approval. `policy_auto` remains fail-closed because the bootstrap has no semantic approver;
`autonomous` also remains fail-closed until its kill-switch gate exists. Direct submission of
an `OrderIntent` is outside the provider contract, and the bootstrap has no live submission
path.

## Durable paper state

Approval and execution are separate state machines. Hard denial and manual rejection are
terminal approval results. Approval atomically creates a queued outbox row. Dispatch claims
that row with an expiring lease; an exception, lost acknowledgement, process crash, or expired
lease produces `unknown`, never an automatic retry. Provider `accepted` is an acknowledgement,
not a fill. `unknown` and `accepted` block further admission and dispatch until a complete
provider reconciliation snapshot accounts for every known and external order without gaps.

Immutable contracts live in a content-addressed store. For the Agent path the outbox row additionally
binds the `DecisionAdmission` artifact hash; the restart validator also reopens the Execution Plan,
all six Judgment, final validation-event, and metrics artifacts, treatment agreement, Signal
validity, and Order binding. Before that immutable copy is admitted, the source Agent runtime
authorities must have reopened and validated the actual runs; a caller cannot promote a fabricated
but internally consistent run bundle. An
abstaining admission cannot enter the outbox. A
low-level mock-only
admission remains available for isolated lifecycle contract tests and cannot be rebound later to an
Agent admission with the same client order identity. SQLite owns the current approval,
outbox, attempt, event, reconciliation, and global-gate state with WAL, full synchronous
writes, and immediate transactions around claims and transitions. The data-input receipt
journal and Attention Watch outbox are deliberately not execution authorities.

## Notifications

Manual approval requests should support macOS notifications and a durable inbox. A
notification is only an alert: the approval must be bound to the exact intent hash,
expiry, mandate, and actor. Editing an intent invalidates its approval.

## Initial safety state

The bootstrap contains no live provider, credential loader, autonomous approver, Agent-facing
execution tool, automatic Judgment-to-paper dispatcher, or notification click-to-trade path. The
Decision Admission contract remains mock-diagnostic only, and no real checkpoint has yet passed the
v4 Query Gate or completed the registered paired Judgment runs. The only executable provider is a
paper-only mock whose optional SQLite truth survives restart for contract tests and exposes no
account capability. Local mock acceptance and the experimental provenance contract do not upgrade
any external Provider or authorize live. These omissions are acceptance criteria, not unfinished
shortcuts.
