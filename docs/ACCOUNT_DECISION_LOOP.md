# Account decision loop

This document owns the path from a sealed Agent judgment to a controlled account action. It does
not grant paper or live capability. The Harness remains the only orchestration, portfolio-policy,
approval, execution-state and reconciliation authority.

## Smallest complete loop

```text
Trigger Admission / scheduled review
  -> frozen evidence and Authorized Decision View
  -> Agent Judgment and portfolio-action proposal
  -> Harness Portfolio Decision
  -> Signal Intent
  -> deterministic Order Sizing Decision
  -> Order Intent
  -> hard policy and Trading Mandate
  -> approval
  -> durable submit / cancel / replace request
  -> Provider
  -> complete order / fill / position / cash reconciliation
```

The two event entrances remain different only before `Trigger Admission`:

- pre-registered policy, earnings and macro checkpoints select the first event matching their
  frozen experimental rule;
- broad material events first require formal Event Impact Triage, an authoritative EventAssessment
  and the deterministic Materiality Gate.

After admission they use the same evidence, Judgment, portfolio, policy, approval and execution
boundaries. A missing optional input is visible degradation; missing authority for a trigger,
tradable target, raw price basis, account reconciliation, mandate, approval or execution state is a
hard blocker at that owning boundary.

## Authorized Decision View

An Agent run may discover and call every registered read-only capability authorized for that task:

- frozen event, expectation, market, industry, macro, positioning and tradability inputs;
- a credential-free Position Snapshot projected from a complete Account State Snapshot;
- open-order and recent-fill summaries needed to avoid duplicate or contradictory actions;
- registered historical analogies and Research Method Skills with their evidence lane intact; and
- local-first Monitoring Scope / Retrieval Resolution for missing information.

The view is broad but not omniscient. It contains exact Snapshot and tool identities, cutoff,
freshness, coverage, licensing and cost limits. When frozen local state is insufficient, the Agent
may request a bounded retrieval need. The Harness selects an accepted route, journals the result and
starts a fresh run over the new Snapshot. A transient HTTP response cannot be appended to the active
decision context.

No research or decision Agent receives a broker session, credential, arbitrary URL fetcher, raw
submission capability or mutable account ledger.

## Portfolio actions and authority

The Agent may propose only trading-related dispositions:

| Proposal | Meaning | Mutation authority |
| --- | --- | --- |
| `abstain` / `observe` / `hold` | no order; optionally create a bounded Watch | Harness only |
| `open` / `increase` | add exposure | Harness sizing, mandate, policy and approval |
| `reduce` / `close` | lower existing exposure | Harness sizing, holdings and approval |
| `rotate` | linked reduce plus open candidates | Harness evaluates each leg; no atomicity claim |
| `cancel` | request cancellation of an open order | execution service after reconciliation |
| `replace` | cancel an exact order and create a new idempotent intent | execution service; never in-place mutation |
| `kill` | stop new dispatch and request operational escalation | Harness kill-switch authority |

The Agent may suggest target, direction, horizon, urgency and desired exposure. It cannot choose the
final executable quantity. The Harness computes or rejects quantity from the exact Signal,
unadjusted Price Basis, lot/tick/tradability rules, complete Account State Snapshot, Trading Mandate
and versioned sizing policy. Model confidence remains observational and cannot size a position.

Deposits, withdrawals, credential access, account-profile or permission changes, data-entitlement
purchases and broker-session administration are outside the Agent tool surface.

## State ownership

`Account State Snapshot` is a content-identified, cutoff-bound, credential-free reconciliation of
cash, positions, open orders, recent fills and explicit gaps for one opaque account reference and
environment. The Provider reports broker facts; the Harness normalizes, persists and decides
whether the snapshot is complete. An incomplete or stale snapshot may support risk notification but
cannot authorize an exposure-increasing order.

`Portfolio Decision` binds one Judgment/Signal candidate, the exact Account State and Position
Snapshots, open-order conflicts and a disposition. It is a proposal-admission boundary, not an
order. `Order Sizing Decision` is deterministic Harness output and is the only path that may create
the quantity used by an Agent-originated `OrderIntent`.

Submit, cancel and replace each have stable request identity, durable outbox state and an
`unknown` outcome for ambiguous transport. No ambiguous operation is retried automatically.
Reconciliation, rather than transport success, establishes broker order, fill, position and cash
state.

## Delivery gates

1. **Read-only portfolio context.** Add normalized Account State and full Position Snapshot
   contracts, fixture Provider acceptance and frozen Agent tools. No mutation capability.
2. **Decision and sizing.** Add portfolio-action proposal plus deterministic sizing/rejection and
   bind them into Decision Admission. Exercise open, increase, reduce, close, abstain and conflicting
   open-order cases against the durable mock.
3. **Operation lifecycle.** Extend the provider-neutral execution port and outbox for cancel and
   replace, complete reconciliation, restart and fault injection. Replacement is cancel plus a new
   intent, never mutation of an admitted order.
4. **Operational control.** Add durable approval inbox/notifications, kill switch and reconciliation
   escalation. Wake remains separately gated.
5. **IBKR paper.** Implement and independently accept `ibkr-nautilus-paper` for account, submit,
   cancel, replace, fill and restart reconciliation. Mock evidence cannot satisfy this gate.
6. **Live.** Require explicit authorization, versioned live mandate and limits, credential
   isolation, tested kill switch and separate live Provider Acceptance.

Backtest and prospective work share EventAssessment, Signal, portfolio-policy and order semantics.
Backtests use cutoff-bound simulated Account State and deterministic fills; paper/live use actual
reconciled state. Neither lane upgrades the other's evidence authority.
