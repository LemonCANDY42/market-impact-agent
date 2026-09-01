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

## Continuous market and account sensing

Market monitoring and position monitoring are one operating loop, not two competing authorities.
The Harness continuously collects accepted source routes, while either a new-evidence trigger or a
scheduled portfolio review freezes one Authorized Decision View and starts a bounded Agent run. The
same view may include:

- news, official releases, macro vintages, expectations and positioning;
- raw tradable bars plus cutoff-correct adjusted research series, volume, turnover, liquidity and
  volatility;
- stock and index valuation/context fields such as PE, PB, market value and dividend yield;
- instrument status, limits and the then-effective index, industry and ETF constituent graph; and
- credential-free cash, positions, open orders, recent fills, concentration and account gaps.

A broad discovery run may find a new issuer, industry, ETF, constituent set or information aspect
and request a bounded Watch. A holdings review starts from every current position and open-order
conflict before considering new exposure. Both may request accepted local-first retrieval, but only
the Harness can collect, journal and freeze new data for a fresh Run.

The Agent keeps three conclusions distinct:

1. **Market/company thesis:** what changed, confirming and disconfirming evidence, catalyst and
   invalidation conditions.
2. **Security readiness:** whether the mapped stock or ETF is liquid, tradable, valued and timed well
   enough to consider.
3. **Portfolio action:** whether the exact reconciled account should observe, hold, reduce, close,
   rotate or add exposure.

There is no requirement to trade every day and no universal model-authored score. Missing optional
context is visible; missing or stale account truth can still support a risk alert or reduction
proposal, but cannot authorize an order when position or open-order coverage is uncertain. This
separation prevents a valid macro thesis from silently becoming an unsuitable order for the current
account.

## Authorized Decision View

An Agent run may discover and call every registered read-only capability authorized for that task:

- frozen event, expectation, market, industry, macro, positioning and tradability inputs;
- a credential-free Position Snapshot projected from an accepted Account State Snapshot;
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
unadjusted Price Basis, side-applicable lot/tick/tradability rules, trusted Account State Snapshot,
Trading Mandate and versioned sizing policy. Model confidence remains observational and cannot size
a position.

Deposits, withdrawals, credential access, account-profile or permission changes, data-entitlement
purchases and broker-session administration are outside the Agent tool surface.

## State ownership

`Account State Snapshot` is a content-identified, cutoff-bound, credential-free reconciliation of
cash, positions, open orders, recent fills and explicit gaps for one opaque account reference and
environment. The Provider reports broker facts; the Harness normalizes, persists and decides
whether the snapshot is complete. An incomplete or stale snapshot may support risk notification,
but an order requires authoritative position and open-order coverage; exposure increase additionally
requires a gap-free view.

The serialized account reference is a Harness-keyed pseudonym, not an unkeyed hash of a broker
identifier. The pseudonymization key is atomically published into private state before any reader can
use it, and neither the key nor the raw reference enters the Snapshot or Agent view. A Position
Snapshot cannot be evaluated before its reconciliation timestamp.

The first real read-only adapter now connects only to a local IB Gateway Paper session with a
nonzero client identity and exposes no submit, cancel or replace method. It waits for API readiness,
then independently closes the account-download, account-summary, all-open-orders and executions
barriers before the Harness can mint an Account State Snapshot. Closing the fourth barrier freezes
one immutable callback state under the reader lock; later broker callbacks cannot drift content
beneath the earlier reconciliation time. Cash, positions, open orders and recent fills use typed
absence when a section cannot be proven complete; broker account and
order/execution identifiers are keyed pseudonyms. Its 2026-09-01 local acceptance observed one cash
record and empty position, API-open-order and recent-fill sets. IBKR requires client 0 binding to
observe manually submitted TWS orders, which is outside this non-mutating adapter; the snapshot
therefore retains `manual_tws_open_orders_not_observed`, remains usable for position-risk review and
blocks every order mutation, including an apparently risk-reducing sell: an unseen manual order may
already close or reverse the position. Exact amounts, identifiers and artifacts remain in ignored
private state.
The separate `ibkr-nautilus-paper` candidate probe has also completed one real mutation-free local
run through pinned NautilusTrader `1.231.0` and the official IB adapter. It waits until Nautilus has
connected, reconciled execution state, initialized its portfolio and started a zero-Strategy Trader,
then compares its account/open-order/open-position counts with the direct reader. That proves the
selected engine can observe the same bounded Paper account state; it neither removes the manual-TWS
coverage gap nor exercises an order command. The report and logs remain in ignored private state.
The Authorized Decision View recomputes freshness at its own cutoff instead of copying an earlier
readiness result, rejects exposure-increase readiness whenever any account observation gap remains,
and mints the read tool only for the exact content-identified Position Snapshot it names. A
caller-supplied lookalike tool is not part of this authority boundary. Cutoff and freeze instants are
canonical UTC in serialized content identity, so equivalent aware timestamps cannot fork replay IDs.

`Portfolio Decision` binds one Judgment/Signal candidate, the exact Position Snapshot,
open-order conflicts and a disposition. Decision Admission v2 additionally binds its parent Account
State Snapshot. The paper composition root reprojects the Position Snapshot from that trusted
parent, rebuilds the Authorized Decision View, and checks venue/class against a trusted Instrument
Master projection. `Portfolio Decision` is a proposal-admission boundary, not an order. `Order
Sizing Decision` is deterministic Harness output and is the only path that may create the quantity
used by an Agent-originated `OrderIntent`.

Agent-directed admission, human approval and dispatch each re-read the trusted current Account State
source. The exact snapshot must still match the admitted parent and remain inside the Harness
composition-root maximum age; a caller-provided Position Snapshot cannot widen it. Dispatch repeats
the check after the durable claim and the Provider capability validator repeats it with all hard
expiries immediately before acceptance. Otherwise the pending approval or claimed outbox row expires
without a Provider call and a fresh decision is required. A manual approval never extends
account-state validity. Legacy Decision Admission v1 is replay-only and cannot mutate paper state.
Close quantities also obey the applicable lot rule; the Harness does not silently submit a
nonconforming full-position quantity.

Submit and cancel each have stable request identity, a durable attempt lease and an `unknown`
outcome for ambiguous transport. No ambiguous operation is retried automatically. A replacement is
never an in-place mutation: it durably links one exact cancellation to a new Order Intent identity,
and that new intent cannot be admitted until complete reconciliation proves the old provider order
canceled. An Agent-directed replacement additionally requires a fresh Decision Admission for the
new quantity, price and account state.

Cancellation uses an optional Provider port and a sealed Harness capability bound to the exact
request, manual approval, Provider identity/version, provider order and current durable attempt. A
Provider command receipt is only acknowledgement; a globally complete reconciliation must
explicitly report `canceled` before the Harness marks either the cancellation or original order
terminal. Pending cancellation work has priority over new submissions. Every later complete
reconciliation also rechecks each durable accepted open order rather than treating its first
reconciliation as permanent truth.

The durable kill switch blocks every new submission claim while leaving exact cancellation and
reconciliation available. It does not silently mass-cancel orders, and it cannot be cleared until a
new complete reconciliation has occurred after activation. Reconciliation v2 binds the kill-switch
generation observed before the Provider snapshot call, so a pre-activation snapshot cannot race and
clear a later kill. Restart with any durable accepted open order also blocks new execution until a
fresh reconciliation, including legacy rows that cannot prove their Provider binding. Transport
success never establishes broker order, fill, position or cash state.

## Delivery gates

1. **Read-only portfolio context.** Complete. Normalized Account State and full Position Snapshot
   contracts, fixture acceptance, one real local IB Gateway Paper read and the frozen
   `AuthorizedDecisionView`/`read_position_snapshot` Agent tool are accepted. No credential or
   mutation capability is exposed. This is bounded `ACCOUNT` read acceptance only; its explicit
   manual-order coverage gap keeps all order mutations closed.
2. **Decision and sizing.** Complete for the provider-neutral `manual_each` mock path. Content-
   identified Portfolio Decision and Order Sizing Decision contracts bind the exact Signal, parent
   Account State, Authorized Decision View, Position Snapshot, Trading Mandate, raw Price Basis,
   trusted Instrument Master identity, side-applicable venue/class rule and versioned sizing policy.
   Exposure increase requires a gap-free view and no conflicting open order. Reduction and close
   require authoritative open-order coverage; the current accepted A-share rule artifact only claims
   ordinary buy-order scope, so long-position sells remain fail-closed until an accepted sell rule is
   added. The Agent cannot write quantity and confidence never sizes an order. Decision Admission v2
   reopens this full chain after restart before approval or dispatch; both stages also require the
   bound Account State to remain current and unchanged. Open, blocked increase, blocked uncertain
   reduction, non-lot close rejection, hold, rotate rejection, adjusted-price rejection and the
   complete manual approval→mock accepted→reconciliation path pass locally. This is still synthetic
   account/mock execution acceptance, not external broker-paper evidence.
3. **Operation lifecycle.** Complete for the Provider-neutral mock boundary. Cancel has exact manual
   approval, sealed capability, durable attempt state, restart recovery, ambiguous-ACK handling and
   reconciliation-established terminal state. Replace is cancel plus a new intent and cannot submit
   before the old order is reconciled canceled; cancellation creation and replacement linkage share
   one SQLite transaction. This does not accept an external Provider or yet
   prove close/rotate portfolio behavior against a broker.
4. **Operational control.** The durable kill switch is complete for the mock boundary: it blocks new
   submissions, preserves cancel/reconciliation, survives restart and requires a post-activation
   complete reconciliation before clearing. Durable approval inbox/notifications and reconciliation
   escalation remain open. Wake remains separately gated.
5. **IBKR paper execution.** Implement and independently accept `ibkr-nautilus-paper` for submit,
   cancel, replace, fill, ambiguous acknowledgement, restart and complete account reconciliation.
   The accepted direct account read, mutation-free Nautilus readiness probe and mock evidence cannot
   satisfy this gate.
6. **Live.** Require explicit authorization, versioned live mandate and limits, credential
   isolation, tested kill switch and separate live Provider Acceptance.

Backtest and prospective work share EventAssessment, Signal, portfolio-policy and order semantics.
Backtests use cutoff-bound simulated Account State and deterministic fills; paper/live use actual
reconciled state. Neither lane upgrades the other's evidence authority.

## Decision-path parity and replay

Backtest, paper and live use the same decision path up to the execution Provider. They must bind the
same frozen evidence/tool view, Agent runtime and Skill surface, Judgment validation, Signal,
portfolio-action policy, deterministic sizing, Order Intent, hard policy and mandate semantics.
Environment-specific code may supply only the owning facts it must own:

- a backtest supplies cutoff-bound simulated account state and later deterministic fills;
- paper supplies reconciled paper-account state and broker events; and
- live supplies separately authorized live-account state and broker events.

This is decision-path equivalence plus replay-safe idempotence, not a claim that a nondeterministic
model will independently emit byte-identical text. The first physical model call is journaled under
one Run identity; restart or audit reopens its frozen response and tool results rather than calling
the model again. A materially different Snapshot, retrieval result, account state or policy creates
a new Run. Research features may use cutoff-correct adjusted/total-return prices, while fills, fees,
limits and order prices always use the raw tradable basis.

## Effectiveness evidence

Engineering acceptance and investment effectiveness are separate. The common loop must first prove
that it can replay the same frozen inputs, account state, policies and costs across historical,
mock-paper and broker-paper environments. Strategy or Skill promotion then requires independent
chronological cases in its declared market, event family and horizon, with later outcomes opened only
after the decision is sealed.

Reports compare after-cost results against the relevant cash/no-action, scheduled investment,
index/ETF buy-and-hold and simple trend or volatility baselines. They report return together with
maximum drawdown, tail loss/CVaR, Sharpe and Sortino, adverse excursion, downside capture, turnover,
liquidity and the opportunity cost of avoided exposure. A claimed risk-avoidance decision must show
the loss reduced relative to holding and also disclose rallies it missed. One lucky event, one market
regime or a lower drawdown obtained only by staying in cash cannot promote a strategy.

A reusable general or domain-specific Skill remains a candidate until it has more than one
independent validation, no unresolved material counterexample or conflict with an existing Skill,
and an observational trace showing when it was offered, loaded, reported as used and influenced the
proposal. Promotion is limited to the demonstrated domain; with/without-Skill comparisons test
incremental value rather than attributing the whole Agent result to the Skill.
