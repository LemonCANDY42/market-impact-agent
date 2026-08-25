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

1. Validate the `OrderIntent`, including its explicit creation-to-expiry window, and its
   parent signal/evidence references.
2. Evaluate non-overridable hard rules.
3. Return `DENY`, `REQUIRE_MANUAL`, or `ELIGIBLE`.
4. Only an `ELIGIBLE` intent may reach a configured semantic auto approver.
5. Record the approval, provider submission, execution events, and reconciliation result.

Hard rules include order-intent and mandate time bounds, account and environment matching,
instrument and side allowlists, positive finite price references, order-notional limits,
provider capability/trust, stale-data thresholds, and kill switches. The semantic approver
can choose a stricter result; it cannot override `DENY` or `REQUIRE_MANUAL`.

Only the paper execution gateway can turn an `ELIGIBLE` result into the sealed capability
accepted by a provider. Its Trading Mandate, clock, and price source are bound by the
trusted composition root, not supplied by the order caller. `policy_auto` remains
fail-closed because the bootstrap has no semantic approver. Direct submission of an
`OrderIntent` is outside the provider contract, and the bootstrap has no live submission
path.

## Notifications

Manual approval requests should support macOS notifications and a durable inbox. A
notification is only an alert: the approval must be bound to the exact intent hash,
expiry, mandate, and actor. Editing an intent invalidates its approval.

## Initial safety state

The bootstrap contains no live provider, credential loader, autonomous approver, or
notification click-to-trade path. The only executable provider is an in-memory paper
mock. These omissions are acceptance criteria, not unfinished shortcuts.
