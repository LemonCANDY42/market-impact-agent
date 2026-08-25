# Roadmap

Roadmap items are evidence gates, not a feature inventory. A later phase does
not begin because an earlier API exists; its acceptance evidence must exist.

## Phase 0 — Auditable skeleton

- [x] Freeze vocabulary and authority boundaries.
- [x] Publish provider, signal, order, mandate, and policy contracts.
- [x] Add a fail-closed mock execution provider and local verification.
- [ ] Complete independent review and confirm local and remote evidence agree.
- [ ] Tag the accepted bootstrap.

## Phase 1 — Event research vertical slice

- Build immutable Evidence Item and Event Envelope materialization with occurrence,
  publication, visibility, retrieval, revision, and duplicate-claim semantics.
- Implement event-archetype, transmission-channel, directness, revelation-mode, and
  lifecycle contracts without a universal topic ontology.
- Implement fast/deep/combined routing with source-tier and depth/branch caps.
- Produce `event_transmission.json` for one synthetic and one real energy
  supply-shock event.
- Add negative cases: future visibility, missing evidence, contradiction,
  unrelated target, excessive depth, and duplicate reporting.

Exit gate: every path is evidence-linked and independently inspectable; no
broker or backtest mutation is reachable from the research skill.

## Phase 2 — Historical replay and calibration

- Compare the accepted Nautilus 1.x line with the then-current stable 2.x line on Python
  3.13/3.14, then pin one exact engine version.
- Define the narrow engine-neutral backtest request/result port and implement only the
  `NautilusBacktestBridge`; Nautilus types do not cross the Harness boundary.
- Add the Tushare HTTP adapter and fixed pre-event A-share universes.
- Record the data granularity, book type, fill model, fee model, venue rules, engine
  version, adapter version, and configuration in every replay manifest.
- Compare event reasoning with sentiment, momentum, fixed mapping, and simple
  hold-period baselines.
- Evaluate next-day, 3-day, and 10-day horizons with T+1, limits, costs, and
  event-cluster walk-forward splits; repeated frozen runs must be deterministic.

Exit gate: reproducible results beat at least one meaningful baseline without a
single event dominating the outcome. This gate verifies backtesting only; it grants no
paper or live capability. Failure stops expansion.

## Phase 3 — Event-family discovery

- Add market-state/style rotation and probabilistic climate/agriculture cases.
- Research broader single-event and cumulative-narrative families, including
  CPO/AI infrastructure, policy themes, scheduled surprises, and uncertainty
  resolution.
- Promote only repeatable, falsifiable families to reference packs.

Exit gate: event families have pre-registered universes, analogues, negative
cases, and regime tags. Examples alone are not taxonomy evidence.

## Phase 4 — Paper execution

- Add durable intent/outbox and approval records.
- Add an independently registered `ibkr-nautilus-paper` Provider over the pinned Nautilus
  engine and official IB adapter; create a direct IBKR Provider only if that path cannot
  pass lifecycle and reconciliation acceptance.
- Add CLI, MCP approval tools, generic webhook, and macOS notifications.
- Pass crash/restart/reconciliation and duplicate-order acceptance.

Exit gate: paper state reconciles with IBKR after every injected failure.

IBKR Paper acceptance covers Harness-to-Nautilus-to-IB order identity, ambiguous submit
outcomes, partial and duplicate fills, disconnects, gateway and process restart, external
orders, and complete order/fill/position/account reconciliation. VeighNa remains a sibling
Provider bridge, not a NautilusTrader plugin, and must pass a separate acceptance program
on a gateway-supported host before any A-share execution claim is made.

## Phase 5 — Controlled live research

- Add expiring Trading Mandates, hard portfolio limits, notification escalation,
  and a tested kill switch.
- Run `manual_each`, then timeboxed/policy-auto modes with deliberately tiny risk.
- Keep VeighNa A-share live as a separate vendor/host acceptance program.

Exit gate: explicit user authorization plus independent operational review.
There is no scheduled date and no implied entitlement to advance.
