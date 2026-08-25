# ADR 0002: Use NautilusTrader as the default reference engine

- Status: Accepted
- Date: 2026-08-25

## Context

An entirely neutral engine abstraction risks becoming a lowest-common-denominator
interface with no production-quality implementation. The project needs one default
foundation whose backtest behavior is tested first and whose later paper lifecycle,
recovery, and reconciliation can be validated separately. It must still support VeighNa
and other engines without coupling the Harness to one runtime or complete object model.

NautilusTrader provides a typed event-driven execution architecture spanning simulation
and live systems. Its public documentation distinguishes local denial, venue rejection,
and unknown outcomes; routes risk before execution; tracks stable order and trade
identities; and combines streams with reconciliation. These are appropriate reference
constraints for the harness.

## Decision

Select NautilusTrader as the default foundational trading and backtest engine and the
behavioral reference for engine integration.

Define separate engine-neutral, low-frequency Harness ports for backtest requests/results
and execution Providers, informed by NautilusTrader's public command, event, risk, clock,
simulation, and execution semantics. Do not expose NautilusTrader objects as the Harness
API or give the default engine a policy/approval bypass.

Implement a `NautilusBacktestBridge` for historical replay first. Later register IBKR
US/HK paper as a separately scoped `ibkr-nautilus-paper` Provider over a pinned Nautilus
version and the official IB adapter. Keep VeighNa as a sibling external-process Provider
bridge for supported A-share gateways. Allow a direct IBKR, LEAN, or other integration
when it passes the relevant Harness acceptance suite.

Do not pin a Nautilus version in the bootstrap. Phase 2 first compares the accepted 1.x
line with the then-current stable 2.x line on Python 3.13 and 3.14; release-candidate
availability alone is not production acceptance.

The bootstrap records this decision but does not install, import, enable, or claim a
working NautilusTrader integration.

## Consequences

- The project has one opinionated implementation path instead of several shallow adapters.
- Backtest acceptance and Provider conformance are separate evidence gates rather than an
  implied capability inherited from the engine.
- Backtest and paper may share Nautilus strategy, command, and event models where
  supported, but their data, fill, venue, and recovery behavior remain separately tested.
- The Harness must keep its common contracts narrower than NautilusTrader's full OMS,
  strategy, execution-algorithm, and order-type surface.
- VeighNa and other engines incur an adapter, but remain independent of the NautilusTrader
  process and version lifecycle.
- NautilusTrader is an external LGPL-3.0-only dependency; copied or modified code, if
  ever needed, requires a separate license-compliance review.
