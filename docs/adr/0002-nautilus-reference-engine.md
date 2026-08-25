# ADR 0002: Use NautilusTrader as the default reference engine

- Status: Accepted
- Date: 2026-08-25

## Context

An entirely neutral Provider abstraction risks becoming a lowest-common-denominator
interface with no production-quality implementation. The project needs one default path
whose backtest, paper, order lifecycle, recovery, and reconciliation behavior can be
tested end to end. It must still support VeighNa and other engines without coupling the
harness to one runtime or complete object model.

NautilusTrader provides a typed event-driven execution architecture spanning simulation
and live systems. Its public documentation distinguishes local denial, venue rejection,
and unknown outcomes; routes risk before execution; tracks stable order and trade
identities; and combines streams with reconciliation. These are appropriate reference
constraints for the harness.

## Decision

Select NautilusTrader as the default trading engine and first real Provider conformance
implementation.

Define an engine-neutral, low-frequency Harness Provider Contract inspired by the public
NautilusTrader execution semantics. Integrate NautilusTrader through a
`NautilusProviderAdapter`; do not expose NautilusTrader objects as the harness API or give
the default adapter a policy/approval bypass.

Use the default path for historical replay first, then for IBKR US/HK paper validation.
Keep VeighNa as a sibling external-process Provider bridge for supported A-share gateways.
Allow a direct IBKR, LEAN, or other Provider when it passes the same conformance suite.

The bootstrap records this decision but does not install, import, enable, or claim a
working NautilusTrader integration.

## Consequences

- The project has one opinionated implementation path instead of several shallow adapters.
- Provider conformance can be derived from tested execution behavior rather than imagined
  portability.
- Backtest and paper execution can share a reference lifecycle and event vocabulary.
- The harness must keep its common contract narrower than NautilusTrader's full OMS,
  strategy, execution-algorithm, and order-type surface.
- VeighNa and other engines incur an adapter, but remain independent of the NautilusTrader
  process and version lifecycle.
- NautilusTrader is an external LGPL-3.0-or-later dependency; copied or modified code, if
  ever needed, requires a separate license-compliance review.
