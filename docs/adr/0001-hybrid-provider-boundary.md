# ADR 0001: Use a hybrid provider boundary

- Status: Accepted
- Date: 2026-08-25

## Context

The Harness must integrate mature trading engines, brokers, and market-data systems
without reimplementing them. Candidate systems expose different integration
surfaces: Python APIs, long-running gateways, REST, gRPC, and MCP. Agents also need a
standard way to discover callable capabilities. Transport standardization alone cannot
guarantee order identity, recovery, or account truth.

## Decision

Use small typed Harness ports by capability. Backtest engines use a backtest request/result
port; market-data and execution Providers use their corresponding contracts and may be
native Python, MCP, HTTP, or gRPC adapters. Register every external Provider through a
versioned capability manifest. Separate declared from verified capability and require
stream plus reconciliation support before live validation.

Agents create `SignalIntent` and `OrderIntent` objects. Deterministic hard policy and the
configured approval state run in the harness gateway before provider invocation. The
trusted composition root binds the mandate, clock, and reference-price source into that
gateway. The gateway binds an eligible intent with no pending approval to a sealed
submission capability; execution providers accept that capability and reject raw intents.
Providers remain authoritative for broker acknowledgements, fills, positions, and cash;
the harness remains authoritative for evidence, policy, approval, and audit.

## Consequences

- Existing engines can be adopted without making MCP a universal engine or broker
  protocol.
- The same policy and audit path applies to native and remote integrations.
- Each live adapter needs explicit recovery and reconciliation certification.
- The bootstrap can ship useful contracts and a mock without implying broker readiness.
- Adapter-specific data models must be translated and may expose only a conservative
  common subset initially.
