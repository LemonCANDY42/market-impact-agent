# Lessons From Earlier Prototypes

This bootstrap deliberately absorbs failure modes observed in broad trading-agent
prototypes without inheriting their architecture.

## What failed

- Building ingestion, agents, dashboards, knowledge graphs, backtests, and live trading at
  once produced many surfaces but no trustworthy vertical slice.
- Treating every news item as useful created volume, duplication, and unverifiable causal
  stories rather than signal.
- Letting research outputs and execution state share an informal interface obscured which
  component was authoritative.
- Attractive demonstrations substituted for timestamp-correct, cost-aware, out-of-sample
  evidence.
- Provider names in configuration implied capability even when recovery, idempotency, and
  reconciliation had not been tested.

## Constraints derived from those failures

- One event family and simple baselines precede broad coverage.
- The event casebook is curated and point-in-time; it is not a universal news graph.
- Research emits expiring, cited `SignalIntent` objects. Deterministic policy emits an
  approval outcome. Providers emit execution facts.
- Declared capability and verified capability are separate fields.
- Every complex feature must improve a stated acceptance metric over a smaller baseline.
- No UI, live credential path, or multi-agent debate topology belongs in the bootstrap.

These constraints may be relaxed only through evidence and an architecture decision
record, not because another integration becomes available.
