# ADR 0003: Freeze Agent judgment before deterministic replay

- Status: Accepted
- Date: 2026-08-26

The product must evaluate Agent reasoning over point-in-time, multi-source evidence without
turning a stochastic model loop into a trading or replay authority. Implement a local,
engine-neutral Harness that seals one exact Evidence Pack, runtime configuration, tool
history, and structured proposal into a Judgment Artifact. A deterministic admission
validator may translate an accepted candidate into the existing Signal Intent contract;
Nautilus replays that frozen decision and never calls the model. Re-running the Agent is a
separate model-quality experiment with pre-registered replicates, not replay reproduction.

Full trading-agent products were rejected as runtime dependencies because their debate
topologies, mutable memories, current-data tools, and broker surfaces enlarge the trusted
computing and look-ahead boundary. Their narrow patterns may be cleanly adapted, while the
Harness retains model Provider, journal, context compaction, Skill, MCP, permission, budget,
recovery, and audit ownership.
