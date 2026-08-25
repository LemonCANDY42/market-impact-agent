# Project Goal

## Objective

Build an auditable event-driven trading agent harness that transforms
point-in-time evidence into layered market-impact assessments, versioned signal
intents, and policy-gated actions executed by replaceable quantitative engines.

The product advantage should come from event understanding, fast-versus-deep
routing, historical analogue selection, multi-level transmission, persistence
and exit reasoning—not from rebuilding broker infrastructure.

## First useful outcome

Given a target market and `as_of` time, the system can:

1. construct an Event Envelope without future information;
2. choose fast, deep, or combined assessment;
3. trace direct and secondary Transmission Paths with evidence and blockers;
4. emit a time-bounded Signal Intent and explicit invalidation conditions;
5. replay that frozen intent through a mature backtest engine;
6. compare it with simple baselines after realistic costs and market rules;
7. route a paper Order Intent through hard policy, approval, execution, and
   reconciliation without duplicate submission.

NautilusTrader is the default foundational trading and backtest engine and the behavioral
reference for engine integration. The Harness contract remains engine-neutral and does not
expose NautilusTrader types. Historical replay through a Harness-owned Nautilus backtest
bridge comes first; IBKR and VeighNa Provider adapters are later, independently validated
integrations.

## Success criteria

The project advances claims one gate at a time:

| Gate | Required evidence |
| --- | --- |
| Contract | Versioned artifacts reject missing or future-visible evidence. |
| Research | One fixture and one real event produce inspectable 3–4 level paths, including counterevidence. |
| Backtest | Event-cluster walk-forward evaluation uses executable prices, costs, and fixed pre-event universes. |
| Paper | IBKR Paper survives submit-before-ack, ack-before-persist, partial-fill, disconnect, and restart cases without duplicate orders. |
| Live | A bounded Trading Mandate, kill switch, reconciliation, and independent acceptance exist; live remains disabled until then. |

No gate is satisfied by a mocked UI, a generated report, a passing schema test,
or an LLM claim alone.

## Initial market boundary

- A-share historical research and backtesting are primary.
- U.S. and Hong Kong paper validation use IBKR when the Paper gate is reached.
- A-share live execution is deferred until a specific VeighNa/vendor gateway is
  independently validated on a supported host and brokerage environment.

## Research sequence

The first infrastructure slice uses an energy physical-supply shock because it
has observable international leading assets and multi-level A-share effects.
After that slice passes, technology-to-dividend rotation and El Niño-to-
agriculture are added as distinct mechanisms. A wider historical discovery pass
then evaluates scheduled surprises, uncertainty resolution, policy events,
cumulative narratives such as CPO/AI infrastructure, and other event clusters.

Examples seed the research; they do not define the final taxonomy.

## Non-goals

- guaranteed profitability, Sharpe, hit rate, or market-beating claims;
- a universal news knowledge graph;
- a general-purpose multi-agent framework;
- a new matching engine, portfolio ledger, broker SDK, or market-data lake;
- live trading enabled by default;
- a UI, hosted service, or multiple execution engines in the first slice.
