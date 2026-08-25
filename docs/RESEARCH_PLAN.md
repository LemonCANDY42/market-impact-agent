# Event Research Plan

The research engine asks a narrower question than general news sentiment:

> Given what the market already expected and how it is positioned, does this new event
> create a tradable multi-horizon price impact, through which transmission paths, and
> under what invalidation or exit conditions?

## Event representation

An event record separates publication time, event time, source verification, market
expectation, surprise, affected physical or financial variable, transmission edges, and
observable price response. It may contain multiple reports, but duplicate reports do not
become independent evidence.

Transmission uses four layers:

1. **Direct exposure** — revenue, cost, production, inventory, or asset value.
2. **Second-order exposure** — substitutes, customers, suppliers, transport, power,
   financing, or policy response.
3. **Market overlay** — A-share thematic rotation, northbound or institutional flows,
   crowding, relative strength, limits, and liquidity.
4. **Empirical edge** — a repeatable reaction that is not obvious from industry labels;
   it is retained only with causal rationale, recurrence, and out-of-sample evidence.

This explicitly covers effects such as oil shocks propagating beyond producers into
refining, shipping, chemicals, power, airlines, fertilizer, and market-style rotation.

## Historical analog skill

The analog system is hierarchical and lazy-loaded:

1. Match a new event against a compact top-level taxonomy.
2. Load only relevant event-family packs, industry packs, or heavyweight-company packs.
3. Retrieve point-in-time analogs and their transmission paths.
4. Compare surprise, prior positioning, market regime, liquidity, and response shape.
5. Return differences and uncertainty, not just nearest-neighbor similarity.

Initial taxonomy research includes scheduled surprise events, physical supply shocks,
policy/regulatory changes, climate forecasts, capital-flow/style rotation, corporate
actions, and cumulative narratives such as CPO/AI. User-supplied examples seed research;
they do not define the ontology or imply a profitable rule.

## Modes

The router selects `fast` when sources are verified, the mapping is known, and the
decision horizon is short. It selects `deep` for disputed facts, novel transmission,
conflicting market state, or high-risk orders. Deep mode may enrich research but does not
receive broader trading authority.

## First vertical slice

Start with energy physical-supply shocks across crude, refined products, producers,
refiners, shipping, airlines, coal, chemicals, fertilizer, power, and A-share mappings.
Predict next-session, 3-session, and 10-session outcomes plus continuation and reversal
risk. Benchmark against:

- event-time price momentum;
- simple news sentiment;
- fixed industry exposure mapping;
- buying an energy benchmark after the event and holding three sessions.

Only after the slice passes out-of-sample acceptance should the project add the
technology-to-dividend/low-volatility rotation and El Niño-to-agriculture slices.

## Validation rules

- Use source availability time, not article edit time or database ingestion hindsight.
- Model A-share trading constraints: next executable price, T+1, price limits, suspensions,
  lot sizes, fees, slippage, and overnight gaps.
- Split by event families and time, not random news rows.
- Deduplicate syndicated reporting and keep revisions.
- Compare incremental value over price/volume and simple exposure baselines.
- Report coverage, drawdown, turnover, calibration, and tail loss with Sharpe.
- Run a forward paper shadow before any live-validation proposal.

The project fails this stage if the complex system cannot beat simple baselines after
costs or if the result depends on a few hand-selected episodes.
