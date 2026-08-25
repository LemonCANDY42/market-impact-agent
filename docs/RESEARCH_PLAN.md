# Event Research Plan

The research engine asks a narrower question than general news sentiment:

> Given what the market already expected and how it is positioned, does this new event
> create a tradable multi-horizon price impact, through which transmission paths, and
> under what invalidation or exit conditions?

## Event representation

An event record separates occurrence, publication, market visibility, retrieval, source
verification, revision, prior expectation, affected variable, transmission, and observable
price response. It may contain multiple reports, but syndicated copies of one claim do not
become independent evidence. A scheduled future occurrence is valid when its announcement
was visible by `as_of`; the system must not reject it merely because the occurrence lies in
the future.

The model uses orthogonal axes instead of one growing topic list:

1. **Event Archetype** records the root cause of the new information.
2. **Transmission Channel** records how each causal step changes an economic, financial,
   attention, or market-mechanics variable.
3. **Transmission Directness** records causal distance: direct through fourth-order.
4. **Revelation Mode and Event Stage** record how the evidence arrived and how far the
   event has progressed.
5. **Expectation Delta** records observed-versus-expected only when the prior baseline is
   point-in-time and cited; `unknown` is preferable to a fabricated surprise.

### Event Archetypes

Use one primary archetype per Event Cluster. Related causes and consequences become linked
clusters rather than multiple roots forced into one label.

| Archetype | Boundary |
| --- | --- |
| `issuer_corporate` | Earnings, guidance, operations, transactions, capital structure, assets, or governance originating at an issuer. |
| `macro_real_economy` | Inflation, employment, output, inventory, demand, and official forecast or statistical revisions. |
| `policy_regulatory` | Monetary, fiscal, industrial, competition, trade, licensing, tariff, sanctions, and export-control decisions. |
| `geopolitical_security` | Conflict, attack, escalation or de-escalation, and political-security events; a resulting formal sanction is a linked policy cluster. |
| `physical_supply_logistics` | Changes in production, facilities, inventory, transport routes, ports, shipping, throughput, or delivery capacity. |
| `climate_natural_hazard` | Climate forecasts, drought, flood, storm, earthquake, damage, and recovery. |
| `technology_demand_adoption` | Observable product or process innovation, adoption, substitution, demand structure, or competitive change—not promotional theme language alone. |
| `financial_market_mechanics` | Changes originating in funding conditions, index rebalances, forced liquidations, fund flows, trading rules, or liquidity mechanics. |

The list is a compact top-level dictionary, not the final event ontology. Subtypes are
promoted only after source mapping, labeling guidance, negative cases, and validation.

### Transmission Channels

Every path step selects the channel that carries the effect and names its first affected
variable:

- `revenue_demand`;
- `capacity_cost_inventory`;
- `claims_capital_allocation`;
- `policy_access`;
- `funding_discount_fx`;
- `risk_uncertainty_insurance`;
- `expectations_attention`;
- `positioning_liquidity_mechanics`.

A-share thematic rotation, northbound or institutional flows, crowding, relative strength,
price limits, and liquidity therefore belong to a path or market-state overlay—not to the
Event Archetype. An empirical edge that is not obvious from industry labels is retained
only when it has a causal rationale, repeated point-in-time observations, and incremental
out-of-sample evidence.

This permits a geopolitical attack, the physical disruption it causes, a later sanction,
and the resulting A-share flow response to remain distinguishable while still forming one
inspectable causal chain.

## Historical analog skill

The analog system is hierarchical and lazy-loaded:

1. Match a new event against a compact top-level taxonomy.
2. Load only relevant event-family packs, industry packs, or heavyweight-company packs.
3. Retrieve point-in-time analogs and their transmission paths.
4. Compare surprise, prior positioning, market regime, liquidity, and response shape.
5. Return differences and uncertainty, not just nearest-neighbor similarity.

`scheduled`, `unscheduled`, `continuous`, and `retrospective_revision` are Revelation
Modes, not peer Event Archetypes. Event Stage advances through `pre_event`,
`first_observed`, `corroborated`, `quantified_or_realized`, `diffusing`, `resolved`, and
`revised_or_invalidated`.

Cumulative narratives such as CPO/AI are composed assessments over multiple frozen Event
Clusters. They are promoted to lazy-loaded narrative packs only when they have explicit
claims and counterclaims, observable adoption/attention proxies, invalidation conditions,
and incremental evidence over their component events, price momentum, and fixed industry
exposure. User examples seed discovery; they do not define the ontology or imply a
profitable rule.

## Modes

The router selects `fast` only when evidence is verified and the transmission mapping is
established. It selects `deep` for weak or disputed evidence and unknown transmission
mappings. It selects `combined` for high-impact events or a market state that conflicts
with the established mapping. Deep mode may enrich research but does not receive broader
trading authority.

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
