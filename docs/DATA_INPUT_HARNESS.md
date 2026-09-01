# Data Input Harness

## Purpose and boundary

The Data Input Harness is the single read-only acquisition and replay boundary between external
data sources and Judgment. It turns a semantic query into immutable Source Observations and one
content-identified Data Snapshot. It does not decide whether an observation is Evidence, infer a
Transmission Path, choose a trade, or submit an order.

The Harness owns query identity, source order, Provider/version binding, cutoff enforcement,
degradation, local persistence, and the artifact returned to an Agent tool. A Provider fetches and
normalizes one registered upstream source. The model cannot select a Provider, move the cutoff,
change the required-source policy, read credentials, or bypass Evidence promotion.

The generic contracts and local persistence adapter are in
`src/market_impact_agent/data_inputs.py`. The first concrete source adapter is the prospective
RSS/Atom path in `src/market_impact_agent/syndication_feed.py`; it proves actual receipt of feed
metadata and excerpts only. It does not establish historical PIT, article-body rights, publisher
completeness, or execution readiness. The first accepted A-share route is the CSRC official
publication Provider in `src/market_impact_agent/csrc_news.py`. Its bounded trial uses the same
Harness and Snapshot contract and accepts prospective private-research collection only. The
`tushare-observation` Provider adds twenty-three separately configured private routes for news, market,
instrument, industry, positioning, macro-schedule, and analyst-forecast observations. Each route
has its own content identity and acceptance report; sharing one transport never merges their
semantics or acceptance. The `nbs-macro-release` Provider uses the exact official latest-release
RSS only for discovery and retains each selected direct CPI/PPI article plus its required XLSX as
the original-release authority. It makes no correction or revision-lineage assertion.

## Bounded-context canvas

| Item | Definition |
| --- | --- |
| Context | Data acquisition and frozen replay input |
| Inputs | A Harness-authored Data Query, registered Observation Providers, and source credentials held outside model context |
| Outputs | Source Observations, typed Provider attempts, and a Data Snapshot |
| Owns | Query identity, source policy, PIT cutoff, actual receipt, cache identity, degradation, and persistence |
| Does not own | Evidence admission, event interpretation, Transmission Path, horizon choice, target approval, orders, fills, or broker state |
| Upstream | Direct publishers, official releases, market-data vendors, aggregators, archives, and instrument masters |
| Downstream | Evidence promotion, Event Envelope construction, frozen Agent tools, Backtest Requests, and later paper/live research cycles |

## Canonical contracts

### Data Query

A Data Query binds one semantic capability to:

- a UTC `as_of` cutoff and optional window start;
- immutable parameters;
- an ordered Provider/version/upstream-source list;
- the complete hash of every registered Observation Provider manifest;
- the complete hash of every secret-free Source Route Configuration;
- required-source flags and a minimum number of sources with accepted data; and
- a versioned `source_policy_id`.

Changing any field changes `query_id`. Host time, implicit fallback sources, credential values, and
model-selected Provider names are not query fields.

New writes use `market-impact.data-query.v2` and `market-impact.data-snapshot.v2`: every source
binding includes its public Source Route Configuration hash. Existing v1 snapshots remain
content-identical, readable, and available only for `cache_only` replay; a missing v1 cache entry
cannot be refetched through a new Provider route.

### Source Route Configuration

A Source Route Configuration freezes the part of a connection that may safely enter an audit
artifact: stable source ID, request URL, expected final redirect URL, publisher, content scope, and
license scope. The Harness stores the exact configuration in its private content-addressed store and
rejects a query when the active Provider's configuration hash differs. Credentials, cookies, account
identifiers, and entitlement tokens never enter this configuration.

### Source Observation

The generic Source Observation preserves Provider and upstream identity, source record and lineage,
the existing `ObservationTimes`, optional historical authority, raw-content hash, normalized payload,
and license scope. The Provider supplies both its full raw response and the exact raw bytes for every
normalized observation. The Harness verifies each record hash and stores those bytes in the private
content-addressed store; missing, misbound, or mismatched raw records turn the Provider attempt into
a typed failure. Normalization does not promote the record to Evidence.

### Data Snapshot

A Data Snapshot binds the exact query, one attempt per registered source in order, accepted Source
Observations, typed failure states, rejection counts, and completion time. A source record is exposed
only when `available_at <= query.as_of`. Missing availability and post-cutoff records are counted and
excluded. Coverage is complete only when every required source completed and enough sources supplied
at least one accepted record. A prospective snapshot also remains incomplete until actual receipt has
reached its declared `as_of`; an earlier collection is an auditable diagnostic, never a successful
cache entry for a future cutoff.

Incomplete snapshots are retained for audit but are not reused as successful cache entries. A later
retry creates another immutable snapshot instead of rewriting the failure.

The query selects one explicit PIT lane. `strict` rejects modeled-latency availability and requires
`available_at <= authority_at <= cutoff`; `modeled` preserves the authority gap for process
diagnostics; `prospective` requires `available_at == authority_at == retrieved_at` under actual
receipt. `retrospective` archives material first received later at its real receipt time, with any
historical authority gap intact, for postmortem analysis only. The lanes share the record shape but
cannot satisfy one another's claim gates; retrospective material is never a strict or prospective
strategy input.

The prospective syndication command uses two phases. It first collects each registered HTTP response
and freezes its exact actual receipt timestamp. It then builds a query whose `as_of` is the latest
of those receipts and replays only those captured responses through the Harness. Earlier captured
responses remain visible at that cutoff, and the replay performs no second remote request. The
operator cannot supply a different cutoff to that command.

## Agent tool surface

The intended semantic tools are:

| Tool | Returns | Does not return |
| --- | --- | --- |
| `lookup_event_revelation` | Newly available official/news facts and their versions | A conclusion that an event is bullish or bearish |
| `lookup_prior_expectation` | Cited forecast observations or explicitly derived and identified baselines | An invented consensus or expectation delta |
| `lookup_market_context` | PIT prices, breadth, volatility, liquidity, and relevant market state | Adjusted execution prices or a trade |
| `lookup_exposure_candidates` | Effective-dated industry/index/ETF/stock mappings and tradability fields | A stock pick or approved universe |
| `lookup_positioning` | PIT holdings, flows, financing, shorting, or related positioning observations | A causal explanation |
| `lookup_macro_vintage` | Original releases and revision lineage | A current backfill mislabeled as an original vintage |

`DataToolBinding` builds these as ordinary read-only `ToolDescriptor` instances. The binding closes
over the cutoff, source policy, Provider versions, and cache mode. Agent arguments contain only the
domain parameters allowed by that tool's JSON Schema.

`FrozenDataSnapshotToolBinding` covers the second tool mode: it binds one already materialized,
complete Snapshot ID directly into the tool manifest version, then allows only local text/publisher
selection and a result limit. Descriptor creation requires the enclosing run's explicit
`FrozenDataSnapshotInput` declaration to authorize that exact Snapshot ID. Those arguments cannot
create another Data Query or move the cutoff. The binding is ready for a registered diagnostic, but
the generic Agent CLI does not add it implicitly; an undeclared complete snapshot cannot become a
source of Agent context.

New `ProspectiveCheckpointSnapshotSet` artifacts use schema v2 with tool manifest v2 and return
Checkpoint Decision Inputs rather than exposing Provider-specific `normalized_payload` rows as the
Agent contract. The published schema continues to validate legacy schema-v1/manifest-v1 artifacts
without reinterpreting them as v2 output. Each new record has a content ID and keeps its Observation
ID, Snapshot ID, source-specific route kinds, source/lineage,
occurred/published/source-updated/available/authority times, and explicit price-basis and
completeness-gap fields. The projection normalizes names only: it does not infer consensus,
expectation surprise, causal direction, then-effective taxonomy when the source interval is absent,
total return, or execution eligibility. The enclosing tool result is also content-identified and
remains bound to the immutable checkpoint barrier and authorized Snapshot set.

`CheckpointMarketUniverseView` is the narrow multi-record consumer for PDI-11 through PDI-13. It
accepts only validated Checkpoint Decision Inputs from one Snapshot Set and one content-identified
exchange rule set. It may join an ETF master `index_code` directly to a taxonomy observation, or
join an ETF to its same-session exchange PCF constituent code and then to an effective current
Shenwan membership and exact SW2021 taxonomy code. When the membership route identifies only the
Shenwan family, the join retains `taxonomy_version_unverified` rather than assigning a version to
the source row. Both paths are exact-code only; names never create a mapping. The
view normalizes SSE/SZSE venue aliases and attaches the general auction buy-lot and tick rules
effective at the barrier. It does not rewrite source records or create a composite Snapshot. A
current SW2021 observation is reported as `observed_at_barrier`; without an effective taxonomy
interval its `effective_at_barrier` remains unknown. PCF quantity is not silently converted into
industry weight, and PCF publication/revision gaps remain explicit. Likewise, an eligible listed
ETF with a raw daily bar remains decision-time-tradability `unverified` until suspension/status
evidence is present. Index prices remain non-executable, and raw fund prices do not become adjusted
or total-return research series.

The bundled 2026 exchange rule set cites the current
[SSE trading rules](https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml)
and [SZSE trading rules](https://www.szse.cn/lawrules/rule/trade/current/t20260424_620190.html),
effective 2026-07-06. It covers ordinary auction buy orders only: 100 shares/units for A-shares and
funds, with CNY ticks of 0.01 and 0.001 respectively. Instrument-specific exceptions, odd-lot sales,
and later exchange adjustments remain explicit exceptions rather than inferred defaults.

A Transmission Path remains a cited Judgment output assembled from facts and exposure evidence. A
mechanism-appropriate horizon set remains a versioned research-method input. Treating either as a
vendor field would hide an inference inside the data plane.

### Prospective diagnostic binding

The `Prospective Diagnostic Registration` freezes requirements before acquisition. It names three
first-eligible future event mechanisms and fixes each cutoff rule, all six declared capability
slots, route/source targets, freshness and gap limits, allowed instruments, horizons, paired arms,
the replicate rule, the aggregate model budget, hidden-outcome rule, and stop/go conditions. It
deliberately contains no Provider IDs and grants no model or execution authority. The immutable v1
registration retains its original all-required semantics. The superseding v2 registration requires
only the actual-receipt event trigger for model dispatch; expectation, market, exposure, positioning,
and macro slots are optional observed information whose absence remains part of the input. V3 keeps
that boundary while starting with two complete control/treatment pairs and requiring the third
complete pair only when either arm's first two decisions disagree.

`ProspectiveCheckpointRoutePlan` separately pre-binds those registered semantic route kinds to
accepted durable Collection Jobs. A separate SQLite/CAS admission record uses the Harness clock to
prevent observations already known before admission from being promoted as future triggers; the
checked-in plan cannot backdate or authorize itself. Admission takes the SQLite write lock before
sampling that clock and commits the timestamp and CAS binding in the same transaction, so a
qualifying Journal write cannot interleave before durable admission. The plan does not make a
Provider an authority and does not select an event. The plan's canonical content and schema fix
this behavior as `sqlite_begin_immediate_then_harness_clock_v1`; a pre-protocol plan ID or admission
row cannot be grandfathered into the current loader or readiness audit.
`ProspectiveCheckpointReadinessReport` then
audits only source identity, Job activation, and content-identified post-admission observation
versions. Opportunity evidence is truncated to scheduled, started, and completed times no later
than `evaluated_at`. If the mutable Job row was updated later than that cutoff, historical health is
not reconstructible from the current schema and the audit fails closed rather than borrowing its
current lag, backoff, failure, or miss state. Its states intentionally distinguish
`waiting_for_post_admission_trigger` from
`trigger_route_unconfigured`; neither state spends model budget. An observed version remains an
unclassified candidate until a separate eligibility selection and session-barrier calculation are
sealed.

`ProspectiveCheckpointSnapshotSet` is the later non-authoritative barrier reconciliation. Every
selected input must still bind an accepted route, exact Collection Policy, complete Journal-frozen
prospective Snapshot, raw response hash, and immutable barrier. Schema v2 retains the original
capability-complete artifact. Schema v3 can freeze a partial set, carries every unmet route,
diversity, observation, and freshness target as `capability_gaps`, authorizes only the Snapshots that
actually passed structural validation, and materializes tools only for present capabilities. It
never combines observations into a new data authority.

`ProspectiveQueryGateResult` classifies those gaps. A missing required event trigger, invalid
cutoff/Snapshot identity, or failure of a registered dispatch minimum blocks the model call. Missing
optional information, missing corroborating routes after one valid trigger, and Evidence Pack data
gaps are passed through as nonblocking context. The Agent may propose or abstain under partial
observation; evaluation stratifies results by the frozen missingness rather than silently treating
coverage as complete.

For frozen historical experiments the tool mode is `cache_only`. A scheduler or Harness operation
must acquire and freeze the snapshot before the Judgment Run. Prospective collectors first freeze
actual HTTP receipts, then replay those receipts under their generated latest-receipt cutoff. A
subsequent remote fetch is a new collection, never an addition to the already frozen snapshot.

## Storage and existing-framework integration

The local adapter uses content-addressed raw/JSON artifacts plus an append-only SQLite index. This gives
the framework a server-free acceptance path and preserves repository boundaries. The store interface
is replaceable; licensed bodies and private rows remain outside Git.

Bulk data should use the component best suited to its semantics:

- [NautilusTrader `ParquetDataCatalog`](https://nautilustrader.io/docs/latest/concepts/data/) for
  execution-grade bars, ticks, instruments, and deterministic replay. Adjusted research series and
  news Evidence do not become fill prices merely because they share a snapshot reference.
- Parquet/Arrow for immutable columnar observations. DuckDB may be added as a local query adapter
  when predicate/projection pushdown materially improves research access; it is not an authority or
  mandatory runtime dependency.
- [Feast point-in-time retrieval](https://docs.feast.dev/getting-started) as an optional later adapter
  for reusable derived features and historical/online serving consistency. Feast event-time joins do
  not prove publisher version authority or reconstruct an unavailable news revision.
- [OpenBB Provider extensions](https://docs.openbb.co/odp/python/developer/extension_types/provider)
  as optional connector implementations. Every connector remains behind the Observation Provider
  manifest and must pass timestamp, revision, license, and degradation acceptance.
- PostgreSQL/Timescale, an object store, Kafka, or an online feature store only after measured scale
  or paper/live latency requires them. They implement persistence or transport; none becomes a second
  orchestration authority.

## Aggregate and relationship map

```text
Data Query
  -> ordered Data Source Bindings
      -> Observation Provider calls
          -> Provider attempts + Source Observations
  -> Data Snapshot
      -> optional Evidence promotion
          -> Event Envelope / Evidence Pack
              -> Judgment Run
                  -> Signal / Order Intent
                      -> hard policy -> trading engine
```

The Data Snapshot is the acquisition aggregate. Evidence Pack remains the Judgment-input aggregate;
Backtest Request remains the replay aggregate; Trading Mandate and broker state remain execution
aggregates. Their identifiers may reference one another, but their authority does not merge.

## Invariants

1. One query has one capability, one cutoff, one source policy, and an exact ordered source set.
2. Provider identity, version, capability, and upstream source must match the registered binding.
3. Provider exceptions and identity drift become typed failed attempts, never empty data.
4. Only records with known `available_at` at or before the cutoff reach the Agent-facing snapshot.
5. Retrieval time never substitutes for historical availability or authority.
6. An incomplete snapshot is auditable and retryable but cannot satisfy `cache_only`.
7. The Agent cannot pass Provider IDs, source policy, cutoff, credentials, or cache mode as tool
   arguments.
8. Data tools are read-only with respect to markets and brokers. Their local artifact writes do not
   grant execution capability.
9. A Data Snapshot does not qualify Evidence and does not prove historical completeness.
10. Historical backtests, prospective paper research, and live research share contracts but use
    separately admitted snapshots and source policies.

## Main scenarios

### Historical replay

1. An operator registers exact historical source versions and a cutoff.
2. Acquisition materializes a complete Data Snapshot or retains an incomplete diagnostic.
3. Qualification checks availability and authority before Evidence promotion.
4. The Agent receives cache-only tools over the frozen snapshot.
5. The Backtest Request cites the accepted market-data snapshot; Nautilus uses raw executable prices.

### Prospective paper research

1. The collector records one immutable HTTP receipt for every registered source.
2. It generates the snapshot cutoff from the latest actual receipt and replays exactly those
   captured responses through the Harness.
3. The Agent queries only that snapshot and produces a Judgment.
4. Hard policy may later create an intent; paper execution remains separately gated.

### Degraded source

1. One required Provider is rate-limited or returns an identity mismatch.
2. Other records remain retained, but coverage is incomplete.
3. No fallback source is inserted implicitly and no successful empty result is fabricated.
4. The same query may be retried; the failure snapshot remains immutable.

## Acceptance sequence

PDI-01 v1 through v4 remain immutable. The current v5 registration
`prospective-diagnostic-registration-cbd6330be9ba30422db941d413888ec708af3f5906084704c5df600bf616cdce`
retains v4's required-trigger/optional-information boundary, four checkpoints, adaptive paired
replication and CPA-priced model profile. Its only semantic change aligns the broad material-event
trigger with the purchased news Jobs' real 300-second poll and 900-second maximum gap. Policy,
Earnings and NBS cadence remain unchanged. The CSRC, purchased Tushare news and issuer-event, and NBS
original-release routes remain separate accepted source identities.
Route validation binds exact accepted upstream-source IDs as well as capability, Provider and
semantic scope; a Tushare forecast/express source cannot masquerade as established news merely
because both expose `event_revelation`.
These are route contracts, not complete checkpoint sets: direct publisher coverage, complete market
semantics, tradability fields, taxonomy effective intervals, official macro release/revision
lineage, and future post-registration receipts still have to reconcile at each checkpoint barrier.
Those gaps limit coverage claims and later execution where relevant; they no longer all block a
prospective process diagnostic. No model call begins until the structural Query Gate passes.

The current v6 route plan is frozen in
`examples/research/prospective-checkpoint-route-plan-v6.json`. It copies the accepted v5 bindings
under the new registration and has no predecessor in that registration. V4 registration and v5
route-plan history remain immutable. The new plan was durably admitted as follows:

```bash
market-impact data checkpoint-route-admit \
  --registration examples/research/prospective-diagnostic-registration-v5.json \
  --route-plan examples/research/prospective-checkpoint-route-plan-v6.json \
  --state-root .market-impact/data-inputs
```

Admission is idempotent and records only the Harness clock; readiness refuses an unadmitted plan.
The checked-in v1 plan remains a readable legacy artifact. New route plans use schema v2 and must
name `replaces_plan_id` when a current head exists. Admission validates every bound Job and accepted
source contract before a single `BEGIN IMMEDIATE` transaction closes the predecessor's half-open
effective interval and swaps the registration's current head. Two replacements from the same
predecessor cannot both commit. Historical admissions, misses, receipts, and observations remain
append-only. A headless legacy history requires explicit re-admission of the exact selected plan;
the Harness never infers a winner from timestamps. Readiness checks route effectiveness at
`evaluated_at`, and Event Impact Triage checks it again at `frozen_at`.
Then run the non-mutating readiness audit:

```bash
market-impact data checkpoint-readiness \
  --registration examples/research/prospective-diagnostic-registration-v5.json \
  --route-plan examples/research/prospective-checkpoint-route-plan-v6.json \
  --state-root .market-impact/data-inputs
```

The readiness command persists only the content-identified report in the private CAS. A healthy
`waiting_for_post_admission_trigger` result is expected external wait, not a failed collector or a
Query Gate pass. A historical `--evaluated-at` may fail once that Job has newer state; creating a
current report or adding a future versioned health-snapshot contract is required instead of
retroactively reusing live aggregates.

Readiness also reopens the append-only Event Impact Triage Decision Store for the exact registration,
checkpoint, route plan and admission epoch. A fully admitted direct-run v1, immutable legacy Work v2
or current authority-time-bound Work v3 Decision suppresses its candidate versions. A failed blind
comparison suppresses versions only after the state authority records a content-bound terminal batch;
that terminal record is not a semantic Decision and cannot grant Trigger Admission. Operator
inspection, a Proposal alone, an unterminated failed Work plan or a Decision from another route epoch
cannot mark a version handled. The first real legacy Work v2 Decision removed its exact nine versions
from readiness while leaving 26 later actual-receipt versions as new unclassified candidates. New Work admissions
use v3 and require `decided_at` to equal the fully reopened Work receipt's `finished_at`.

After the v8 material-event failure was terminalized, its 29 versions stopped occupying the active
head. The next audit exposed that v4's broad material-event 120/600-second cadence was tighter than
the active 300/900-second purchased-news Jobs. V5 corrected this by versioning the registration rather
than mutating v4 or waiving the gate. Route plan v6 admission then produced 4/4 operational
checkpoints, seven operational material-event trigger routes and zero post-admission candidates. The
zero was the expected clean epoch boundary: only later actual receipts could enter the next v9
batch. Eleven such versions were later frozen and terminalized by the failed pristine v9 comparison;
they were not converted into a semantic Decision. A later sealed v11 comparison projected the
registered eligibility/exclusion, venue and instrument-class bounds into the existing one-call
ingress. It is also terminal negative evidence: its 12 versions cannot be recycled, it created no
Decision, and it does not reinterpret the terminal v9 or v10 versions as a Decision. Later
non-comparison-bound receipts may use v11 once to create a Triage Decision; an EventAssessment target
still requires the existing deterministic Materiality Gate before Trigger Admission.

The longer soak also found that four 120-second purchased-news Jobs can each become incomplete after
a real post-admission misfire even though their latest outcome is healthy and lag is near zero. The
one-shot supervisor already uses deadline ordering and four-way concurrency; the observed misses
occurred when a prior batch plus the one-minute host relaunch delay exceeded the Jobs' 90-second
misfire grace. This does not erase receipts or make the three still-operational material routes
unhealthy, but it invalidates the earlier ten-opportunity soak as long-run proof. Fix that scheduling
envelope through versioned Job replacements and a later route epoch; do not reset the Journal,
forgive missed opportunities, or relax PIT timestamps.

In parallel, run the small vendor trial defined in `PIT_EVIDENCE_RECOVERY.md` and start prospective
actual-receipt collection. The trial proves source contracts; it does not require a large purchase or
open paper/live execution. Provider-specific adapters follow only after one sample passes its frozen
timestamp, version, revision, taxonomy, license, and replay checks.

## First prospective feed adapter

The syndication adapter uses the maintained `feedparser` library for RSS/Atom normalization. It
collects one exact response per registered source, freezes the actual receipt timestamps, generates
the cutoff from their maximum, and replays those response bytes without another HTTP request. For a
feed inside its excerpt-only license scope, the Harness retains the exact HTTP response and exact XML
bytes for every selected item. It rejects unregistered redirects, non-HTTPS endpoints and article
links, malformed XML, external entities, empty 200 responses, RSS `content:encoded`, Atom `content`,
missing publication times, source-clock timestamps after receipt, unsupported query parameters, and
source-configuration drift. A rejected full-content feed has no raw response or item bytes persisted.
It records publication time as a publisher field and actual receipt as
`available_at == authority_at == retrieved_at`; publication time never becomes receipt authority.

The checked-in Federal Reserve press-feed configuration is a reproducible public example because
the [Federal Reserve explicitly publishes RSS feeds](https://www.federalreserve.gov/feeds/feeds.htm).
It establishes the connector's prospective acquisition path, not relevance to an A-share checkpoint.

The Bloomberg URLs discussed in the referenced Reddit thread are useful discovery evidence: several
still resolve to rolling Bloomberg-hosted feeds. They are not registered here. The feed exposes
headlines, excerpts, links, GUIDs, and publication times rather than a historical version archive,
and [Bloomberg's site terms](https://www.bloomberg.com/notices/tos/) require a separate license review
before automated retention or trading use. Google News RSS remains an aggregator discovery route;
its receipt or timestamp cannot become Bloomberg publisher authority.

Example prospective collection and generated-cutoff freeze:

```bash
market-impact data capture-feed \
  --source-config examples/providers/federal-reserve-press-feed-v1.json \
  --window-start 2026-08-27T07:00:00Z \
  --source-policy-id official-public-feeds-v1
```

The command returns a private Data Snapshot ID and its generated `capture_cutoff_at`. It does not
promote observations to Evidence or grant any paper/live capability.

## First accepted A-share official-event route

The CSRC Provider reads one registered official publication channel through its exact JSON endpoint.
It binds the channel, publisher, Asia/Shanghai publication clock, page and byte limits, rights-review
reference, private retention scope, and no-redistribution rule in a content-identified source
configuration. Pagination follows the endpoint's descending publication-date contract while
preserving exact intraday times; records on the same date are not assumed to be time-sorted. Every
selected JSON record and the framed multi-page response are retained by hash in private storage.

Run the bounded acceptance trial with:

```bash
market-impact data accept-csrc-news \
  --source-config examples/providers/csrc-official-news-v1.json \
  --window-start 2026-08-01T00:00:00Z
```

The command performs one real capture, stores the exact CSRC legal-notice response as rights
evidence, replays the captured source responses in an isolated store, and evaluates the seven route
gates defined in [DATA_PLATFORM_PLAN.md](DATA_PLATFORM_PLAN.md). A successful report binds the
Provider manifest, source configuration, rights evidence, original Snapshot, and identical replay
Snapshot. It retains CSRC publication time separately from
`available_at == authority_at == retrieved_at`; therefore it is prospective actual-receipt evidence,
not a reconstruction of historical availability.

The 2026-08-28 acceptance selected three observations and passed all gates in the private report
`source-route-acceptance-report-0671f5669de1cd78741350d8cb373a5fbd8d4535cb5efafcb1b5a5714a8d7216`.
The report explicitly fixes `historical_pit_claim`, `evidence_promoted`, and
`execution_capability` to false. Acceptance is for private research retention without
redistribution; it does not accept CSRC as a complete news source or fill the other A-share data
families.

## NBS CPI/PPI original-release route

The NBS Provider fetches exactly `https://www.stats.gov.cn/sj/zxfb/rss.xml`, rejects redirects,
origin/path or media-type drift, DTD/entity declarations, malformed XML, and incomplete publication-
date window coverage, then chooses the newest matching CPI and/or PPI item. The RSS remains
discovery metadata. For each selected item, the Provider requires one matching official direct
article, a unique `ArticleTitle`, a unique visibly corroborated `PubDate` to the minute, and exactly
one unique same-directory XLSX whose ZIP contains the OpenXML content-types and workbook members.
Missing or mismatched authority is a typed source error; truncated or structurally incomplete HTML
is rejected. A fully covered feed with no matching post-window item is completed `NO_DATA` with the
feed bytes retained, while a partially matching configured CPI+PPI scope is a typed source error.
RSS descriptions remain discovery-only and cannot change normalized release identity.

Run the bounded acceptance trial with:

```bash
market-impact data accept-nbs-macro-release \
  --source-config examples/providers/nbs-macro-release-cpi-ppi-v1.json \
  --window-start 2026-08-01T00:00:00Z
```

The private binary capture bundle deterministically frames the exact RSS, article, and XLSX bytes;
stored-bundle replay must reproduce the same Snapshot. Direct article `PubDate` is retained as
`occurred_at` and `published_at`; `source_updated_at` remains null because the article exposes no
separate update time. Meanwhile,
`available_at == authority_at == retrieved_at` records first actual receipt. The normalized
`original_release` projects to a checkpoint `macro_original_release` with the current observation
as `original_release_observation_id`; only `revision_lineage_missing` remains. Schedule observations
retain the legacy `original_release_missing` plus `revision_lineage_missing` gaps.

Acceptance is bound to exactly the configured CPI+PPI route and requires one observation for each
indicator; a CLI subset or missing release cannot qualify the broader route. The corrected
2026-08-29 canonical private-state run selected two observations and passed all seven gates in
`source-route-acceptance-report-0f7416edc7faa22e04835f60d74b118e249dc2aa2d75afc399450aec4a68e7d6`,
binding Snapshot
`data-snapshot-4d2ee53f32618314c3075636989815d2ce75f7b61d07ff24ab8d2c863131d00e`.
The revision strategy still appends content versions without asserting a revision relation;
historical PIT, Evidence promotion, paper, and live execution remain false.

## Continuous prospective collection

For future PIT evidence, repeat the one-shot capture under a content-identified collection policy
and append every result to the local Prospective Receipt Journal:

Accepted CSRC, Tushare, and NBS macro-release routes can also be registered as content-identified
Collection Jobs. The Harness, rather than an OS scheduler, owns cadence, due time, retry/misfire
semantics, Provider binding, and logical opportunity identity. `collection-run-due` is deliberately
one-shot so a later authorized host supervisor only starts or restarts a bounded process.
Rolling policies resolve their half-open source window from the logical due time and registered
timezone; they do not read the host clock to mutate a query. The current purchased-news route uses
2-minute polling with a 10-minute overlap for Sina, WallstreetCN, CLS, and Yicai; 5-minute polling
with a 20-minute overlap for 10jqka, Eastmoney, and Jinrongjie; and 15-minute polling with a 2-hour
overlap for `major_news`. Tushare documents pull queries, not a WebSocket/push subscription, so the
system describes this honestly as rolling polling plus event Wake.
The rolling `datetime_format` is part of Policy identity. Resolution round-trips the rendered
Provider parameters so a date-only `%Y%m%d` request also has a matching effective UTC window; the
reader remains compatible with already-staged pre-fix Snapshots whose request dates and raw window
still reconcile. Tushare `forecast_vip`, `express_vip`, raw `daily`, and `adj_factor` use the
documented date-only form rather than timestamp text.
Concurrent invocations share
an expiring lease; restart resumes staged data, and every due opportunity remains inspectable via
`collection-health`. Automatically selected due Jobs are ordered by the earliest absolute deadline
(`next_due_at + misfire_grace_seconds`) and executed with bounded per-opportunity concurrency. The
worker samples its Harness clock separately at each actual claim instead of reusing one stale batch
start; explicit `--now` remains the deterministic replay/test clock. The concurrency bound is
content-identified in supervisor plan v4, defaults to four, and is constrained to 1-16. This does
not turn per-Job byte preflight into a global atomic byte reservation; disk-budget health and the
existing fail-closed preflight remain separate controls. `SIGINT` or `SIGTERM` requests are checked before Provider collection and
again before the Journal commit. An in-flight Provider request is still bounded by its timeout; a
crash still relies on lease expiry and restart recovery. `--now` controls logical due, misfire,
backoff, and scheduling decisions only; actual opportunity completion uses the runtime UTC clock
and cannot precede its bound Snapshot receipt. Both `collection-run-due` and
`collection-service-run` require `--maximum-state-bytes` and fail closed before Provider collection
when the current state exceeds that bound.

Every terminal opportunity also appends a `CollectionUsageRecord`. Tushare captured bundles expose
exact request/page/response-byte totals, selected rows, attempts, and elapsed time; `collection-health`
reports rolling-24-hour and lifetime totals plus per-opportunity averages. A successful provider
window with no rows is terminal `no_data`, remains healthy, and makes both one-shot worker commands
exit successfully. Each nullable summary dimension also includes an `*_unknown_records` count: an
all-unknown dimension stays `null`, while an observed zero remains `0` with no unknown records.
Tushare records `flat_subscription_not_allocated_per_request` with a null incremental cost; the
official CSRC and NBS routes record `not_applicable` rather than inheriting a Tushare subscription
claim. A future cost-relevant route without evidence must record `unknown`. Only Tushare observation
Jobs currently support rolling windows; CSRC and NBS rolling registrations fail closed. These records
are collection operations evidence, not the Agent model Usage Ledger or a billing invoice.
Usage Record v2 adds the explicit cost fields. The reader preserves both original v1 records and the
short pre-v2 transition form without rewriting their content identities, so a collector upgrade does
not invalidate or strand the append-only runtime history.

When a registered Job's immutable request/schedule configuration is wrong, register the corrected
successor first and use the audited replacement transition instead of editing SQLite or restarting
the supervisor:

```bash
market-impact data collection-replace \
  --predecessor-job-id prospective-collection-job-OLD \
  --successor-job-id prospective-collection-job-NEW \
  --reason provider_datetime_format_corrected
```

The Harness requires both Jobs to bind the same accepted source route and refuses replacement while
the predecessor owns a lease or has an unsettled staged actual receipt. Prior failures, usage and
Snapshot artifacts remain append-only; only the predecessor's due status becomes `replaced`. When
`--replaced-at` is omitted, a retry of the same predecessor/successor/reason reopens the already
committed transition and returns its original Harness timestamp. An explicitly supplied conflicting
timestamp remains a conflict rather than silently changing identity.

The content-identified tracer report accepts exactly one CSRC event Job and one Tushare market Job
only when both latest opportunities end in complete prospective actual-receipt Snapshots, their
route reports still match, and no miss/failure/cancellation makes the interval incomplete. Its
explicit evaluation cutoff rejects opportunities, Snapshots, or receipts completed later than the
cutoff, and rejects a next due opportunity that remains unmaterialized beyond its misfire grace. It
does not qualify a full checkpoint or install a service.

The host service entry point is separate from an interactive shell invocation:

```bash
market-impact data collection-supervisor-plan \
  --host-name HOST \
  --host-uid UID \
  --service-definition-path /absolute/path/to/LaunchAgents/LABEL.plist \
  --executable-path /absolute/path/to/.venv/bin/market-impact \
  --working-directory /absolute/project/path \
  --state-root /absolute/private/state/path \
  --environment-file /absolute/private/collection.env \
  --stdout-path /absolute/private/collection.log \
  --stderr-path /absolute/private/collection.err.log \
  --maximum-state-bytes 10000000000 \
  --maximum-concurrent-opportunities 4
```

The plan is disabled and secret-free. Its plist carries `Disabled=true`. Disabled installation
means materializing that reviewed plist and applying the reported `launchctl disable` command; it
does not bootstrap a launchd job. Activation is a separate ordered pair: `launchctl enable`, then
`launchctl bootstrap`. `collection-service-run` accepts only an owner-private environment file
containing `TUSHARE_TOKEN`, either directly or in the registered shell `export` form; both it and the state root use canonical absolute
paths, must remain disjoint, and reject symlinked files or ancestors. The value is passed to the
Provider process and is not written to a Job, Snapshot, report, plist, or command output. Generating
this plan does not install or enable launchd.

The v4 plist launches through `/usr/bin/env -i`, explicitly restores only `PATH` and
`PYTHONUNBUFFERED`, and requires the worker to verify that no other host environment keys reached
Python apart from macOS's two non-secret locale/text-encoding keys. A non-isolated process fails
before the private Tushare file is read. PDI-21 acceptance is recorded only by a private,
content-identified `ProspectiveSupervisorReceipt` whose ordered gates bind disabled installation,
environment isolation, lifecycle, bounded failure recovery, health visibility, log redaction,
rollback/reinstallation, and the canonical machine registry. The receipt grants no historical-PIT,
model, or execution authority. Its writer canonicalizes the authoritative state root, rejects any
symlinked state, parent, or final receipt path, and enforces owner-private `0600` permissions even
when identical content already exists.

The authorized host accepted PDI-21 on 2026-08-28 with private receipt
`prospective-supervisor-receipt-84aaffced904893571f51a7a680c453ee4bc71c2718ba8589f06e68e061a2e70`,
binding source commit `6518ff34989769e6119566603ac66de4f9fdd0a0` and v3 plan
`prospective-supervisor-plan-3194dc9ac937da8d19a59176314b30feeec943506d9b907a6303bef5bf19ea65`.
Acceptance observed 16 successful one-shot runs, an explicit stop/reload cycle, bounded failure then
next-interval recovery, complete rollback/reinstallation, visible health, and zero secret-value
matches across 5,246 scanned plist/log/state files. The host itself was not rebooted; the recorded
lifecycle evidence is launchd bootout/bootstrap/reload. The installed service remains enabled, but
the current root is no longer a no-op. On 2026-08-28 UTC, the accepted CSRC event route and Tushare
`index_daily` market route were registered as active Jobs. CSRC polls every 300 seconds and its first
two formal opportunities completed successfully with complete two-observation actual-receipt
Snapshots; the second unchanged capture added later sightings without adding duplicate content
versions. The Tushare Job is aligned to 18:00 Asia/Shanghai and remains pending until its first
scheduled opportunity. Both policies use frozen finite windows for pre-registration accrual. This
is not the registered PDI-22 soak, and it proves neither an indefinite rolling-window contract nor
complete PDI-17/PDI-22 acceptance.

Rollback is ordered and exact: `launchctl bootout gui/UID PLIST`, then
`launchctl disable gui/UID/LABEL` to clear any persistent enable override, then
`/bin/rm -- PLIST`. A later reinstall therefore starts from the plist's disabled state.

Operational backups stay outside the authoritative state root:

```bash
market-impact data state-backup \
  --state-root /absolute/private/state/path \
  --backup-parent /absolute/private/backup/path
market-impact data state-verify-backup --backup /absolute/content-identified/backup
market-impact data state-restore \
  --backup /absolute/content-identified/backup \
  --destination /absolute/new/restore/path
```

Backup creation rejects file or directory symlinks before scanning state, uses a consistent SQLite
snapshot, and copies the immutable CAS, Parquet/ZSTD projections, and private reports. Verification
checks every file hash, SQLite integrity and foreign keys, table counts, Snapshot and Collection
Policy identities, dataset hashes, and row counts; any regular file absent from the manifest is a
verification failure, with `manifest.json` as the sole metadata exception. Restore refuses an
existing destination or one inside the backup root, validates the exact inventory before creating
it, and copies only manifested files. The bounded retention policy keeps authoritative receipts
append-only; reaching the registered byte budget
stops collection with an observable error instead of deleting old PIT evidence.

```bash
market-impact data collect-feed \
  --source-config examples/providers/federal-reserve-press-feed-v1.json \
  --window-start 2026-08-28T07:00:00Z \
  --poll-interval-seconds 300 \
  --maximum-gap-seconds 600 \
  --cycles 2
```

Every source attempt remains visible. An identical upstream record and content hash becomes a new
sighting of the existing version; changed content creates a new immutable version under the same
lineage. `--cycles 0` is the explicit foreground continuous mode. It does not install a daemon or
scheduler.

Freeze only after the journal covers the requested interval:

```bash
market-impact data freeze-feed-dataset \
  --policy-id prospective-collection-policy-<sha256> \
  --window-start 2026-08-28T07:00:00Z \
  --not-after 2026-08-28T07:10:00Z
```

The requested upper bound is not asserted as a receipt. The output Snapshot uses the last selected
actual receipt as its effective cutoff. Missing start coverage, an internal cadence gap, a stale
final receipt, or a failed required source makes the Snapshot incomplete and therefore unavailable
to `FrozenDataSnapshotToolBinding`. A successful freeze also writes a private content-identified
Parquet/ZSTD projection for efficient research scans. Exact source bytes remain in the existing
content-addressed artifact store.

The projection is created only for a complete Snapshot and binds that exact Snapshot ID and window;
an incomplete cadence audit produces no standalone dataset manifest.

## Bounded Attention Watch runtime

`src/market_impact_agent/attention_watch.py` adds scheduling state without adding another receipt or
Snapshot authority. A Harness-approved `AttentionWatchPolicy` binds one existing Prospective
Collection Policy and initial complete Journal-frozen aggregate to an event/Judgment reference,
cluster key, start, expiry, cooldown, and poll/byte/wake budgets. The bound Collection Policy is the
only fixed-cadence authority. The model does not receive a URL, Provider selector, filesystem path,
notification destination, or execution capability.

The initial aggregate must cover the Collection Policy's full window and cannot include receipts
after Watch creation. This prevents old versions omitted by a shortened baseline from later being
misclassified as new information.

An external process supervisor calls `AttentionWatchService.run_due` with a collector already bound
to the stored collection policy. An atomic expiring lease admits only one logical due run, preventing
concurrent supervisors from double-spending state budgets or adding duplicate outbox work while
allowing crash recovery. Every actual attempt still enters `ProspectiveDataJournal`. Typed source
failures and raised collector exceptions produce durable non-terminal backoff and no wake. A
successful poll freezes a standard complete Data Snapshot, compares immutable observation-version
IDs with the Watch's durable seen set, and creates at most one pending `AttentionWake` for a new
version. The wake binds both the prior and new Snapshot IDs and explicitly has no execution
capability. Identical sightings, restarts, and duplicate trigger evaluation cannot add a second
outbox row.

Cooldown suppresses Agent wake-up only; polling continues at the registered cadence so the Harness
does not manufacture a receipt gap. A Harness-owned consumer can read `pending_wakes()` and call
`mark_wake_delivered()` after it has started a fresh bounded Agent run. This slice does not itself
install a scheduler, keep a model resident, dispatch an Agent, promote Evidence, or submit an order.
Adaptive cadence and corroboration/materiality/contradiction triggers remain later Watch gates.
If a real receipt gap occurs, later polls remain append-only and cannot make the affected aggregate
complete; an operator must explicitly approve a new complete baseline/policy before wake eligibility
resumes.

### Scoped local-first retrieval

Attention Watch v2 replaces the v1 event-only metadata boundary with a content-identified
`MonitoringScope`. A Scope may name an event cluster, industry, issuer, instrument, ETF, frozen
subject list, or registered information aspect such as earnings, regulation, supply disruption, or
tradability. Industry and ETF scopes additionally bind the then-effective taxonomy/membership
context; a frozen list records every exact member instead of referring to a mutable watchlist.

The Scope contains a deterministic matcher over allowlisted normalized fields. A
`RegisteredQueryTemplate` limits which fields and match modes are legal and caps clauses, terms and
term lengths. `RetrievalPlan.bind` then resolves the Scope to one exact accepted Prospective
Collection Policy, source set, PIT lane, cadence, freshness/coverage requirements, and fetch/byte
budget. The Agent cannot name a URL, Provider route, credential, filesystem path, notification
destination, or execution capability.

`resolve_retrieval` implements the decision boundary, not network acquisition:

1. accept an exact qualifying cached Snapshot;
2. otherwise accept an exact qualifying Journal-frozen Snapshot;
3. otherwise return `fetch_required` only when the registered Plan permits a positive fetch and byte
   budget;
4. after the Harness performs that registered acquisition, its existing Collection Usage Record
   proves observable request/page/byte use, and the Journal freezes a qualifying Snapshot; a fresh
   resolution may then return `journal_freeze` with that Snapshot ID;
5. otherwise return typed PIT, source, coverage, freshness, cache, or acquisition gaps.

The requested UTC instant participates in Retrieval Resolution identity, so a later freshness
decision cannot alias an earlier one. Cache and Journal results are resolved by Snapshot ID through
their owning stores; a caller-supplied Data Snapshot cannot merely claim cache or Journal
provenance. Match outputs contain only Observation/version IDs, matched
field paths, and license scope; licensed or raw bodies remain in the private store. A decision Agent
may request additional information, but the current Run cannot consume mutable fetch output. The
Harness must journal and freeze it, then start a fresh bounded Run over the exact Snapshot.

### Agent-proposed Watch admission

`AgentWatchAdmissionService` now exposes a filtered catalog of content-identified
`WatchDelegateProfile` records. A profile uses a short name and description for model selection but
the Harness owns its callback Agent profile, exact Skill hashes, read-only capabilities, parent and
subject types, registered query template, Collection Policy, collection limits, callback limits and
lineage/branch/global caps. The model-authored `AgentWatchRequest` contains only the offered profile,
anchored subject, optional frozen members or registered information aspect, deterministic matcher,
question, rationale and evidence references. Its closed schema rejects Provider, URL, credential,
cadence, budget, callback configuration and execution fields.

Admission is durable under the same SQLite WAL/FULL state root as Watch state. It injects the parent
type and lineage, validates then-effective context for industry/ETF scopes, reopens the registered
matcher and route, and records a content-identified admitted, reused or rejected result. Branch and
global limits are committed under one immediate transaction. Equivalent active scopes use one
Attention Watch rather than duplicate collection; every accepted parent remains a distinct callback
subscriber, so sharing collection never silently transfers lineage. A cancelled or expired Watch is
not reused.

This is bounded mechanics, not PDI-40 acceptance. A caller-created parent projection remains
self-declaration even when it is append-only and content-addressed, so the project removed the
context-minting path. The concrete store currently rejects every candidate context; profile offers,
admission, lookup, callbacks and restart activation stay fail-closed. The next workslice must bind one
named parent Run/Decision authority and derive allowed evidence references, subjects/frozen members
and matcher terms from its reopened durable artifact. Only then can the existing unrelated-scope and
admission-before-Watch activation recovery tests become operational evidence. Shared due collection
and the bounded acquisition executor remain later PDI-40 gates.

The callback lookup is intentionally a read-only binding over a durable Wake, accepted Admission and
the exact still-registered delegate profile. It does not claim or acknowledge the Wake, call a model,
or dispatch a task. Shared due-opportunity fan-out, the bounded acquisition executor and installed
scheduler remain PDI-40 work; idempotent Wake-to-fresh-Run dispatch remains PDI-41.

Prospective EventAssessment applies the same separation between observation throughput and decision
authority. A completed no-path Watch or unresolved review remains a ready-time-order blocker for
Trigger Admission, but it does not stop later candidates from receiving their own durable bounded
assessment. The Harness may therefore keep monitoring and analyzing new information without letting
a later event bypass the unresolved predecessor into Snapshot Set, Query Gate or an Order Intent.

See [DATA_PLATFORM_PLAN.md](DATA_PLATFORM_PLAN.md) for layer ownership, the infrastructure adoption
matrix, A-share source order, operational limits, and evolution gates.
