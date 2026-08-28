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
`tushare-observation` Provider adds twelve separately configured private routes for news, market,
instrument, industry, positioning, macro-schedule, and analyst-forecast observations. Each route
has its own content identity and acceptance report; sharing one transport never merges their
semantics or acceptance.

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
receipt. The lanes share the record shape but cannot satisfy one another's claim gates.

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
| `lookup_prior_expectation` | Cited consensus, forecast, positioning, or market-implied baselines | An invented expectation delta |
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

A Transmission Path remains a cited Judgment output assembled from facts and exposure evidence. A
mechanism-appropriate horizon set remains a versioned research-method input. Treating either as a
vendor field would hide an inference inside the data plane.

### Prospective diagnostic binding

The `Prospective Diagnostic Registration` freezes requirements before acquisition. It names three
first-eligible future event mechanisms and fixes each cutoff rule, all six capability slots,
route/source minima, freshness and gap limits, allowed instruments, horizons, paired arms, three
replicates, the aggregate model budget, hidden-outcome rule, and stop/go conditions. It deliberately
contains no Provider IDs and grants no model or execution authority.

`ProspectiveCheckpointSnapshotSet` is the later non-authoritative barrier reconciliation. For every
required slot it requires accepted route identity, the exact Collection Policy, a complete
Journal-frozen prospective Snapshot, raw response hashes, source diversity, observation minima, and
freshness at one immutable barrier. It then authorizes the exact Snapshot ID set through
`FrozenDataSnapshotInput` and materializes the six capability-specific read-only tools. It never
combines the underlying observations into a new data authority. A missing, stale, unaccepted, or
post-hoc `not_applicable` slot makes the checkpoint ineligible.

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

PDI-01 now freezes three first-eligible EOD checkpoints with policy/regulation,
earnings-expectation-delta, and macro-cycle mechanisms. The current Tushare Observation Provider has
twelve route-level seven-gate acceptance reports, and the CSRC official-event route remains accepted.
These are route contracts, not complete checkpoint sets: direct publisher coverage, complete market
semantics, tradability fields, effective industry-to-ETF mapping, official macro release/revision
lineage, and future post-registration receipts still have to reconcile at each checkpoint barrier.
No model call begins until that Query Gate passes.

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

## Continuous prospective collection

For future PIT evidence, repeat the one-shot capture under a content-identified collection policy
and append every result to the local Prospective Receipt Journal:

Accepted CSRC and Tushare routes can also be registered as content-identified Collection Jobs. The
Harness, rather than an OS scheduler, owns cadence, due time, retry/misfire semantics, Provider
binding, and logical opportunity identity. `collection-run-due` is deliberately one-shot so a later
authorized host supervisor only starts or restarts a bounded process. Concurrent invocations share
an expiring lease; restart resumes staged data, and every due opportunity remains inspectable via
`collection-health`. `SIGINT` or `SIGTERM` requests are checked before Provider collection and
again before the Journal commit. An in-flight Provider request is still bounded by its timeout; a
crash still relies on lease expiry and restart recovery. `--now` controls logical due, misfire,
backoff, and scheduling decisions only; actual opportunity completion uses the runtime UTC clock
and cannot precede its bound Snapshot receipt.

The content-identified tracer report accepts exactly one CSRC event Job and one Tushare market Job
only when both latest opportunities end in complete prospective actual-receipt Snapshots, their
route reports still match, and no miss/failure/cancellation makes the interval incomplete. Its
explicit evaluation cutoff rejects opportunities, Snapshots, or receipts completed later than the
cutoff, and rejects a next due opportunity that remains unmaterialized beyond its misfire grace. It
does not qualify a full checkpoint or install a service.

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

See [DATA_PLATFORM_PLAN.md](DATA_PLATFORM_PLAN.md) for layer ownership, the infrastructure adoption
matrix, A-share source order, operational limits, and evolution gates.
