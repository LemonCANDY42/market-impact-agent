# Tushare data boundary

The first Tushare slice is a read-only, language-neutral HTTPS adapter. It materializes
deterministic tables and a fixed SSE/SZSE universe without making Tushare, its Python SDK,
or a data vendor the orchestration owner.

Tushare's official Python examples and its direct-HTTP contract address the same token-authenticated
HTTP service and expose the same API permissions. The Harness therefore keeps the direct HTTPS
Adapter: exact response pages and bytes, API error codes, bounded pagination, replay inputs, and
credential isolation remain explicit in the Provider contract. Installing the SDK would add a
client convenience layer, not news coverage, lower latency, or a push channel, so it is not a
runtime dependency.

Tushare is fully usable as an upstream source under this project's deployment entitlement: the
owner supplies their purchased token, uses it only inside the private Harness, and does not resell
or redistribute the data. That fact authorizes Provider integration; it does not make any route
Agent-visible by itself. The legacy `tushare-http` historical-market bundle Provider remains
`disabled`/`unverified` for its historical and execution claims. The separate
`tushare-observation` Provider is enabled for contract-validated prospective collection, while
every API route still requires its own accepted Source Route Configuration and report before a
checkpoint may cite it.

Official contract references: [`stock_basic`](https://tushare.pro/document/1?doc_id=25),
[`trade_cal`](https://tushare.pro/document/2?doc_id=26), and
[`daily`](https://tushare.pro/document/2?doc_id=27). The hardened v2 bundle additionally
uses [`adj_factor`](https://tushare.pro/document/2?doc_id=28) and
[`stk_limit`](https://tushare.pro/document/2?doc_id=183).
The separate research-only market-context panel uses
[`index_daily`](https://tushare.pro/document/2?doc_id=95),
[`index_classify`](https://tushare.pro/document/2?doc_id=181), and
[`sw_daily`](https://tushare.pro/document/2?doc_id=327).

## Prospective Observation Provider

`src/market_impact_agent/tushare_observation.py` implements one credential-isolated HTTPS transport
and thirty content-identified route configurations. The owner's separately purchased news
entitlement and 10,000-plus-point account are treated as fully usable for this private Harness; the
token is read only from `TUSHARE_TOKEN` and is excluded from requests persisted for replay, hashes,
logs, errors, configs, and Agent tools.

The official route contracts are [`news`](https://tushare.pro/document/2?doc_id=143),
[`major_news`](https://tushare.pro/document/2?doc_id=195),
[`index_daily`](https://tushare.pro/document/2?doc_id=95),
[`daily_basic`](https://tushare.pro/document/2?doc_id=32),
[`index_dailybasic`](https://tushare.pro/document/2?doc_id=128),
[`fund_daily`](https://tushare.pro/document/2?doc_id=127),
[`trade_cal`](https://tushare.pro/document/2?doc_id=26),
[`etf_basic`](https://tushare.pro/document/2?doc_id=385),
[`stock_basic`](https://tushare.pro/document/2?doc_id=25),
[`stk_limit`](https://tushare.pro/document/2?doc_id=183),
[`index_classify`](https://tushare.pro/document/2?doc_id=181),
[`index_member_all`](https://tushare.pro/document/2?doc_id=335),
[`etf_sh_cons`](https://tushare.pro/document/2?doc_id=471),
[`etf_sz_cons`](https://tushare.pro/document/2?doc_id=472),
[`margin`](https://tushare.pro/document/2?doc_id=58),
[`cn_schedule`](https://tushare.pro/document/2?doc_id=461), and
[`report_rc`](https://tushare.pro/document/2?doc_id=292). Every configuration fixes its API,
capability, fields, primary key, allowed/fixed parameters, time fields, source semantics, rights and
documentation URLs, page size, and page ceiling. Pagination is bounded at 1,000 rows per page and
100 pages, every individual HTTPS response is capped at 32 MiB before JSON parsing, and one capture
cannot accumulate more than 256 MiB of response bodies.

Collection retains the exact response pages and selected record bytes, sorts and deduplicates by the
route-specific primary key, and records actual receipt as prospective availability and authority.
The same private capture bundle must reproduce an identical Snapshot in an isolated store before a
route passes. No-data, permission denial, field mismatch, duplicate keys, page overflow, response
overflow, and transport failure remain distinct typed outcomes. The CLI performs one capture,
Journal write, rights-page capture, isolated replay, and seven-gate qualification:

```bash
uv run market-impact data accept-tushare-observation \
  --source-config examples/providers/tushare-observation-index-daily-v1.json \
  --parameters-json '{"ts_code":"000300.SH","start_date":"20260827","end_date":"20260827"}' \
  --window-start 2026-08-28T11:17:00Z \
  --poll-interval-seconds 300 \
  --maximum-gap-seconds 1800
```

The original twelve routes passed the seven route gates on 2026-08-28. The two exchange-PCF routes
passed the same capture, Journal, isolated replay, rights, and seven-gate path on 2026-08-29:

| Route | Capability | Accepted observations | Acceptance report |
| --- | --- | ---: | --- |
| `index_daily` | market context | 1 | `source-route-acceptance-report-653c4bc24cbad5d3d020cf567b37295702d8b8636cee4b5e62127093d94c11c5` |
| `daily_basic` (2026-08-31 full A-share date) | market context | 5,545 | `source-route-acceptance-report-5ce22b2192206cf3be6d6b074a4091ccf74376be14365901a6cc5bd447533a63` |
| `index_dailybasic` (`000300.SH`, 2026-08-31) | market context | 1 | `source-route-acceptance-report-2e542b4a478b9dddac2c38ede159ee2e4e1dfa6e5e4eb1f383bc718e4480af55` |
| `fund_daily` | market context | 1 | `source-route-acceptance-report-3fd9945ec5fb09815f929f7b9d0b48f92a71814a91ac98693eb5ef7265324d84` |
| `trade_cal` | market context | 1 | `source-route-acceptance-report-4dc04dc822718ae1b75089ea9f0bc25dedbbe7be2d665e70ad601b675e910216` |
| `etf_basic` | exposure candidates | 1 | `source-route-acceptance-report-59730b892100961a32fdf4eaa6ed789974831202db6ce21ba9c26df519ed43a3` |
| `stock_basic` | exposure candidates | 1 | `source-route-acceptance-report-a37d3dfb6201ecded01d8933e2380802bbc30adf47b8cdf93ca393dfed90340d` |
| `stk_limit` | exposure candidates | 1 | `source-route-acceptance-report-c8fd0804e7c67a8e55d35fe24d7cb3441a60edde40f2f9183d09ed36b242cf09` |
| `index_classify` | exposure candidates | 31 | `source-route-acceptance-report-f6c4c9082b1b65839dfc7b74f9cc5eede7d35a89654615c48191e4db0d0352cb` |
| `index_member_all` | exposure candidates | 126 | `source-route-acceptance-report-f626fc923bc68930cce7782bf5e02f7d37b338594f441b0c841c5658a8e51565` |
| `etf_sh_cons` (`517030.SH`, 2026-08-28 PCF) | exposure candidates | 300 | `source-route-acceptance-report-2191215db92de1eb58bc7de988f53ec119123708a1b7254fced13a0a51cebed8` |
| `etf_sz_cons` (`159051.SZ`, 2026-08-28 PCF) | exposure candidates | 99 | `source-route-acceptance-report-50c86e9eda5cc290c0a59b07d34b1c7edd68e16264c908515da537543dcc3636` |
| `margin` | positioning | 3 | `source-route-acceptance-report-287411904eab4b6597614cff1652c2dd4feae69b60e84eff0ee2734ecb591237` |
| `cn_schedule` | macro schedule | 14 | `source-route-acceptance-report-52d94fbcbf6bd5959d82a16013d67413c37a04f5d31b6cf6fc1fb74f5635da2c` |
| `report_rc` | prior expectation observations | 4,802 | `source-route-acceptance-report-a79d8525ea67763d8022a8909dcfd7a85683f5c476ab6ff2f89149aec9fba8ff` |
| `news` (`src=sina`) | event revelation | 29 | `source-route-acceptance-report-e9bc974b0b3e0101701fed3b0dd37e57cc7fc595b93dff3802129fb125b9dde8` |

The accepted `stock_basic` route is also registered as a daily prospective
`tradable_instrument_master` Job. Its first scheduled post-admission run on 2026-09-02 accepted
5,554 records through six bounded requests in about 64 seconds with zero failures, misses or lag.
Route plan v7 binds that Job to all four prospective checkpoint families. The private Snapshot and
licensed rows remain ignored; only counts and content identities are documented here. The first
receipt can improve newly frozen decisions but cannot be injected into an earlier Triage or
EventAssessment run.

The two valuation/context routes passed all seven gates on 2026-09-01 and are registered as bounded
daily prospective Jobs after the normal post-close publication window. `daily_basic` contributes
close, turnover, PE/PB/PS, dividend yield, share base, market value and limit status for the received
trade date; `index_dailybasic` contributes the corresponding supported-index valuation and market
value fields. They are actual-receipt context, not strict historical PIT or an execution price.

The purchased news entitlement was then exercised across every documented short-news source and
`major_news` with bounded current windows. Seven additional short-news sources and `major_news`
produced non-empty actual-receipt Snapshots and passed the same seven route gates. The acceptance
artifacts retain hashes and counts only in Git; licensed rows remain private:

| Route | Accepted observations | Acceptance report |
| --- | ---: | --- |
| `news` (`src=sina`, repeated current-window acceptance) | 354 | `source-route-acceptance-report-4727fb86...` |
| `news` (`src=wallstreetcn`) | 147 | `source-route-acceptance-report-a6aae3...` |
| `news` (`src=10jqka`) | 57 | `source-route-acceptance-report-11ef27...` |
| `news` (`src=eastmoney`) | 194 | `source-route-acceptance-report-27dcd1...` |
| `news` (`src=jinrongjie`) | 115 | `source-route-acceptance-report-096f27...` |
| `news` (`src=cls`) | 11 | `source-route-acceptance-report-a86dc9...` |
| `news` (`src=yicai`) | 74 | `source-route-acceptance-report-b8eb2b...` |
| `major_news` | 4 | `source-route-acceptance-report-e81748953a21e52cb908934151ed59118ebe83f3bba399ca21415e91b542cfe3` |

`yuncaijing` and `fenghuang` returned valid empty results in both 24-hour and seven-day probes. Their
checked-in configurations remain available, but no active Job or route-acceptance claim is created
until a non-empty bounded capture can pass replay. `anns_d` returned the Provider's explicit
permission error and is not configured. An empty entitlement-backed window is evidence of `NO_DATA`,
not a transport failure and not proof of complete publisher coverage.

## Rolling news collection

The official news APIs are pull APIs; no documented WebSocket or push-subscription contract is
claimed. The practical low-latency path is Harness-owned rolling polling plus a later event Wake:

| Route | Cadence | Lookback | Purpose |
| --- | ---: | ---: | --- |
| Sina, WallstreetCN, CLS, Yicai short news | 2 minutes | 10 minutes | Low-latency event discovery with overlap |
| 10jqka, Eastmoney, Jinrongjie short news | 5 minutes | 20 minutes | Broader periodic corroboration and catch-up |
| `major_news` | 15 minutes | 2 hours | Longer-form major-event context |

Each query window is deterministically resolved from the Collection Opportunity's logical due time
in its registered timezone. The overlap is intentional: the Journal deduplicates exact content
versions while retaining later sightings and measured receipt lag. A valid empty interval completes
as healthy `no_data`; permission, contract, transport, and replay failures remain typed failures.
The one-shot launchd-supervised worker discovers the registered Jobs on its normal minute tick, so
registration does not restart the service and the process is normally absent between invocations.

Every terminal opportunity appends a content-identified Collection Usage Record. For captured
Tushare bundles it reports exact request/page/response-byte totals, selected rows, attempts, and
latency; rolling 24-hour and lifetime summaries expose per-opportunity averages and totals.
Subscription spend is not fabricated as a request price: estimated cost is null with
`flat_subscription_not_allocated_per_request`. Other Providers keep request/page counts null unless
their captured artifacts can prove them.

This is route-level prospective evidence, not completion of PDI-10 through PDI-16. In particular,
Tushare `news` is an aggregator route even when `src=sina`; its receipt does not establish a direct
publisher archive or historical authority. `cn_schedule` is advisory scheduling, not an original
NBS release or revision lineage. `report_rc` supplies cited forecast observations, not a consensus
unless the registered population/window/method derives one. Listing, ETF, limit, PCF, and industry
rows do not by themselves prove decision-time tradability, full market completeness, taxonomy
effective intervals, or revision lineage. PCF quantity is not a portfolio weight and is never
converted into one without a decision-time valuation basis.

On 2026-08-29 a separate isolated semantic probe captured all 1,658 listed `etf_basic` rows into
complete prospective Snapshot
`data-snapshot-0f99e095245aac464aba334b58d541675c8452acf9f6ffd19bb6236614d9da2d`;
private route report
`source-route-acceptance-report-954cc5b8100c884dc82da1cbb5fdeefa2778a966c172ea1115791a69f5eea9f3`
passed all seven gates. Of 1,631 ETFs with an `index_code`, zero exactly matched the 31 accepted
SW2021 Level-1 taxonomy codes. That rules out treating the current Tushare ETF master plus SW2021
classification as a proven direct industry-to-ETF relation. The Harness records the missing mapping;
it does not derive one from similar names or silently substitute another taxonomy.
No Checkpoint Decision Input was created from this isolated probe because it was not bound to a
registered event checkpoint or barrier. Under the later v2 partial-observation contract, the missing
cross-taxonomy relation would remain an explicit optional gap rather than a global model blocker.

The accepted PCF routes close the earlier cross-taxonomy ETF mapping blocker without weakening that
Snapshot-set boundary. `etf_sh_cons` and `etf_sz_cons` expose an exchange PCF's ETF code and exact
constituent codes for one trading date. `CheckpointMarketUniverseView` can therefore bind
ETF → PCF constituent → effective current SW membership → exact SW2021 taxonomy code. The member
row identifies the Shenwan family but not a taxonomy version, so the view retains
`taxonomy_version_unverified`. A private 2026-08-29
compatibility probe found 211 exact current SW-member code matches among the 300 SSE PCF rows and 98
among the 99 SZSE PCF rows; it did not use constituent names. The accepted Snapshots were
`data-snapshot-b37b79afd8cebe9c84eb9bcc0b575a13dd31c6063bff6787b92476121bb2df07`
and `data-snapshot-b11f8c4ff96aad93bcaca96c4cdd3fdf8b3b8125d2ab20287451d0c629bc4962`.
They were received on 2026-08-29 and therefore make no historical authority claim for 2026-08-28.

Two bounded daily collection Jobs are registered in the private runtime, starting at 08:50
Asia/Shanghai on 2026-08-31. Each requests one representative ETF with a fixed 2026-08-28 lower
bound, so later receipts append a bounded growing PCF window without a mutable host-date template.
The Jobs are `prospective-collection-job-488181db9f794bf89ba253889e904bfa16e1c0f33e705990be024f76558d8045`
and `prospective-collection-job-cf1fa7f0bed68a250bcf21d0a147642e2641b841227abdf900737b4c9a391601`.
Registration did not restart either existing collector.

Decision-time ETF tradability remains fail-closed. The purchased account does not have the separate
`rt_etf_k` entitlement, and a real-time bar would in any case prove observed activity rather than
exchange acceptance of a future order. Tushare `suspend_d` is documented for stocks, not ETFs. The
SSE publishes structured positive fund-suspension records and the SZSE publishes official
suspension notices, but the current Harness has no accepted symmetric, complete, per-instrument
absence/status contract across both registered venues. Consequently an otherwise eligible ETF
still returns `suspension_status_unverified`; PDI-12 remains open.

The accepted `index_daily` route also participates in the first PDI-20 scheduled collection tracer.
On 2026-08-28 the Harness-owned one-shot worker captured 20 CSI 300 observations into complete
prospective Snapshot
`data-snapshot-a4323eb473a4a36b4cc127b5ed80c0d5f7de76ff44fdda4800dcfe74b4c4a50b`
without persisting the purchased token. That proves the real scheduled acquisition path and actual
receipt semantics. It does not by itself satisfy PDI-11 breadth, volatility, liquidity,
corporate-action, or checkpoint-barrier requirements.

## Accepted contract surface

The adapter calls the fixed official JSON-over-HTTPS endpoint with an explicitly supplied
token; alternate HTTPS origins are rejected because endpoint identity is part of the Provider
contract. It requests only these bounded interfaces and fields:

- `stock_basic`: `ts_code`, `symbol`, `name`, `exchange`, `list_status`, `list_date`, and
  `delist_date`, separately for SSE/SZSE and each documented `L`, `D`, `P`, or `G` status;
- `trade_cal`: `exchange`, `cal_date`, `is_open`, and `pretrade_date` for one bounded date
  range;
- `daily`: one SH/SZ `ts_code`, one bounded date range, and unadjusted OHLC, prior close,
  volume, and amount fields;
- `adj_factor`: the same instrument/date window with `ts_code`, `trade_date`, and
  `adj_factor`; and
- `stk_limit`: the same instrument/date window with source `pre_close`, `up_limit`, and
  `down_limit`.

The research-only regime adapter surface additionally permits bounded `index_daily` and `sw_daily`
queries for the registered market and SW2021 Level-1 price indices, plus `index_classify` to verify
the source taxonomy. Regime capture first reads the complete `SW2021`/`L1` classification table and
requires every registered proxy's source, code, and Chinese industry name to match it before making
any `sw_daily` request. The normalized taxonomy table content hash and retrieval time are bound into
the panel. These rows are stored in a separate private content-identified panel, never in
the stock Data Snapshot and never as Agent-visible evidence. The source rows remain unchanged; its
OHLC coherence check tolerates only source rounding discrepancies up to one basis point and rejects
larger inconsistencies. See `MARKET_REGIME_RESEARCH.md`.

Responses must have the exact requested field set, scalar rectangular rows, valid dates,
matching query identities, unique primary keys, coherent OHLC values, and fewer than the
documented 6,000-row response ceiling. A `trade_cal` response must also contain exactly one
row for every natural date in its requested inclusive interval; omitting the same date from
both calendar and daily responses therefore cannot hide a gap. Fields and rows are normalized
before hashing, so transport order does not change table identity. The token is excluded from
hashes, returned objects, error text, examples, and committed artifacts.

The regime registry accepts only `SW2021` proxies with unique six-digit `.SI` Tushare codes. A
missing, duplicate, unsupported, or taxonomy-mismatched proxy fails capture; no industry proxy is
silently omitted. A regime panel likewise accepts only the fixed `tushare-http` Provider/version,
the declared historical-vintage label, price-return `index_daily` market series, price-return
`sw_daily` industry series, and a complete catalog-to-series mapping.

## Listing snapshot and universe semantics

`fetch_stock_listings` combines eight exchange/status queries into one immutable Listing
Snapshot. A Provider row whose `ts_code` cannot produce the supported six-digit SH/SZ
canonical instrument ID is retained verbatim in the snapshot's normalized anomaly set with
reason `unsupported_tushare_stock_code`; it is neither silently corrected nor admitted to the
universe. `build_pre_event_universe` includes a canonical instrument when:

1. its exchange is selected;
2. `list_date` is on or before the requested date; and
3. `delist_date` is absent or after the requested date.

Tushare `.SH` and `.SZ` codes become canonical `.XSHG` and `.XSHE` instrument IDs. The
Provider identity, adapter version, retrieval time, normalized listings, retained anomalies,
and query hashes determine the Listing Snapshot hash. The sorted canonical instrument set,
cutoff date, exchanges, and that exact snapshot hash determine the universe identity.

This is a reconstruction from data retrieved now, not evidence that the same metadata was
visible at the historical cutoff. Provider revisions, omissions, status-history gaps, and
survivorship bias remain possible. Every downstream replay must retain the exact local
snapshot and must not relabel current retrieval as point-in-time source truth.

## Private Data Snapshot bundle

The capture command reads the token only from `TUSHARE_TOKEN`; there is deliberately no
command-line token flag. For example, the first post-envelope Abqaiq data window can be
requested with:

```bash
uv run market-impact tushare capture \
  --instrument 600028.SH \
  --as-of-date 20190918 \
  --data-start-date 20190912 \
  --start-date 20190919 \
  --end-date 20191010
```

The command writes under the ignored `.market-impact/tushare/` directory. Each bundle has
mode `0700` and contains mode-`0600` files:

- `manifest.json`: provider manifest, request cutoff/window, Listing Snapshot identity,
  Pre-event Universe identity, endpoint/API/fields/non-secret parameters/content hash and
  retrieval time for each of the eight listing queries plus calendar and daily queries, exact
  table hashes and schemas, row counts, writer version, bundle hash, and stable
  `data_snapshot_id`;
- `listings.parquet`: normalized reported listing lifecycles;
- `listing_anomalies.parquet`: normalized source rows that cannot safely become canonical
  instruments, plus their deterministic exclusion reason;
- `universe.parquet`: the complete frozen canonical SSE/SZSE membership set derived after
  those explicit exclusions;
- `trade_calendar.parquet`: the requested target-exchange calendar;
- `daily.parquet`: the requested instrument's unadjusted daily data.

A hardened v2 bundle also contains `adj_factors.parquet` and `stock_limits.parquet`. Its
`data_start_date` may precede the evaluation start so a pre-cutoff momentum rule can observe
cutoff-normalized adjusted closes. For the last factor `F_c` whose session ends by the cutoff,
the research series is `raw_close_t * F_t / F_c`; sessions and factors after the cutoff do not
enter it. This removes corporate-action discontinuities without presenting an adjusted price as
an executable quote. A bundle retrieved later is still a retrospective reconstruction and does
not prove that its factor version existed at the historical cutoff. Adjustment-factor changes are
allowed only in that observation segment.
The evaluation segment must keep one factor, and the replay uses unadjusted daily prices plus
source-provided limits from `evaluation_start_date` onward. Every hardened table must cover
exactly the same open sessions and retain independent query provenance and hashes.

`market-impact tushare validate <bundle-directory>` recomputes the manifest identity; checks
every file hash, exact Parquet schema, normalized logical identity, and row count; reconstructs
the Listing Snapshot and Pre-event Universe; and verifies the target, exchange, date window,
complete natural-day calendar, and open-session/daily relationship. It rejects widened
permissions, symlinks, unexpected files, secret fields, any Provider Manifest other than the
fixed disabled/unverified Tushare contract, and internally resealed source or universe
mismatches. Calendar, daily, and all listing-source content hashes are recomputed from the
normalized persisted observations before the validator returns an ID a later Backtest Request
may cite. The v1 manifest is closed at every authority-bearing level: unknown or missing
top-level, format, request, Listing Snapshot, Universe, table, and query-provenance fields fail;
the validator also checks the pinned writer declaration and actual ZSTD column compression.
The data extra pins the Parquet writer; the development group includes the same pin so the
repository's standard test command exercises this boundary. No Tushare SDK is introduced.

For every accepted window, every exchange-open session must have a daily row. A gap
fails closed because `daily` alone cannot distinguish a genuine suspension from incomplete
data. The pipeline does not synthesize order-book depth, suspension state, or executable
opening liquidity from OHLCV. Adjustment factors are used only for registered pre-cutoff
observation rules; they do not turn the source into observed executable prices.

The regime panel is different: `index_daily` and `sw_daily` are price indices, so stock-style
adjustment factors do not apply. They are suitable for descriptive price-index movement. A claim
about investor total return requires a point-in-time total-return index or an implementable ETF
path including distributions, costs, and corporate actions. See `PIT_EVIDENCE_RECOVERY.md`.

## Acceptance status

Contract tests cover the successful request/normalization path with a deterministic
transport double, missing or malformed fields, invalid dates and prices, duplicate keys,
permission errors, secret redaction, row-order stability, natural-day calendar omissions,
partial-write cleanup, exact request provenance, and semantically inconsistent bundles whose
files and manifest have been recomputed after tampering. An anonymous call reached the official
endpoint and received the expected missing-token failure, proving transport reachability only.

On 2026-08-25, the first token-backed acceptance queried all eight listing partitions plus
the SSE calendar and unadjusted daily series for `600028.SH` from 2019-09-19 through
2019-10-10, using 2019-09-18 as the universe cutoff. It retained one noncanonical Provider
row as an anomaly rather than correcting or dropping it, then wrote and independently
revalidated the private local bundle
`tushare-600028-sh-20190919-20191010-5289178b9ba1a6bc`. The accepted shape was 5,545
canonical listings, one anomaly, 3,684 universe members, 22 natural calendar days, and 11
daily rows. Directory/file modes, Git exclusion, closed-manifest semantics, and absence of
token bytes in every artifact were checked separately.

This proves one real read/capture/validate path and the account permissions needed by that
window. It does not establish behavior for every optional API, historical completeness, source
correctness, historical PIT authority, or execution-grade replay validity. The narrow historical
market-data Provider Manifest therefore remains `enabled: false`, with no verified capabilities and
trust tier `unverified`; this does not revoke or question the project's Tushare entitlement.

The separate `tushare-xshg-modeled-open.v1` replay gate validates a bounded
`600028.SH`/SSE bundle, then consumes hash-bound daily/calendar Parquet bytes in memory
without a token, network call, or derived licensed fixture. Its one-lot daily-open liquidity
is explicitly modeled rather than observed. Generated-bundle tests cover that contract; two
token-free 1/3/10-session runs of the named private bundle completed with identical replay
identity on 2026-08-25. The Phase 2 calibration gate then rejected that single manual event
as insufficient research evidence. Only non-reversible identity hashes are recorded in
`A_SHARE_REPLAY.md` and `PHASE2_CALIBRATION.md`; licensed observations and metrics stay
private.

On 2026-08-26, the hardened v2 path captured and validated seven private windows for the
pre-registered energy-supply-shock cohort. The account successfully read `adj_factor` and
`stk_limit` for those exact instrument/windows. The adapter separated adjusted pre-cutoff
observation history from the constant-factor unadjusted evaluation segment and bound source
price limits into every replay. Twenty-five registered buys each completed two deterministic
Nautilus runs. The v2 calibration gate rejected the cohort because candidate mean net return
was not positive. This proves the bounded data/replay contract under those windows, not
general Tushare permissions, Provider verification, alpha, or execution readiness.

## Modeled historical account input authority

`HistoricalAShareInputs` reopens explicit `LocalDataSnapshotStore` snapshot IDs,
source configurations, complete provider attempts and raw CAS records. It checks
that normalized Tushare rows match their stored raw fields/values. No data ledger,
network client or execution engine is added. `with_snapshots` binds newly captured
references for the next decision view; it does not rewrite earlier views.
Each frozen input binding verifies and caches its immutable source-table tuple once,
under a lock. A new binding, including `with_snapshots(())`, verifies the CAS graph
again; cached decision views do not silently reread changed artifacts.

The constructor requires `ModeledHistoricalPolicy` and effective-dated, versioned
Harness trading-rule artifact hashes. A rule records its symbol, equity versus
`exchange_traded_fund` class, effective interval, lot, tick, price-limit ratio and
fees, and references the captured source document. `stock_basic`/`etf_basic` listing
and exchange fields provide corroboration; current index/ETF mappings never replace
the explicit historical rule. Missing or overlapping versions fail closed.

`reopen_security(symbol, cutoff)` is the 09:25 Shanghai preopen decision projection.
Raw price and turnover come exclusively from the calendar's previous completed
trading session, with `raw_price_observed_at` retained as that prior close. Today's
calendar, price limits and full-day halt information are usable only under the
explicit modeled 09:00 venue-fact availability assumption. Their exclusive validity
ends at 09:30:00.000001, covering the single opening tick through the named
`opening_tick_validity_microseconds=1` policy. This is not a general extension of
price freshness. Actual provider receipt timestamps remain unchanged; backfilled
2025 rows captured in 2026 cannot become StrictPIT observations.

`session(symbol, date)` is an executor-only projection and may contain that session's
full raw OHLC/volume. It returns a source-backed instrument specification, an
`AShareDailyBar`, corporate actions and typed gaps. Opening quote size is explicitly
modeled as a configured fraction (at most 1%) of reported daily volume, converted
from Tushare lots to shares. It is not an observed opening book and must not enter
the preopen Agent view. Source daily limits are checked against the effective rule.
The rule check uses the raw daily reference price. An absent optional
`stk_limit.pre_close` remains absent in source evidence; it is compared only when
both sources supply a value. Reported limits that disagree with the exchange tick
calculation still block, including older provider values rounded to cents.

A separately selected `limit_basis="qualified_seed_etf_exchange_rule_v1"` is a
**MODELED** normal-session scenario for `510300.SH` and `510500.SH` only. The
default `reported_stk_limit` retains the behavior above and is omitted from policy
serialization so existing frozen identities remain unchanged. The new basis is
included in frame, research, experiment and baseline bindings and must use its own
versioned policy/registration; it never grants StrictPIT or live authority.

This scenario requires the effective source-backed rule to specify a 10% limit and
CNY 0.001 tick. Its `qualified_limit_reference` object must carry
`schema_version="market-impact.qualified-seed-etf-limit-reference.v1"`,
`normal_session_assumption=true`, `domestic_equity_etf=true`, an ISO `listing_date`
matching captured `etf_basic`, and `identity_source_artifact_hash` plus HTTPS
`identity_source_url` for the captured historical issuer identity/listing evidence.
The rule's existing source capture and effective interval remain required. The
normal-session status is an explicit bounded assumption, not a verified claim that
all exceptional regimes have been exhaustively ruled out.

Admission and execution share a qualification check: the exact previous trading
calendar session's positive, tick-aligned raw close must equal today's positive,
tick-aligned `fund_daily.pre_close`. Bounds use that completed close with the
existing exchange-rule `ROUND_HALF_UP` formula. Current-day OHLC and daily turnover
remain outside the Agent view. Both session and admission diagnostics retain
reported limits, optional reported reference, source-row hash, signed
reported-minus-derived differences and an unresolved comparison status. Reported
limit absence or disagreement alone does not block this scenario; it does not
resolve the vendor discrepancy. Historical listing, calendar and halt gaps still
block. Missing/nonpositive factors, an exact prior/current factor discontinuity,
unknown action dates and ex-date through settlement sessions are excluded, even
when the legacy scenario could model a same-day cash dividend. This conservative
scenario adds no corporate-action or execution engine.

The separate opt-in `qualified_seed_etf_cash_only_inception_v1` basis requires an
aware `cash_only_inception_at` timestamp equal to the registered bootstrap open.
Only an implemented, positive cash distribution with `record_date < ex_date <=
pay_date` and record-date 15:00 Shanghai strictly before that inception can exempt
a post-ex-date session through payment. All prior-close/pre-close equality, tick,
stable-factor, listing, calendar and halt checks still apply. Ex-date itself,
unknown dates, and possible entitlement remain blocked. The source retains the
distribution hashes and emits the actual payment-date action with its original
record cutoff. The engine independently resolves missing pre-inception holdings
to zero, even when the account bought shares after record date. It persists
`cash-only-inception-v1` in its configuration and rejects earlier sessions or a
changed inception on replay. Policies without inception retain their serialized
identities and behavior. This exception adds no receivable accounting or historical
backfill from current data.

The added [fund_adj](https://tushare.pro/document/2?doc_id=199),
[fund_div](https://tushare.pro/document/2?doc_id=120) and
[dividend](https://tushare.pro/document/2?doc_id=103) source templates preserve actual
receipt semantics and private-research rights. `fund_div` exposes per-share cash,
record, ex-dividend and payment dates. A supported same-day ex/payment event pays
actual cash through Nautilus against the persisted record-date position; recovery
reopens that entitlement rather than using holdings on the payment date. Dividend
reinvestment, bonus shares, splits and delayed ex/payment settlement with possible
entitlement remain explicit gaps. `fund_adj`/`adj_factor` are anomaly/research evidence only: unexplained changes
block; no adjusted factor can create shares or cash. Pending receivable valuation
has not passed the pinned engine account contract, so a 120-day run crossing an
unsupported event cannot be reported complete.

The fund-dividend template uses full source-row identity because a single event can
have multiple disclosures differing in fields such as net ex-date and distribution
base. Exact repeated rows normalize once while every occurrence remains in raw
capture pages; distinct disclosures remain separate evidence. Settlement groups
matching symbol, record, ex-dividend and payment dates into one cash effect. Different
cash amounts for that event produce `corporate_action_conflicting_payment_effects`
and block settlement instead of selecting a revision or paying twice.

Received pages are persisted even when provider parsing or primary-key validation
fails. Such failed attempts retain a raw-response CAS hash with zero accepted
observations, enabling offline reparsing after a source-contract fix. Secret-bearing
or over-limit response bodies are excluded from this diagnostic retention.

The official [suspend_d](https://tushare.pro/document/2?doc_id=214) contract documents
stocks. Complete empty stock coverage can establish no reported suspension under
the modeled policy; an empty ETF response cannot. For Shanghai ETFs,
`sse_fund_suspension.py` captures the official exchange fund-suspension query through
the same durable claims, authenticated Journal receipts and raw CAS. Unsigned
diagnostic captures cannot authorize execution. Absence is usable only after exact
symbol, unfiltered query, complete pagination and continuous history from inception
have been verified. An overlapping suspension record remains a typed gap until its
session semantics are mapped; neither a blank time nor a reason string proves a
full-day trading halt. Unknown post-dispatch state requires reconciliation, while
saved responses replay without another fetch. Each historical input binding verifies
the bound SSE CAS and authenticated receipt graph once under a lock, then projects
sessions from immutable coverage ranges, record intervals and source hashes. A new
binding (including `with_snapshots(())`) and every standalone reopen verify the
graph afresh; session projections never replay the parent Run Journal.

The coverage artifact is part of every historical source binding and baseline
replay identity. New coverage creates a new version; the actual current receipt
never gains historical StrictPIT authority. Other exchanges or unsupported fund
status sources retain `halt_status_unverified`. The synthetic positive fixtures exercise the same provider,
CAS, source authority, dynamic admission and real Nautilus route, while actual ETF
source availability and user entitlement must be verified independently.

## Generic dynamic A-share qualification

`SourceBackedAShareRulePolicy` reopens a root-signed
`ashare.rule_policy.accepted` Journal event and its immutable policy artifact.
The ordinary `research.public.received` event records acquisition provenance; it
is **not** a signed acceptance. Harness-only `accept_ashare_rule_policy` binds the
reviewed 2026 revision to the source receipts, actual receipt times, source URLs
and verified raw CAS bytes. Reopening checks the root signature, fixed registered
numeric policy and complete source coverage. Agent dictionaries cannot mint this
acceptance. The accepted source interval begins on 2026-07-06; its end is absent
unless an actual bound is supplied. A current qualification cannot precede policy
acceptance. Historical use remains explicitly modeled and cannot authorize dates
before this source revision became effective.

Required source coverage includes the SSE and SZSE 2026 notices and full rules,
the SSE deferred-implementation attachment, the applicable tax notice, and Tushare
ETF/fund classification documentation. Auction buy sizing follows clauses 3.3.8
and 3.3.11: 100 shares/units, CNY 0.01 equity tick and CNY 0.001 fund tick. Deferred
block-trade behavior is not enabled. Commission is the separately registered
**SYNTHETIC** account policy (0.0003, minimum CNY 5), not an exchange/broker fact.
The modeled stamp-tax rates are 0.0005 for equity sells and zero for equity ETFs.
This is a bounded local account fee model, not a claim of complete broker charges.

`qualify_ashare_security` selects a concrete acquired security by venue and
supported class, without a symbol whitelist or manually injected symbol rule.
Current mainboard stock identity excludes known special/delisting names and
unsupported board prefixes. ETF qualification requires acquired `etf_basic`
domestic/tracked-index identity plus matching `fund_basic` exchange-listed,
`股票型` classification and listing date. `etf_type=境内` alone does not distinguish
equity from bond or commodity funds. The added `lookup_fund_asset_class(ts_code)`
route uses [fund_basic](https://tushare.pro/document/2?doc_id=19), with the existing
source configuration, capture and successor mechanisms.

The resolver persists `market-impact.ashare-security-qualification.v1` and a
concrete `market-impact.ashare-qualified-rule.v1` artifact. The rule's static
lot/tick/class identity is distinct from session execution readiness and stays
stable when unchanged source evidence is reopened at a later cutoff. In particular,
a domestic stock ETF is **not** automatically a 10% product: SZSE 3.3.13 includes
20% tracked-index exceptions. The bounded normal-regime rule requires the session's
reported limits to match the accepted 10% formula before execution. Stock execution
also requires at least five prior observed exchange sessions since listing;
calendar days and names without “ST” do not prove a normal historical regime.
Unknown/special regimes remain gaps.

`ProspectiveAShareInputs` uses actual received identity and daily facts for static
qualification and completed-session reference valuation. `raw_price_observed_at`
remains the actual prior daily close; receipt today cannot make it an intraday
quote. This base adapter always carries `fresh_intraday_quote_missing` and grants
no executable quote authority. Research may still finish outside market hours.
The `lookup_stock_quote`/`lookup_fund_quote` routes accept only `freq=1MIN` and
provide actually observed bars, not an order book. Frozen quote reuse requires
both source observation and receipt within two minutes of the frozen cutoff.
A stale view queues the existing acquisition continuation; fulfillment cannot
silently refresh the active Run. Repeated requests in the same frozen binding
reuse its existing request/completion identities.

Historical expansion is opt-in through
`ModeledHistoricalPolicy(research_projection="dynamic_ashare_sources_v1")` plus
accepted generic qualification policy. Route-specific projections include immutable
listing identity, calendar, preopen reported limits/full-day halts, completed
adjustment factors and announcement-dated action schedules. Current company names,
industry and stock/fund classifications are not backfilled. Their historical
regime authority requires observations actually received by that cutoff; otherwise
the result records a gap. Intraday quote routes remain unprojectable. The old
`qualified_seed_etf_exchange_rule_v1`, including its two-symbol restriction and
serialized default-policy identities, remains unchanged.
