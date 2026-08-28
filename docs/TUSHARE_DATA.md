# Tushare data boundary

The first Tushare slice is a read-only, language-neutral HTTPS adapter. It materializes
deterministic tables and a fixed SSE/SZSE universe without making Tushare, its Python SDK,
or a data vendor the orchestration owner.

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
and twelve content-identified route configurations. The owner's purchased 10,000-plus-point account
is treated as fully usable for this private Harness; the token is read only from `TUSHARE_TOKEN` and
is excluded from requests persisted for replay, hashes, logs, errors, configs, and Agent tools.

The official route contracts are [`news`](https://tushare.pro/document/2?doc_id=143),
[`index_daily`](https://tushare.pro/document/2?doc_id=95),
[`fund_daily`](https://tushare.pro/document/2?doc_id=127),
[`trade_cal`](https://tushare.pro/document/2?doc_id=26),
[`etf_basic`](https://tushare.pro/document/2?doc_id=385),
[`stock_basic`](https://tushare.pro/document/2?doc_id=25),
[`stk_limit`](https://tushare.pro/document/2?doc_id=183),
[`index_classify`](https://tushare.pro/document/2?doc_id=181),
[`index_member_all`](https://tushare.pro/document/2?doc_id=335),
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

On 2026-08-28 all twelve checked-in routes passed the seven route gates after their official
documentation locators were bound and the new config identities were reaccepted:

| Route | Capability | Accepted observations | Acceptance report |
| --- | --- | ---: | --- |
| `index_daily` | market context | 1 | `source-route-acceptance-report-653c4bc24cbad5d3d020cf567b37295702d8b8636cee4b5e62127093d94c11c5` |
| `fund_daily` | market context | 1 | `source-route-acceptance-report-3fd9945ec5fb09815f929f7b9d0b48f92a71814a91ac98693eb5ef7265324d84` |
| `trade_cal` | market context | 1 | `source-route-acceptance-report-4dc04dc822718ae1b75089ea9f0bc25dedbbe7be2d665e70ad601b675e910216` |
| `etf_basic` | exposure candidates | 1 | `source-route-acceptance-report-59730b892100961a32fdf4eaa6ed789974831202db6ce21ba9c26df519ed43a3` |
| `stock_basic` | exposure candidates | 1 | `source-route-acceptance-report-a37d3dfb6201ecded01d8933e2380802bbc30adf47b8cdf93ca393dfed90340d` |
| `stk_limit` | exposure candidates | 1 | `source-route-acceptance-report-c8fd0804e7c67a8e55d35fe24d7cb3441a60edde40f2f9183d09ed36b242cf09` |
| `index_classify` | exposure candidates | 31 | `source-route-acceptance-report-f6c4c9082b1b65839dfc7b74f9cc5eede7d35a89654615c48191e4db0d0352cb` |
| `index_member_all` | exposure candidates | 126 | `source-route-acceptance-report-f626fc923bc68930cce7782bf5e02f7d37b338594f441b0c841c5658a8e51565` |
| `margin` | positioning | 3 | `source-route-acceptance-report-287411904eab4b6597614cff1652c2dd4feae69b60e84eff0ee2734ecb591237` |
| `cn_schedule` | macro schedule | 14 | `source-route-acceptance-report-52d94fbcbf6bd5959d82a16013d67413c37a04f5d31b6cf6fc1fb74f5635da2c` |
| `report_rc` | prior expectation observations | 4,802 | `source-route-acceptance-report-a79d8525ea67763d8022a8909dcfd7a85683f5c476ab6ff2f89149aec9fba8ff` |
| `news` (`src=sina`) | event revelation | 29 | `source-route-acceptance-report-e9bc974b0b3e0101701fed3b0dd37e57cc7fc595b93dff3802129fb125b9dde8` |

This is route-level prospective evidence, not completion of PDI-10 through PDI-16. In particular,
Tushare `news` is an aggregator route even when `src=sina`; its receipt does not establish a direct
publisher archive or historical authority. `cn_schedule` is advisory scheduling, not an original
NBS release or revision lineage. `report_rc` supplies cited forecast observations, not a consensus
unless the registered population/window/method derives one. Listing, ETF, limit, and industry rows
also do not by themselves prove lot/tick size, decision-time tradability, full market completeness,
or an effective industry-to-tradable-ETF mapping.

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
