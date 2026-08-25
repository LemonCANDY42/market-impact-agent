# Tushare data boundary

The first Tushare slice is a read-only, language-neutral HTTPS adapter. It materializes
deterministic tables and a fixed SSE/SZSE universe without making Tushare, its Python SDK,
or a data vendor the orchestration owner.

Official contract references: [`stock_basic`](https://tushare.pro/document/1?doc_id=25),
[`trade_cal`](https://tushare.pro/document/2?doc_id=26), and
[`daily`](https://tushare.pro/document/2?doc_id=27). The hardened v2 bundle additionally
uses [`adj_factor`](https://tushare.pro/document/2?doc_id=28) and
[`stk_limit`](https://tushare.pro/document/2?doc_id=183).

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

Responses must have the exact requested field set, scalar rectangular rows, valid dates,
matching query identities, unique primary keys, coherent OHLC values, and fewer than the
documented 6,000-row response ceiling. A `trade_cal` response must also contain exactly one
row for every natural date in its requested inclusive interval; omitting the same date from
both calendar and daily responses therefore cannot hide a gap. Fields and rows are normalized
before hashing, so transport order does not change table identity. The token is excluded from
hashes, returned objects, error text, examples, and committed artifacts.

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
adjusted closes. Adjustment-factor changes are allowed only in that observation segment.
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
window. It does not establish general quota behavior, historical completeness, source
correctness, or licensed replay validity. The Provider therefore remains `enabled: false`,
with no verified capabilities and trust tier `unverified`.

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
