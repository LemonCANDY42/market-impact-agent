# Tushare data boundary

The first Tushare slice is a read-only, language-neutral HTTPS adapter. It materializes
deterministic tables and a fixed SSE/SZSE universe without making Tushare, its Python SDK,
or a data vendor the orchestration owner.

## Accepted contract surface

The adapter calls the official JSON-over-HTTPS endpoint with an explicitly supplied token.
It requests only these bounded interfaces and fields:

- `stock_basic`: `ts_code`, `symbol`, `name`, `exchange`, `list_status`, `list_date`, and
  `delist_date`, separately for SSE/SZSE and each documented `L`, `D`, `P`, or `G` status;
- `trade_cal`: `exchange`, `cal_date`, `is_open`, and `pretrade_date` for one bounded date
  range;
- `daily`: one SH/SZ `ts_code`, one bounded date range, and unadjusted OHLC, prior close,
  volume, and amount fields.

Responses must have the exact requested field set, scalar rectangular rows, valid dates,
matching query identities, unique primary keys, coherent OHLC values, and fewer than the
documented 6,000-row response ceiling. Fields and rows are normalized before hashing, so
transport order does not change table identity. The token is excluded from hashes, returned
objects, error text, examples, and committed artifacts.

## Listing snapshot and universe semantics

`fetch_stock_listings` combines eight exchange/status queries into one immutable Listing
Snapshot. `build_pre_event_universe` then includes an instrument when:

1. its exchange is selected;
2. `list_date` is on or before the requested date; and
3. `delist_date` is absent or after the requested date.

Tushare `.SH` and `.SZ` codes become canonical `.XSHG` and `.XSHE` instrument IDs. The
Provider identity, adapter version, retrieval time, normalized listings, and query hashes
determine the Listing Snapshot hash. The sorted instrument set, cutoff date, exchanges, and
that exact snapshot hash determine the universe identity.

This is a reconstruction from data retrieved now, not evidence that the same metadata was
visible at the historical cutoff. Provider revisions, omissions, status-history gaps, and
survivorship bias remain possible. Every downstream replay must retain the exact local
snapshot and must not relabel current retrieval as point-in-time source truth.

## Acceptance status

Contract tests cover the successful request/normalization path with a deterministic
transport double, missing or malformed fields, invalid dates and prices, duplicate keys,
permission errors, secret redaction, row-order stability, and universe membership. An
anonymous call reached the official endpoint and received the expected missing-token
failure, proving transport reachability only.

No `TUSHARE_TOKEN` is available in the acceptance environment, so a real successful
response, account permissions, quota behavior, completeness, and licensed snapshot replay
have not been verified. The Provider manifest therefore remains `enabled: false`, with no
verified capabilities and trust tier `unverified`.

Token-backed acceptance must query a small known SSE/SZSE window, compare calendar and bar
shape with the official contract, confirm all eight listing partitions are complete, retain
the raw licensed payload only in approved local storage, and bind its normalized hashes into
a replay before this status can change.
