# Synthetic A-share Replay Rules

## Scope

`xshg_cash_equity_fixture.v1` is a deterministic test ruleset for the first Nautilus
bridge acceptance. It is not a complete SSE rulebook, a current fee schedule, exchange
certification, or a reusable claim about real A-share execution.

The fixture contains generated prices only. It contains no licensed market data and tests
mechanics rather than strategy quality.

## Frozen assumptions

- One XSHG cash equity, CNY account, no cash borrowing, and a 100-share order and lot.
- Daily OHLCV bars with explicit session-open and session-close timestamps.
- Each opening top-of-book event is derived from that session's frozen daily open. The
  strategy receives events chronologically and cannot access that session's close early.
- A suspended session has flat prices, zero volume, and zero opening liquidity.
- A one-price upper-limit opening lock has zero ask liquidity, so a buy cannot execute.
- Prices must remain within a synthetic 10% daily band rounded to the `0.01 CNY` tick.
- Entry occurs at the first opening with ask liquidity. Exit occurs at the first opening
  with bid liquidity after the requested number of held sessions. A positive horizon
  therefore cannot sell on the purchase session and is T+1 safe within this fixture.
- The fixture is long-only. Its bound `BUY` Signal Intent drives buy entry and sell exit;
  a `SELL` Signal Intent fails explicitly. The first executable buy must occur before the
  bound signal expires.
- `next_executable_open_one_tick_slippage.v1` applies one deterministic tick against each
  market order with random seed `7`.
- `a_share_fixture_fee.v1` charges 0.03% commission with a `5 CNY` minimum on each side,
  plus a synthetic 0.05% sell-side stamp charge. These are test constants, not a statement
  of current statutory or broker fees.

## Acceptance fixture

`examples/backtests/synthetic-xshg-600028-20260825-v1.json` first presents a suspension,
then an upper-limit opening lock, followed by executable sessions. The strategy buys the
first executable open, holds three sessions, and exits at the next eligible open.

The request embeds the complete immutable Signal Intent, including target, direction,
validity, evidence, and invalidation fields. That content, the exact engine configuration,
snapshot bytes, normalized metrics, and failure reasons are bound into canonical hashes.
Per-run UUID and wall-clock execution time are excluded from result identity, so two
frozen runs must produce the same result hash.

## Validated Tushare modeled-open gate

`tushare-xshg-modeled-open` version `1.0.0` is a separate adapter for the first private
Tushare integration replay. It always runs `validate_tushare_data_bundle`, then re-reads each
consumed Parquet file as hash-bound private bytes before parsing it in memory. It writes no
derived fixture, needs no token or network access, and does not enable or verify the Tushare
Provider.

The adapter is deliberately bounded to validated `600028.SH`/`600028.XSHG` SSE data and its
versioned XSHG main-board rules. It accepts any fully validated bundle whose snapshot and
request window match, including generated bundles in contract tests; it does not hardcode a
private bundle identity as an executable input. The committed request
`examples/backtests/real-abqaiq-600028-tushare-request-v1.json` binds the existing Abqaiq
Event Assessment evidence and identifies its target selection as
`manual-integration-fixture:abqaiq-600028.v1`; target selection is an integration fixture,
not a research or listing-validity conclusion. The named private bundle and request below are
the first local acceptance evidence, not the adapter's only accepted path.

The versioned simulation contract is:

- unadjusted Tushare daily OHLCV (`tushare_unadjusted_daily.v1`);
- Tushare `vol` converted exactly from hands to shares;
- Shanghai session timestamps at 09:30 and 15:00 Asia/Shanghai;
- a modeled 10% XSHG main-board band rounded half-up to the `0.01 CNY` tick;
- exactly one 100-share lot of modeled opening bid and ask liquidity for positive-volume,
  non-limit-locked sessions;
- zero modeled ask at an upper-limit open and zero modeled bid at a lower-limit open;
- no slippage, a 0.03%/5 CNY commission assumption, and a 0.1% sell stamp assumption.

The one-lot QuoteTick exists only because NautilusTrader `1.231.0` requires an executable
opening event for this strategy. It is a deterministic simulation assumption, not an
observation of a book, queue, suspension, liquidity, or factual fillability. The adapter
fails closed on an unvalidated or tampered bundle; any target, snapshot, cutoff, window, or
simulation mismatch; unsupported venue, board, or rules; missing daily open sessions;
nonintegral hands-to-shares conversion; malformed or off-tick OHLCV; price-band breaches;
previous-close discontinuity/corporate-action ambiguity; or insufficient horizon.

JSON Schema validates the closed wire shape and canonical decimal strings. The codecs and
domain contracts additionally fail closed on relationships JSON Schema does not express,
including signal/window validity, request-instrument binding, canonical horizon order, and
hash identity.

Run the private gate locally without `TUSHARE_TOKEN`:

```bash
uv run market-impact backtest run \
  --request examples/backtests/real-abqaiq-600028-tushare-request-v1.json \
  --data-snapshot .market-impact/tushare/tushare-600028-sh-20190919-20191010-5289178b9ba1a6bc
```

The normalized result may be printed locally. Licensed observations, derived licensed
fixtures, and licensed replay metrics must not be committed. Synthetic generated-bundle
tests establish implementation behavior; the private command remains the separate local
acceptance evidence for the named real bundle.

The local acceptance completed on 2026-08-25 with `TUSHARE_TOKEN` explicitly removed from
both replay processes. Two runs had distinct run IDs and execution times but identical
request, named input, engine-configuration, metric, artifact, and result identities:

- request hash: `df32253c97d031d544d3c7774c02bcef6524cf375363dfd3da9367dcdf8e6037`;
- engine-configuration hash:
  `189aefe0649113e7455f81219cd2c275f8ca6e666f9a99a052fe6f998a8639a3`;
- result hash: `661063d99b4a596a66692ec6abe2e79803ca711cf54f1caf6c572a941ef10c61`.

The two normalized results remain private under the ignored `.market-impact/replays/`
directory with a `0700` acceptance directory and `0600` files. No licensed metric is
recorded here.

## Explicit non-claims

The modeled-open integration replay is not source truth, historical listing truth, actual
liquidity or fillability, historical fee or venue-rule correctness, instrument-selection
validity, alpha, baseline superiority, Provider verification, or paper/live readiness. It
does not cover other XSHG/XSHE boards, ST securities, IPO windows, corporate actions,
auctions, partial fills, queues, observed suspensions, data revisions, or survivorship.
