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

## Explicit non-claims

This slice does not yet cover board-specific limits, ST securities, IPO windows, corporate
actions, auctions, partial fills, order queues, real suspensions, holidays, data revisions,
survivorship, or real Tushare data. Those require separate fixtures and acceptance before
the ruleset can be promoted beyond synthetic mechanics.
