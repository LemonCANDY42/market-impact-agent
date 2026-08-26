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

The current local acceptance completed on 2026-08-25 with `TUSHARE_TOKEN` explicitly removed
from both replay processes. Each run created a fresh Nautilus engine for the 1-, 3-, and
10-session horizons. Two runs had distinct run IDs and execution times but identical
request, named input, engine-configuration, metric, artifact, and result identities:

- request hash: `0e108692ad42361bac28a20ac8155670f60ea68d290121bd4e4c604945357935`;
- engine-configuration hash:
  `ddd6f3ba3fdaa93d7bf63a9aa0e7e39cef5191d57abf7a770f78e35f3e020bcc`;
- result hash: `a974181a4e65ec91e6203876647c52211be00f234be5ec6e10df602e8a75a726`.

The two normalized results remain private under the ignored `.market-impact/replays/`
directory with a `0700` acceptance directory and `0600` files. No licensed metric is
recorded here.

The Phase 2 calibration gate then consumed those repeated Results as one
`event_reasoning` test observation. It correctly rejected the evidence because it has no
training cohort or four baseline variants, target selection is a manual integration fixture,
single-event dominance cannot be cleared, and candidate mean net return is not positive
under the frozen assumptions. The private evidence/report hashes are recorded in
`docs/PHASE2_CALIBRATION.md`; metrics remain private.

## Hardened cohort adapter v2

`tushare-xshg-modeled-open` adapter version `2.0.0` preserves the one-lot modeled-open
execution boundary and adds source-bound `adj_factor` and `stk_limit` tables. Its Simulation
Specification names `tushare_unadjusted_daily_with_source_limits.v2` and
`xshg_main_board_source_limit.v2`; target selection is the public registered mapping
`registered-a-share-integrated-oil-proxy:600028.v1`, not the historical manual Abqaiq
fixture.

The v2 Data Snapshot may begin before the event cutoff. Those earlier sessions exist only
for point-in-time observation rules. Momentum compares `close * adj_factor` using sessions
whose close was visible by the cutoff. The Nautilus replay snapshot starts at the registered
evaluation session, uses unadjusted OHLCV and source daily price limits, and requires a
constant adjustment factor throughout that evaluation segment. A pre-evaluation corporate
action can therefore inform an adjusted observation without introducing a discontinuity
into the executable replay.

The first public cohort was frozen before its five test windows were captured. Seven private
snapshots produced one private execution registration with 35 Variant Decisions: 25 buys
and 10 honest abstentions. Every buy completed twice, with a fresh Nautilus engine for each
1/3/10-session horizon. The v2 gate verified the registered/repeated evidence and rejected
the cohort only because candidate mean net return was not positive. Exact identities and
the no-retuning boundary are in `docs/PHASE2_CALIBRATION.md`.

## Explicit non-claims

The modeled-open integration replay is not source truth, historical listing truth, actual
liquidity or fillability, historical fee or venue-rule correctness, instrument-selection
validity, alpha, baseline superiority, Provider verification, or paper/live readiness. It
does not cover other XSHG/XSHE boards, ST securities, IPO windows, corporate actions,
auctions, partial fills, queues, observed suspensions, data revisions, or survivorship.
V2 source adjustment factors and daily limits narrow two inference gaps; they do not make
modeled opening liquidity observable or establish that the integrated-oil proxy is the right
economic exposure.

## Frozen Agent judgment integration

The research runtime now has a separate deterministic registration step. It validates a
sealed Judgment Artifact against its exact Evidence Pack, applies a fixed confidence and
direction admission rule, creates the existing Signal Intent, and binds the resulting
Backtest Request to `judgment-artifact:<artifact_id>`. The candidate's frozen horizon is the
only replay horizon. Mixed, unknown, abstaining, out-of-scope, and below-threshold candidates
fail before Nautilus starts.

`tests/test_judgment_replay.py` drives a generated Agent judgment through that registration
and the existing synthetic `600028.XSHG` Nautilus bridge twice with identical result
identity. This establishes the architectural chain Agent judgment -> deterministic Signal
Intent -> Backtest Request -> Nautilus. The model is never called from the replay. The test
does not claim that the synthetic judgment is accurate, that the real MiniMax energy result
is replayable against a matching market snapshot, or that Phase 2 calibration has passed.

The same test module now also builds five isolated generated Judgment results, obtains exact
three-of-five agreement on the selection-eligible `600938.XSHG/up/3 sessions`, validates
every agreeing Artifact against the frozen execution binding and Exposure Registry, and
replays the resulting Ensemble Decision twice. A registry control target is rejected before
Nautilus starts. The generated 600938 snapshot reuses the synthetic bridge's price path, so
the fixture remains deterministic at CNY 47.43 net PnL and
`0.04387604070305272895467160037` net return. Those numbers belong only to the generated
interface fixture and do not validate 600938 market behavior. The real MiniMax ensemble
selected `600938.XSHG/up/1 session`, so it was not attached to this three-session snapshot
and has no reported return.
