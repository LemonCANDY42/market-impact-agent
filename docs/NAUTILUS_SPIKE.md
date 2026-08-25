# NautilusTrader Compatibility Spike

Date: 2026-08-25

## Question

Which exact NautilusTrader release should be the first implementation candidate for the
engine-neutral backtest bridge, without implying paper or live execution acceptance?

## Boundaries

- Used disposable virtual environments only; no project or machine-global dependency was
  added.
- Tested package installation, public backtest types, `BacktestEngine` construction, and
  disposal. No historical data, strategy, simulated venue, order, or broker adapter was
  exercised.
- Did not connect IBKR, VeighNa, credentials, accounts, or network market-data streams.
- This is compatibility evidence, not deterministic replay, performance, Provider, paper,
  or live acceptance.

## Official evidence

- The [installation guide](https://nautilustrader.io/docs/latest/getting_started/installation/)
  supports Python 3.12–3.14 and macOS ARM64 and distinguishes stable installs from v2
  release candidates.
- [PyPI release history](https://pypi.org/project/nautilus-trader/) listed `1.231.0` as the
  stable release and `2.0.0rc3` as a pre-release at the time of the spike.
- The NautilusTrader [PyPI project page](https://pypi.org/project/nautilus-trader/) states
  that release candidates are for community testing and are not recommended for production.
- The [v2 roadmap RFC](https://github.com/nautechsystems/nautilus_trader/issues/4042)
  describes ongoing parity validation and breaking changes through the RC phase.
- The [low-level backtest guide](https://nautilustrader.io/docs/latest/getting_started/backtest_low_level/)
  identifies `BacktestEngine`; the latest v2 documentation does not preserve the v1 import
  path.

## Local matrix

Host: Apple M1, macOS 26.6.2. Interpreters were official Homebrew CPython builds.
The tested `1.231.0` macOS wheels were tagged `macosx_26_0`; this spike does not locally
accept macOS 15–25 for that release.

| NautilusTrader | Python | Wheel install | Backtest types | Engine construct/dispose |
| --- | --- | --- | --- | --- |
| `1.231.0` | `3.13.15` | Pass | Pass via v1 module paths | Pass |
| `1.231.0` | `3.14.6` | Pass | Pass via v1 module paths | Pass |
| `2.0.0rc3` | `3.13.15` | Pass | Pass via v2 package exports | Pass |
| `2.0.0rc3` | `3.14.6` | Pass | Pass via v2 package exports | Pass |

Observed compatibility difference:

```text
1.231.0: nautilus_trader.backtest.engine / nautilus_trader.backtest.node
2.0.0rc3: nautilus_trader.backtest package exports
```

This difference stays inside `NautilusBacktestBridge`; Harness public contracts import no
NautilusTrader type.

## Compatibility decision

Use stable `1.231.0` as the first Phase 2 implementation candidate. Keep `2.0.0rc3` as a
migration comparison only. Do not add either package to project dependencies until the
bridge can run the first deterministic A-share replay twice with identical normalized
results.

That gate has now passed for the bounded synthetic replay described below, so stable
`1.231.0` is an exact optional project dependency. The RC remains comparison-only.

## Replay acceptance criteria

The first replay must bind an immutable Backtest Request to:

- the exact Signal Intent content, target, direction, and validity window;
- a frozen data snapshot and fixed pre-event universe;
- daily-bar granularity for the initial slice;
- next-executable-price timing;
- A-share T+1, price limits, suspensions, lot sizes, fees, and slippage;
- exact engine and bridge versions plus configuration hashes;
- canonical request and result SHA-256 identities, with result identity excluding per-run
  metadata, plus explicit failure artifacts.

Passing that replay grants backtest capability only. It grants no Provider, paper, live,
IBKR, or VeighNa capability.

## First replay acceptance

The repository fixture `synthetic-xshg-600028-20260825-v1` is deliberately synthetic and
tests mechanics, not alpha or historical performance. Its daily bars are replayed in
chronological order; each session open is represented as a top-of-book event derived from
that same frozen bar so Nautilus does not substitute the daily close for an opening fill.

The long-only replay passed twice with bridge version `0.2.0`; a bound `SELL` Signal
Intent fails closed instead of replaying the long strategy. The accepted `BUY` replay has:

- request hash `9c633288a772b62e18116457237769622e2d1fc9f433a2a5bc1136bbb277b0da`;
- engine-configuration hash
  `2c6839122f50ffc71bcb505d5055517f315fb32bc53198d5f79ab283e84a3f35`;
- result hash `d18eddabac9e67e72c4ff0e6ffd07b621d9e4815b6e3e31da25812eb4e8edbf2`;
- entry delayed two sessions by a suspension and an upper-limit opening lock;
- entry `10.81`, exit `11.39`, three held sessions, `10.57 CNY` costs, and
  `47.43 CNY` normalized net PnL for the 100-share fixture trade.

The acceptance is executable in `tests/test_nautilus_backtest.py`. It is not exchange-rule
certification, real-data validation, a profitability claim, or permission to advance to
paper execution. Exact synthetic assumptions are recorded in `docs/A_SHARE_REPLAY.md`.
