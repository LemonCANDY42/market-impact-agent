# Phase 2 Calibration Protocol

## Status and purpose

Phase 2 is active and has not passed its exit gate. The deterministic replay capability
is accepted for a bounded integration slice; profitability, baseline superiority, and
cross-event robustness are not.

The calibration gate exists to keep those claims separate. It consumes repeated,
engine-neutral Backtest Results and produces a content-identified acceptance report. It
does not select instruments, generate a Signal Intent, tune a strategy, or grant paper or
live capability.

## Frozen first-family protocol

`energy-supply-shock-calibration.v1` requires:

- horizons of 1, 3, and 10 held sessions in every request;
- two independently executed Results with the same deterministic result identity for every
  Event Cluster and variant;
- one `event_reasoning` candidate and four baselines for each cluster:
  `sentiment`, `momentum`, `fixed_mapping`, and `simple_hold`;
- at least two chronologically earlier training Event Clusters and three later test Event
  Clusters, with no cluster crossing the split;
- identical as-of time, evaluation window, instrument set, Data Snapshot, horizons, and
  Simulation Specification across variants within one cluster, plus identical manifest
  engine, bridge, adapter, named input hashes, engine-configuration hash, and artifact refs;
- one frozen strategy reference per variant across the cohort;
- a positive mean candidate `net_return` after modeled costs on the test partition;
- the candidate beating at least one complete baseline on the same test cells; and
- no one test Event Cluster contributing more than 50% of the candidate's absolute
  cross-horizon outcome.

The event count and dominance bound are minimum anti-demo safeguards, not claims of
statistical significance. Later research may strengthen them through a versioned protocol;
it may not weaken them after seeing results.

`net_return` is normalized from trade PnL after modeled fees divided by entry notional.
This avoids comparing absolute one-lot PnL across differently priced securities or using
arbitrary starting cash as the denominator. The gate accepts this metric only with unit
`ratio`; a same-named currency or other-unit metric is missing evidence, not a conversion.

The gate verifies stable strategy references and content identities but cannot prove when a
local plan was first written. For the real cohort, a separately retained registration
artifact must freeze case membership, partitions, rules, and request identities before test
Results are opened. That temporal attestation is required research evidence; the generated
passing cohort in tests proves gate mechanics only.

## Independent horizon execution

One Backtest Request may bind multiple horizons. The bridge creates and disposes a fresh
Nautilus BacktestEngine for each horizon, then prefixes normalized metrics with
`horizon_<sessions>.`. Engine state, orders, cash, and strategy state are therefore not
shared across parameter runs. A single Result binds all horizon outputs to the same request,
snapshot, adapter, engine configuration, and deterministic result identity.

This follows NautilusTrader's official repeated-run guidance: low-level BacktestEngine is
appropriate for non-catalog in-memory data and independent configurations should use fresh
engines. The harness retains the low-level boundary because the current private Data
Snapshot is not a Nautilus ParquetDataCatalog.

## Current real evidence

The private Abqaiq/`600028.XSHG` bundle was replayed twice without `TUSHARE_TOKEN` for
1/3/10 sessions. The runs had separate run IDs and execution times and identical request,
configuration, metrics, artifacts, and result identities:

- request hash: `0e108692ad42361bac28a20ac8155670f60ea68d290121bd4e4c604945357935`;
- engine-configuration hash:
  `ddd6f3ba3fdaa93d7bf63a9aa0e7e39cef5191d57abf7a770f78e35f3e020bcc`;
- result hash: `a974181a4e65ec91e6203876647c52211be00f234be5ec6e10df602e8a75a726`.

The private calibration evidence and report are ignored, mode `0600`, and have identities:

- evidence hash: `975d34df4a81f65b8642051d4f693cacdc5b9ec639ea73138d7d78b805d0c72b`;
- report hash: `881bd45136fdf0167fd1aa3ee94cbc423fbc87544be5b5de60e2118fda49dd1b`.

The gate correctly rejected this evidence. It is one test event with no training cohort or
baseline variants, its target selection is explicitly a manual integration fixture, its
single-event dominance cannot be cleared, and its candidate mean net return is not positive
under the frozen modeled-open assumptions. Licensed observations and metric values remain
private.

## Next admissible work

Remain in Phase 2. Before another gate run:

1. pre-register a point-in-time energy supply-shock Event Cluster cohort and chronological
   split;
2. define one frozen, falsifiable signal rule for the candidate and each baseline without
   inspecting test outcomes;
3. harden the Data Snapshot with source-provided adjustment factors and daily price limits,
   or reject any case that needs corporate-action or venue-rule inference;
4. capture compatible private data, execute every variant twice, and retain failure Results;
5. run the same gate without changing `energy-supply-shock-calibration.v1`.

Failure remains a valid result and stops expansion. Phase 3 event-family promotion and Phase
4 paper execution do not begin until the gate accepts real out-of-sample evidence.

The first real cohort and the pre-outcome correction for honest long-only abstention semantics
are now frozen in `docs/PHASE2_REAL_COHORT.md` and
`examples/calibration/energy-supply-shock-cohort-v1.json`. The existing v1 gate and its
single-event rejection remain historical evidence; the registered cohort must use the
versioned v2 semantics described there.
