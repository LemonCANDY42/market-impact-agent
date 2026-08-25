# Phase 2 Calibration Protocol

## Status and purpose

Phase 2 has completed its first pre-registered real-cohort gate and failed it. The
deterministic replay capability is accepted for a bounded integration slice; profitability,
baseline superiority, cross-event robustness, Phase 3 promotion, paper execution, and live
execution are not. The failed v2 cohort must not be retuned or rerun as unseen evidence.

The calibration gate exists to keep those claims separate. It consumes repeated,
engine-neutral Backtest Results and produces a content-identified acceptance report. It
does not select instruments, generate a Signal Intent, tune a strategy, or grant paper or
live capability.

## Historical v1 protocol

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

## Frozen v2 long-or-abstain protocol

`energy-supply-shock-calibration.v2` preserves the v1 walk-forward, repeatability,
same-cell provenance, positive-return, baseline, and concentration gates while correcting
one semantic error: a long-only rule may honestly abstain. It adds a content-identified
execution registration created before any test replay:

- every Calibration Cell binds one Event Cluster, visibility cutoff, chronological
  partition, target mapping, evaluation window, Data Snapshot, horizons, and Simulation
  Specification;
- every Variant Decision binds `buy` or `abstain`, the frozen rule reference, and exact
  decision-input hashes;
- `buy` binds one exact Backtest Request and must produce two completed deterministic
  Results after registration;
- `abstain` has no Signal Intent, request, or Result and contributes zero exposure to the
  fixed test denominator;
- `simple_hold` must buy every cell and supplies the common runtime/input comparison anchor;
- a beaten baseline must trade at least one test cell and differ from the candidate's
  action pattern; and
- extra Results for abstentions, missing Results for buys, request drift, non-determinism,
  incomparable runtimes, or outcomes predating the registration fail closed.

The public cohort was committed before test-data capture. The private execution
registration then bound the captured Data Snapshots, adjusted pre-cutoff momentum inputs,
and exact request identities before execution. JSON timestamps alone are not the temporal
proof; the retained public commit and content hashes are part of the evidence chain.

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

## Historical one-event v1 evidence

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

## First real-cohort v2 result

The pre-registered cohort in `examples/calibration/energy-supply-shock-cohort-v1.json`
contains two train and five later test Event Clusters. Seven private hardened Tushare
bundles bind unadjusted daily data, source adjustment factors, source daily price limits,
calendar data, listing retrieval, and universe reconstruction. The execution registration
contains 35 decisions: 25 registered buys and 10 abstentions. All 25 buys were run twice;
the gate validated the repeated Results and rejected the cohort with exactly one reason:
`candidate_net_return_not_positive`.

The retained non-reversible identities are:

- public registration source SHA-256:
  `cc7a6cd61d407f6c2b23b51efd9593a42a96a3a1815923d6ec0bccb63e974f9e`;
- private execution registration:
  `600f71726a95822445deb8a0245a711806f0547418b8ceb66abfc29440fa5805`;
- evidence:
  `09a6e62eac404e2be69176f771fedda79810a60d606b3e99af49cee1d8170265`;
- gate report:
  `6d2fef9285cc6b1abe0af9a32d98547eb44ecab291ea41f4e409ed9176cdb579`.

Licensed observations and metric values remain private under `.market-impact/`. This is a
valid negative research result, not an implementation failure and not permission to tune
the same test cohort.

## Next admissible work

Remain in Phase 2 and stop capability expansion. The next workslice is a research reset,
not another v2 run:

1. diagnose the rejected mechanism using the opened cohort only as training/research
   material and record alternative explanations, especially proxy mapping, event timing,
   persistence, market regime, and long-only exposure;
2. independently research the broader event-mechanism taxonomy and candidate target maps;
3. define a materially new, falsifiable v3 hypothesis and rules before selecting later,
   previously unseen test Event Clusters;
4. pre-register a new chronological holdout and only then capture/open its outcomes.

The five opened v2 test clusters cannot be relabeled as unseen v3 evidence. Phase 3
event-family promotion, Agent runtime mutation, IBKR/VeighNa integration, paper execution,
and live execution do not begin until a new real out-of-sample gate accepts.

The cohort, outcome boundary, and long-only abstention correction are frozen in
`docs/PHASE2_REAL_COHORT.md` and
`examples/calibration/energy-supply-shock-cohort-v1.json`. V1 and v2 remain reproducible
historical protocols; neither may be silently weakened or overwritten.
