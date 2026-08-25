# Phase 2 Real Cohort Registration

## Frozen before outcome retrieval

`examples/calibration/energy-supply-shock-cohort-v1.json` is the public first-stage
registration for the first real Phase 2 cohort. It was written before any Tushare query or
replay for the five test Event Clusters. The existing Abqaiq observation was already known
and is therefore confined to the training partition.

This registration freezes:

- two training and five later test Event Clusters;
- the point-in-time evidence cutoff, source references, data window, and evaluation window
  for every cluster;
- one A-share integrated-oil proxy and one common Simulation Specification;
- the event-reasoning rule, four baseline rules, and all non-momentum decisions; and
- a long-or-abstain boundary. An abstention is zero exposure and must not be represented by
  a fabricated Signal Intent or a synthetic successful order.

The cohort deliberately contains persistent contractions and resolved or expansion cases.
It is not a sample of only events expected to produce a long signal. `600028.XSHG` is a
pre-registered integration proxy, not a claim that the issuer is a pure upstream exposure
or that event research selected it without a product-level mapping study.

## Two-stage registration

The public registration cannot name private Data Snapshot and Backtest Request identities
before capture. The admissible sequence is therefore:

1. commit this public cohort and rule registration;
2. capture source-bound private market observations without opening post-event returns;
3. derive the momentum decisions from only pre-cutoff adjusted closes;
4. write and hash a private execution plan binding every case and decision to exact Data
   Snapshot and request identities;
5. run the same requests twice; then open outcomes and run the gate.

Changing cohort membership, partitions, test decisions, windows, target, or rules after
step 1 creates a new protocol and cannot be used to pass this gate. A permission failure,
data ambiguity, non-deterministic replay, or rejected gate is a Phase 2 result rather than
permission to substitute cases.

## Required semantic correction

The original `energy-supply-shock-calibration.v1` assumed every variant necessarily emitted
a Signal Intent and Backtest Result. That is incompatible with the registered long-only
rules: a legitimate baseline or candidate may abstain. The real cohort therefore requires
`energy-supply-shock-calibration.v2`, which must:

- bind each decision to this registration and one exact evaluation cell;
- require repeated Backtest Results only for `buy` decisions;
- assign exactly zero return to a registered `abstain` decision without inventing a signal;
- require a beaten baseline to be active on at least one test cell and differ from the
  candidate on at least one test cell; and
- preserve the v1 requirements for chronological split, horizons, costs, determinism,
  positive candidate mean, and single-event dominance.

The semantic correction is frozen before the new test outcomes. V1 remains the historical
gate used for the earlier single-event rejection; it cannot accept this real cohort.
