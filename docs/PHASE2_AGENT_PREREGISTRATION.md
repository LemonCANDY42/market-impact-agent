# Prospective Agent Phase 2 Preregistration

## Status

The first materially new study after the failed v2 cohort is registered but has not begun
accruing events. It grants no calibration, alpha, Phase 3, paper, account, order, or live
execution claim. The previously opened v2 events remain research material and are named by
hash in the registration; none can become holdout evidence again.

The public artifacts are:

- `examples/research/a-share-energy-exposure-registry-v1.json`, Exposure Registry hash
  `c864087a7f68b3732d7caf11ca915eca56c52f9cf25f5effac2a6aefc1a2326f`;
- `examples/calibration/agent-physical-energy-prospective-v1.json`, registration hash
  `0e5df98482c956d76b53f6330814dd287f692cf1328119b5678cb1a429ae1aaa`.

Both identifiers are derived from canonical content. Editing a rule, source, target,
timestamp, baseline, or acceptance condition invalidates the identifier rather than silently
changing the study.

## Why this is a different hypothesis

The rejected v2 rule applied one integrated refining-heavy proxy and fixed action logic over
historical cases. The new hypothesis changes three decision owners while preserving the same
engine-neutral replay boundary:

1. a pre-outcome Exposure Registry limits selection to economically distinct upstream
   targets and retains the old integrated downstream proxy only as a control;
2. five independent Judgment Artifacts choose target and 1/3/10-session horizon from the
   same point-in-time Evidence Pack, with deterministic three-of-five exact agreement; and
3. the cohort is the first five future qualifying physical disruptions, not another chosen
   list of historical winners or losers.

This is still a falsifiable long-or-abstain hypothesis. An Agent can abstain, disagree, or be
wrong. The registration tests whether the ensemble adds value over simpler rules; it does
not presume that model reasoning is alpha.

## Prospective event accrual

Accrual opens after `2026-08-27T00:00:00Z` and closes at
`2027-12-31T23:59:59Z`. The first five independent qualifying Event Clusters are admitted,
with at least ten calendar days between event occurrence times. Qualification requires an official
or directly involved primary source to confirm an unplanned physical production, transport,
or storage loss of at least 500,000 barrels of oil equivalent per day or five percent of the
affected regional supply using the latest official denominator visible before the event,
expected or observed to last at least 24 hours.

Policy-only targets, demand-only price moves, pre-announced maintenance, and events known
only retrospectively are excluded. Once admitted, an Accrued Event is never replaced. If
fewer than five qualify before close, the study is inconclusive and cannot promote.

Event eligibility uses only occurrence facts. Benchmark reaction, target prices, later
restoration detail, and model agreement are not allowed to decide whether an event joins the
cohort. This keeps event admission independent of the result being measured.

## Point-in-time Agent protocol

For each Accrued Event, the evidence cutoff is exactly 60 minutes after the first qualifying
source version becomes strategy-visible. Only Evidence Items available at or before that
cutoff may enter the frozen Evidence Pack. The modeled entry is the first executable XSHG
open strictly after the cutoff.

The Harness then runs five independent MiniMax M3 Judgment replicates with:

- the same content-identified Evidence Pack, Pattern Pack, runtime reference, model, Skill,
  and read-only tool surface;
- no cross-replicate memory or debate;
- only the `energy-supply` Skill and `read_evidence`/`read_pattern_pack` tools;
- long-or-abstain direction and eligible horizons of 1, 3, or 10 sessions; and
- exact three-of-five agreement on target, direction, and horizon.

Every input hash is frozen before replicate one. Invalid artifacts, fewer than three matching
proposals, or no eligible target produce an abstention. The deterministic Ensemble Decision
cannot invent or average a proposal that no qualifying replicate made.

## Targets and baselines

The official issuer material frozen in the Exposure Registry supports these roles:

| Instrument | Registered role | Candidate eligible |
| --- | --- | --- |
| `600938.XSHG` CNOOC | upstream producer, direct exposure | yes |
| `601857.XSHG` PetroChina | integrated upstream, second-order exposure | yes |
| `600028.XSHG` Sinopec | integrated downstream control, third-order exposure | no |

The candidate is compared on the same Accrued Events with five frozen baselines:

- commodity confirmation followed by a three-session CNOOC position;
- fixed three-session CNOOC exposure;
- simple ten-session CNOOC hold;
- the first valid single-Agent proposal without ensemble agreement; and
- pre-cutoff five-session target momentum followed by a three-session hold.

The ensemble must beat both the fixed-upstream and single-Agent baselines and at least two
meaningful baselines in total. This separately tests whether upstream remapping helps and
whether five independent judgments improve on simply accepting one model answer.

## Missingness and acceptance

Missing post-accrual benchmark, target, Evidence Pack, Agent, or market inputs cannot delete
or replace an event. The candidate contributes an explicit abstention and zero return to the
primary all-event denominator. Candidate coverage must still reach at least three events.
A Common-Support View is reported only as a secondary diagnostic; it cannot replace the
primary result or make the gate pass.

Every registered trade requires two deterministic Backtest Results. Acceptance additionally
requires positive mean candidate net return after modeled costs, the registered baseline
wins, and no single event contributing more than 40% of absolute candidate outcome. Reports
must include coverage, net return, drawdown, turnover, Sharpe, calibration, and tail loss.

## Next operational work

The private append-only Accrual Ledger is implemented. It binds every Candidate Event
Observation to this registration, requires actual-receipt availability, preserves source
publication/update/retrieval and raw-content hashes, replays revision lineage and admission,
retains explicit non-admission reasons, computes the 60-minute cutoff only for admitted
events, and rejects duplicate-content drift, receipt reordering, or stored-row/hash-chain
tampering. It exposes no broker, paper, order, or account capability.

The ledger does not fetch or interpret arbitrary URLs. Its input must be produced by a
source capture/adapter and contains a claim summary plus the hash of privately retained raw
content; licensed or mutable source bodies do not enter the repository. Aggregator or news
observations may be recorded for discovery but cannot qualify until an official or directly
involved primary-source revision supersedes them.

Critical onset, commodity, magnitude, unit, or duration fields may be explicitly unknown.
Such observations remain in the ledger with `missing_critical_data` and cannot accrue; a
lineaged later revision may fill the missing fact. This implements the registered
retain-and-abstain rule instead of silently improving the cohort by dropping incomplete
candidates.

Next, implement fixed direct-source capture/monitoring for the registered physical-energy
family, retain its raw versions privately, and feed valid Candidate Event Observations into
the ledger. When the first event is admitted, a scheduler must wait until the recorded
cutoff and freeze the Evidence Pack before replicate one. No real event has yet been
recorded or admitted.

Validate the frozen public contracts with:

```bash
uv run market-impact agent study-validate \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json
```

Record one adapter-produced observation and validate the resulting private ledger with:

```bash
uv run market-impact agent study-observe \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --observation CANDIDATE_OBSERVATION.json \
  --raw-source RAW_SOURCE_FILE \
  --ledger LEDGER.sqlite3

uv run market-impact agent study-ledger-validate \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --ledger LEDGER.sqlite3
```
