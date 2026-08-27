# Abqaiq Opened Development Benchmark

## Purpose and claim boundary

This is the first real, outcome-opened development case for the method-quality harness. It asks a
narrow question: can the same frozen Agent/runtime compare four research-method arms, react
coherently when material recovery evidence arrives, and hand its policy decision to the existing
Nautilus replay boundary without seeing the market outcome first?

It is deliberately **not** a retrospective holdout. The builder knew the event and outcome, the
Pattern Pack was written after the event, and the Agent input is identity-masked rather than
source-authenticated historical evidence. The current masking coarsens quantities, facility and
issuer names, restoration and shipment details, and shifts Agent-visible calendar dates to an
unrelated part of the year. It preserves only decision-relevant relative sequence and lag. This is
stronger linkage resistance, not an authenticated holdout: a capable model may still infer the
event from residual narrative linkage, mechanism, ordering, target role, aliases, or memorized
associations. The two
information states are one Event Case, not two independent observations. Any future result is
eligible for implementation diagnosis only; it cannot rank methods, establish historical alpha,
support a prospective claim, or grant execution capability.

The content-identified registration is
`examples/calibration/method-development-abqaiq-v1.json`, case id
`method-development-case-63fef0a73fee4d204ae4037aa93d810f212a0eaf55eeb56e1cf7db8f93e838e1`.
Its JSON Schema and strict loader require the opened-development limitations above.

## Why this case

The 14 September 2019 attack on Saudi Aramco's Abqaiq and Khurais facilities is unusually useful
for a first real development diagnostic:

- the physical shock was discrete and large: Aramco reported that 5.7 million barrels per day of
  production was suspended;
- the mitigation update arrived quickly and was concrete: on 17 September Aramco reported restored
  output, continued customer shipments using inventory and other fields, and expected full capacity
  by the end of September;
- the market reaction and reversal are heavily documented by independent public energy authorities.
  EIA described 16 September as the largest one-day Brent and WTI price increase in a decade, while
  IEA's October report observed Brent near USD 58 and below its pre-attack level after the rapid
  restoration;
- it therefore supplies two materially different point-in-time information states without turning
  them into two statistically independent events.

The primary evidence comes from Saudi Aramco's
[incident notice](https://www.aramco.com/en/news-media/news/2019/incidents-at-abqaiq-and-khurais)
and [17 September recovery update](https://www.aramco.com/en/news-media/news/2019/saudi-aramco-swiftly-restores-production-capacity).
Independent outcome context comes from EIA's
[market-reaction review](https://www.eia.gov/todayinenergy/detail.php?id=41413), IEA's
[October 2019 Oil Market Report](https://www.iea.org/reports/oil-market-report-october-2019),
and Aramco's
[2019 annual report](https://www.aramco.com/-/media/publications/corporate-reports/saudi-aramco-ara-2019-english.pdf),
which records restoration of pre-attack production on 25 September.

Other famous oil shocks were weaker first choices for this diagnostic. The March 2020 OPEC+ rupture
overlapped the COVID demand collapse; the 2022 Russia shock is a prolonged sanctions, trade-flow,
policy, and macro complex; and the Suez blockage was shorter and offered a less direct A-share
upstream mapping. Those cases may still become separate development strata, but should not be
silently pooled with this physical-loss/recovery mechanism.

## Frozen design

The target is PetroChina `601857.XSHG`, exposed to the Agent only as
`integrated-upstream-a`. It was selected from the existing upstream Exposure Registry before the
Agent runs; the Agent cannot substitute a ticker. Both states use the same target, long-or-abstain
action space, one-session horizon, MiniMax M3 Provider Profile, five independent model runs per
arm, and exact three-of-five ensemble rule.

| State | Relative cutoff | Evaluation session | Information added |
| --- | --- | --- | --- |
| attack | after the physical-loss notice | first eligible session after the frozen cutoff | physical loss, baseline, exposure and unresolved mitigation |
| recovery | three days after the attack-state evidence | first eligible session after the later frozen cutoff | restored output, inventory/other-field mitigation and short restoration guidance |

The four arms are `neutral_evidence`, `general_methods`, `general_pattern`, and `family_guided`.
Their registered research-method instructions differ. In addition, the two Pattern-enabled arms
receive the registered `pattern.read` capability, `read_pattern_pack` tool, and frozen Pattern Pack
content; the two non-Pattern arms do not. Base evidence, target, Provider/model, sampling, action
space, horizon, ensemble rule, costs, and outcome-opening boundary remain fixed. The builder already
knew the outcomes, as the registration explicitly declares; the Agent does not receive them.

The horizon is only one session because the private Tushare adjustment-factor series changes on
24 September. The v2 adapter rejected a longer evaluation segment rather than treating the
corporate-action discontinuity as price performance. This prevents a convenient longer horizon
from contaminating the comparison.

Committed public inputs are under `examples/agent/abqaiq_development/` and
`examples/backtests/real-abqaiq-601857-*-state-request-v1.json`. Licensed Tushare snapshots,
normalized price metrics, model transcripts, usage rows, and evaluation artifacts remain in the
ignored `.market-impact/` tree.

## Accepted opened-development run

The date-shifted inputs and content-bound four-arm treatment contract were accepted on 27 August
2026. The fresh comparison completed 40 of 40 required MiniMax M3 runs: both states, all four arms,
and five valid Judgment Artifacts per arm. Exact treatment route, execution binding, canonical
ensemble and replicate identities, and arm-bound totals were verified before outcome opening. Both
normalized reports and both frozen Backtest Requests then passed one joint preflight before the
evaluator invoked either private replay.

The attack-state proposal counts for `neutral_evidence`, `general_methods`, `general_pattern`, and
`family_guided` were respectively 1/5, 0/5, 1/5, and 0/5. All four recovery-state counts were 0/5.
Every three-of-five ensemble therefore abstained. The fixed-long control was net negative in both
states under the registered costs. Each state replayed twice with identical result hashes:
`56aab4e7bc5916915ab9bcf89e7504ea93d39a0c252c363b49a94d360e7046ca` for attack and
`414c9fea73000b7372cfe2bd86d830d68b661e4b8ec6e4da886fad9df3c909ba` for recovery.

Provider cost was 397,066 micro-USD in total: 82,398 for `neutral_evidence`, 111,270 for
`general_methods`, 111,483 for `general_pattern`, and 91,915 for `family_guided`. Licensed prices,
exact return metrics, transcripts, and Usage Ledger rows remain private. The accepted evaluation is
`method-development-evaluation-3abf3d9a243c9b4484d79aac5ec01f1daf6f9c7e9916b4ac7c1a79cf7a79c84b`
with artifact hash
`f1441ead2347b3c295aeb37d9623d230ffd98a148ae9c0b30158811196cfc4f6`.

Every earlier private method report, cost total, replay result, and evaluation artifact belongs to
an obsolete case/input identity and remains invalid. None may be combined with or quoted as part of
the accepted result.

Each state is valid only when all four arms have exactly five `completed` runs and every completed
run yields a valid Judgment Artifact bound to that arm's frozen execution binding. Failed and
budget-exhausted attempts remain in the append-only Usage Ledger, but the runner then fails without
producing an evaluable method report. The evaluator independently validates both states and both
Backtest Requests before it opens either outcome.

## What this does and does not tell us

The accepted run establishes a stronger masked-input contract, exact content-bound treatment
identity, fail-closed replicate completeness, joint outcome-opening preflight, deterministic
replay, and one coherent evidence-update diagnostic. Recovery evidence removed the two isolated
attack-state proposals, while all ensemble decisions remained abstentions and avoided the negative
fixed-long controls. That is useful implementation and overtrading evidence, not proof that the
Agent predicted the event or that abstention is generally profitable.

This single opened Event Case provides no between-case sample and no arm-level ensemble
difference. It therefore does **not** establish relative method quality, alpha, a prospective
result, or a preferred default method. The 1/5 proposal difference cannot rank Pattern or Skill
arms. The remaining event clues also leave residual memorization and narrative-linkage risk.

The most informative next development case is not another conveniently negative oil event. It
should have a source-documented physical shock, a clean target mapping, no corporate action in the
evaluation window, and a positive fixed-long outcome under the same costs. That tests whether the
current Harness is prudently selective or simply over-abstaining. The remaining development corpus
must then cover offset-dominant, missing-data, ambiguous-target, revision, and other mechanism
families before any method comparison is credible. Retrospective holdout admission remains blocked
separately on source-specific publisher-time authentication and frozen latency calibration.
