# Evidence-gated research method Skills

These Skills translate durable public investment-research procedures into small, persona-free
instructions. They do not ask the model to imitate a famous investor, import a portfolio style, or
convert a name into authority. A method is eligible only when the frozen point-in-time context asks
for its analysis and contains its declared evidence. Selection never uses realized case labels or
returns.

## Frozen v1 catalog

| Skill | Public lineage | Best-fit question | Required evidence | Explicit non-capability |
| --- | --- | --- | --- | --- |
| `owner-value-discipline` | Buffett/Munger public Berkshire materials | What range of operating value and opportunity cost is supported? | earning power or cash flow plus price or valuation | no one-session timing, precise intrinsic value, or “good company means buy” |
| `second-level-cycle-context` | Howard Marks' second-level and cycle memos | What does consensus appear to embed, and how could the view differ? | price/market context plus consensus or positioning | no mechanical cycle timer or automatic contrarian trade |
| `expectations-base-rates` | Michael Mauboussin/Counterpoint Global | What is the defensible reference class, and how should new evidence update it? | reference class plus new evidence | no cherry-picked class, causal claim, or history-as-destiny |
| `reflexive-feedback-check` | George Soros' public reflexivity lectures | Can belief and allocation change an operating or financing fact and feed back? | belief/flow evidence plus fundamental feedback evidence | no “every trend is reflexive” or turning-point forecast |
| `narrative-diffusion-assessment` | Robert Shiller's narrative-economics research | Which timestamped story propagated, through which action channel, against what alternatives? | timestamped, source-linked narrative corpus | no virality-as-truth, causality, or trade signal |

The source materials are registered in `docs/SOURCE_REGISTER.md`. They constrain what each method
can legitimately ask; they do not establish that an LLM implementation improves forecasts. A
recent reflexivity-prompt study is retained only as a useful experimental precedent because it
uses accumulating prompt conditions, anonymized inputs, and both directional and Sharpe outcomes.
Its model- and context-dependent results do not justify loading a reflexivity method by default.

## Routing boundary

The router binds five point-in-time fields before any model request:

1. deployable market state from data no later than the prior executable session;
2. contemporaneous narrative salience;
3. the analysis question that remains unresolved;
4. a content-identified Method Evidence Declaration mapping each available type to exact frozen
   Evidence Item or Pattern Pack references; and
5. whether the builder already knows outcomes.

Market state and narrative salience narrow applicability, but evidence gates decide whether a Skill
may load. CLI callers cannot self-report evidence-type labels. Missing or out-of-bundle references
produce a recorded rejection, not a weaker version of the method.
`outcomes_opened=true` marks a development diagnostic; outcomes still never enter the Agent prompt.
The route, catalog, Skill manifests, evidence, Provider Profile, common input, and tool surface are
content identified.

## Minimal paired diagnostic

The first diagnostic deliberately changed only one thing: it appended
`expectations-base-rates` to a control containing evidence discipline, event/market context,
equity exposure, adversarial review, and the same frozen Pattern Pack. Both arms used CLIProxyAPI
`gpt-5.6-luna` with `reasoning_effort=xhigh`, the same identity-masked Abqaiq recovery information
state, the same research instruction, and the same read-only tools. Three interleaved paired
replicates produced six Agent runs.

CPA Usage Keeper `v1.14.5` priced the frozen Profile at $0.20 per million ordinary input Tokens and
$1.20 per million output Tokens, with the separately recorded cache prices and no active rule for
this model. The preflight multiplies each Agent run's cumulative input/output budget across six
runs and then applies a 1.25 safety factor. The corrected v2 estimate prices all input at the
largest of ordinary, cache-read, and cache-write rates: raw maximum $0.985932 and guarded maximum
$1.232415 under a hard $10 cap. Version 2 calls these six units `agent_run_count`
and records a conservative provider-request upper bound; the initial v1 runtime artifact called
them `model_call_count`. That label was wrong even though the cumulative-token cost bound was not.
The immutable run has a separate schema-validated, content-identified correction rather than a
rewritten report. Its correction ID is
`method-skill-ablation-audit-correction-c20bcd5af79639f8b31940b2a636db4c2a84f774b3c7d3f547a66f38c0d1ac63`.

All six runs completed. Every run read all five Evidence Items and the Pattern Pack. Both arms
abstained 3/3 because the frozen state lacked a pre-entry benchmark settlement and target-specific
immediate net sensitivity, while recovery, maintained shipments, and integrated offsets weakened
one-session persistence. The treatment therefore did not change the final decision in this case.

| Diagnostic | Control | Plus base-rate Skill | Difference |
| --- | ---: | ---: | ---: |
| completed Agent runs | 3 | 3 | 0 |
| abstentions | 3 | 3 | 0 |
| complete evidence/Pattern coverage | 3 | 3 | 0 |
| input Tokens | 28,845 | 30,513 | +5.8% |
| output Tokens | 7,583 | 7,559 | -0.3% |
| conservative project-ledger cost | $0.014871 | $0.015174 | +2.0% |

The six Agent runs made 12 successful Provider requests. A redacted, content-addressed CPA event
artifact (`5deb04b3f90a147bfaa9a381a21f6ed0fdb07e865e6593ea0f9ac36af6f55a54`) measured 59,358
input, 15,142 output, and 40,448 cache-read Tokens for $0.02276136. The project ledger reported a
more conservative $0.030045 because it does not subtract cache-read discounts. No broker, account,
paper, or live capability was reachable.

This result is a valid process diagnostic and an uninformative effectiveness result. It shows that
the Skill preserves fail-closed evidence use at small incremental cost; it does not show improved
judgment, alpha, or that the method is useless. The selected state already contained strong hard
abstention blockers, and only one opened Event Case was used. The method must remain optional until
chronological outcome-blinded cases exercise genuine reference-class ambiguity and compare it with
the same control. Do not add more famous-method Skills merely because a name is well known.

## Representative-case follow-on

The next comparison is registered across the 15-case market-state dataset rather than forcing all
five methods into one-session event tests. Candidate use is case-specific:

- `second-level-cycle-context` covers prolonged rallies, bears, rebounds, rotation, and whipsaw;
- `narrative-diffusion-assessment` is reserved for a multi-publisher timestamped corpus;
- `reflexive-feedback-check` is limited to leveraged, liquidity, or policy-flow loops with both
  participant-flow and fundamental-feedback evidence;
- `expectations-base-rates` covers bounded policy, closure, reopening, or market-mechanism events;
  and
- `owner-value-discipline` appears only in the 2016-2018 quality rise and 2020-2021 structural
  recovery, where contemporaneous filing fundamentals and valuation evidence are required.

The content-identified registration requires two established-news publishers and at least eight
accepted articles per checkpoint, in addition to official context, macro vintages, positioning,
market, and industry data. Bloomberg and Reuters are entitlement-dependent registered routes;
GDELT and Common Crawl may locate records but cannot substitute for publisher identity or historical
availability. The selected 2024 policy-rally case was initially reported as passing three
case-local checkpoints, including an explicit source-timestamp event-revelation check. Independent
review found that implementation omitted `authority_at <= cutoff`: the exact archive capture and
current publisher/provider versions were later than the historical decisions. Correct replay now
fails all three checkpoints and all 18 selected six-case checkpoints.

The earlier 18-run policy diagnostic and later 108-call six-case diagnostic are therefore retained
only as invalid/superseded behavior and cost evidence. Their apparent narrative-Skill increment,
returns, and directional scores cannot support method quality because the PIT admission gate did
not pass. The six-case calls instead expose an always-abstain behavior with no Skill decision
increment; descriptive same-window baselines show missed rallies and sector rotation. No method is
promoted, and no execution gate changes.

## Reproduction boundary

The public command is `agent method-skill-ablation-run`. It requires exactly three paired
replicates, one appended treatment Skill, a content-bound Method Evidence Declaration, a live local
CPA pricing snapshot matching the frozen Provider Profile, and a hard cap no greater than $10.
Registration is written only after evidence-reference, pricing, routing, dependency, capability,
and common-input checks pass. Every terminal run enters the
append-only Usage Ledger; the report remains inference-ineligible and execution-free.

The private result is under the ignored `.market-impact/method-skill-ablation-runs/` root. Its
report ID is
`method-skill-ablation-report-a36dfa2f213f8c545e3b2a04c55959d789f968b1dce87766ea618b78ee5eeb4b`
and Usage Ledger hash is
`d8f3b00e35e3b0289004868d480e43f3f190b4007b331fe7ff1fa2b6c1a2bbbb`.
The correction is also stored as content-addressed artifact
`9c383d44b2e1ca7b00b6b9fe25d0d06adca31a8b2b9230ac3bcbf1f24a938017`.
