# Continuous study retrospective, 2026-09-06

This is an offline, posthoc diagnostic of the completed bounded batch, not a new
experiment or a change to its signed acceptance result. The [execution report](CONTINUOUS_STUDY_RESULTS_20260905.md)
owns completion and cost totals; [continuous study](CONTINUOUS_DECISION_STUDY.md)
owns registration. Proposed changes below are not implemented or accepted here.
No additional model requests or live execution were made for this analysis.

## Decisions supported by the existing evidence

1. Fix avoidable proposal/interface friction before ranking models by failure rate.
   Action exposure differs materially, and a separate Recall discovery defect was
   found during this audit.
2. Improve the research task and its evidence together. Price-based forecasts are
   legitimate, but sparse event evidence cannot test sustained event interpretation,
   industry transmission or company selection.
3. Measure forecast value against simple rules on identical opportunities, then
   measure its conversion into exposure. Positive account returns alone establish
   neither research value nor good portfolio construction.
4. Buy complete, paired observations in the next pilot. The current 39 budget stops
   do not support extrapolating how many complete comparisons USD 24/50/100 would buy.

## Failure attribution

The 15 stopped initialized trajectories retain their original rejected outputs.
The separate initial Luna portfolio rejection is not added to this denominator.

| Observed stop | Count | What it establishes | Smallest next change to evaluate |
| --- | ---: | --- | --- |
| Position identity mismatch | 5 | Proposal disagreed with the authoritative holding identity; refusal protected account scope | Resolve an existing holding from its bound position reference; avoid retyping derivable identity/class fields |
| OPEN sizing class mismatch | 6 | All six rolling OPEN proposals used mandate permission class `unlevered_exchange_traded_fund`, while sizing rules required `exchange_traded_fund` | Keep permission taxonomy internal; derive the concrete class from the admitted instrument and test the real OPEN path |
| Invalid portfolio evidence reference | 3 | Invented/truncated references were rejected; successful research/acquisition did not guarantee valid handoff | Expose concise bound choices; derive continuation/source bookkeeping where uniquely provable; never silently accept an unknown citation |
| Unknown Recall ID | 1 | The requested ID was not returned by tools; a separate discovery defect also occurred in that same research Run | Apply authorization scope before result limiting; preserve unknown-ID refusal and prior-as-opinion semantics |

All six rolling OPEN proposals were from Terra (3) and Sol (3); Luna proposed
none. This is not six independent demonstrations that the larger models are
worse at tools. It exposes a repeatable contract trap, with model-specific action
selection confounding raw failure counts. Conversely, it does not prove a better
interface would make every rejected economic proposal acceptable. Validate the
unchanged mandate, sizing, evidence and position boundaries after simplification.

The Recall defect is concrete: `read_current_thesis` requests eight globally
ranked records and only then filters `allowed_source_run_ids`. In the failing
frozen Run (journal sequence 4099; tool result 4109), exact tool descriptor hashes
match the reconstructed two-source authorization set. A lower-bound reconstruction
of records already projected then contains 15 records; the injected valid prior
ranks tenth, and all first eight records are outside this Run's allowed scope.
Filtering first returns two allowed records, newest equal to the injected prior.
Thus the null discovery result was incorrect. The prior was also already supplied
in the model input, and the later unknown ID remains a model tool error. There is
no counterfactual evidence that repairing discovery prevents that error. Do not
relabel the signed stop or count this as an additional stopped trajectory.

## Cost and latency: attribution rather than model ranking

The current v4 identities account for USD **22.084627 / 636 requests**: initial
USD 2.636596 / 82, rolling USD 19.448031 / 554. Prior identities account for the
remaining USD 9.448644 / 170 of the cumulative USD 31.533271 / 806. The rolling
stage ledger includes earlier rolling work; its USD 21.992732 / 600 must not be
mislabelled as current-v4-only cost. No unknown reservation is included as known spend.

| Current rolling model | Known USD | Requests | Paid terminal Runs | Median Run USD | Median recorded Run latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| Luna Max | 1.595304 | 241 | 133 | 0.008343 | 99.3 s |
| Terra High | 4.968002 | 165 | 134 | 0.025785 | 23.2 s |
| Sol High | 12.884725 | 148 | 108 | 0.087288 | 38.2 s |

These are heterogeneous research/portfolio/continuation Runs, not equal tasks or
independent trials. Recorded Run latency sums are not elapsed batch duration.
Sol accounts for about 66% of current rolling dollars; Luna accounts for about
66% of summed recorded rolling Run latency. Research costs USD 13.490033 versus
portfolio USD 5.957998. A cheap-token default is therefore not automatically a
fast default. Test Terra as a pilot default and Luna as a cost challenger; retain
Sol only where matched evidence demonstrates incremental value. This is a pilot
allocation hypothesis, not a production model promotion.

## What can be learned about market judgment

There are 211 unique validated theses: 30 initial and 181 rolling. The initial
thesis shared by three cadences is counted once. Eighteen initial theses (six of
ten for each model) received only price repository evidence. Rolling context can
carry forward the latest qualified checkpoint with a staleness gap; its presence
does not mean new news arrived each day. Acquisition snapshots are not counted as
new event reporting merely because they have a non-price evidence ID.

The current research prompt requires `up/down/rangebound` and says “never abstain”.
It also asks about incremental impact and transmission. This can conflate a neutral
market forecast with insufficient evidence for an event explanation. The original
outputs sometimes explicitly acknowledge that limitation; the defect is not that
every model pretended to have event evidence.

### Broad-market direction diagnostic

Evaluator-only outcomes use both frozen broad ETF proxies, 510300 and 510500.
For a preopen thesis on session D and its chosen H-session horizon, each return
runs from its last visible completed close to the close of session D+H−1 on the
registered execution calendar. Both endpoints use one consistent adjustment
basis. This close-to-close movement is not an executable strategy return.
All 211 outcomes fit the registered source horizon. An up/down forecast is scored
only when both proxies move in the same nonzero direction. Divergent proxies are
left unscored. `rangebound` is unscored because no numerical band was frozen.

| Stage/model | All theses | Rangebound | Direction scored | Direction hits |
| --- | ---: | ---: | ---: | ---: |
| Initial Luna | 10 | 7 | 3 | 3 |
| Initial Terra | 10 | 5 | 5 | 3 |
| Initial Sol | 10 | 1 | 8 | 4 |
| Rolling Luna | 60 | 40 | 17 | 17 |
| Rolling Terra | 67 | 34 | 29 | 28 |
| Rolling Sol | 54 | 24 | 26 | 24 |

These conditional hit counts are **not comparable model accuracy estimates**.
Models choose different horizons and amounts of rangebound output; cadences share
market days, outcomes overlap, and budget/model stops select surviving prefixes.
The market-universe target is broader than these two proxies. No industry/company
forecast score, significance test or independent-sample claim follows.

A posthoc five-session momentum comparator uses only the original model-visible
closes. On the exact subset where model direction, future proxy direction and
past momentum direction are all evaluable, rolling hits are Luna **17/17 versus
17/17**, Terra **26/27 versus 22/27**, and Sol **23/25 versus 21/25**. These are
matched *within* model, not common opportunities across models. The comparator
was selected after the run and differs from the registered three-session portfolio
momentum baseline. This supports examining specific revisions, not declaring an
edge. Initial matched counts are respectively 2/2 versus 1/2, 2/4 versus 2/4,
and 2/6 versus 2/6.

### Diagnostic cases, deliberately including misses

| Cutoff/case | Observed forecast | Subsequent proxy movement over its horizon | Optimization question |
| --- | --- | --- | --- |
| 2015-06-15 | Sol up, H=5 | −13.12% / −13.32% | How does a continuation thesis recognize downside fragility when event coverage is absent? |
| 2018-01-25 | Sol up, H=10 | −7.88% / −10.69% | The thesis identified policy support but also prior pricing and weak breadth; how should those counterarguments affect the base case and review timing? |
| 2024-02-06 | Luna up; Terra and Sol down, all H=3 | +4.80% / +15.74% | How to discriminate forced-selling continuation from exhaustion? Sol explicitly described rebound as its counter-scenario but selected down |
| 2020-02-07 | One Terra and one Sol rolling review up, H=3 | +1.18% / +1.49% | Can revision after a rebound add value beyond a fixed trend rule? Five-session momentum was still negative; another Terra cadence retained rangebound |

These are selected diagnostic examples, not a representative performance sample.
The 2018 thesis did articulate incremental policy reasoning, so its miss cannot
be attributed simply to missing news. The 2020 revision explicitly recognized
price recovery and distinguished old policy messaging from a fresh catalyst.
There is observable adaptation, but its incremental economic value remains unproven.

The six complete account trajectories returned 11.88%–21.18%. The registered
policy-window baseline artifacts report 17.13% for retaining the half-invested
seed, 32.35% for broad ETF hold and 33.35% for the registered momentum rule.
The cash baseline is 0.46% because it liquidates the common overnight seed on the
first session, not because cash earns that return. Baselines label a comparable
seed and report the same initial NAV; their account/policy hashes differ from
Agent trajectories. Treat these as contextual observations pending explicit
semantic reconciliation of those bindings, not accepted excess returns. They
nevertheless make exposure conversion a necessary next diagnostic: report holdings,
cash, timing and fees alongside forecasts. This rising six-session window cannot
establish a superior risk-adjusted portfolio policy.

## Smallest next research and pilot design

**First, offline engineering acceptance.** Correct scoped Recall discovery and
remove uniquely derivable proposal metadata from the model-facing surface. Reuse
the current Harness, pi runtime and authoritative bindings. Replay the original
failure examples, valid OPEN/holding cases, cross-account refusal, unknown citations
and legacy signed outputs. Preserve policy rejection where ambiguity remains.
Passing replay does not establish real-model tool usability.

**Second, align the research question with available evidence.** Retain a market
forecast when only prices support it, but separately represent whether an event
impact is established, uncertain or unsupported. Do not force an industry/company
story from an index-only input. Start with existing thesis fields and typed gaps;
add a field only if the evaluator or downstream portfolio boundary needs it.
Acquire evidence for an identified decision-changing question: publication timing,
what changed, what expectations were already priced, who is exposed, and what
would distinguish the counter-scenario. Carry evidence age explicitly. Do not
equate more text or more tool calls with better research.

**Third, freeze one small complete paired pilot before purchasing calls.**
Use one default model and one review cadence initially. Select two manageable,
source-qualified windows with different behavior, including a reversal; treat
already-inspected windows as development data. Compare price-only with qualified
event-plus-price inputs on identical cutoffs, targets and fixed evaluation horizons.
Keep forecast and portfolio policy changes separate so an improvement is attributable.
Score every registered opportunity, with a predeclared rangebound band and explicit
missing/unsupported outcomes. Include the simple price rule, same-account hold,
drawdown, turnover, fees, cash exposure, cost and completion. Do not manufacture
probabilities just to calculate a calibration metric.

Reserve the full pair's worst-case calls, allowed continuation budget and repair
allowance before launch; reduce window length or number of pairs if the approved
cap cannot fund them. Do not distribute the budget over dozens of prefixes. An
initial gate requires valid completion of both arms and no unresolved interface
defect; expand only for ambiguous direction or meaningful incremental value.
Two windows support usability/cost decisions, not investment acceptance. A later
unseen chronological sample is required before capability promotion. Paid execution
requires a separately concrete budget decision; this retrospective starts none.

## Reproduction and evidence boundary

The private batch artifact is `2822a2c48e970c4cdb3eaaca539d11f9e9b12e111386bac914a489b9facd7ddd`;
the source/baseline preflight is `0cc7079515ec99b0fa24a1db7cfe50d9f5d0df5d9b51526d06c5e3fc68101ba1`.
The ignored offline script `.market-impact/continuous-20260905/retrospective_v1.py`
reads the usage journal in read-only mode and verified content-addressed artifacts.
Its private `retrospective-v1` outputs retain run/source hashes, thesis-level
denominators, horizons and proxy outcomes. They contain private model/source data
and must not be committed. This document publishes only aggregate diagnostics and
short paraphrases. Original registration, responses, costs and stops stay intact.

Validation: independent read-only review found no concrete report defect. Local
checks passed Ruff, formatting, Pyright and all 2,115 tests; the suite emitted 198
existing pandas timestamp deprecation warnings. Separate arithmetic/date-boundary
checks covered all 211 thesis horizons and 422 proxy returns. These checks validate
the retrospective and existing code, not the proposed fixes or a new trading claim.

## Reusable offline extraction

`market_impact_agent.continuous_retrospective` now exposes
`run_continuous_retrospective(ContinuousRetrospectivePaths(...))`. The caller
explicitly supplies the completed report path, existing runtime-store root, and
preflight artifact hash. It returns private cost, thesis, proxy-outcome and
matched-momentum diagnostics in memory; it has no default output path and does
not start an Agent, call a Provider, or reach a broker.

`costs.current_identity_runs` and `costs.groups` retain the previous current-v4
views. `costs.cumulative_attribution` separately reports current identities,
the signed report's cumulative known spend, their prior-identity difference,
and its unsettled reservation. For this batch those values are USD 22.084627 /
636 requests, USD 31.533271 / 806, USD 9.448644 / 170, and USD 0.011769 / one
unsettled request respectively. The reservation is never added to known spend.

The current [read-only audit boundary](READ_ONLY_AUDIT.md) uses one short SQLite
read transaction for events, Usage and terminal Run records. Retrospective costs
consume that same materialized Usage tuple rather than reopening a later ledger
view. WAL/checkpoint bookkeeping is not a domain-record mutation.

The entry uses `market_impact_agent.offline_authority` to open only existing
regular files. Its SQLite connections use `mode=ro`; the authority adapter
verifies every Run Journal hash chain and root signature, and the Usage Ledger
chain before exposing the source/CAS store. Every Usage Record must also match
its same-root terminal Run's status, terminal artifact and final Journal hash.
Artifact writes, snapshot writes, run claims and journal/ledger appends are
rejected. A compatibility reader for legacy range-projection evidence also uses
the same read-only connection, so a write attempt still fails at SQLite rather
than acquiring authority. Reopening a source snapshot continues to validate its
content-addressed raw record and projection bindings.

The `market-impact agent continuous-study retrospective` CLI writes private detail
to an explicit ignored output root and prints aggregate diagnostics. See the
[reproduction commands](STAGED_RESEARCH_PREPARATION.md#offline-commands).
