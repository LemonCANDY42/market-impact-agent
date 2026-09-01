# Market-state and sector-rotation research dataset

> Regime research owns historical case construction and lane-specific diagnostics. It may propose
> hypotheses and Skill candidates, but promotion is governed only by
> [Agent Effectiveness Acceptance](AGENT_EFFECTIVENESS_ACCEPTANCE.md).

This research slice supplies market context for later Agent and baseline comparisons. It does
not add a strategy, accepted alpha, paper-trading capability, or live execution path. Historical
case identity and outcome labels are evaluator-only and must never enter Agent-visible inputs.

## Why this is not a seven-bucket taxonomy

Market path, volatility, sector dispersion, narrative attention, and causal clarity are different
variables. A single headline bucket loses cases such as a low-volatility index with a violent
industry rotation, or a rapid rally whose contemporaneous explanation remains contested.

The deployable price detector therefore has only four directional states plus abstention:

- `up_fast` and `up_mild`;
- `down_fast` and `down_mild`; and
- `unclassified` when 20- and 60-session directions conflict, history is insufficient, or the
  detector cannot safely decide.

For a first executable session `t`, every detector input ends at `t-1`. The frozen detector uses
20-session log return, 20-session realized volatility, their volatility-scaled magnitude, and
20/60-session sign agreement. A later HMM may expose filtered probabilities, but a full-sample
smoothed state is a hindsight diagnostic and cannot become a decision-time feature.

Each case separately records path direction/speed, volatility, drawdown, recovery, narrative
salience, causal complexity, and causal directness. Narrative salience is one of
`corroborated_obvious`, `authority_obvious`, `diffuse`, `contested`, or `unavailable`. “Obvious”
means supported by evidence available at the relevant cutoff, not an explanation discovered after
the price move. Causal directness remains the existing Transmission Path concept and is not inferred
from attention or return magnitude.

## Coverage design

The v1 public registry contains 15 A-share retrospective research candidates from 2014 through
2024. A case may test several capabilities; cases are not selected to fill equally sized story
buckets. The set deliberately covers:

- rapid broad rallies, rapid broad selloffs, and a prolonged bear path;
- a long lower-volatility quality-led rise;
- market closure and circuit-breaker microstructure stress;
- a flat or weak headline index with high sector/size dispersion;
- a broad recovery in which sector selection has little ex-post value;
- policy-visible event windows and diffuse or contested narratives; and
- a post-rally whipsaw in which a lagged trend state can encourage chasing.

The registry is content-identified. Cases remain `retrospective_research_candidate` and
`identity_sensitive`. Missing market or industry anchors retain an `incomplete` case in the report;
they are never dropped from the denominator.

### Accelerated effectiveness lane

The two-week convergence program may use broad opened-outcome history to discover or reject event,
industry, macro, rotation and risk-control hypotheses, but promotion evidence comes only from a
separately frozen chronological holdout whose outcome labels and post-cutoff inputs are unavailable
to the Agent. The same Harness decision runtime and cutoff-bound tools used by prospective paper
must run that holdout. Full-information analysis can propose a Skill candidate; it cannot score the
candidate that it created.

Coverage must cross materially different trend, volatility, dispersion and shock states and include
short, medium and long registered horizons. A candidate is retained only after more than one
non-overlapping validation slice shows no material contradiction, and conflict checks compare it
against existing Skill logic before merge, specialization or rejection. Evaluation reports lead
time, after-cost return, maximum drawdown, CVaR, adverse excursion, turnover, upside capture,
downside participation, avoided loss and the opportunity cost of false avoidance. Results remain
separate by Strict-PIT, Modeled-PIT and retrospective lane and by model; repeating one event or model
response does not increase the independent sample count.

## Rich-source study registration

`examples/research/market-regime-study-registration-v1.json` binds all 15 cases to a checkpoint
cadence, bilingual case query terms, candidate method Skills, and minimum source requirements.
Every case requires market and industry paths, official context, macro vintages, positioning or
expectations, and at least eight established-news records from at least two distinct registered
publishers per checkpoint. Owner-value cases additionally require contemporaneous issuer or sector
fundamentals. Bloomberg and Reuters are registered as entitlement-dependent news routes; GDELT and
Common Crawl are discovery aids and cannot satisfy the established-news requirement by themselves.
Licensed content and market rows remain private.

The registration deliberately reports every current retrospective case as not ready for an
outcome-blinded Agent effectiveness run. The Tushare panel is implemented but is a current retrieval
of history. CSRC, State Council, and NBS source classes now have authenticated archive and
publication/update-time extraction; exchange official sources, PBC and other macro sources,
positioning, filings, Bloomberg, and Reuters still require their own historical-version paths.
This is a useful fail-closed result: three source adapters or a query plan are not mislabeled as a
completed historical evidence corpus.

## Frozen checkpoint and evidence qualification

The registration freezes a 09:25 Asia/Shanghai decision cutoff, a 60-session price lookback, a
14-day weekly/event-news window, a 31-day monthly-news window, and category-specific maximum ages
for official, macro, positioning, and fundamental records. Weekly and monthly checkpoints use the
first actual trading session in each bucket. Every evidence record separately retains occurrence,
publication, source update, strategy availability, and archive/provider authority times.
Availability must explicitly be one of actual receipt, source-reported time, or a frozen latency
model; modeled latency without a model ID and hash is rejected.

`Regime Evidence Manifest` binds content-identified records to the exact dataset, registration, and
private panel. The qualification report then evaluates every case/checkpoint/source minimum,
distinguishing content completeness from point-in-time authority. Market and industry history in
the current Tushare panel remains usable for descriptive baselines but is intentionally not granted
original-vintage authority.

The selected six-case slice is content-rich but does **not** pass the strict point-in-time gate.
It covers the 2018 bear market, 2019 Q1 rebound, 2020 closure shock, 2021 sector rotation, 2024
policy rally, and 2024 post-rally whipsaw. Each selected checkpoint has at least eight
established-news records from Xinhua and SCMP plus the requested market, industry, official, macro,
and positioning content. Discovery metadata never substitutes for a direct publisher version.
When an exact private article body is already content-matched, the materializer reuses it;
otherwise the publisher excerpt and its declared limitation remain explicit.

The first policy checkpoint has a source timestamp of 09:10:58 Asia/Shanghai for the CSRC live
transcript, but its exact archive authority was captured only on 7 October, after the 24 September
09:25 cutoff. It therefore cannot authenticate that historical source version for that decision.
The same distinction invalidates current Tushare backfills, 2026 publisher snapshots, and later
positioning verification as PIT authority even when their source-reported publication time plus a
modeled latency falls before the checkpoint.

The corrected qualification requires both strategy availability and immutable authority time to
precede the cutoff. Its private report,
`regime-evidence-qualification-report-c597952dfb3e6003efaea51395bce8836808289fa5b990779ea418d295a25c8d`,
qualifies 0/18 selected checkpoints. The earlier `e3d76a...` report is invalidated audit evidence:
it checked authority kind but omitted the authority-time comparison. The whole 15-case study is
still not source-complete, and Bloomberg/Reuters entitlement routes were not silently replaced by
free sources.

The bounded publisher-archive recovery path is now implemented. It locates exact Xinhua/SCMP URL
captures before each checkpoint, distinguishes `not_found` from `source_error`, and treats a found
locator as candidate-only until its replay body, publisher identity, publication/update time, and
cutoff all verify. The first managed-environment audit for the 2021 sector-rotation case covered 11
checkpoints and retained all 32 attempted lookups as `source_error` because the archive index was
unreachable; it does not establish zero coverage. The recovery contract, licensed fallback trial,
prospective receipt path, and price-basis rules are in `PIT_EVIDENCE_RECOVERY.md`.

The later full-access audit completed all 230 exact-URL lookups across the six selected cases with
162 found candidates, 68 genuine `not_found` results, and zero source errors. Replay verified 115 of
120 unique original-record/archive-version pairs. Five stayed rejected by timestamp invariants.
Replacing 98 current snapshots with 100 canonical historical versions produced Manifest
`af6c3d...0344`; strict Qualification Report `6e163e...eca9` raises established-news readiness to
8/61 six-case checkpoints and 2/18 frozen validation checkpoints. The complete frozen checkpoint
gate is still 0/18: market, industry, and positioning authority remain 0/18, macro is 16/18, and the
first 2024 policy checkpoint still lacks a pre-cutoff event revelation.

A later document-recovery pass replayed the 115 accepted archive versions so the Agent could read
the exact registered article payload rather than only metadata or a publisher excerpt. One hundred
replays succeeded with matching content hashes; 15 remained network failures, with no digest or
publisher-verification mismatch. The rebuilt validation inputs contain every exact news payload at
13/18 checkpoints and six of eight at the remaining five. These bodies improve the model-visible
corpus; they do not change the strict authority result.

## Frozen return windows

Every covered market index and industry proxy reports the same three price-return windows:

1. `path_return`: registered path-start close to end close. This is an ex-post scenario descriptor.
2. `event_return`: the declared event price anchor to end close. It is `null` for a path-only case
   and may use prior close, session open, or session close. A non-executable event anchor is not
   represented as a possible fill.
3. `tradable_return`: first registered executable-session open to end close.

The current panel uses Shanghai Composite, CSI 300, CSI 500, CSI 1000, ChiNext, and all 31 SW2021
Level-1 price indices. Capture first retrieves the complete `SW2021`/`L1` `index_classify` table,
checks every registered code and industry name against it, then permits the corresponding
`sw_daily` reads. The panel retains the taxonomy query's normalized content hash and retrieval
provenance, so a sector mapping cannot be relabelled without invalidating the panel. The sector top,
bottom, dispersion, and positive fraction are explicitly
`hindsight_only` opportunity bounds. They answer whether sector choice mattered in that interval;
they do not show that an Agent could select the winner.

These are price-index returns, not stock-style adjusted prices or investor total returns. A future
wealth/performance claim must use a historically versioned total-return index or a tradable ETF
path with distributions, costs, and corporate actions. Cases predating the current classification
also require the industry taxonomy effective at their checkpoint; authenticated SW2021 backcasts
remain descriptive hindsight bounds.

Future executed comparisons must add the same-window static market, equal-sector, lagged sector
momentum, lagged inverse-volatility, event-only, and cash/abstain baselines. Accepted rotation
capability additionally requires a point-in-time constituent universe, frozen signal lag, weights,
rebalance rule, transaction costs, fill constraints, turnover, drawdown, CVaR, Sharpe/information
ratio, upside capture, downside loss participation, and chronological holdout. A hindsight winner,
smoothed state, present-day constituent list, or ex-post event selector is never a valid win.

The first implemented long-horizon comparator now covers cash, the primary index, static
equal-sector buy-and-hold, and a lagged 20-session top-three sector-momentum rule rebalanced at the
first session of each month. It charges 10 basis points per one-way turnover and reports daily-path
total/annualized return, volatility, Sharpe, maximum drawdown, 95% CVaR, information ratio, upside
capture, downside loss participation, turnover, and modeled cost. Sharpe uses the registration's
explicit zero annual risk-free assumption. Fewer than 20 sessions suppress
annualized risk metrics. Industry price indices remain non-executable proxies; inverse-volatility,
ETF implementation, realistic fills, and Agent decisions remain later gates.

The private 2026-08-27 run covered all 15 cases. Twelve had at least 20 sessions. Equal-sector
buy-and-hold beat the primary index on total return in 8/15 cases and on Sharpe in 6/12 eligible
cases. Lagged sector momentum beat the primary index on total return in 6/15 and on Sharpe in 5/12,
but improved maximum drawdown in only 2/15. It lost more than the primary index in the 2015 crash
and 2018 and 2022 bear windows, and it also lagged in the 2016-2018 quality rise and 2024 broad
rebound. These opened, selected windows are comparator diagnostics, not inference or alpha.

## Invalidated six-case Agent diagnostic

The attempted validation compared the general Harness with the same Harness plus one routed method
Skill over six cases and three pre-open checkpoints per case. Each arm made three independent
CLIProxyAPI `gpt-5.6-luna`, `reasoning_effort=xhigh` calls, so 108 calls completed with structurally
valid outputs and evidence/Pattern references. The post-run PIT audit invalidates them as formal
Regime Agent experiments because their qualification report is not reproducible under the corrected
authority-time gate. They remain descriptive, outcome-opened behavior and cost evidence only.

Both arms produced the same majority decision at all 18 checkpoints: abstain. Each therefore
returned 0%, with zero turnover and drawdown; Sharpe is undefined because the return path has zero
volatility. The 12/18 directional-hit count means cash happened to match the sign test on 12
checkpoint horizons. It does not show useful market timing: neither the routed Skills nor the
general arm entered any positive regime, and the routed Skill was helpful at 0 checkpoints,
harmful at 0, and decision-identical at 18.

The four registered baselines now use the same case windows and 10-basis-point one-way cost model:

| Opened case | Primary index return / Sharpe / max DD | Equal-sector return / Sharpe / max DD | Lagged sector momentum return / Sharpe / max DD |
| --- | ---: | ---: | ---: |
| 2018 bear | -32.41% / -1.87 / -32.41% | -34.00% / -1.93 / -35.27% | -41.73% / -2.39 / -41.73% |
| 2019 Q1 rebound | +32.67% / 5.00 / -4.95% | +34.81% / 5.24 / -4.29% | +23.36% / 3.23 / -6.60% |
| 2020 closure shock | -3.11% / -0.60 / -16.08% | +2.19% / 0.65 / -14.00% | +10.25% / 1.82 / -14.18% |
| 2021 sector rotation | -11.43% / -0.75 / -17.78% | +5.24% / 0.50 / -9.22% | +0.75% / 0.17 / -19.09% |
| 2024 policy rally | +31.04% / n/a / 0.00% | +32.19% / n/a / 0.00% | +36.40% / n/a / 0.00% |
| 2024 whipsaw | -5.71% / -0.96 / -9.22% | -2.13% / -0.23 / -8.45% | -6.45% / -0.57 / -13.39% |

The 2024 policy window has only six sessions, so annualized risk metrics are deliberately `null`
under the registered 20-session minimum. Across cases, mean primary-index, equal-sector, and
lagged-momentum returns are +1.84%, +6.38%, and +3.76%. Holding cash beats those baselines in only
4/6, 2/6, and 2/6 cases respectively. In particular, cash misses the 2019 and 2024 broad rallies
and the positive industry paths during the 2020 and 2021 rotation windows.

The attempted six-case calls cost $1.028187. The $2.436518 all-diagnostic total recorded after this
run was later found to omit 166 terminal Usage Ledger records. The Usage Ledger Union in the
Modeled-PIT section supersedes that number. Cost reconciliation for this invalidated validation
still derives its case totals from exact reports. The old
private aggregate report
`regime-agent-validation-report-20a1b4f3c042041d9499a4a584f8678b28729fd9468b74cb016131f9f8fa8202`
is invalidated and must not be used as acceptance evidence.

The descriptive result still shows no Skill increment and an always-abstain failure mode, but it
accepts neither the evidence gate nor the Agent validation pipeline. Exact Provider, panel,
Manifest, and qualification identities are now bound into the validation registration and checked
against every case report. No paper or live authority follows. The next model call must wait for
source versions whose archive/provider authority actually predates each cutoff; rerunning the same
inputs or merely adding personas is not evidence-bearing.

## Opened-outcome Modeled-PIT process diagnostic

### Decision-readiness checkpoint boundary

A later Modeled-PIT rerun may assemble a content-addressed readiness checkpoint before any
Judgment model is invoked. Only `ProspectiveDecisionPipeline`, the existing Harness composition
root, can materialize it. The pipeline reopens the frozen diagnostic registration, exact
`ProspectiveCheckpointSnapshotSet`, durable Trigger Admission and EventAssessment context, and
`LocalDataSnapshotStore`. It rematerializes Decision Inputs and builds the production Market
Universe internally; there is no caller-buildable source bundle, readiness authority, Universe,
horizon, hedge, or price surface. EventAssessment horizons must be present in the durable diagnostic
preregistration.

Pipeline output is accepted only through an authority index in that same
`LocalDataSnapshotStore` root. The index binds the checkpoint ID and hash, stored artifact hash,
Harness authority ID, frozen registration and Snapshot Set identities, Trigger Admission,
EventAssessment, and exchange rule set. Authoritative reopen is by checkpoint ID through the
pipeline and reconstructs the checkpoint from the current durable sources. A self-hashed JSON
object, an artifact copied to another root, or schema parsing alone does not establish readiness
authority.

Snapshot membership alone is not evidence authority. Matching Snapshot and Observation IDs cannot
authorize changed source, time, payload, or content-hash fields. Caller-shaped Decision Input
mappings are never accepted: matching Snapshot, Observation, and rehashed record IDs cannot replace
the SourceObservation content reopened from the store. The raw price remains the exact projected
source observation, and its trade-date session close must not follow the checkpoint cutoff.

The checkpoint is only a fail-closed readiness record. It contains no outcome or return labels and
cannot authorize model calls, signals, orders, paper trading, or live trading. Production Market
Universe semantics expose decision-time tradability only as `unverified` or `ineligible`; the
checkpoint preserves those types rather than inventing a verified state. An optional missing prior
expectation and unknown tradability may remain typed information gaps for Judgment readiness, but
Intent readiness is always fail-closed until a later owner proves current tradability, suspension
status, and an executable raw price. Historical close data is not that proof. Hedge readiness remains
typed unavailable because the composition root has no approved exposure-to-hedge mapping; arbitrary
references or risk-reducing booleans cannot change it.

The separate Modeled-PIT lane tests whether the Harness and Agent can use the available historical
content when immutable historical authority is unavailable. Policy
`regime-modeled-pit-policy-b3959de39c87b4abe47a8e6543448cd2958280735389615363ccc3bd61eeb7c8`
uses the prior-session panel snapshot for price context and source availability plus a frozen safety
delay for other categories. Qualification
`regime-modeled-pit-qualification-report-396748a622b5d23e410926917e364860f76ed370cc9798498b29e9b23b519aa0`
admits exactly the registered first/middle/last 18-checkpoint selection. It reports every authority
gap and remains strict-PIT-ineligible.

The first real run exposed a Harness defect: the CLI did not pass the registered eligible horizon,
so the paired runner defaulted to one session while the Evidence Pack declared 102. That six-run
diagnostic cost $0.053516 and is excluded from the formal result. The CLI now requires the horizon
explicitly. The corrected registration
`regime-modeled-pit-agent-validation-e43a3e221fdebb8a99189c7ed35e7977e7dba680d43cf093d7de9c712b14e91e`
completed all 18 checkpoints with three replicates per arm: 108/108 runs completed, every run
abstained, and the routed Skills changed 0/18 majority decisions.

| Opened case | Eligible horizons | Control / routed directional hits | Formal cost |
| --- | --- | ---: | ---: |
| 2018 bear | 102 / 124 / 2 | 3 / 3 | $0.148491 |
| 2019 Q1 rebound | 30 / 29 / 1 | 1 / 1 | $0.146611 |
| 2020 closure shock | 20 / 15 / 1 | 2 / 2 | $0.165758 |
| 2021 sector rotation | 89 / 102 / 9 | 2 / 2 | $0.181482 |
| 2024 policy rally | 4 / 1 / 1 | 1 / 1 | $0.187605 |
| 2024 whipsaw | 28 / 30 / 2 | 3 / 3 | $0.161389 |

The 12/18 aggregate directional-hit count is again the sign score for staying in cash, not market
timing skill. It misses the two positive 2019 checkpoint horizons, the first 2020 rebound horizon,
the final 2021 rotation horizon, and the first two 2024 rally horizons. Both arms retain zero return,
turnover, and drawdown, while the same-window primary/equal-sector/momentum baselines remain those in
the invalidated diagnostic table above.

The 108 judgments contain 493 blocker statements. A run-level text audit found:

- horizon persistence unresolved in 108/108 runs;
- event identity or event-to-target attribution unresolved in 106/108;
- expectation delta or a defensible prior baseline unresolved in 105/108;
- strict authority or modeled-delay limitations cited in 95/108;
- executable target/proxy mapping unresolved in 79/108.

Every full-body checkpoint still produced six abstentions, as did the five checkpoints with two
missing bodies. Restoring article text was useful and necessary, but article retrieval alone does
not fix the current decision contract. The masked pack hides too much of the observed event fact,
the positioning corpus does not state a cited expectation delta, long case-remainder horizons often
exceed what the event evidence can support, and the action space permits only a non-executable broad-
market proxy. This also censors the Skill comparison: a treatment cannot show an incremental
decision when the common input contract blocks both arms before the routed method matters.

The next diagnostic should freeze an event-revelation record that states the newly observed fact,
its timestamp and sources, a prior expectation or explicit unknown, a falsifiable transmission path,
and a mechanism-appropriate horizon chosen from a registered set. It should also bind executable
index/ETF and sector targets under the effective taxonomy. Two or three representative checkpoints
are enough for the first rerun. Further 18-checkpoint spending should wait until those blocker rates
change materially.

Final report
`regime-modeled-pit-agent-validation-report-317f79ea1602e7d381eba01f9522123116033bdbbc179180dfa71f46f895f380`
binds the registration, strict and modeled qualifications, Provider, panel, Manifest, every
canonical paired registration/report, every recomputed frozen-input/horizon hash, baseline paths,
all 36 reconstructed arm execution bindings and their local ledgers, and the Usage Ledger Union.
The 108 formal report rows are also rebuilt from ledger-bound terminal Judgment Artifacts and Run
Journals before their decisions enter the aggregate. Terminal replay reparses the final assistant
payload and requires the proposal, raw response, transcript, and metrics to match the hash-chained
validation event. The union covers 70 ledgers, 528 unique terminal runs, zero duplicates or
conflicts, 526 completed and two failed runs. Costs are $3.883472
preexisting, $0.053516 invalid horizon, and $0.991336 formal Modeled-PIT, totaling $4.928324 under
the shared $20 cap. This report is process evidence only: strict PIT, effectiveness, inference,
paper, and live gates remain closed.

## Private data and commands

The public registry contains dates, source references, and proxy identities but no licensed market
rows. The capture uses `TUSHARE_TOKEN` from the environment and writes only under the ignored
repo-local `.market-impact/regime/` root with directory mode `0700` and manifest mode `0600`.
The command accepts no output-directory override. Evaluation writes its bound report only under
that same root (`reports/<panel-id>.json`), also with mode `0600`:

```bash
uv run market-impact regime validate \
  --dataset examples/research/market-regime-dataset-v1.json

uv run market-impact regime capture \
  --dataset examples/research/market-regime-dataset-v1.json

uv run market-impact regime evaluate \
  --dataset examples/research/market-regime-dataset-v1.json \
  --panel .market-impact/regime/<regime-panel-id>

uv run market-impact regime study-validate \
  --dataset examples/research/market-regime-dataset-v1.json \
  --method-catalog examples/research/famous-method-skill-catalog-v1.json \
  --registration examples/research/market-regime-study-registration-v1.json

uv run market-impact regime study-evaluate \
  --dataset examples/research/market-regime-dataset-v1.json \
  --method-catalog examples/research/famous-method-skill-catalog-v1.json \
  --registration examples/research/market-regime-study-registration-v1.json \
  --panel .market-impact/regime/<regime-panel-id>

uv run market-impact regime evidence-manifest \
  --dataset examples/research/market-regime-dataset-v1.json \
  --method-catalog examples/research/famous-method-skill-catalog-v1.json \
  --registration examples/research/market-regime-study-registration-v1.json \
  --panel .market-impact/regime/<regime-panel-id> \
  --record .market-impact/regime/evidence/records/<record-id>.json

uv run market-impact regime evidence-qualify \
  --dataset examples/research/market-regime-dataset-v1.json \
  --method-catalog examples/research/famous-method-skill-catalog-v1.json \
  --registration examples/research/market-regime-study-registration-v1.json \
  --panel .market-impact/regime/<regime-panel-id> \
  --manifest .market-impact/regime/evidence/manifests/<manifest-id>.json

uv run market-impact regime evidence-capture-state-council \
  --locator examples/research/gov-cn-2024-stimulus-common-crawl-v1.json \
  --case-key cn-2024-policy-melt-up \
  --case-key cn-2024-post-rally-whipsaw \
  --claim-id state-council-2024-09-25-stimulus-summary \
  --lineage-id state-council-content-WS66f3602ec6d0868f4e8eb3c0

uv run market-impact regime evidence-capture-nbs \
  --locator examples/research/nbs-2024-08-economy-internet-archive-v1.json \
  --case-key cn-2024-policy-melt-up \
  --case-key cn-2024-post-rally-whipsaw \
  --claim-id nbs-2024-08-national-economy \
  --lineage-id nbs-t20240914-1956487
```

The panel binds the dataset hash, fixed Tushare Provider/version, retrieval time, SW2021 L1 taxonomy
content and provenance, series rows, proxy resolution, and a content hash. The report repeats the
exact panel ID/hash, Provider/version, and retrieval time. It is labelled `retrieved_historical_not_original_vintage`: present retrieval of
historical Tushare rows is not proof of their original historical availability or lack of revision.
The current results are price-index diagnostics, not total-return acceptance evidence. Event/news
sentiment still requires a separately frozen corpus with publication/update availability and a
predeclared latency model.

These axes may feed the point-in-time Research Method Skill router only when computed at the frozen
cutoff. Famous-investor lineage does not authorize an ex-post regime story: a method also requires
its declared evidence type, and a missing gate is recorded as a rejection. The current method
catalog and its first bounded ablation are described in `docs/METHOD_SKILLS.md`.

## Roadmap boundary

This slice can qualify data, expose missingness, measure market/sector paths, and define a later
comparison. It does not modify the existing Event Archetype, Method Quality score, Phase 2
registrations, or Phase 3 gate. An Agent result must later be replayed on masked inputs and compared
against the frozen deployable baselines before any market-state or rotation claim can advance.
