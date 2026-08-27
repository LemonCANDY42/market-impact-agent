# Market-state and sector-rotation research dataset

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

The 2026-08-28 case-local qualification now binds 169 records to the exact dataset, registration,
and private panel. The 2024 policy-rally case passes all six registered categories at 24 September,
30 September, and 8 October. The first checkpoint additionally binds the archived CSRC live
transcript segment timestamped 09:10:58 Asia/Shanghai: its operative reserve-ratio, policy-rate,
mortgage, swap-facility, and buyback-relending statements were visible before the frozen 09:25
cutoff. A new semantic event-revelation gate rejects an event checkpoint that merely has enough old
official documents. Xinhua/SCMP publisher records, NBS vintages, fixed Tushare market/industry
versions, and exchange margin summaries remain bound to their own publication/update/availability
semantics; later or mismatched publisher versions are metadata-only and never substituted.

The whole 15-case study is still not source-complete, and no multi-case method-effect claim is open.
Only this selected, outcome-opened development case may run the bounded diagnostic below. Its
qualification is evidence for one input path, not proof that the historical corpus is generally
complete or that retrieved Tushare history is an original vintage.

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

## First complete representative-case Agent diagnostic

The first end-to-end policy-rally experiment freezes three pre-open checkpoints and compares the
general Agent Harness with the same Harness plus only `narrative-diffusion-assessment`. Each arm ran
three times per checkpoint on CLIProxyAPI `gpt-5.6-luna`, `reasoning_effort=xhigh`, for 18/18 valid
runs. All runs used the same six Evidence Items, Pattern Pack, target alias, tools, and eligible
horizon. The Agent never saw realized outcomes, broker access, or an executable instrument.

| Checkpoint | Eligible horizon return | General 3-run majority | Plus narrative Skill | Increment |
| --- | ---: | --- | --- | --- |
| 2024-09-24, 4 sessions | +14.15% | propose up, 3/3 | propose up, 3/3 | same decision |
| 2024-09-30, 1 session | +4.45% | abstain, 3/3 | propose up, 2/3 | helpful in this case |
| 2024-10-08, 1 session | -4.37% | propose up, 2/3 | abstain, 3/3 | helpful in this case |

With registered-open fills and 10 basis points per one-way turnover, the general arm returned
13.04%, the added-Skill arm 36.89%, CSI 300 buy-and-hold 31.04%, and cash 0%. The general arm's
close-valued maximum drawdown was -4.46%; the Skill arm's was 0%. The Skill arm made the correct
direction/abstention choice at 3/3 checkpoints versus 1/3 for control. All 31 registered industries
rose over the case interval; their median open-to-close return was 31.13%, confirming unusually
broad opportunity rather than a normal market sample.

Sharpe, annualized return, and annualized volatility are deliberately `null`: the case has only six
trading sessions, below the registered 20-session minimum. CVaR and drawdown are close-valued path
diagnostics and do not erase the separately reported 8 October open-to-close reversal. Formal v3
model cost was $0.240318; including invalid or superseded v1/v2 diagnostics, total actual model cost
was $0.669513 under the $10 cap. The content-identified private report is
`regime-agent-experiment-report-549b24bbebaac242f1bee3bb6c633d9160f5199382ae0793100b9fa50aa08e4e`.

This is a successful full-process diagnostic and a promising in-case Skill increment, not a method
acceptance result. Outcomes were already known to the builder, the case was selected, and three
checkpoints cannot estimate generalization. The next evidence-bearing step is the same frozen
three-pair protocol across additional qualified policy, bear, quiet, rotation, and black-swan cases;
failed source gates must remain in the denominator without model calls.

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
