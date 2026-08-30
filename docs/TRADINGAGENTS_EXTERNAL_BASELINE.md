# TradingAgents External Baseline Diagnostic

## Status and boundary

On 27 August 2026, TradingAgents `0.3.1` was deployed as an isolated external baseline at
upstream commit
[`a33fd4c0f134485a43553a2c23a63cb14adbd88f`](https://github.com/TauricResearch/TradingAgents/commit/a33fd4c0f134485a43553a2c23a63cb14adbd88f).
The clone, virtual environment, adapter, model transcripts, and reports remain under the ignored
`.market-impact/` tree. No third-party code or runtime dependency entered this repository, no
secret was written to a file, and no broker, account, paper, or live-execution capability was
reachable. The current runner sets no memory log, disables upstream's pending-decision return
resolver, isolates every graph, and rejects an existing experiment id. No current or later Yahoo
return can enter a repeated run.

Two distinct experiments must not be conflated:

- The earlier MiniMax smoke supplied strongly masked evidence and a target alias. It exposed a
  mismatch between native role prompts and an evidence-only test: the graph recovered historical
  identities, named analogues, unsupported quantities, and one structured-output degradation. It
  remains a negative input-isolation diagnostic.
- The current Luna xhigh experiment is deliberately native. It supplies the real Abqaiq event and
  `601857.SH` target, cutoff-bound registered news and Tushare OHLCV, and preserves TradingAgents'
  role prompts, debate, risk graph, and model prior knowledge. These native priors are part of the
  external baseline, not a contamination failure.

The current experiment compares decisions, stability, process, latency, and Token use with the
Harness. It is not a registered Harness arm or a causal method ranking: this is one already opened
Event Case, and the two systems receive different model-visible identity and orchestration.

## Native-capability deployment

The current runner uses the dedicated CLIProxyAPI project Key, exact local model
`gpt-5.6-luna`, `reasoning_effort=xhigh`, temperature `0.1`, one investment-debate round, and one
risk-debate round. It enables market, social/news sentiment, news, and fundamentals analysts,
followed by bull/bear researchers, a research manager, a trader, three risk perspectives, and the
portfolio manager. The upstream unknown-model warning is expected because `0.3.1` predates Luna;
the gateway and Harness both reject silent model substitution.

Every TradingAgents data tool is replaced before graph construction:

- event and global-news tools return the same registered event items visible no later than each
  cutoff: the 15 September attack state and 18 September recovery state;
- market and indicator tools return only the registered Tushare `601857.SH` snapshot filtered no
  later than the cutoff;
- the fundamentals tool returns the registered target/exposure context;
- social, macro, accounting, insider, and prediction-market paths return explicit unavailable
  sentinels when no historical point-in-time input is registered;
- cross-run memory and outcome reflection are disabled while native role reasoning remains intact;
  and
- `Buy` or `Overweight` maps to the Harness's one-sided long proposal; every other native rating
  maps to abstention. Native short views remain visible in the report but cannot silently expand
  the Harness action space.

## Earlier masked MiniMax diagnostic

| Information state | Native result | Harness mapping | LLM calls | Input tokens | Output tokens | Estimated cost | Wall time |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Attack | `Hold` | Abstain | 17 | 84,009 | 39,329 | USD 0.072398 | 447.457 s |
| Recovery | `Hold` | Abstain | 15 | 63,503 | 48,927 | USD 0.077764 | 641.849 s |
| Total | two `Hold` results | two abstentions | 32 | 147,512 | 88,256 | USD 0.150162 | 1,089.306 s |

Both masked-smoke abstentions avoid the registered fixed-long controls, which were net negative in
both opened states. This is not evidence that either reasoning method predicted returns:
abstention has zero research score, both states belong to one already opened Event Case, and this
legacy external method ran only once per state.

The ten accepted `family_guided` Harness runs across the same two states cost USD 0.091915. The two
single-run TradingAgents diagnostics therefore cost about 1.63 times that entire ten-run arm. The
full four-arm Harness comparison cost USD 0.397066 for 40 runs. These legacy numbers describe
observed resource use, not an apples-to-apples quality comparison. They are not used to estimate
Luna included-usage cost, which has no asserted USD/token price.

## Native Luna xhigh 5-by-2 result

Experiment `abqaiq-ta-native-luna-xhigh-20260827-v1` completed five interleaved attack/recovery
replicates. All ten runs used the exact registered input bindings and completed without a
structured-output degradation.

| Information state | Native ratings | Harness long mapping | LLM calls | Input Tokens | Output Tokens | Cumulative time |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Attack | 4 `Hold`, 1 `Sell` | 5 abstentions | 89 | 445,205 | 195,274 | 3,922.862 s |
| Recovery | 3 `Hold`, 2 `Underweight` | 5 abstentions | 85 | 458,446 | 181,525 | 3,608.247 s |
| Total | 7 `Hold`, 2 `Underweight`, 1 `Sell` | 10 abstentions | 174 | 903,651 | 376,799 | 7,531.109 s |

The content hash of the private report is
`06fbb8435f9648d9647a4e65614c261e7b5cd7a088fb07c10b82bd93837172f2`.
Included Codex OAuth usage is recorded in Tokens and latency; no USD/token rate or zero-consumption
claim is made.

The result shows that the native graph is not a fixed-rule echo: its ratings varied between neutral
and negative views. It also shows no stable long thesis for either state. Mapping every non-long
rating to abstention preserves the Harness's registered action space but does not score the
external framework's short or underweight ability.

The external reports are broader than one Harness Judgment: they combine technical, fundamental,
sentiment, news, bull/bear, trader, and risk views into an actionable memorandum. That breadth is
expensive and does not enforce the same evidence discipline. Across inspected runs, the graph
generated precise support levels and `5.x/10` sentiment values from four market rows, and final
horizons drifted from the requested one session to several weeks or one-to-three months. Reports
also repeatedly called unavailable macro, prediction, social, and accounting tools. These are
observable native behaviors, not reasons to rewrite the external project.

The Harness's comparable opened-development runs were cheaper, retained exact Evidence references,
and kept the one-session/action-space boundary, but they also abstained throughout this sparse
case. Neither system has demonstrated alpha, calibration across Event Cases, or readiness for paper
or live execution.

## What the earlier masked smoke established

The earlier masked attack transcript introduced facts and analogues absent from every
model-visible input,
including the original Abqaiq identity and year, named geopolitical episodes, named oil
benchmarks and peer issuers, historical price moves, probabilities, betas, and transmission
coefficients. The recovery transcript did not name the original event, but still invented a
specific sentiment score, revenue-accrual mechanics, trigger levels, and execution assumptions
that were not in evidence. Raw transcripts remain private; this document records only the failure
classes.

The earlier attack run also exercised upstream's graceful structured-output fallback for two
decision roles. The run completed, but free text replaced the intended typed contract. That is a
useful availability behavior for an interactive assistant and an unacceptable silent quality
change for an auditable benchmark unless the degradation is recorded and scored.

Post-run review found another upstream lifecycle hazard: `propagate()` resolves prior pending
decisions before each graph run by fetching ticker and benchmark returns from Yahoo and may inject
an outcome reflection into `past_context`. The fresh paths made that branch a no-op for these two
one-shot observations, but the original adapter would not have been frozen on reuse. The hardened
private runner now sets no memory log, disables the resolver, and refuses an existing experiment
directory. The existing smoke outputs are not relabeled as evidence of that later hardening.

The two earlier `Hold` outputs are consequently external-system smoke results, not same-input
method evidence. The current native-capability baseline addresses a different question: how the
unmodified external method behaves when it is allowed to know the real event and target while its
retrieved data remain bounded by time. It does not relabel the masked smoke or make the two system
inputs identical. The existing `research-discipline`, `evidence-core`, `event-market-context`, and
`adversarial-risk` Skills continue to enforce the Harness's stricter evidence boundary.

## News collection and post-processing review

The pinned upstream routes configured vendors through its
[`dataflows/interface.py`](https://github.com/TauricResearch/TradingAgents/blob/a33fd4c0f134485a43553a2c23a63cb14adbd88f/tradingagents/dataflows/interface.py).
Price, indicator, fundamentals, and news categories can use Yahoo Finance or Alpha Vantage; macro
context uses FRED and prediction context uses Polymarket. An explicit configured vendor list is the
fallback chain, so an unselected source is not silently queried. Rate limits, missing configuration,
no-data responses, and other errors are distinguished by the router. Macro and prediction markets
are optional enrichments and may degrade to an explicit unavailable result.

Ticker and global Yahoo news are normalized in
[`yfinance_news.py`](https://github.com/TauricResearch/TradingAgents/blob/a33fd4c0f134485a43553a2c23a63cb14adbd88f/tradingagents/dataflows/yfinance_news.py).
Nested `content.pubDate` and flat `providerPublishTime` values are parsed as UTC. Reads use a
half-open `[start, end + 1 day)` window and reject dated articles outside it. Undated articles are
rejected only when the requested end is more than one day behind the host's current time; current
or future-shifted masked windows accept them. The past/current behavior has dedicated
[`test_news_lookahead.py`](https://github.com/TauricResearch/TradingAgents/blob/a33fd4c0f134485a43553a2c23a63cb14adbd88f/tests/test_news_lookahead.py)
coverage, but its passing tests do not cover a future-shifted historical replay like this case.
Global search queries several fixed macro themes, deduplicates by title, truncates to a limit, and
then renders selected articles as Markdown. Alpha Vantage instead returns its raw `NEWS_SENTIMENT`
response, so normalization is not yet uniform across vendors.

The sentiment analyst prefetches Yahoo news, the latest public StockTwits symbol stream, and Reddit
RSS searches across finance communities. It produces a structured sentiment band, zero-to-ten
score, confidence, and narrative. Its prompt usefully requires sample-size awareness,
cross-source divergence, event-versus-opinion separation, recurring narratives, and lower
confidence when sources are sparse. The news analyst separately combines ticker/global news,
FRED, and live Polymarket context into prose before the debate and decision graph.

### Patterns absorbed in the current slice

- Require UTC-aware half-open windows.
- Make the configured source chain explicit; never let fallback change source identity silently.
- Preserve typed distinctions among no data, not configured, rate limited, and failed, with
  optional enrichment represented as unavailable rather than fabricated neutral evidence.
- Allow a derived sentiment feature to describe sample size, cross-source disagreement,
  event-versus-opinion mix, and confidence. It remains a derived assessment, never canonical
  evidence or an automatic trading weight.
- Reuse analyst/risk/countercase roles as bounded decomposition inside a registered Skill Research
  Study when the question benefits from it. Role count, debate rounds, and repeated calls over one
  Event Case remain one analysis unit and cannot satisfy independent-validation gates.
- Test every source adapter for future-dated and undated-record leakage.

The public `NewsObservationBatch` contract now implements the ordered source chain, typed fetch
states, strict UTC half-open filtering before limits, unconditional undated rejection for
historical/masked replay, exact-version publication/update/availability gates, lineage-based
deduplication, and reconciled rejection counts. The optional `news-evidence-assessment` Skill
implements the qualitative sample/independence/disagreement review with read-only Evidence access.
Its first sparse-case 20-run paired diagnostic changed no decision or visible process outcome while
adding Tokens, so it remains opt-in until a genuinely multi-source batch exercises its precondition.

### Patterns not to absorb

- Do not flatten articles into prose before preserving `published_at`, `source_updated_at`,
  source/version identity, immutable content binding, and Evidence Item identifiers.
- Do not use title-only deduplication; use normalized source identity and content/version lineage.
- Do not apply a global article limit before the registered time and source filters, because early
  out-of-window items can starve valid records.
- Do not decide whether an undated article is admissible by comparing a replay date with the host's
  current time. Every historical or masked replay must reject it regardless of whether its shifted
  date lies in the past or future.
- Do not treat current StockTwits, Reddit, or open Polymarket results as historical point-in-time
  observations. The inspected paths are live/recent and do not reconstruct historical availability.
- Do not return catch-all strings such as `Error fetching news` as though they were data; they can
  defeat typed vendor fallback and contaminate prompts.
- Do not silently replace a failed typed decision with free text. Record a degradation event and
  make benchmark validity fail closed when the output contract is required.
- Do not let a research run resolve earlier decisions with current market data or inject
  outcome-derived reflection. Outcome opening and train-only lessons remain Harness-owned gates.
- Do not accept multi-agent prose or debate as evidence without claim-to-Evidence-Item lineage.

The Harness's Observation and Evidence contracts remain the authority. TradingAgents contributes
useful adapter tests and derived-analysis patterns, not canonical state or orchestration ownership.

## Comparison boundary and next gate

No evidence-locked rewrite is required for the native external baseline. Such a rewrite would
measure a project-specific modification rather than TradingAgents' actual role and prior-knowledge
method. The current 5-by-2 experiment therefore keeps native reasoning and freezes only the parts
needed for a fair behavioral observation: event/state, cutoff-bound retrieved inputs, target,
model, reasoning effort, role graph, debate rounds, temperature, action mapping, no memory or
outcome reflection, no post-cutoff/live source, and no broker reachability. Every structured-output
degradation remains counted rather than hidden.

Completion of this experiment can support only a description of behavior, stability, process, and
resource use on one opened Event Case. Any quality or promotion comparison requires multiple
independent pre-registered Event Cases with authenticated point-in-time evidence and outcomes,
simple baselines, an explicit treatment identity, and the Harness's existing promotion evaluator.
The external framework remains outside Harness authority and cannot gain paper or live execution
capability from a favorable research result.
