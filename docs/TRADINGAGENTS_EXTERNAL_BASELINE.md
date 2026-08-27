# TradingAgents External Baseline Diagnostic

## Status and boundary

On 27 August 2026, TradingAgents `0.3.1` was deployed as an isolated external baseline at
upstream commit
[`a33fd4c0f134485a43553a2c23a63cb14adbd88f`](https://github.com/TauricResearch/TradingAgents/commit/a33fd4c0f134485a43553a2c23a63cb14adbd88f).
The clone, virtual environment, adapter, model transcripts, and reports remain under the ignored
`.market-impact/` tree. No third-party code or runtime dependency entered this repository, no
secret was written to a file, and no broker, account, paper, or live-execution capability was
reachable. The two first runs had empty per-run memory paths, so upstream's pending-decision return
resolver had nothing to process and no live market read entered their output. Review nevertheless
found that resolver remained latent and could contact Yahoo on an experiment-directory reuse; the
private runner now disables memory and the resolver and rejects an existing experiment id.

This was a behavior and cost diagnostic against the same two strongly masked information states
used by the opened Abqaiq development case. It was not a registered benchmark arm. Its result
cannot rank TradingAgents against the Harness because the native role prompts did not preserve the
frozen-input boundary.

## Frozen-input deployment

The external runner used the upstream MiniMax China Provider, exact model `MiniMax-M3`, temperature
zero, one investment-debate round, and one risk-debate round. It enabled market, social/news
sentiment, news, and fundamentals analysts, followed by bull/bear researchers, a research manager,
a trader, three risk perspectives, and the portfolio manager.

Every TradingAgents data tool was replaced before graph construction:

- news tools returned only the state-specific masked Evidence Pack and evidence documents;
- the fundamentals tool returned only the frozen target-exposure mapping;
- price, technical, social, macro, accounting, insider, and prediction-market tools returned
  explicit unavailable sentinels;
- the real ticker and issuer were unavailable; the graph received only
  `integrated-upstream-a`;
- each observed run began with a fresh private memory path, so no earlier decision or reflection
  was loaded, although upstream appended a pending decision after completion; and
- `Buy` or `Overweight` mapped to the Harness's long proposal, while every other native rating
  mapped to abstention.

## Observed result

| Information state | Native result | Harness mapping | LLM calls | Input tokens | Output tokens | Estimated cost | Wall time |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Attack | `Hold` | Abstain | 17 | 84,009 | 39,329 | USD 0.072398 | 447.457 s |
| Recovery | `Hold` | Abstain | 15 | 63,503 | 48,927 | USD 0.077764 | 641.849 s |
| Total | two `Hold` results | two abstentions | 32 | 147,512 | 88,256 | USD 0.150162 | 1,089.306 s |

Both abstentions avoid the registered fixed-long controls, which were net negative in both opened
states. This is not evidence that either reasoning method predicted returns: abstention has zero
research score, both states belong to one already opened Event Case, and the external method ran
only once per state.

The ten accepted `family_guided` Harness runs across the same two states cost USD 0.091915. The two
single-run TradingAgents diagnostics therefore cost about 1.63 times that entire ten-run arm. The
full four-arm Harness comparison cost USD 0.397066 for 40 runs. These numbers describe observed
resource use, not an apples-to-apples quality comparison. Projecting the two external observations
to five replicates per state would cost roughly USD 0.75 and take roughly 91 minutes if behavior
scaled linearly; that work was not spent after the validity failure became clear.

## Why the native result is not a clean comparison

The attack transcript introduced facts and analogues absent from every model-visible input,
including the original Abqaiq identity and year, named geopolitical episodes, named oil
benchmarks and peer issuers, historical price moves, probabilities, betas, and transmission
coefficients. The recovery transcript did not name the original event, but still invented a
specific sentiment score, revenue-accrual mechanics, trigger levels, and execution assumptions
that were not in evidence. Raw transcripts remain private; this document records only the failure
classes.

The attack run also exercised upstream's graceful structured-output fallback for two decision
roles. The run completed, but free text replaced the intended typed contract. That is a useful
availability behavior for an interactive assistant and an unacceptable silent quality change for
an auditable benchmark unless the degradation is recorded and scored.

Post-run review found another upstream lifecycle hazard: `propagate()` resolves prior pending
decisions before each graph run by fetching ticker and benchmark returns from Yahoo and may inject
an outcome reflection into `past_context`. The fresh paths made that branch a no-op for these two
one-shot observations, but the original adapter would not have been frozen on reuse. The hardened
private runner now sets no memory log, disables the resolver, and refuses an existing experiment
directory. The existing smoke outputs are not relabeled as evidence of that later hardening.

The final `Hold` is consequently an external-system smoke result, not same-input method evidence.
Running more replicates would increase apparent sample size and cost without repairing treatment
identity. The existing `research-discipline`, `evidence-core`, `event-market-context`, and
`adversarial-risk` Skills already forbid these behaviors; the diagnostic supports those controls
and does not justify duplicating them.

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

### Patterns to absorb

- Require UTC-aware half-open windows.
- Make the configured source chain explicit; never let fallback change source identity silently.
- Preserve typed distinctions among no data, not configured, rate limited, and failed, with
  optional enrichment represented as unavailable rather than fabricated neutral evidence.
- Allow a derived sentiment feature to describe sample size, cross-source disagreement,
  event-versus-opinion mix, and confidence. It remains a derived assessment, never canonical
  evidence or an automatic trading weight.
- Test every source adapter for future-dated and undated-record leakage.

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

## Next comparison gate

Keep the pinned native deployment as a private negative/control diagnostic. A formal external
comparison may begin only after a separate evidence-locked adapter:

1. scans every role output for forbidden identity tokens, unsupported named entities, numbers, and
   external-history claims before any downstream role sees it;
2. preserves Evidence Item citations and publication/availability metadata through the final
   decision;
3. disables outcome reflection, live/recent data, and cross-run memory;
4. records every structured-to-free-text fallback as a validity-affecting degradation; and
5. passes one state with zero contamination under a frozen cost and token ceiling.

That adapter would be a modified evidence-only TradingAgents treatment, not a measurement of the
native project. Only after the gate passes should it run the registered five replicates per state.
The external framework must remain outside Harness authority and cannot gain execution capability
from a favorable research result.
