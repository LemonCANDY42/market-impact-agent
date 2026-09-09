# Stage 1 model/evidence pilot — 2026-09-06

## Decision

The registered two-window comparison is complete: eight research Runs completed,
with no research terminal failures or unknown spending. **Luna is not promoted to
the proposed default research model in this pilot.** It was cheaper and preferred
in three blinded qualitative pairs, but one event-arm answer introduced an
unsupported liquidity-measures channel. This fails the registered source-quality
gate. No trading-effectiveness claim, Stage 2/3 advancement, paper action or live
execution is accepted.

This is an observed gate failure in a small development study, not a general claim
that Terra is superior or that Luna cannot be useful. It also does not justify
repeating the opened cases until Luna passes.

## What was measured

The target was 510300.SH in two selected historical windows:

- Rebound window: cutoff 2024-02-06 09:25 Asia/Shanghai, H5 through 2024-02-20.
- Closure-shock window: cutoff 2020-02-03 09:25 Asia/Shanghai, H5 through 2020-02-07.

Each window paired price-only and price-plus-event inputs under Luna max and Terra
high. Models received the same available source pack, target, question and horizon
within each matched arm; their native evidence-read choices could differ. Evidence
was frozen before calls, ordering was counterbalanced, and no acquisition
successors or semantic answer repairs were used in the eight research Runs.

These are **two independent selected windows**, not eight independent market
samples. Historical pages establish modeled availability, not contemporaneous
receipt. H5 return is adjustment-consistent close-to-close movement, not strategy
PnL. H1 is only a diagnostic of the same H5 forecast.

## Cost, completion and speed

Costs use returned physical token usage and the frozen provider prices. They are
local ledger charges, not an independently obtained provider billing invoice.

| Research panel only | Luna max | Terra high |
|---|---:|---:|
| Completed Runs | 4/4 | 4/4 |
| Physical model requests | 8 | 7 |
| Native evidence tool calls | 14 | 6 |
| Direction coverage | 4/4 | 3/4 |
| H5 hits / scored directions | 2/4 | 1/3 |
| Unknown directions | 0 | 1 |
| Cost | $0.060429 | $0.199774 |
| Median cumulative provider latency per Run | 151.8 s | 33.6 s |
| Provider latency range per Run | 119.5–234.3 s | 21.6–38.7 s |

Luna cost **69.75% less** in these matched tasks, with about **4.52×** the median
provider latency. Efforts were Luna **max** and Terra **high**; this is a comparison
of those configured routes, not a controlled equal-compute benchmark. Four Runs per
route do not support a reliable tail-latency claim.

Research cost was **$0.260203**. Three qualification batches cost **$0.050396**.
The new authorization's total is **$0.310599 across 21 physical requests**, with
zero unsettled requests and zero retained request reservations. The nominal
unspent ceiling is $139.689401, but protected group allocations remain held; that
number must not be treated as freely reallocatable capacity. No further model
calls are planned for this completed panel.

The complete panel's worst-case pre-dispatch allocation was $44.064768. This is a
funding bound for possible physical attempts, not a spending target or a cost
forecast. The USD140 parent was neither reset nor replaced.

## Did event evidence improve the result?

| Window / available evidence | Luna H5 | Terra H5 | Realized H5 movement |
|---|---|---|---:|
| 2024 / price only | down | down | +6.0162%, up |
| 2024 / price + event | rangebound | rangebound | +6.0162%, up |
| 2020 / price only | down | unknown | −2.7027%, down |
| 2020 / price + event | down | down | −2.7027%, down |

Event evidence changed both models from down to rangebound in 2024, but neither
identified the realized upward direction. In 2020 it changed Terra from abstention
to a correct down call; Luna's direction stayed down. Thus the study demonstrates
that event inputs can change reasoning and direction coverage, **not that they
reliably improve prediction**. The models' event arms each hit one of two windows.
Terra's unknown is a completed analytical answer, not a failed Run or an incorrect
directional prediction. Conditional hit rates with different coverage are not a
standalone model ranking.

The same-forecast H1 movements were +3.2107% and −8.4334%. The 2020 difference from
H5 also explains why a generic opening rebound is an inadequate falsifier for a
five-session directional conclusion.

## Blinded quality assessment

An independent reviewer received two immutable packets without model identities,
costs or realized outcomes. A/B labels were randomized separately for each pair.
Preferences were fixed before the lead opened the identity key.

| Pair | Blinded preference | Revealed route | Important limitation |
|---|---|---|---|
| 2024 price only | A, modestly | Luna | More concrete counter-tests, but mixed raw/adjusted levels |
| 2024 price + event | A, narrowly | Luna | More specific effective policy and thresholds, but overly firm floor language |
| 2020 price only | B, modestly | Luna | Checked price/volume arithmetic and clearer counter-test; abstention itself was not penalized |
| 2020 price + event | B, clearly on grounding | Terra | Luna added an unsupported liquidity-measures channel |

The consequential source-quality finding is Luna's phrase **“liquidity measures”**
in the 2020 event transmission. Frozen content supports medical mobilization,
hospital construction, resilience commentary and regulatory preparedness. It does
not establish the added liquidity channel. This is an unsupported material policy
channel; the review does not establish whether it arose from memory or generic
inference, and does not prove post-cutoff leakage.

Other reproducible gaps:

- Luna mixed raw and adjusted prices in the 2024 window. The November 10 adjusted
  close is 3.578438, while raw is 3.655; comparisons through 3.208 give approximately
  −10.352% and −12.230% respectively. Some historical resistance levels also mixed
  adjustment regimes. The broad declining trend survives this correction.
- Terra's 2024 price-only wording omitted the final-session uptick from 3.188 to
  3.208 (+0.627%). Its broader trend observation remained defensible.
- Both event answers overinterpreted modeled end-of-day availability as proof that
  official communication was not reflected in the prior close. Modeled availability
  does not authenticate publication time or rule out anticipation.
- Both routes sometimes supplied vague persistence criteria and stronger priced-in
  labels than their evidence could measure. Terra additionally attached CPI/PPI
  metadata to a rebound mechanism that was actually supported by the news corpus.

These findings prohibit unconditional default promotion despite Luna's cost and
specificity advantages. Existing model defaults were not changed.

## What the qualification failures established

All prior failures remain stored and charged:

1. Luna completed; Terra returned a `typed_unknowns` object. The prompt had not
   specified its array type. Explicit field typing was added. Batch cost: $0.019223.
2. Luna completed; Terra returned well-formed JSON with a supported event, unknown
   target direction, citations and an explicit missing-linkage gap. The domain
   validator incorrectly required supported events to have transmission. Batch
   cost: $0.014965.
3. After the domain/schema correction, both routes completed and native budget
   replay passed. Batch cost: $0.016208.

The fix separates evidence that an event occurred from evidence of its impact on
the selected target. Supported events still require citations; absent transmission
still requires explicit gaps. A diagnostic parse of the second answer demonstrates
the correction but does not turn the old failed terminal into a completed one.

Corrections form an immutable failed-predecessor chain. One successor per
predecessor prevents forks; every attempt shares the original six-request / $0.50
route allowance. No new qualification budget was issued.

## Acceptance evidence and artifacts

- Final full local suite: **2213 passed**, 212 existing warnings.
- Ruff, formatting, Pyright, pi TypeScript and all four Node tests passed.
- Final domain, recovery and execution subset: **48 passed**.
- Independent code review closed the budget ancestry, freeze/replay, recovery-fork
  and schema findings; content review remained blinded until preferences were fixed.
- Repeating the completed production command produced an identical report and
  kept the parent at **21 requests / $0.310599**: zero new model dispatches.

Private evidence root: `.market-impact/stage1-model-pilot-20260906-usd140/`.
Canonical native panel: `panels/paired-v3/report.json`; companion selection:
`model-assessment.json`; quality details: `blinded-quality-review.json`; replay:
`replay-proof.json`; final tests: `verification/full-pytest-final.log`.
The companion assessment references the native report and review artifact hashes;
it does not rewrite native terminals or the raw execution report's pending-review
field. Source captures and model-native responses remain private and uncommitted.
