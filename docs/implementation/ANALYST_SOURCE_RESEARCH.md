# Analyst-source research extension

Status: research design and source feasibility prepared on 2026-09-07. The user
approved evaluating this extension and preparing experiments, separately from the
USD 4 second historical account pair. No analyst-source paid batch, collection
schedule, trading authority or strategy promotion has been activated.

## Decision and evidence

The idea is testable: timely outside analysis could add an overlooked fact, a
counter-scenario or useful risk timing. It can also repeat news, amplify a bull/bear
bias, induce turnover, or supply hindsight-selected winners. Poor model performance
is not yet attributed to missing social information; the two historical account
pairs are development evidence, not a diagnosis of that cause.

[Finfluencers](https://jhfinance.web.unc.edu/wp-content/uploads/sites/12369/2023/11/Finfluencers.pdf)
finds heterogeneous skill and an adverse relationship between popularity and skill
in its studied social network. This supports testing author selection, not assuming
that followers identify skill; its estimates do not transfer to A shares or named
Chinese authors. The [CFA Institute report](https://rpc.cfainstitute.org/research/reports/2024/finfluencer-appeal)
provides complementary evidence on consumer use, disclosures and commercial
incentives; it is not proof of investment alpha.

Li Daxiao is a user-suggested discovery candidate. This review did not establish an
authenticated original feed plus a complete, dated and independently scored record.
No skill score, endorsement or privileged input weight is assigned. Reposts claiming
successful calls are leads, not evidence of the complete forecasting denominator.
[Oaktree's original memo archive](https://www.oaktreecapital.com/insights) is another
candidate for attributable long-horizon reasoning; it is not automatically relevant
to five-day A-share timing, nor does public availability establish automated reuse
permission. No author has passed admission in this extension.

## Current access and sustainable acquisition

The local Agent Reach finance route delegates desktop access to OpenCLI, using the
existing browser session. On this date OpenCLI 1.8.6 doctor passed and Xueqiu whoami
returned logged_in=true. A single quote read returned a price-bearing record; one
followed-user feed read returned an empty list. Thus authenticated quote access is
verified; recent analysis retrieval and arbitrary-author history are unverified.
The current adapter offers feed and stock discussion reads; its `search` searches
stocks, not people or historical analyst claims. Do not promise an author crawler
from these capabilities. No cookies, user account IDs or feed text enter this repo.

The [current Xueqiu service agreement](https://xueqiu.com/about/faq), section III
items 5–7, restricts automated extraction and AI use without the specified consent.
A successful browser request or low request frequency does not establish permission
for a maintained research corpus. Xueqiu recurring ingestion is therefore not ready.
Resolve source/platform reuse permission or use an author-controlled source with
appropriate rights. User-supplied material also needs sufficient reuse rights; it is
not a workaround for a platform restriction. No permission request was sent.

For eligible sources prefer a permitted original RSS/API/email subscription or
licensed archive. Do not install another service or develop a custom crawler now.
Proposed daily-horizon trial cadence is one incremental check after the A-share close,
not real-time polling; use provider limits if stricter. Check only changed entries,
cap the pilot at eight authors and two independent opinions per author/day. Preserve
cursor gaps and stale/missing status rather than claiming completeness. On 401/403,
429 or a challenge, pause that source and honor Retry-After; no identity/IP rotation
or retry loop. Weekly author-pool review and monthly score snapshots are sufficient
starting points, subject to source rights and observed coverage. These are proposed
operating settings, not platform-approved safe rates or a running automation.

## Pool maintenance and capability scoring

Keep one versioned study registry in existing artifact storage, with derived views.
Use candidate, observe, eligible and paused statuses; append reasons and effective
cutoffs, retaining departed authors in the historical denominator. Eight authors is
an initial acquisition ceiling, not a target to fill with weak sources.

Admission checks original identity/provenance, permitted acquisition and AI reuse,
publication/version availability, actual activity, conflicts/disclosures and the
fraction of statements that can be scored. Unknown conflicts stay unknown. Popularity,
credentials, reputation and self-reported account screenshots do not substitute for
skill. Pool discovery includes bullish, bearish, industry-fundamental and valuation
perspectives, without manufacturing equal sample counts or selecting recent winners.

Measure capability by market/instrument, explicit forecast horizon and stated
forecast type. Upward opportunity detection and downside warning are separate cells;
market regime, if used, is defined solely from data available at the decision cutoff.
Do not label regimes using the realized future. Both supportive and opposing views
remain visible. A bull specialist does not gain weight simply because the Agent
already wants to buy, and a long-term valuation essay is not scored as a five-day call.

Every eligible opinion needs an original reference, author identity, publication and
first-observed timestamps, content/version hash, asset scope, stance, explicit horizon,
conditions and invalidation where stated. Missing horizon, target or usable direction
means unscorable for that cell; do not make the author echo Harness IDs. Store bounded
permitted excerpts and exact provenance in private artifacts; derived records point
back to those artifacts. Reposts and repeated same-thesis messages share a lineage;
count at most one independent author/asset/horizon episode until its outcome matures.
Track contradictions, edits, deletions and collection gaps, never just surviving hits.

For the initial broad-ETF H5 cell, score bullish calls on positive next-executable-open
to fifth-session-close return, and risk warnings on a decline of at least 2% from that
open to the minimum close in the window. These are trial labels, not universal skill
definitions. Compare each against its expanding, cutoff-known event base rate and
always-bull/always-risk controls. Report warning recall, false-alarm rate, missed
upside, turnover and drawdown alongside precision. Brier score is used only for an
explicit author probability; do not invent probabilities from emphatic language.

Proposed weighting is deliberately small and frozen before evaluation: with n
independent matured calls and h hits in a direction/horizon cell, shrink precision
toward the cutoff-known base rate p0 as (h + 20*p0)/(n + 20). With fewer than 20
independent matured calls, retain neutral weight. Otherwise use
clip(1 + shrunk_precision - p0, 0.8, 1.2) as a relative opinion weight. Apply one
lineage-level contribution cap so copied consensus is not multiple votes. This is
research metadata, never a position-size multiplier or policy bypass. Twenty calls
is an eligibility heuristic, not significance or promotion evidence. No automatic
contrarian inversion of a weak author is proposed.

## Preregistered comparisons and acceptance

The companion [preparation record](../research/analyst-source-20260907/preparation.json)
freezes the following sequence. It is not executable paid authority.

1. Source feasibility: prove an eligible original source, version/receipt timestamps,
   a complete bounded collection interval and usable forecast coverage. A synthetic
   corpus may verify mechanics but cannot substitute for these facts.
2. Prospective observation: archive eligible opinions at their actual receipt time,
   without orders or paid model extraction. Freeze a calibration prefix and exclude
   any outcomes that have not matured before each weight snapshot. Past posts first
   retrieved now are development-only, unless contemporaneous availability is proven.
3. First incremental test: A = existing price + event + unchanged strategy/Recall;
   B = the same plus eligible unweighted analyst opinions. Same source cutoff,
   instrument, account, execution costs, model and maximum budget per arm. Register
   exact dates, account data, author set, prompts and costs before any paid calls.
   Opinion text is bounded to 12,000 UTF-8 bytes across at most eight independent
   lineages per decision, with deterministic relevance/recency selection before the
   outcome. Primary market facts are not truncated to make room for opinions.
4. If B yields useful complete-pair evidence, test C against B on new dates: identical
   opinion set/text and token ceiling, with only frozen conditional skill metadata
   added. This isolates weighting from better content selection. Include a zero-model
   mechanical opinion-consensus account control to separate the Agent's interpretation
   from simply following exposure signals.
5. Start with 20 non-overlapping H5 decision episodes for developmental feasibility;
   count shared-market episodes as one cluster, not one per author or message. Stop
   early for missing rights/PIT, persistent incomplete pairs or cost-cap failure.
   Do not stop early just because returns look favorable. This sample supports a
   continue/redesign decision only. A later powered confirmatory protocol must freeze
   its holdout and multiplicity control before testing; no alpha claim from this pilot.

Primary paired outcome is net account return after actual modeled execution costs.
Also report maximum drawdown, turnover, false alarms, missed upside, source delay,
model and collection cost, complete/incomplete denominators and failure costs. Report
investment P&L and operating expense separately unless an exchange-rate/accounting
rule was preregistered. A result caused solely by lower average exposure must be
shown against a matched-exposure control. Missing opinions are marked unavailable;
retain the baseline opportunity and count acquisition failure, not a clean matched
opinion trial. Expired/unavailable sources are not forward-filled indefinitely.

## Integration boundary and next gate

Reuse `EvidenceItem` provenance and `EvidenceTier.COMMUNITY`/`SPECIALIST`, existing
artifact hashing, frozen input packs, Research Thesis, ModelBudget group admission,
Nautilus account replay and paired metrics. An authenticated source proves what an
author said, not that the forecast is true. A factual statement quoted by an author
needs primary-source corroboration to acquire that authority. External author text
is untrusted evidence, never tool instructions, mandate edits or order authority.

Do not insert human authors into `DecisionRecallStore` as fabricated signed Agent
Runs, and do not add Xueqiu to the established-news publisher allowlist. When source
readiness passes, implement the smallest evidence adapter/projection needed inside
existing owners, with focused PIT/version/deduplication tests. No new database server,
UI, debate framework or execution engine is required.

This extension follows the current event-account interpretation work as a separate
information-source ablation. It does not replace the frozen physical-energy source
criteria, advance the target-selection/continuous-review stages without predecessors,
or consume the approved USD 4 historical-pair budget. Current next gate: a permitted,
verifiable opinion corpus. No scheduled harvesting or real-model opinion study has
been started, and no human-source investment benefit has yet been measured.
