# Point-in-time evidence recovery

This plan closes historical evidence gaps without relabelling present-day backfills as
decision-time truth. It adds no paper or live execution capability.

## Recovery order

Use the least expansive source that can prove the required historical version:

1. Verify the exact publisher request target in an immutable web archive. Wayback's historical
   HTTP/HTTPS scheme substitution is accepted only for the same host, path, path parameters, and
   query, with only the scheme's default port and no user information; fragments are irrelevant
   because HTTP never sends them. The capture must precede the checkpoint, the replay digest must
   match the locator, and source-specific extraction must recover the publisher identity and
   publication/update time from that archived payload.
2. Use an entitled point-in-time vendor export for gaps that public archives cannot represent:
   versioned established news, market/index history, industry history and taxonomy, positioning,
   expectations, and revision vintages. A vendor name or historical query result is insufficient;
   the export must expose an immutable version or delivery identity and its historical authority
   time.
3. For future events, retain actual-receipt artifacts at ingestion: raw response digest, source
   version, publisher publication/update times, requested/retrieved times, and typed failure. These
   receipts calibrate real latency but do not rewrite past cases.

Public RSS/Atom is useful for the third lane when the publisher explicitly offers the feed and the
license scope permits local retention. A current feed receipt proves only that the Harness received
that exact feed version now. `pubDate`, `lastBuildDate`, an aggregator timestamp, or a current rolling
feed does not authenticate historical availability. Feed discovery through Google News, GDELT, or a
community-maintained URL list remains discovery-only until the canonical publisher source and use
terms pass the same route/configuration gate.

Internet Archive and Common Crawl are recovery authorities, not discovery substitutes for an
established publisher. GDELT remains discovery-only. A located capture is only a candidate until
its replay body is digest-verified and passes the publisher extractor.

The preferred licensed acceptance trial is a small frozen export, not a full integration. It must
cover at least one checkpoint from each selected event family and prove the exact required
identities before purchase or adapter expansion:

- the registered market index and a total-return equivalent when an investment-return claim is
  intended;
- every registered industry series under the classification that was effective at the checkpoint,
  including its taxonomy/methodology version;
- at least two established-news publishers with publication/update and historical version times;
- the relevant exchange positioning series and macro release revisions; and
- stable identifiers, export timestamps, hashes, licensing scope, and deterministic re-import.

LSEG Machine Readable News plus Tick History or an equivalent Bloomberg point-in-time export is
the first broad trial route. China-specific Wind exports may complement it only when the export
contract proves historical version/taxonomy identity. No provider is accepted merely because it
returns old dates.

### Frozen vendor trial acceptance matrix

Run one small, read-only export trial before implementing an entitlement adapter. Freeze the trial
request first: provider and product, selected checkpoints, requested identifiers and fields, query
time zone, revision policy, delivery channel, and licensing scope. Keep the raw export private.
The reviewable trial artifact retains only the request identity, immutable delivery/export identity,
historical authority time, source/version identifiers, row and payload hashes, field coverage,
license classification, and deterministic re-import result. A current export timestamp is retrieval
evidence; it cannot replace the historical authority time.

| Evidence slice | Minimum frozen sample | Required version/timestamp contract | Pass condition | Fail-closed result |
| --- | --- | --- | --- | --- |
| Market index | Each selected case's exact registered primary index at one early and one later checkpoint; include the provider's total-return counterpart where a wealth claim is intended | Stable series identifier, price-versus-total-return basis, methodology/currency/calendar version, original observation or correction time, immutable historical delivery/version identity, and `authority_at` | Re-import is byte- or row-hash deterministic; all rows visible at the checkpoint bind one historical version; any correction is separate lineage; the exported series either matches the bound panel hash or is used to build and register a new provider-bound panel | Keep the current price panel descriptive; do not mint authority for Tushare rows from a different provider export |
| Industry index and taxonomy | Every required industry proxy for the sample checkpoints, using the classification effective on each date | Stable series and constituent/classification identifiers, taxonomy and methodology version, effective-from/effective-to dates, revision history, observation time, and immutable historical delivery/version identity | Every proxy resolves under the then-effective taxonomy without current-classification backfill; rows and taxonomy re-import deterministically; a new provider-bound panel/registration is frozen when identities differ | Retain SW2021 backcasts as hindsight-only opportunity bounds |
| Positioning or expectations | Exact SSE/SZSE margin or other registered positioning rows for the sampled decision dates | Exchange/source row identity, market date, publisher update schedule, original release time, correction/version identity, historical authority time, units and coverage | The export proves which version was available before each cutoff and represents later corrections as new lineage | Keep the category unqualified; a current historical query does not authenticate past availability |
| Macro vintage | The two currently missing checkpoint vintages plus one known passing revision case | Release-series identifier, reference period, release/vintage number, original publication time, revision publication times, immutable version/delivery identity, and historical authority time | Original and revised values import as distinct versions; qualification selects only the version whose availability and authority precede the cutoff | Preserve the missing macro blockers; never copy today's revised value into an earlier checkpoint |
| Established news | At least eight items from at least two registered established publishers inside one sampled news window; include one updated story | Publisher and story/urn identity, `published_at`, `source_updated_at`, version sequence, historical delivery/authority time, payload or canonical-text hash, and entitlement scope | The same frozen query reproduces the ordered version set and hashes; publication/update extraction agrees with the publisher payload; later revisions do not replace the pre-cutoff version | Keep the news gate closed; discovery timestamps and vendor export time cannot serve as publisher or authority time |

The trial passes only when every row has a historical authority supplied by the source contract,
the raw-to-normalized import is deterministic, and license terms permit the intended private
retention. A passing trial authorizes an adapter design review; it does not by itself change the
Study Registration, Panel, Manifest, Qualification Report, or paper/live capability.

## Implemented archive recovery path

`market-impact regime evidence-audit-publisher-archives` reads one bound Regime Evidence Manifest,
its strict Qualification Report, and the frozen Study Registration. For each exact Xinhua or SCMP
URL in the registered news window it reports one of:

- `capture_found_unverified`: a pre-cutoff locator exists, but no evidence is promoted yet;
- `not_found`: the archive answered successfully and contained no qualifying capture; or
- `source_error`: availability could not be determined and must not be treated as no data.

The private, content-identified report includes the projected news minimum only under the explicit
condition that every found payload later verifies. It stores no article body and grants no
execution capability.

`market-impact regime evidence-capture-publisher-archive` then accepts one original evidence
record, its locator, and a checkpoint cutoff. It verifies the replay digest, the scheme-normalized
exact request target, publisher/source identity, original publication time, update/availability
cutoff, and archive capture cutoff before writing a new `verified_archive` evidence record. For a
later publisher version, `--supersedes-record` binds the prior recovered record and requires the same
source, claim, and lineage plus strictly later availability. When the digest-verified archive entity
is gzip encoded, publisher extraction uses a bounded decompressed view while the authority and
content hash remain bound to the original archived bytes. Rebuilding and qualifying a Manifest
remains a separate explicit step; the command never mutates an existing Manifest.

The first bounded run on 2026-08-28 exercised 11 checkpoints for the 2021 sector-rotation case and
identified 32 archive lookups. This managed environment could not reach the Internet Archive index,
so all 32 are retained as `source_error`. That run accepts failure classification and private report
materialization only; it establishes neither archive coverage nor recovered evidence.

A later full-access run on the same date completed all six selected cases: 61 checkpoints and 230
exact-URL lookups produced 162 found candidates, 68 genuine `not_found` results, and zero
`source_error` results. The found candidates collapsed to 120 unique original-record/archive-version
pairs. Digest replay and publisher extraction materialized 115; three failed because the publisher
modified time preceded publication, and two failed because archive authority preceded modeled
availability. Those five remain fail-closed.

The 115 passing captures cover 98 original current-snapshot records. The replacement Manifest keeps
one earliest-authority capture for duplicate captures of the same semantic version and links the two
true updated versions through the reproducible `--supersedes-record` path. It contains 100 recovered
historical records and is
`regime-evidence-manifest-af6c3d0739bcc61ddd0cbdbdac7bfe41ca8badbb4766866168dd95c12c680344`.
Strict requalification is
`regime-evidence-qualification-report-6e163ecb607a803d06cc1775b68845674747296b535543a6e153e53006c5eca9`.
Across the six cases, established news now passes 8 of 61 checkpoints. Within the frozen 18-checkpoint
validation selection, it passes 2 of 18; the complete checkpoint gate remains 0 of 18.

The evidence records above prove archive authority, but Agent replay also needs readable article
content. A second replay pass recovered exact bodies for 100/115 accepted archive versions with
matching content hashes. Fifteen remained network failures; no replayed body failed digest or
publisher verification. The rebuilt 18 checkpoint inputs have full exact news bodies at 13
checkpoints and six of eight at five checkpoints. This document materialization is private and does
not modify the Manifest or authority timestamps.

## Three evidence lanes

Historical recovery and future ingestion now share one `RegimeEvidenceRecord` contract but enter
three deliberately non-interchangeable lanes:

1. **Strict historical PIT** requires both source content/availability and immutable historical
   authority at or before the decision cutoff. Only the strict Qualification Report can admit this
   lane.
2. **Modeled-PIT replay** keeps the same record, Manifest, source minima, cutoff, and revision
   lineage, but applies a content-identified category policy: prior-session panel snapshots for
   price context and `available_at` plus a frozen safety delay for other sources. The unresolved
   `authority_at` gap remains visible and no record is backdated. This lane is limited to opened
   process diagnostics.
3. **Prospective actual receipt** stores the immutable Source Observation when the Harness receives
   it, then adapts that observation into the same Regime Evidence Record with
   `available_at == authority_at == retrieved_at`. It proves what the future strategy actually had
   and supplies honest latency samples for later policies; it never retroactively authenticates an
   older case.

The frozen modeled policy is
`regime-modeled-pit-policy-b3959de39c87b4abe47a8e6543448cd2958280735389615363ccc3bd61eeb7c8`.
Its Qualification Report is
`regime-modeled-pit-qualification-report-396748a622b5d23e410926917e364860f76ed370cc9798498b29e9b23b519aa0`:
18 of 166 study checkpoints are eligible because they are exactly the registered six-case
first/middle/last selection; none of those 18 is strict-ready. The separate modeled Agent
registration binds those exact checkpoints, three paired replicates, the Luna xhigh Provider
Profile, and the shared $20 cap. Strict and modeled qualification objects reject each other's
execution entry points.

The registered diagnostic completed all 18 checkpoints and 108/108 Agent runs. Both arms abstained
at every checkpoint, including all 13 full-body news inputs. Horizon persistence, event attribution,
and expectation delta were unresolved in 108, 106, and 105 runs respectively. The result therefore
supports the reusable three-lane data contract while identifying a separate Agent-input problem:
the next bundle must state the observed event fact, its cited prior expectation, a falsifiable
transmission path, a registered mechanism-appropriate horizon, and executable target mappings.
Further archive text alone is unlikely to change the decision surface.

The content-identified aggregate is
`regime-modeled-pit-agent-validation-report-317f79ea1602e7d381eba01f9522123116033bdbbc179180dfa71f46f895f380`.
Its checkpoint rows carry the canonical paired registration ID/hash and the recomputed common-input
hash. The report also reconstructs both expected arm execution bindings and matches all 36 hashes
to their content-addressed binding artifacts and six-record local Usage Ledgers. This proves that
all 18 formal checkpoints executed the prompt derived from the frozen Evidence Pack and eligible
horizon recorded by the Harness; the registration separately content-binds the Method Evidence
Declaration used for routing. Every reported run decision, candidate, summary, metric, and evidence
coverage row is rebuilt from the ledger-bound terminal Judgment Artifact and Run Journal before the
checkpoint majority is admitted. The terminal replay also reparses the final model-turn assistant
payload and matches the proposal, raw response, transcript, and metrics against the hash-chained
validation event.
Its Usage Ledger Union covers 70 ledgers and 528 unique runs with no duplicate or conflicting Run
IDs. Total estimated model cost is $4.928324 under the shared $20 cap; this supersedes the earlier
incomplete $2.436518 total.

This split is the reusable data-source design. A new source adapter supplies normalized source,
publisher, version, occurrence/publication/update, receipt/availability, immutable content hash,
authority, license, and revision-lineage fields once. The lane-specific qualifier then decides what
claim that evidence may support. Vendor backfills, web archives, and future streaming sources do not
need parallel schemas or independent orchestration authority.

Material that remains historically unqualified is not discarded. The generic `retrospective` Data
PIT lane archives it using the real later receipt time and preserves the missing `authority_at` as a
gap. Postmortem tools may compare that later context with the original decision, but strict
backtests, prospective Judgment inputs, strategy promotion, and order creation cannot consume it.
`modeled` remains a distinct frozen visibility assumption for process diagnostics rather than a
catch-all label for hindsight material.

## Price basis

“Adjusted price” has different meanings by instrument and use:

- Stock and ETF research signals use an **as-of adjusted** series. For a cutoff factor `F_c`, a
  historical close is `raw_close_t * F_t / F_c`. Only sessions ending by the cutoff enter the
  series. A factor table retrieved later is still retrospective reconstruction until an archive or
  historical provider version proves its cutoff-time authority.
- Stock and ETF execution, fees, limits, fills, and order prices use **raw tradable prices** plus
  separate corporate-action state. Adjusted prices are never sent to Nautilus as executable bars.
- A price index has no stock-style ex-right adjustment. The current Regime Panel intentionally
  measures price-index movement. Any claim about investor wealth or distributed income must use a
  point-in-time total-return index or an implementable ETF path with distributions, costs, and
  corporate actions.

Accordingly, the current Tushare stock helper normalizes adjusted closes to the last factor whose
session ends by the research cutoff. The deterministic replay path remains unchanged and continues
to consume unadjusted OHLC plus source limits. The Regime Panel's `return_basis: price` remains a
descriptive price-index basis, not a total-return or executable-investment claim.

## Remaining blockers

Archive recovery can close only some official/news gaps. The frozen 18-checkpoint selection still
needs historically authoritative market, industry, and positioning versions at every checkpoint,
16 news windows, two macro authorities, and the first 2024 policy revelation before 09:25.
Older industry cases also need the classification effective at their checkpoint; the current
SW2021 backcast remains a hindsight-only opportunity bound. Until a new Qualification Report passes,
Agent comparisons remain diagnostic and cannot support alpha, paper, or live claims.

In parallel, the Prospective Receipt Journal and source-acceptance sequence in
[DATA_PLATFORM_PLAN.md](DATA_PLATFORM_PLAN.md) collect genuine future availability/authority and
build reusable datasets. That path improves future backtests and paper/live inputs, but it does not
change the remaining historical strict-PIT blockers above.

## Outcome-opened event-logic stress tests

A private, reproducible historical stress test now covers 17 `news_first` Event Clusters and 37
asset exposures from the outcome-opened catalog. It verifies upstream hashes, aggregates by Event
Cluster, freezes a 1,000-draw random-date comparison, and reports window, pre-event drift,
market-relative, state, family, overlap and multiple-testing sensitivity. Its content identity is
`historical-robustness-06168192f3db05225851c10cdac45a9eeb9ec967085763f8f7bac94e45a502d8`.

The descriptive signed cluster mean is 3.38% at five sessions, while the signed 20-session
pre-event drift is -0.98%; the five-session random-date empirical p-value is 0.0010 and its
five-window BH q-value is 0.0017. These values are materially selection-sensitive: clusters found
only from news average 1.15% at five sessions, while clusters also marked `price_first` average
6.55%. The test cannot repair post-outcome event-directory, direction, window, state or exposure
selection and therefore supplies neither a causal result nor a strategy/backtest admission claim.

An independently predeclared adversarial pass then challenged the five-session result. All 17
leave-one-cluster-out estimates remained positive at 2.53%–3.72%; excluding every dual
`price_first` cluster retained 1.15%, excluding the dense 2024 calendar retained 2.59%, and a
uniform geometric market-relative specification retained 1.85%. Every computable leave-one-family
and leave-one-year block also stayed positive. This narrows the specific concern that one cluster,
one family, one year or the price-first subset alone creates the direction. It does not narrow the
larger post-outcome directory and timing-selection problem. The adversarial analysis identity is
`historical-robustness-adversarial-ea65fbf2a744b7a2555c7a501c3c27d4057a2665ba26a4d3183ec55abe47d98f`.

A separate price-first discovery pass retained 20 candidate event windows after starting from
industry/index anomalies rather than named stories. Ten have tier-A, five tier-B and five tier-C
source/mechanism support; eleven are P0 candidates for subsequent source-version and PIT rebuilding.
The private content identity is
`sha256:6b2cf67e1fa034b4eade8890df030d396edd1cfe4c9d578ec37fcf2c1a047283`.
The package preserves official URLs, aggregate reactions, confounders and PIT gaps but deliberately
omits licensed market rows, so its exact price calculations cannot be reproduced from that package
alone. It is a discovery pool, not a backtest corpus or causal ranking.

A follow-up two-route audit preserved that 20-candidate denominator. The routes assigned a broad
event family to 14 and 16 candidates; their intersection was 13 and union 17, but only ten had both
routes, a nearby date and first-party metadata. Those figures are not blind-recall estimates: the
source candidate IDs and event anchors exposed event names before external lookup, so the audit is
label-leakage-contaminated. Strict PIT remained 0/20. A valid repeat requires a pristine batch whose
human-readable anchors, families and URLs are sealed behind irreversible anonymous identities before
either discovery route starts, plus a frozen source index for comparable date-placebo opportunity
rates.

A separate leave-block-out transfer audit used 14 event-archetype folds and ten year folds. Across
17 events and 37 paths it formed 26 conditional hypotheses; after outcomes were opened, 18 retained
the predicted sign and eight were opposite or zero, while 48 insufficiently supported paths
abstained. All paths remain action-ineligible because point-in-time prior expectations and source
availability are incomplete. Hidden miss families were inspected only after the folds for error
analysis and were not converted into event-specific keyword rules.

A second independently predeclared transfer validation did not support promotion of those rules.
Across six support/consistency thresholds, only 17 block-plus-path fingerprints persisted. At the
default threshold it produced 24 conditional hypotheses, 18 same-sign outcomes, six opposite or
zero outcomes and 50 abstentions; in 499 within-cell outcome-sign permutations, both 24 hypotheses
and 18 same-sign outcomes fell inside their random-label reference intervals. The current rule set
therefore remains a hypothesis-generation and abstention aid, not demonstrated generalizable event
logic. The hidden cases stayed outside the population, folds, permutations and code constants.

An independent falsification pass then froze and recomputed the same 17-cluster/37-exposure
outcome-opened denominator. It reproduced the five-session signed mean of 3.38%, but classified the
directory as `NO_GO` for strategy, alpha, causality, tradability or backtest promotion. No member has
decision-time `available_at` or `authority_at`; the package also lacks a cutoff-valid security
universe, original executable prices separated from research adjustments, corporate actions,
trading status/limits, order-side liquidity, fees, slippage, capacity and turnover. Date jitter,
random-date anchors and leave-block-out results remain descriptive because the directory, direction,
date and window were opened after outcomes. The minimum scientifically interpretable next slice is
therefore one future pre-registered event through the existing Data Snapshot, Pre-event Universe,
Method Quality Market Snapshot, Outcome Seal and Outcome Opening sequence, with actual receipt and
raw execution inputs frozen before opening results.

A separate two-case Strict-PIT recovery probe reached the same boundary from source authority. The
2024-09-24 pre-open policy package failed because the verified official replay identities were later
than the 09:25 Asia/Shanghai cutoff. For the 2021-10-19 NDRC coal-price intervention, one official
event source passed with a 2021-10-19T13:55:12Z immutable replay, but the complete mechanism still
failed because the market series, coal-industry series and then-effective taxonomy lacked
cutoff-valid authority. Both cases may remain content-identified retrospective context; neither is
admitted to Strict-PIT, and this probe did not create a separate Modeled-PIT availability policy.

These artifacts may refine general event structure, transmission, countercase and abstention Skills.
They may not enter strict historical inputs, retroactively grant authority, tune an opened cohort
and then reuse it as unseen evidence, or authorize prospective Judgment, paper or live execution.
