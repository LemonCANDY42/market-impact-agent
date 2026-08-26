# Method Quality Benchmark

## Status and claim boundary

The v2 method-quality protocol supersedes v1 before a retrospective holdout corpus or any outcome
opening. Review found that v1 incorrectly treated five stochastic Agent replicates of one Event
Case as five independent market observations. That case-replicate pseudoreplication would have
inflated the nominal sample from 24 to 120 and narrowed uncertainty without adding an event. v1 is
retained as negative audit evidence and is ineligible for any claim.

v2 keeps eight development cases, 24 identity-masked retrospective holdout cases, five replicates,
two comparison suites, an all-event denominator, and the existing cost/risk gates. Its executable
clustered estimator first averages the five registered runs within each case and arm, then performs
paired inference across 24 independent cases (eight for the family diagnostic). The committed
synthetic case validates provenance, masking, and contract behavior only; it is not a holdout case
and contains no market outcome.

This benchmark may establish whether one frozen research method adds repeatable value relative to
another under the registered inputs. It cannot establish uncontaminated historical alpha by itself,
rewrite the existing prospective physical-energy study, or grant paper, account, order, or live
execution capability.

Public artifacts:

- `examples/calibration/method-quality-benchmark-v2.json`, active registration hash
  `b7c83c9c67fb3604d9daa20a49f8ed9f080549afafacdf0d7817b2f8b7472ee3`;
- `examples/calibration/method-quality-evaluation-specification-v2.json`, active specification
  hash `8ab6d1a187a8e1e3ae72b138ff1cf3bf8e4a4e3bbdad4f64cfb25318d4c7ec1b`;
- `examples/calibration/method-quality-benchmark-v1.json` and
  `method-quality-evaluation-specification-v1.json`, retired pre-outcome audit artifacts;
- `examples/research/research-method-catalog-v2.json`, catalog hash
  `2bea3e78b3d8af253a0d0baea5274a0b082501de551c6e7c7648bff1be6d31e0`;
- `examples/research/synthetic-energy-historical-evidence-v1.json`, development provenance
  hash `233dfcb0cf1b7aa8ab9804f88766786bcabfbe5a5c4edfed0d2ac5ab32c94043`;
- `examples/research/synthetic-energy-masked-input-manifest-v1.json`, masked-input hash
  `5ff211fbb265863c710642f5973772752ae2ba506a277ca84ec51a78c43d939e`.

## Research absorbed into Agent-usable form

The implementation reviewed primary papers and official repositories, but did not import a
third-party trading framework or copy its prompts.

| Source | Useful idea | Form used here | Deliberately not inherited |
| --- | --- | --- | --- |
| [TradingAgents](https://github.com/TauricResearch/TradingAgents) and its [paper](https://arxiv.org/abs/2412.20138) | Fundamental, news, market, bull/bear, risk, and structured-manager responsibilities | Persona-free Research Method Skills and explicit countercase requirements | Multi-Agent authority, debate as evidence, and automatic outcome reflection inside a holdout |
| [Vibe-Trading](https://github.com/HKUDS/Vibe-Trading) | On-demand financial Skills, research-discipline checks, event/macro/factor tools | Content-identified Skill catalog and the new `research-discipline` treatment | Fixed sentiment weights, one universal event decay, mutable self-evolving Skills, and broker reachability |
| [FinMem](https://arxiv.org/abs/2311.13743) and its [code](https://github.com/pipiku915/FinMem-LLM-StockTrading) | Time-layered memory and compact outcome reflection | Train-only lessons compiled into pre-cutoff Pattern Packs | Cross-case mutable memory or outcome-derived lessons during evaluation |
| [StockBench](https://arxiv.org/abs/2510.02209) and its [project](https://stockbench.github.io/) | Sequential market inputs, repeated model runs, return and drawdown reporting | Repeated-run noise, cost-aware outcome metrics, and simple baselines | Treating a short return leaderboard as proof of transferable reasoning |
| [KTD-Fin](https://arxiv.org/abs/2605.28359) | Consistent identifier/date masking and return-source attribution | Prompt/tool aliases, preserved economic roles and source tiers, benchmark-adjusted return | Letting a model identify a memorized historical event from ticker and calendar clues; unfrozen style attribution |
| [CLQT](https://arxiv.org/abs/2606.29771) | Hard time gate, cost-aware diagnosis, and recomputable decision records | Zero-tolerance time gate, Usage Ledger, content bindings, process and outcome scorecard | A new execution loop or role committee |
| [Agent Market Arena](https://github.com/The-FinAI/Agent_Market_Arena) and its [paper](https://arxiv.org/abs/2510.11695) | Same-market comparison and prospective live evaluation | Existing prospective holdout remains final promotion evidence | Remote agent endpoints, paper accounts, or execution in this phase |
| [Hurlbert on pseudoreplication](https://esajournals.onlinelibrary.wiley.com/doi/10.2307/1942661), [Harvey, Liu, and Zhu](https://academic.oup.com/rfs/article-abstract/29/1/5/1843824), and the [backtest-overfitting literature](https://academic.oup.com/jrssig/article/18/6/22/7038278) | Independent experimental units, preselection, higher evidence hurdles, and explicit control of repeated method search | Event Case as the statistical unit, one primary promotion contrast, no best-observed-arm selection, and prospective confirmation for secondary findings | Treating repeated Agent samples, a favorable retrospective arm, or a small family slice as independent discovery evidence |

TradingAgents and StockBench are Apache-2.0; Vibe-Trading and FinMem are MIT. No third-party code
was copied, so the repository's Apache-2.0 licensing and current attribution boundary do not change.

## Evidence ladder

1. **Development**: eight synthetic or already opened cases exercise success, contradiction,
   missingness, alias consistency, revision, abstention, and cost paths. Results may guide code and
   prompt repair but make no method-quality claim.
2. **Retrospective holdout**: v2 still cannot admit any case. The Common Crawl adapter now
   authenticates one immutable archive capture, but the generic WARC record does not authenticate
   the publisher's original `published_at`. Admission remains fail-closed until a source-specific
   publication-time extractor and frozen latency calibration are accepted. Recomputing receipt,
   calibration, or manifest hashes cannot cross this boundary.
3. **Prospective holdout**: future actual-receipt evidence remains the strongest gate. Historical
   findings may motivate a new preregistration but cannot retune the already frozen physical-energy
   study in `docs/PHASE2_AGENT_PREREGISTRATION.md`.

The Historical Evidence Manifest is separate from the Evidence Pack. Each evidence version embeds
a content-identified Source Version Receipt binding the exact `source_ref`, Provider/archive and
immutable archive version, source-version identity, raw/extracted hashes, publication,
source-update, retrieval, availability, basis, and trust status. Modeled availability additionally
binds a content-identified Latency Calibration and must equal publication plus its frozen offset.
The runtime compares the Receipt to the Evidence Reference and rejects mismatched source, content,
or time. `synthetic_contract_only` and `contract_validated_untrusted` prove only internal contract
validity and ordered chronology; arbitrary ordered metadata is not authenticated no-lookahead
proof. The current CLI reports the synthetic fixture as non-authenticated, and retrospective
holdout construction remains fail-closed until source-specific publisher-time and latency gates
pass on top of the archive-capture authority.

The Agent receives the separate masked Evidence Pack, masked evidence documents, and masked
Pattern Pack documents, not their originals, hidden outcome metadata, or an external search tool.
The Masked Agent Input Manifest content-binds both Evidence Packs, both evidence-document sets,
both Pattern Pack bundles, and the one-to-one alias map. Validation compares the complete canonical
alias transform, including `data_gaps` and all nested fields, while recomputing content-derived
document hashes and pack identities. A forbidden-token scan covers the prompt, `read_evidence`, and
`read_pattern_pack` surfaces.

## Frozen cases and suites

The 24 retrospective cases allocate eight physical-supply/logistics, four issuer-corporate, and
three each macro-real-economy, policy-regulatory, geopolitical-security, and financial-market-
mechanics cases. Every stratum includes at least one registered case where missing or offsetting
evidence requires abstention. Climate and cumulative technology narratives require different
rolling-state labels and are excluded from v2 rather than forced into a single-event score.

The `general_methods` suite compares neutral evidence, general methods, and general methods plus a
Pattern Pack on all 24 cases. The `family_increment` suite adds the physical-energy family method
only on the eight matching cases. This prevents a family Skill from being penalized or silently
substituted outside its declared mechanism.

Research Method Catalog v2 adds `research-discipline` after neutral evidence and before event,
exposure, and adversarial analysis. It checks target/source coverage, narrative labels, recency,
historical identity leakage, and outcome-memory isolation without preferring a direction. Catalog
v1 and its completed Method Ablation Registration remain unchanged. The benchmark registration
also freezes the seven actual suite/arm routes: the cross-mechanism suite uses one explicit generic
context and the family suite uses `physical_energy_supply_shock`. Each route binds requested and
loaded Skill names, manifest hashes, instruction hashes, capabilities, tools, and route identity;
validation recomputes them from the registered catalog and local Skill Registry.

## Outcome opening and research scoring

All arms use the same masked Evidence Pack, aliases, target universe, Provider Profile, model,
output contract, horizons, scoring assumptions, and per-run budget. Runs are interleaved by
case, then replicate, then arm. Failures and abstentions remain in the all-event denominator.
Before the first run, every case must bind a content-identified Market Snapshot and Outcome Seal.
The Market Snapshot binds the specification identity, source vintage, case `as_of`, cutoff session,
calendar sessions,
corporate actions, adjusted instrument and benchmark rows, exact decimal encodings, source-version
ids, fee rows, and venue rules. The Seal contains no outcome payload and binds the registration,
specification, snapshot, case archetype, and the exact case-replicate-arm run and Evidence Pack
matrix.

The active registration content-binds the schema-validated v2 Evaluation Specification rather than
symbolic procedure names. The public Market Snapshot, Outcome Seal, and Outcome Opening schemas
and strict loaders enforce exact types, cardinality, uniqueness, IDs/hashes, nullable fill states,
and decimal strings. Opening repeats all seal bindings; binds every expected Judgment Artifact id,
content hash, run id, Evidence Pack, case, replicate, and arm; and rejects partial or extra
matrices. Result validation consumes the exact bound Evaluation Specification. It derives the
first eligible post-cutoff entry within three sessions, trade/missing/suspension status, price ticks
and limits, the exact-horizon exit, exactly effective non-overlapping fee and venue rules, and the
largest affordable board-lot quantity after entry cost proxies. It then recomputes reference
values, per-component rounded round-trip cost proxies, the directional score, benchmark move, and
benchmark-adjusted directional score. Abstain, mixed, unknown, no-fill, and missing-market-data
states contribute zero while every registered judgment remains in the denominator.

This is deliberately not an executable cash portfolio. `down` changes only the direction
multiplier used by the research score; it does not represent a short, borrow, order, executable
position, or investable return. The scorer does not invoke NautilusTrader. Nautilus remains the
replaceable engine boundary for later backtest and trading workflows after their separate gates.

One case-replicate-arm value is the arithmetic mean of every exact candidate target and declared
horizon in its Judgment Artifact. Selecting a favorable target/horizon or reweighting rows is
invalid. v2 then averages all five replicate values inside each case and arm. Only the resulting
Event Case means enter inference: `difference = candidate case mean - comparator case mean`, sample
variance uses `case_count - 1`, and the two-sided 95% lower bound uses the registered Student-t
constant. General methods use 24 independent cases (`df=23`, `t=2.069`); the family diagnostic uses
eight (`df=7`, `t=2.365`). The five replicates measure stochastic stability and never change degrees
of freedom. Missing any case-replicate-arm cell makes the estimate inconclusive; pair deletion is
forbidden.

The only promotion-eligible contrast is frozen in advance as `general_methods - neutral_evidence`.
`general_pattern - general_methods` and `family_guided - general_pattern` are diagnostics. A
favorable secondary result may motivate a new prospective preregistration but cannot promote a
method, and no best-observed-arm selection is allowed. The clustered estimator is implemented and
schema validated, but it cannot produce benchmark evidence until the future content-identified
case and run matrix is bound by Outcome Seals/Openings.

Only the specification and synthetic masking/provenance fixtures exist. The strict evaluation
schemas and validators have unit-level synthetic contract examples, not committed market outcome
fixtures. The development and holdout corpus, per-case Market Snapshots, Outcome Seals, Outcome
Openings, result artifacts, pair set, and all benchmark runs remain unbuilt; no outcome has been
opened.
The opening contract requires sequence one and no prior opening, but this repository implements
only artifact/validator-boundary enforcement, not a transactional global uniqueness store.

The registration freezes intended future overall gates, but the overall promotion evaluator is not
implemented. A future evaluator must combine time-gate integrity, required-abstention recall,
registered baselines, positive strata, concentration, directional-score drawdown, CVaR, and cost
with the future paired interval. No overall promotion claim is made; an unavailable or inconclusive
paired computation does not permit selection by the most attractive point estimate.

Style attribution is explicitly deferred diagnostic work and is neither a registered promotion
metric nor a promotion dependency. The v2 contract does not claim a style-adjusted result because
the investable universe, lag convention, breakpoints, portfolio weights, rebalance timing,
missingness, and regression inputs are not frozen. The benchmark-adjusted directional research
score remains the registered machine-validated result and is not investable PnL.

## Next workslice

1. independently review the accepted Common Crawl capture adapter and add a source-specific
   publisher-time extractor plus a frozen latency calibration; never substitute archive capture or
   current local retrieval time for the publisher's historical publication time;
2. extend the validated masked-input materialization from the committed synthetic fixture to each
   future development and holdout case, including its market snapshot;
3. create the remaining seven development cases and pass all negative paths without opening a claim;
4. define outcome-independent case admission queries and accept the source-specific receipt path;
5. only after that authority exists, freeze all 24 authenticated Historical Evidence Manifests,
   Market Snapshots, and Outcome Seals before any method run;
6. execute the registered suites, bind Judgment Artifacts, create sequence-one Outcome Openings,
   run the deterministic research evaluation, and publish one acceptance or rejection report.

Validate the frozen protocol and the first development provenance contract with:

```bash
uv run market-impact agent method-benchmark-validate \
  --registration examples/calibration/method-quality-benchmark-v2.json \
  --method-catalog examples/research/research-method-catalog-v2.json \
  --provider-profile examples/providers/minimax-m3-research-v1.json \
  --evaluation-specification examples/calibration/method-quality-evaluation-specification-v2.json \
  --historical-manifest examples/research/synthetic-energy-historical-evidence-v1.json \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --masked-input-manifest examples/research/synthetic-energy-masked-input-manifest-v1.json \
  --masked-evidence-pack examples/agent/energy_supply/masked-evidence-pack.json \
  --masked-evidence-documents examples/agent/energy_supply/masked-evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json \
  --masked-pattern-pack examples/agent/energy_supply/masked-pattern-pack.json \
  --skill-root skills
```
