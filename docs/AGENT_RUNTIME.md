# Agent Runtime Acceptance Boundary

The historical-readiness v2 entry composes the existing runtime as two independent analysts per
arm plus one conditional Judge on decision disagreement. It does not add a debate runtime, Agent
execution capability or a new Judgment schema. The Judge uses an independently frozen accepted
Provider Profile, receives reopened same-arm Judgment proposals and the original frozen source
tools, and emits the ordinary JudgmentProposal. Its actual input/binding is persisted before
dispatch and all usage is charged to the same case. See the historical-readiness v2 section in
`AGENT_EFFECTIVENESS_ACCEPTANCE.md` for disagreement, scope, failure and evidence-claim rules.

## Status

The bounded research-only Agent runtime passed its current local runtime gate. It includes
redirect denial for credential-bearing requests, same-connection MCP snapshot revalidation,
terminal-artifact/run/journal binding, exact tool execution-limit identity,
optional-dependency isolation, and explicit compactor identity. Fresh private MiniMax M3 and
CLIProxyAPI `gpt-5.6-luna` xhigh runs both completed against the same full synthetic Agent surface.
This establishes bounded Model Provider substitution through the tool/Judgment/audit chain; it is
not model quality, event-family quality, alpha, paper, or live acceptance. No model, Skill, MCP
server, or tool receives broker credentials or a path around hard policy.

The machine-local environment names are:

- `MINIMAX_API_KEY`: secret, loaded from the user's Keychain-backed environment;
- `MINIMAX_BASE_URL=https://api.minimaxi.com`: China API origin; the adapter appends the
  versioned API path;
- `MINIMAX_MODEL=MiniMax-M3`: explicit model identity with no silent substitution.
- `MARKET_IMPACT_CLIPROXY_API_KEY`: a dedicated local project credential injected only into the
  experiment process;
- CLIProxyAPI Profile values are fixed to `http://127.0.0.1:8317` and `gpt-5.6-luna`;
  `reasoning_effort=xhigh` and `reasoning_effort=max` are accepted, while alternate origins,
  models, or effort values are rejected.

On the accepted local host, private model values live in
`~/.config/market-impact-agent/model.env` with mode `0600`. The
`market-impact-model` launcher parses only the allowlisted names above, exports them to one child
process, and starts the project CLI. It does not source executable shell syntax and does not expose
the model environment to the prospective collector. Callers therefore do not need to retrieve or
inject a key for every run.

Model identity, endpoint, reasoning effort, sampling, context limits, retry policy, pricing, and
budgets remain in a content-identified Model Provider Profile. These are not secret setup burden:
the Harness selects the registered profile and freezes the effective profile in each execution
plan and Run record. An environment value may supply a credential or assert the expected MiniMax
origin/model, but it may not silently override the frozen profile. This keeps the ordinary command
simple without making old experiments unreproducible.

New Luna epochs select the CPA `cliproxyapi-luna-max-cpa-v1.json` Profile; it
has a distinct content-derived identity from the existing CPA xhigh Profile. Its 300,000
micro-USD per-run estimated-cost cap is 1.5 times the CPA xhigh Profile's 200,000 cap, while its
turn, tool, token, wall-time, retry, and pricing values remain the same. A future newly registered
experiment may use a 1.5-times-prior aggregate cap. Active and frozen xhigh cohorts, including
their registrations and pending callbacks, retain their old limits and identity and must not be
relabeled. The [official Luna model documentation](https://developers.openai.com/api/docs/models/gpt-5.6-luna)
lists `max` as a supported effort. A separate synthetic local CPA canary completed on 2026-09-02
through this max Profile: two physical requests, five frozen read-only tool calls, 6,614 input /
6,634 output Tokens and 9,284 microusd estimated cost. Its terminal artifact is
`65158865b3b288f29fa38bd2501e94712ba5bd9f4b22d7fbb6c57775f2056dea`.
Post-run Usage reconciliation reopened the authoritative terminal and Journal; the single-record
ledger hash is `1c1ce5b0dc4fef310605368257b4f3d5faa39bf7e9d782e1bbdfa3eb7ce084e9`.
This verifies a completed configured-max request/tool/Judgment path, not upstream reasoning
internals, max-versus-xhigh quality, real-market correctness or execution readiness. It is separate
from the three historical xhigh pilots and grants no new trading authority.

`MARKET_IMPACT_MODEL_MAX_CONCURRENT_REQUESTS` is the sole non-secret execution default in the
machine-local model environment. It defaults to `3`, is restricted to `1..8`, and is copied into a
new Triage Work Execution Plan before any request is dispatched. Changing the environment therefore
does not reinterpret an active or historical Plan. The ceiling applies only to independent members
inside one phase: all map roles finish before partition, and partition finishes before classify.
Independent members run in deterministic waves of at most the ceiling. The Harness waits for the
admitted wave, records each exact Run/Usage identity, appends the hash-chained Usage Ledger serially
(including successful peers when a sibling raises), and starts no later wave or phase after a
terminal failure. Ambiguity takes precedence in the aggregate status. It neither
retries an ambiguous dispatch nor grants additional budget or authority. Legacy v2-v11 Plans remain
serial. Separate collector concurrency is unrelated and cannot dispatch model calls.
The environment default is resolved only for the selected Triage command, and an explicit valid CLI
value takes precedence; a malformed value does not break unrelated commands or help.

MiniMax's official [M3 model page](https://www.minimax.io/models/text/m3),
[OpenAI-compatible API guide](https://platform.minimax.io/docs/api-reference/text-openai-api),
[China/international endpoint guide](https://platform.minimax.io/docs/token-plan/cursor), and
[pricing page](https://platform.minimax.io/subscribe/token-plan?tab=api-enterprise) are the
Provider references. The API key never enters prompts, run artifacts, model history, tool
arguments, logs, or committed configuration. The runtime records a versioned cost estimate;
it is not an invoice and currently does not subtract automatic prompt-cache discounts.
The CLIProxyAPI credential remains only in trusted mode-`0600` machine configuration and is not
copied to the repository or global shell configuration. Codex OAuth is included usage with no asserted
USD/token rate, so the Usage Ledger records Token and latency budgets while its marginal estimated
USD cost is zero; that zero must not be read as unlimited quota or no resource consumption.

## Smallest complete runtime

The Harness remains the single orchestration owner. An accepted runtime needs each boundary
below; copying an entire coding-agent product is neither required nor sufficient.

### Model Provider

- one typed Provider adapter with explicit origin, model, API/version, streaming mode,
  timeout, retry, and maximum-output settings;
- lossless preservation of assistant messages, tool calls/results, and Provider response
  metadata required for a continued tool-calling turn;
- normalized usage, latency, finish reason, error class, and request/response identities;
- declared capability checks for tools, streaming, context size, and structured output;
- no silent model or endpoint fallback; an unavailable configured model fails closed;
- credential-bearing requests reject every HTTP redirect and recheck the exact adapter-specific
  pinned origin at the network boundary, including the MiniMax China origin and CLIProxy loopback;
- secret injection at process/request boundary only, plus redacted errors and traces.

### Run lifecycle and durable state

- stable run, turn, message, tool-call, and artifact identities;
- append-only event journal plus content-identified checkpoints;
- crash-safe resume, cancellation, bounded retries, and idempotent tool-result replay;
- explicit terminal states for completed, failed, cancelled, budget-exhausted, and
  human-input-required runs;
- wall-time, token, estimated-cost, tool-call, and recursion budgets enforced by the Harness;
- read-only result replay after restart; mutating-tool duplicate-side-effect acceptance is
  outside the current surface;
- terminal replay revalidates the journal chain and binds the stored artifact to the exact run,
  journal tail, terminal status, and run-row timestamps. A completed Judgment also has to match the
  `judgment.validated` proposal, transcript, and metrics hashes plus the final model-turn assistant
  payload and raw-response artifact; a separately rewritten canonical terminal object fails closed.
  Every returned `ModelTurn.model` and replayed Judgment Provider/model identity must exactly match
  the active frozen Provider Profile, including for replaceable custom Provider implementations.

### Context and automatic compaction

- an inspectable context ledger, not only one mutable prompt string;
- deterministic inclusion priorities for system/policy, current mandate, task state, recent
  turns, unresolved tool calls, pinned evidence, and referenced artifacts;
- conservative complete-request estimation, identified in the run artifact, against the
  Provider context bound and reserved output budget; no exact MiniMax tokenizer is claimed;
- automatic compaction before overflow, with source-message ranges, compactor identity,
  summary hash, and retained facts/decisions/unknowns;
- compactor identity participates in the run specification and Judgment Artifact even when no
  compaction occurs;
- large tool output stored as a hash-bound artifact and replaced by a bounded typed summary;
- no silent loss of policy, approvals, invalidation conditions, open tool calls, or user
  corrections;
- post-compaction continuation tests that reach the same decision and tool arguments as the
  uncompacted control where the retained evidence is equivalent.

### Skills

- manifest-based discovery with name, version, source, content hash, scope, dependencies,
  conflicts, and required capabilities;
- on-demand instruction loading; a Skill not selected for a turn does not consume context;
- deterministic precedence among Harness policy, project guidance, user instruction, and
  Skill instruction;
- install/update/remove as separate user-authorized operations with provenance and rollback;
- per-Skill tool/MCP allowlists, filesystem/network scope, and secret declarations;
- validation for broken references, conflicting names, cycles, oversized assets, and stale
  cached versions;
- invocation and loaded-resource hashes in the run trace; a Skill grants no execution
  authority by itself.

`JudgmentSkillTrace.v1` now defines the versioned sidecar for the fuller chain: offered or
dependency-only, selected/rejected/dependency-loaded, exact manifest identity, route reason,
evidence trigger, Agent-reported use, and reportedly influenced proposal paths. The Harness checks
the Judgment, route, execution binding, Evidence Pack references, and proposal paths. The report is
observational self-attribution, not causal Skill evidence, and cannot affect a Signal or execution.
Automatic emission by each model adapter remains open; historical Judgment Artifact v2 identities
are not rewritten.

Outcome-opened multi-case Skill discovery is separately specified in
`docs/SKILL_GOVERNANCE.md`. It may use bounded specialist decomposition, but specialists sharing one
case remain one evaluation unit. A non-executable candidate requires two additional independent
validation blocks and complete catalog conflict governance before any later active-Skill gate.

### MCP and tools

- versioned MCP configuration with server identity, transport, command/origin, environment
  mapping, working directory, enabled state, and per-server permission envelope;
- initialization and capability negotiation before tool discovery;
- every tool handler is constructed from an exact verified server snapshot and, on each fresh
  connection, re-lists and rehashes identity, protocol, tool surface, and schemas immediately
  before invoking the handler;
- closed tool schemas, stable tool identities, argument/result size limits, timeouts,
  cancellation, bounded retry policy, and structured errors;
- managed local-server process lifecycle and health, with no orphaned privileged process;
- tool outputs treated as untrusted data, separated from instructions and checked for prompt
  injection before later use;
- side-effect classification: read-only, reversible local write, destructive local write,
  external mutation, or execution-sensitive;
- explicit human approval for authority-expanding or destructive calls;
- artifact indirection for binary/large results, secret redaction, and complete audit links;
- research acceptance configuration exposes no account, order, paper, or live capability.

### Policy, security, and human control

- Harness hard policy is evaluated before and after model planning where relevant;
- a model may propose Signal or Order Intent but cannot edit a Trading Mandate, mint approval,
  access raw broker credentials, or call an execution Provider directly;
- sandbox and filesystem/network scopes are least-privilege and explicit per tool/server;
- untrusted evidence, retrieved text, tool output, and model-authored instructions retain
  provenance labels;
- prompt-injection, secret-exfiltration, confused-deputy, excessive-agency, and dependency
  substitution cases are mandatory negative tests;
- a kill/cancel control stops the runtime even when a model or tool is unresponsive.

### Observability, evaluation, and portability

- structured traces link every output claim to messages, evidence, tool calls, configuration,
  model identity, Skills, MCP servers, and compaction checkpoints;
- local inspection and an append-only Usage Ledger report tokens, estimated cost, latency,
  retries, context pressure, and terminal state for successful and failed runs without
  exposing secrets or licensed payloads;
- multi-experiment cost reports reconcile every supplied Usage Ledger by exact Run ID, reject
  conflicting duplicate payloads, and bind the content hash, status counts, unique-run count, and
  full estimated-cost union instead of accepting a caller-supplied historical total;
- deterministic fixtures cover ordinary turns, multi-tool turns, tool errors, retry/resume,
  cancellation, compaction, Skill activation, MCP failure, malformed output, and injection;
- one locked MiniMax M3 acceptance corpus is run without broker reachability;
- a second Provider adapter must pass the same engine-neutral acceptance before portability
  is claimed; differences are explicit rather than hidden behind fallback;
- model-quality evaluation and trading-research calibration are distinct gates.

TradingAgents, FinceptTerminal, daily_stock_analysis, Vibe-Trading, QuantDinger,
TradingAgents-CN, last30days-skill, Codex, OpenCode, and Pi were reviewed as prior art. No
whole project became a runtime dependency or vendored core. The implementation cleanly uses
only narrow patterns: typed tools and scopes, append-only recovery, capability manifests,
secret redaction, explicit process cancellation, and content-identified compaction. The
official MCP Python SDK is the only adopted Agent-protocol dependency.

## Acceptance gate

The runtime requires all of the following current-identity evidence:

1. [x] The no-secret deterministic fixture and a fresh hardened MiniMax M3 run finish under
   the current identity.
2. [x] Forced compaction preserves policy, task state, citations, and unresolved tool
   continuity and reaches the same proposal as its uncompacted control.
3. [x] A Skill is discovered, loaded only when selected, traced by hash, and denied an
   undeclared capability;
4. [x] An MCP server completes negotiation, schema-bound discovery, read-only success,
   timeout, cancellation, crash, and restart cases;
5. [x] Checkpoint/resume reproduces the same terminal artifact identity for a read-only run
   and does not repeat its tool handler;
6. [x] Injection, undeclared authority, malformed output, and secret-exfiltration fixtures
   fail closed;
7. [x] Token, estimated cost, latency, retries, contract corrections, and every tool call are
   auditable; and
8. [x] No paper/live/account capability is reachable in the acceptance configuration.

The accepted run `acceptance-minimax-m3-energy-v4-final` used exact model `MiniMax-M3`,
loaded the content-identified `energy-supply@1.0.0` Skill, called one frozen Pattern Pack tool
and all four frozen Evidence Pack items. Its sealed v2 Judgment Artifact identity was
`judgment-219d0c9822e7e5794e51af4b65dd41e04cd9162ad6f561695f6705c7f79a53c4`.
The run used 20,581 input tokens, 4,353 output tokens, four Provider calls, five read-only tool
calls, and 35.44 seconds of recorded Provider latency. Its 12 journal events revalidated as a
complete hash chain; the terminal journal hash matched the Judgment Artifact, the Artifact
matched the run identity and start/finish timestamps, passed its JSON Schema and typed parser,
a direct private-state scan found no API key, and no broker/order/paper/live event was present.
Reopening the same terminal run returned the same artifact with 12 unchanged events and no
replayed tool calls. Current runtime code reconstructs the stored run metrics from those
journal events rather than spending again. The estimated cost was 11,400 micro-USD under the versioned
price assumption. Private state remains under the ignored
`.market-impact/agent-runs/` directory.

The earlier single-run v2 and v3 runs remain historical evidence only. Neither substitutes
for the v4 artifact identity or recovery rules.

## Five-replicate ensemble acceptance

The Harness now freezes one execution-binding artifact before replicate one. It covers the
runtime configuration, initial prompt, selected Skill hashes, tool manifests and model tool
surface, MCP bindings, context estimator, and compactor. Five independent runs then use
separate journals and artifact stores with no shared model context. A content-identified
Ensemble Decision counts only one uniquely eligible candidate per valid replicate and requires
exact three-of-five agreement on target, direction, and horizon. Reused Artifacts or any
execution-binding drift force whole-ensemble abstention.

The first private real-model attempt,
`synthetic-minimax-m3-ensemble-20260826-v1`, is retained as negative runtime evidence. Two
replicates failed the closed output contract and the three valid votes split between one- and
three-session horizons, so the maximum agreement was two and the Ensemble Decision correctly
abstained. Inspection showed an ambiguous pseudo-contract: MiniMax copied a metadata rule as
an output field, overlapped supporting and counterevidence references, or omitted required
empty arrays. The output contract was changed to an explicit metadata wrapper with exact
required fields and disjoint support/counterevidence rules; the previous run was not overwritten.

The fresh normal run `synthetic-minimax-m3-ensemble-20260826-v2` then completed all five
replicates under frozen binding
`3ed71b35946fcbe170fa08683dee3da3a3e94aca9f8b70485305c62c0eb909c0`.
All five selected `600938.XSHG/up`; three selected one session and two selected three sessions.
The resulting exact three-of-five decision is
`agent-ensemble-a4b4cbdc8a740ca6ad01a4a948e3dacc452355d2cbbc0f7fcc2d428fbce984dc`.
It used 41,319 input tokens, 20,921 output tokens, 25 read-only tool calls, 11 Provider
attempts, and an estimated 37,506 micro-USD. The content-addressed decision artifact hash is
`26d8aee3af54e3702b8c223e37e68c4861d2fed2aa18a8e2dce67fec3331c5c4`.
Private state remains under ignored `.market-impact/agent-ensemble-runs/` and
`.market-impact/agent-ensemble-decisions/` directories.

This passes the synthetic-bundle ensemble runtime gate only. The selected target has no
matching frozen replay snapshot in this acceptance slice, and the event itself is synthetic;
therefore no return is reported and no model-quality, calibration, alpha, paper, or live claim
follows. That historical run used the earlier two-target synthetic Evidence Pack, which also
listed the non-selection-eligible `600028.XSHG` control. The study runner now rejects any
Evidence Pack target outside the frozen selection-eligible Exposure Registry before checking
the Provider or creating run state, and the committed fixture is narrowed to `600938.XSHG`.
The historical run remains runtime evidence; it is not relabeled as a prospective study event.

## Research Method Skills and four-arm ablation

The separate evidence-gated public-investor method catalog and its first three-pair Luna xhigh
diagnostic are documented in `docs/METHOD_SKILLS.md`. It adds five persona-free methods rather than
five analyst identities. The Abqaiq recovery comparison changed only the appended
`expectations-base-rates` Skill; both arms abstained 3/3 with complete evidence and Pattern Pack
coverage. CPA observed 12 successful Provider requests costing $0.02276136, while the conservative
project Usage Ledger recorded $0.030045. This is process and cost evidence only, not a method rank.
The v2 registration corrects the original v1 `model_call_count=6` label to six Agent runs, prices
input at the worst ordinary/cache rate, and records a separate Provider-request bound. It also
replaces caller-reported evidence labels with a content-identified declaration of exact
Evidence/Pattern refs. The original report remains immutable; a schema-validated correction and
redacted content-addressed CPA event artifact preserve the repaired semantics.

Stable work normally expressed as TradingAgents-style analyst, bull/bear, and risk roles or
Vibe-Trading-style event teams is represented here as persona-free Research Method Skills.
The committed catalog contains neutral point-in-time evidence discipline, event/market
context, public-equity transmission, adversarial countercase, reusable-pattern review, and
the existing physical-energy family method. A deterministic Skill Route selects only methods
applicable to the frozen asset class, mechanism family, and available Pattern Pack; the route,
reasons, manifests, tools, and capabilities are content identified before any model call.

The separate `news-evidence-assessment` Skill is an optional general evidence-quality method,
adapted from useful sample-size and source-disagreement checks in the external TradingAgents news
pipeline. It inventories admitted news, separates facts from opinions, checks source/claim
independence and timing, and reports only a qualitative coverage-assessment confidence. It depends
on `evidence-core`, permits only `read_evidence`, cannot mint Evidence, and cannot set
`CandidateImpact.confidence`, direction, weight, or execution. It is not inserted into the frozen
four-arm result after the fact; any comparison must use a new content-bound paired registration.

The first such private paired diagnostic used CLIProxyAPI `gpt-5.6-luna` xhigh and interleaved five
replicates per attack/recovery state. All 20 runs completed; both control and treatment abstained in
all ten state-replicates. Control used 70,535 input and 22,198 output Tokens; the optional Skill
used 79,115 input and 23,454 output Tokens, increases of 12.2% and 5.7%. The final Judgments did not
show a systematic new inventory of source count, lineage independence, fact/opinion mix, or
coverage confidence. This sparse opened-development case therefore gives no reason to load the
Skill by default. It remains available for a later genuinely multi-source News Observation Batch,
where its declared precondition can be exercised. The private report is
`private-news-ablation-report-8ad1284720331aa27ea05f85cfe9b03e72ba047c26f35f467504ab50bfea84cb`
under Usage Ledger hash `4d999354ee7a9d923f4ab989e7cec02ed7ae1558deafb1eeb04bad6fd57c7f95`.

The frozen comparison has four arms: neutral evidence; general methods; general methods plus
Pattern Pack review; and those layers plus the energy-family method. The Pattern-enabled arms
differ in both instructions and access: they receive `pattern.read`, `read_pattern_pack`, and the
frozen Pattern Pack content, while the other arms do not. All arms share the same base Evidence
Pack, model profile, action space, target universe, output contract, budget, and five replicates.
Runs are interleaved by replicate round. All four execution bindings are frozen before Provider
availability is checked, every terminal run enters a hash-chained Usage Ledger, and the report
explicitly makes no market-outcome or alpha inference.

This is a narrow adaptation, not copied role prompts or a vendored multi-agent framework.
Sources reviewed include TradingAgents' [market analyst](https://github.com/TauricResearch/TradingAgents/blob/main/tradingagents/agents/analysts/market_analyst.py),
[news analyst](https://github.com/TauricResearch/TradingAgents/blob/main/tradingagents/agents/analysts/news_analyst.py),
and [provider factory](https://github.com/TauricResearch/TradingAgents/blob/main/tradingagents/llm_clients/factory.py),
plus Vibe-Trading's pinned
[event task force](https://github.com/HKUDS/Vibe-Trading/blob/5cd08ee1bd5c28e856b20acae3d077ed9bd919ce/agent/src/swarm/presets/event_driven_task_force.yaml),
[event-driven Skill](https://github.com/HKUDS/Vibe-Trading/blob/5cd08ee1bd5c28e856b20acae3d077ed9bd919ce/agent/src/skills/event-driven/SKILL.md),
and [Provider registry](https://github.com/HKUDS/Vibe-Trading/blob/5cd08ee1bd5c28e856b20acae3d077ed9bd919ce/agent/src/providers/llm_providers.json).
Their outputs are prior art only; this Harness retains its own evidence, policy, and execution
authority boundaries.

Pinned TradingAgents `0.3.1` remains isolated outside the Harness. Its earlier strongly masked
MiniMax smoke is retained only as a negative input-isolation and structured-degradation diagnostic.
The current native-capability baseline instead supplies the real Abqaiq event and `601857.SH`
target, registered news and Tushare market data no later than each historical cutoff, and preserves
the project's native analyst, debate, risk, and model-prior methods on CLIProxyAPI Luna xhigh. It
disables only cross-run memory, the pending-decision outcome resolver, post-cutoff/live retrieval,
and broker reachability, and it rejects experiment-id reuse. This is an external behavior,
stability, and resource-use baseline, not a Harness runtime dependency or a causal method arm. The
deployment and news-source/post-processing findings are recorded in
`docs/TRADINGAGENTS_EXTERNAL_BASELINE.md`.

The native Luna xhigh comparison completed all ten interleaved runs with zero structured-output
degradation. Attack ratings were four `Hold` and one `Sell`; recovery ratings were three `Hold` and
two `Underweight`. All ten map to abstention in the Harness's one-sided long action space. The
external graph used 174 model calls, 903,651 input and 376,799 output Tokens, and 7,531.109
cumulative seconds. Inspected reports provided broad multi-role investment memoranda but also
generated precise levels and sentiment values from sparse inputs and drifted from one session to
weeks or months. This records native behavior rather than treating role count or report length as
quality evidence.

The Model Provider Profile is the single public model-entry contract. It binds adapter kind,
exact origin and model, credential environment reference, context/output limits, sampling,
optional reasoning effort, retry policy, pricing, and per-run budgets. MiniMax and CLIProxyAPI are
the first two concrete adapters. Both passed model discovery, exact-identity, text, function-tool,
redirect/origin, and full synthetic Agent checks through the same Factory and `AgentEngine`.
`agent run --provider-profile` is the uniform command entry; no Provider-specific runtime branch is
needed. Historical registered MiniMax experiments remain bound to their original Profile. Future
new Luna epochs use the distinct CPA max Profile, while explicit legacy/replay work may use xhigh;
existing frozen xhigh epochs retain their original identity. This proves bounded runtime
portability, not equal model behavior or equivalent cost semantics.

The first real-model comparison,
`synthetic-method-ablation-minimax-m3-20260826-v1`, completed all 20 runs and retained them
under one Usage Ledger hash
`88d40d90b556e5e1517f751405658461f652abb82af595f67ad5712da6e58487`. Every run read all
four Evidence Items; both Pattern-enabled arms read the referenced Pattern Pack in all five
replicates. All four arms produced exact three-of-five proposals for `600938.XSHG/up`, but
the selected horizon differed: neutral evidence and family-guided selected one session,
while general methods and general-plus-pattern selected three sessions. Every split was only
three-to-two. One general-plus-pattern replicate repeated one Evidence read, and one
family-guided candidate omitted an explicit counterevidence reference despite reading the
counterevidence item. These are process differences, not a quality ranking.

The comparison used 295,538 input tokens, 105,996 output tokens, 57 Provider attempts, 91
read-only tool calls, and an estimated 215,880 micro-USD in total. No run exceeded 18,029
micro-USD, so the per-run ceiling did not fire. Arm estimates were 42,168 micro-USD for
neutral evidence, 52,416 for general methods, 66,181 for general-plus-pattern, and 55,115 for
family-guided. The content-identified diagnostic report is
`method-ablation-report-2b59cdabd953f7e8550cde6384e828836fcc74b666b2642d538294647d6b6840`.
Reopening the same experiment regenerated diagnostics from the stored journals without any
new model turn or Usage Ledger row.

## Opened real-event development run

The first real outcome-opened development case uses the 2019 Abqaiq–Khurais attack and recovery.
`examples/calibration/method-development-abqaiq-v1.json` content-binds the active v2
benchmark/specification, Provider/model, catalog, strongly masked evidence, posthoc Pattern Pack,
one target alias, and two one-session `601857.XSHG` Backtest Requests. Its schema and strict loader
make `outcomes_known_to_builder=true`, `inference_eligible=false`, one Event Case, and no execution
capability mandatory.

Agent-visible evidence now coarsens quantities, facility and issuer names, restoration and shipment
details, and shifts calendar dates away from the historical fingerprint. It preserves only the
decision-relevant relative sequence and lag. This reduces easy linkage but does not authenticate a
holdout; residual narrative linkage, model memorization, and target-role inference remain risks.

The date-shifted replacement completed all 40 required runs. Attack-state proposal counts across
`neutral_evidence`, `general_methods`, `general_pattern`, and `family_guided` were 1/5, 0/5, 1/5,
and 0/5; all recovery-state counts were 0/5. Every three-of-five ensemble abstained. Both frozen
Backtest Requests passed joint preflight before either outcome opened, and both one-session
Nautilus replays repeated with identical result hashes. The fixed-long control was net negative in
both states. Total Provider cost was 397,066 micro-USD. All earlier private reports, costs, replay
results, and evaluations remain invalid.

This accepts runtime binding, fail-closed completeness, deterministic replay, and one
evidence-update diagnostic. One opened Event Case with no ensemble-level arm difference cannot
rank methods or establish alpha, prospective validity, Provider portability, or execution
readiness.

A state can produce a
method report only after all four interleaved arms have five completed runs with valid judgments;
failed or budget-exhausted attempts are still recorded in the append-only Usage Ledger. The
evaluator jointly preflights both normalized reports and both Backtest Requests, including exact
arm route, execution binding, ensemble, replicate, and totals identities, before opening either
outcome. Full design and non-claims are in
`docs/ABQAIQ_DEVELOPMENT_BENCHMARK.md`.

Run one state with a fresh experiment id:

```bash
uv run market-impact agent method-development-run \
  --case examples/calibration/method-development-abqaiq-v1.json \
  --benchmark-registration examples/calibration/method-quality-benchmark-v2.json \
  --evaluation-specification examples/calibration/method-quality-evaluation-specification-v2.json \
  --method-catalog examples/research/research-method-catalog-v2.json \
  --provider-profile examples/providers/minimax-m3-research-v1.json \
  --state attack \
  --evidence-pack examples/agent/abqaiq_development/evidence-pack-attack.json \
  --evidence-documents examples/agent/abqaiq_development/evidence-documents-attack.json \
  --pattern-pack examples/agent/abqaiq_development/pattern-pack.json \
  --backtest-request examples/backtests/real-abqaiq-601857-attack-state-request-v1.json \
  --experiment-id YOUR_UNIQUE_OPENED_DEVELOPMENT_ID
```

Save each successful command's complete JSON output as the corresponding private method-report
input. The evaluator accepts the exact runner-added `report_artifact_hash` and `state_directory`
fields only when the artifact hash matches the canonical stored report; arbitrary additional keys
or tampering fail closed. Outcome evaluation is a separate command and also requires two ignored
Tushare snapshot paths. It rejects incomplete runs or mismatched case/report/request/decision
bindings and reruns each replay twice before writing the private evaluation artifact.

This one synthetic Evidence Pack has one eligible target and a long-only action space, so it
cannot test target selection, direction, abstention quality, returns, or causal correctness.
The next method-quality gate needs a frozen multi-case corpus containing supported positives,
offset-dominant negatives, missing-critical-data abstentions, ambiguous targets, and several
event families before the arms are used on future real-event outcomes.

Run the frozen local comparison with a new immutable experiment identifier:

```bash
uv run market-impact agent method-ablation-run \
  --ablation-registration examples/calibration/agent-method-ablation-v1.json \
  --parent-registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --method-catalog examples/research/research-method-catalog-v1.json \
  --provider-profile examples/providers/minimax-m3-research-v1.json \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json \
  --experiment-id YOUR_UNIQUE_METHOD_ABLATION_ID
```

Validate the committed synthetic bundle without any model credential:

```bash
uv run market-impact agent validate \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json
```

Run a new private real-model judgment only after the selected Profile's credential environment is
present. The historical default remains the frozen MiniMax Profile. For a future new Luna epoch,
select and freeze the distinct CPA max Profile; do not alter an active or frozen xhigh binding.
The following historical example keeps using xhigh:

```bash
uv run market-impact agent run \
  --provider-profile examples/providers/cliproxyapi-luna-xhigh-v1.json \
  --run-id YOUR_UNIQUE_RUN_ID \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json
```

Each `run_id` is immutable: use a new ID for a new model-quality replicate. Re-running a
terminal ID returns its stored result rather than spending tokens or duplicating tool calls.

Run the registered five-replicate synthetic acceptance with one new ensemble ID:

```bash
uv run market-impact agent study-run-ensemble \
  --registration examples/calibration/agent-physical-energy-prospective-v1.json \
  --exposure-registry examples/research/a-share-energy-exposure-registry-v1.json \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json \
  --ensemble-run-id YOUR_UNIQUE_ENSEMBLE_RUN_ID
```

A completed command may legitimately return an abstaining Ensemble Decision with exit status
zero; operational completion is distinct from three-of-five proposal agreement. Each replicate
status, private state directory, terminal hash, metric set, frozen binding, and decision
artifact is reported without exposing a credential or broker capability.

The deterministic vertical integration translates either a validated candidate from one
frozen Judgment Artifact or an exact three-of-five Ensemble Decision into the existing Signal
Intent and Backtest Request, then replays that request twice through the unchanged Nautilus
bridge with identical result identity. The ensemble path revalidates all three agreeing
Artifacts and their frozen binding. Nautilus does not call the model.

Current non-claims remain explicit: two adapters passed the bounded runtime surface, but one Luna
xhigh run does not rank models or prove repeated behavioral equivalence; the separate max canary
does not establish a max-versus-xhigh quality advantage. The synthetic energy case is pipeline evidence, not event-family
calibration; Skills are installed, updated, or removed only through explicit user-authorized
repository/filesystem changes, never by the model; and the current research runtime exposes
read-only tools only.

The bounded local runtime gate is satisfied. It does not override the failed Phase 2
trading-calibration gate and grants no paper or live capability.
