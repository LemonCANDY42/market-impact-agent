# Agent Runtime Acceptance Boundary

## Status

The exact pi build `ad861366ae8b690df1964ad5558a11ea31e75e57e68a700bae4930d44b6ac400`
was formally admitted on 2026-09-03 Singapore for CPA/Luna `max` Responses and
MiniMax M3 Chat. Acceptance identity: `5222b963…`; evidence: `0629b958…`.
All new model entry points use pinned pi 0.84.4 behind Python Harness callbacks.
Old Python transports, protocol parsers, Agent loop, custom compactor and
fallback registrations are removed. Historical behavior belongs in isolated
offline Git versions, not the current execution tree.

The final repaired-build qualification passed **6/6 cases with 9 requests**:
two CPA concurrent continuations, completed-response restart without
regeneration, MiniMax continuation, two successive compactions and evidence-led
continuation on each route, and zero-dispatch cancellation. Report `00e8b2f3…`
replayed exactly without model access; Journal/Usage reconciliation passed.
The two old failed reports remain failed and immutable. Retained unaffected
cache/tool evidence was explicitly reviewed against the bounded source delta,
not presented as newly measured. Migration history is in [ADR 0006](adr/0006-use-pi-agent-runtime.md).

Final local verification: Ruff, format, Pyright, TypeScript, **1,727 Python
tests and four Node tests**; clean macOS source installation verified.
Independent review's role-ordering finding was fixed and tested through the
production entry. Empty-answer correction and invalid-summary/restart gates
have deterministic fault evidence; this real sequence needed no retries or
corrections and is not described as a real outage-recovery experiment.

New usage: 28,176 input / 13,754 output tokens, estimated **USD .023354**.
Cumulative runtime authorization: **45 / 48 requests, USD .112342 / 3**,
zero unsettled requests/reservations. The remaining three requests /
USD 2.887658 do not authorize reopening a closed batch. CPA's retained cache
groups lost 61.57 and .57 percentage points; the frozen repeated-degradation
rule passed, but consistently high caching is **not** proven.

Runtime admission does not grant investment or broker execution authority.
The separately authorized USD 3 Earnings/pilot slice remains the next mainline
task; its market calls have not run in this qualification. Collectors/Gateway
are unchanged and Live remains closed.

The follow-on [bounded reliability ablation](MODEL_PROVIDER_RELIABILITY.md#bounded-reliability-ablation)
also left this exact runtime build unchanged: 12 offline guard mutations detected,
four real CPA synthetic evidence-ablation Runs completed and replayed, USD .016191
new spend. MiniMax was not called. This adds targeted fault and missing-evidence
handling evidence, not an unlimited stability, caching or investment claim.

The later dynamic-effectiveness epoch separately qualified Luna `max`, Terra
`high` and Sol `high` for its own registered CPA routes. All three Profiles use
272,000 context tokens and request compaction at 258,000; same-model concurrency
remains three and the study-wide cap is six. Sol's per-Run conservative reservation
is USD .60 inside the USD 7 parent analysis stage, rather than an independent
budget. This route qualification does not broaden the earlier runtime Acceptance
or authorize new model names automatically.

One real completed response exposed harmless surrounding whitespace in a
narrative array item. Current Research Thesis and Portfolio V4 parsing therefore
perform a bounded trim only on narrative text and persist the affected field paths.
They do not normalize evidence IDs, model identity, enums, tool calls or
Harness-owned fields, and they never extract an answer from reasoning or mixed
prose. This removes avoidable Provider-format friction without relaxing financial
authority or replay evidence.

## Smallest complete runtime

```text
Frozen Harness input, tools, Skills and business validator
  → shared request/budget admission
  → reusable Node child: public pi Provider and Agent loop
  → awaited native response and tool-result Journal callbacks
  → business Judgment / Triage / EventAssessment / Portfolio output
```

The Harness is the sole authority for PIT, evidence, permission, experiments,
budget, Journal, Usage, trading policy and reconciliation. pi supplies model
protocols, opaque messages, the Agent loop and compaction. Roles use the same loop, retaining their own business output contracts.
Without tools they use one turn; Research Thesis and Portfolio roles may receive
Harness-injected read-only descriptors and then use the frozen Profile turn/tool
bounds. Signed tool completions replay before a dependent model turn. Recall
reads and directly injected prior theses share one cumulative 12,000 UTF-8 byte
upper bound per Run. This conservatively bounds text tokens; it is not a measurement
of 12,000 actual model tokens. New Runs freeze `injected-prior-reference-v1`: after
normal signed-source, scope and PIT reopening, a lookup of the already injected
opinion returns its exact input pointer and content hash instead of another copy.
Other opinions still return their bounded original content. The compact response
is charged normally, including repeated lookups. Existing Run bindings retain their
old descriptors and delivery semantics. Signed initial-history and tool receipts reconstruct consumption
across turns, compaction and replay without charging the same receipt twice or
resetting the allowance. Their signed opinions remain context, never new source facts. The Harness
binds the account/arm source-Run allowlist and tool manifest hashes. Newly acquired
data requires a new frozen Snapshot and continuation Run on the same parent
`ModelBudget`, rather than changing the current accepted evidence IDs.

`analyze_with_acquisition` composes that transition using the existing signed
Research Thesis terminal and parent acquisition Journal. A semantic miss seals an
incomplete `ResearchAcquisitionRequired` Run before acquisition. Its successor
binds the acquired Snapshot, receipt-advanced cutoff, same account/arm and parent
budget in a new Evidence Pack and Run; acquisition waiting is never a portfolio
hold. Explicit modeled historical continuation permits bounded completed-session
`daily` and `fund_daily` records through the existing source routes. Actual receipts
remain current; a separate modeled projection preserves the historical cutoff.
Current news, profiles and constituents remain prohibited in that lane. The final
signed research successor binds its concrete native-query candidates and source
graph to portfolio admission and replay; aggregate research targets are not securities.
Unknown model generation ends the composition without
acquisition or regeneration. `OnDemandResearch.episode_id` freezes each Decision
Episode deadline independently inside the shared study budget; reopening that
episode cannot extend its deadline. Omitting the ID preserves the legacy
parent-wide deadline binding. Analyst, conditional Judge and delegated roles do not create a second
model loop or receive independent spending/financial authority.

### Reuse-first runtime policy

Maintained upstream implementations are the default for model invocation,
management and Agent infrastructure. Depend on pinned packages through public
extension points; do not maintain a private source fork or imitate isolated code
shapes. The behavior being reused includes native protocols, tool continuity,
reasoning metadata, cancellation, cache controls, usage and compaction/recovery.

Before adding custom behavior, identify the exact requirement, evaluated upstream
version, observed gap, why configuration or a public hook is insufficient, the
smallest difference and its removal condition. Finance, language differences and
existing custom code are not sufficient exceptions. The required thin
process/admission/raw-usage differences are recorded in ADR 0006.

No second Agent framework, generic fallback, retry owner, billing ledger or
persistent session database is added. Unaccepted upstream capabilities never
automatically become project permissions. Tests follow
[requirement-driven selection](../CONTRIBUTING.md#test-quality-and-efficiency),
not upstream test duplication or test-count targets.

### Model Provider and configuration

Model Profile v2 freezes endpoint, native API, model, effort, context/output
limits, price schedule, capabilities, logical quota model, cache namespace and
controlled Provider-specific options. Run configuration separately binds prompt,
tools, Skills, budget and experiment identity.

- `MARKET_IMPACT_MODEL_PROFILE` selects a registered alias or profile file.
- Credentials are referenced by environment-variable name and captured once at
  Provider construction. They never enter RPC, artifacts, logs or Git.
- `MARKET_IMPACT_MODEL_STATE_ROOT` selects the single host/project admission
  directory; all project workers must use the same value. The default is
  `~/.local/state/market-impact-agent/model-runtime`.
- `MARKET_IMPACT_MODEL_MAX_CONCURRENT_REQUESTS` defaults to 3 and accepts only
  1–3 under the current authorization.
- Native API, returned model and supported effort must match the frozen Profile.
  There is no protocol fallback or effort downgrade.
- Provider options cannot override endpoint, auth, model, messages, tools,
  sampling, cache/session ownership or admitted output budget.
- Runtime Acceptance binds exact runtime build and model route/capability, not
  every business prompt. A smaller budget or a newly authorized tool does not
  require requalifying an unchanged model protocol.

`ModelProviderFactory` registers only the two pi native API adapters. A v1
Profile may remain as immutable financial experiment data, but cannot create a
new network Provider. Adding an upstream-compatible model/tool is a Profile or
tool registration plus focused acceptance, not a finance-core edit.

### Run lifecycle and durable state

1. Freeze inputs and execution identity in the existing Run Journal.
2. Acquire a same-model request lease; reserve parent budget and persist the
   physical attempt before sending it.
3. Persist the native result before projecting it into a business response or
   executing a requested tool.
4. Authorize each tool against its exact frozen descriptor; persist its result
   before a dependent next model turn.
5. Commit a compaction result before rebuilding model context.
6. Finalize on `agent_end`; do not use a "prepare next turn" callback as proof
   that the last turn completed.

Completed responses, tool results and summaries replay without regeneration.
A started request without a durable response stays unknown and terminates for
review; recovery does not need credentials or a Provider probe. A durable
native result with a missing business projection is projected locally. A lost
final IPC reply does not erase an already committed terminal.

Run claims, content-addressed artifacts and the Journal remain the state owners.
Terminal replay revalidates exact Run, prompt/tool/Skill/runtime bindings,
Journal chain, proposal and metrics. New runtime identities cannot resume old
unfinished contexts. Collection keeps expensive artifact work outside SQL write
locks; see [data input ownership](DATA_INPUT_HARNESS.md).

### Concurrent requests, cost and failures

All ordinary calls, one-shot roles, Judge calls, child work and summaries use
`PiRequestBoundary`. Aliases sharing a logical model use OS-backed leases under
the same project root, across workers and experiment directories. The limit is
not a gateway-wide limit on unrelated projects. Queue waits are cancellable and
bounded by the Run deadline; dependency barriers still belong to the business
scheduler.

`ModelBudget` records in-flight reservations in an existing parent Run Journal.
A child or summary spends that parent's allowance; independent experiments have
separate budgets. Reservation, dispatch, response and Usage are linked, not
separate competing cost ledgers. Unknown generation retains its reservation
and unknown usage is never represented as zero.

Provider lifetime follows its creator: an EventAssessment runner closes its
factory-created child on success, failure or cancellation. Caller-supplied
shared Providers remain the caller's responsibility.

pi/SDK retries are zero. Harness policy alone permits bounded pre-generation
429 retry and the explicitly enabled one-time received-408 regeneration. Local
timeouts, stream loss, cancellation, auth and quota errors are not generic
retry permission. Closing a connection does not prove upstream generation
stopped. See [failure and health ownership](MODEL_PROVIDER_RELIABILITY.md).

### Context, cache and automatic compaction

Business Run ID, conversation identity and cache namespace/key are distinct.
Conversation IDs bind one isolated context; route-supported cache keys identify
stable public prefixes. Independent experiment arms never share answers or
private histories. Stable system/tool prefixes omit Run IDs, clocks and mutable
statistics; variable evidence and task content stay in the variable portion.

The admitted GPT-5.6 study Profiles use the route-recommended **272,000-token
context window** and begin compaction at **258,000 estimated input tokens**.
They reserve 8,192 output tokens, keeping compaction below the hard context
edge. These values are frozen in the Model Profile, included in route identity,
reported by `runtime doctor`, and enforced again by study registration;
individual experiments cannot enlarge or postpone them. A Provider with a
different verified limit needs its own Profile rather than inheriting this
GPT-5.6 setting.

Native tool-call/result pairs, reasoning signatures and continuation metadata
remain opaque pi messages in private artifacts. Total input includes cached
input only once; reasoning is not added again to output. Missing cache counters
remain unknown. Cost uses frozen conservative input/output prices; measured
Provider cache discounts are not invented.

Compaction uses public pi `prepareCompaction`, `compact` and
`buildSessionContext`: safe turn cuts, retained context and incremental
summaries. Fixed authorization/PIT instructions and evidence entrances are
repinned outside the summary. Original history is retained. Summaries are
neither original evidence nor new permission. Summary calls share parent budget
and cancellation; an incomplete summary never replaces history.

High cache reuse is a measurement goal, not a guarantee. Compare the same pinned
pi/model/route with matching prefixes and alternate execution order. Report
cold, warm, expired and first-after-compaction calls separately. Missing counts
cannot pass; two matched groups each losing over five percentage points fail.
No irrelevant padding or extra calls are used to inflate a hit rate.

### Output normalization

Pinned `json-repair` handles minor JSON punctuation defects. The versioned
answer wrapper accepts whitespace and a single whole-answer JSON Markdown
fence. It does not extract a conclusion from mixed prose, thinking, several
answers or truncated fences. Raw answer and transformation evidence are kept.
Semantic evidence, required business facts and authorization validation are
unchanged.

### Skills

The existing manifest registry owns discovery, scopes, dependencies, conflicts,
tool/MCP allowlists and content hashes. Only selected instructions enter the
context. Skills cannot grant account or execution access. Traces bind offered,
loaded and Agent-reported use/influence to the actual Run and evidence; these
observations do not prove incremental effectiveness.

Outcome-opened discovery and candidate conflict governance are owned by
[Skill Governance](SKILL_GOVERNANCE.md). Strategy/Skill promotion is owned only by
[Agent Effectiveness Acceptance](AGENT_EFFECTIVENESS_ACCEPTANCE.md). Analyst/Judge
composition uses the current historical pilot contract, not an unbounded debate.

### MCP and tools

Selected evidence is exposed by zero-filter `read_selected_*()`; searching
and authorized pagination are separate capabilities. Identity and Snapshot
selection are injected by Harness, not guessed by the model.

Durability orders dependent work, not the lifetime of every external task:

- ordinary query: persist the final result before the next dependent step;
- background work: persist an accepted handle, let independent work continue,
  and wait for separately committed completion when its result is needed;
- continuous watch: persist subscription and deduplicated observations, then
  schedule a new Wake/Run. Never inject future information into an old decision.

Implemented surfaces are ordinary negotiated MCP tools and the project's own
Watch/Wake contracts. Generic MCP Tasks, resource subscriptions and arbitrary
server callbacks are **not** part of this runtime acceptance. They require
capability negotiation and lifecycle/duplicate/cancellation evidence before
being exposed. Progress is not completion; pi support alone grants no permission.

MCP handlers re-negotiate and verify server identity/schema before invocation.
Tool results are untrusted data, bounded by time/size and stored with provenance.
Research Runs expose read-only tools, not default shell/file tools, accounts,
orders, paper execution or live execution. Watch/delegation admission is owned
by the existing scope and callback contracts.

## Deployment, qualification and cutover

Source-checkout preparation:

```bash
uv sync
uv run market-impact runtime prepare
uv run market-impact runtime doctor --provider-profile examples/providers/pi-cpa-luna-max-v2.json --provider-profile examples/providers/pi-minimax-m3-v2.json
```

Doctor is read-only: Python/Node versions, locked packages, build identity,
credential-presence booleans, shared admission directory and accepted routes.
It does not test remote health or make model calls. Python installation alone
does not install Node dependencies. `prepare` refuses while any project pi
worker holds the build lease. This release targets a clean macOS source install;
Linux uses the same preparation but remains unverified until actually tested.
No standalone wheel/Node bundle is claimed.

Qualification uses `agent pi-canary prepare|run|replay|accept`.
Prepare requires bound full-check/clean-install/independent-review evidence and
the two prior Usage roots. A bounded permit authorizes only the registered
qualification Runs and shared budget. The direct upstream control executable is
test-only and still uses the same physical admission/audit/budget; it is not a
second production entry.

The explicitly authorized focused continuation is prepared with
`agent pi-canary prepare --followup-of <closed-original-root> --state-root
<new-followup-root> --verification <verified-checks.json>`; it derives cumulative
prior usage instead of accepting caller-supplied counters. No recursive
follow-ups are supported. A newer qualification coordinator may reopen old
terminal reports only with the identical production build, no network dispatch
and exact report equality; it cannot resume an unfinished old coordinator Run.
The installed acceptance bundles both the retained failed parent report and
the new scope's passing evidence. This is not a retroactive parent pass.

The separately approved changed-build qualification uses `--repair-of
<closed-focused-root>` instead. It verifies retained registration/report hashes,
case Journals, terminal artifacts and Usage Ledgers without executing the old
runtime. Retained cache/tool evidence is applicable only after an independent
review of the exact changed source files; it is never described as a fresh
new-build cache measurement. The qualification registry and its host/project
claim are single-use, and all four previous Usage roots remain in the cumulative
denominator. Passing targeted evidence plus reviewed unchanged behavior may
qualify the exact repaired build; no build inherits admission automatically.

A normal next model turn is not a transport retry. Qualification caps are
checked before starting a fresh turn; completed responses may still replay
when a cap is exhausted. This makes a zero-dispatch cap stop an explicit budget
terminal rather than an invented unknown upstream response. Existing 408/429
self-recovery remains bounded by the original deadline, physical-call budget
and durable attempt accounting; no broker or mutating-tool retry is added.

Required gates:

- actual pi production-entry tests for tool consumption, native state, failures,
  budget/concurrency, summary/restart and extension;
- temporary 0.84.3 → 0.84.4 upgrade rehearsal; no second installed production loop;
- Ruff, format, Pyright, pytest, TypeScript and Node tests, clean macOS install,
  then independent read-only review;
- real CPA and MiniMax matched-cache/tool sequences, two incremental summaries
  per route, isolated concurrent workers, completed-response restart and
  pre-dispatch cancellation, all within remaining authorization;
- exact physical attempts/native usage/Usage reconciliation, no unresolved
  generation, no failed or replaced sample;
- acceptance installation reopens every terminal and stores qualification
  evidence by content hash, then atomically publishes exact build/route admission
  only after active workers have drained. A same-build qualification adds or
  replaces evidence only for its exact routes and preserves other already
  accepted routes (for example MiniMax); a changed build must requalify routes
  rather than carrying old-build evidence forward. The one-time v1-record
  migration may re-derive route identities only from that record's immutable
  same-build qualification artifact; it cannot translate an old Profile into a
  different current Profile or preserve an unverified route string.

Old generic implementation is removed before freezing the final qualification
build. Until that build passes, there is **no** fallback production runtime.
Upgrades repeat targeted qualification before new Runs switch; never hot-update
a running child or roll back Journal, Usage, Intent or broker facts.

## Return to the market mainline

After formal cutover, use the separate USD 3 authority for four fixed samples:
one current-cutoff Earnings question and three Modeled-PIT pilots (opportunity,
risk and reasonable abstention; hidden labels). Maximum USD .75 / 24 physical
requests per sample, including both analysts and any conditional Judge.

Keep old missed windows and failures sealed. At least two historical pilots
must cease abstaining merely because of the old input gaps before market sample
expansion. No 24-case holdout, Skill promotion or alpha claim is authorized by
this runtime test. Only genuine eligibility, required Query Gate, nonempty
auditable Intent and existing risk/approval permit the Mock path. Runtime,
investment effectiveness, Mock execution and broker readiness are separate
claims.
