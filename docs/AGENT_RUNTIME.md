# Agent Runtime Acceptance Boundary

## Status

The bounded research-only Agent runtime passed its current local runtime gate. It includes
redirect denial for credential-bearing requests, same-connection MCP snapshot revalidation,
terminal-artifact/run/journal binding, exact tool execution-limit identity,
optional-dependency isolation, and explicit compactor identity. A fresh private MiniMax M3
v4 run completed against that exact surface. This is not model-quality, event-family, alpha,
Provider-portability, paper, or live acceptance. No model, Skill, MCP server, or tool receives
broker credentials or a path around hard policy.

The machine-local environment names are:

- `MINIMAX_API_KEY`: secret, loaded from the user's Keychain-backed environment;
- `MINIMAX_BASE_URL=https://api.minimaxi.com`: China API origin; the adapter appends the
  versioned API path;
- `MINIMAX_MODEL=MiniMax-M3`: explicit model identity with no silent substitution.

MiniMax's official [M3 model page](https://www.minimax.io/models/text/m3),
[OpenAI-compatible API guide](https://platform.minimax.io/docs/api-reference/text-openai-api),
[China/international endpoint guide](https://platform.minimax.io/docs/token-plan/cursor), and
[pricing page](https://platform.minimax.io/subscribe/token-plan?tab=api-enterprise) are the
Provider references. The API key never enters prompts, run artifacts, model history, tool
arguments, logs, or committed configuration. The runtime records a versioned cost estimate;
it is not an invoice and currently does not subtract automatic prompt-cache discounts.

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
- credential-bearing requests reject every HTTP redirect and recheck the exact pinned China
  origin at the network boundary;
- secret injection at process/request boundary only, plus redacted errors and traces.

### Run lifecycle and durable state

- stable run, turn, message, tool-call, and artifact identities;
- append-only event journal plus content-identified checkpoints;
- crash-safe resume, cancellation, bounded retries, and idempotent tool-result replay;
- explicit terminal states for completed, failed, cancelled, budget-exhausted, and
  human-input-required runs;
- wall-time, token, cost, tool-call, and recursion budgets enforced by the Harness;
- read-only result replay after restart; mutating-tool duplicate-side-effect acceptance is
  outside the current surface;
- terminal replay revalidates the journal chain and binds the stored artifact to the exact run,
  journal tail, terminal status, and run-row timestamps.

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
- local inspection reports tokens, cost, latency, retries, context pressure, and terminal
  state without exposing secrets or licensed payloads;
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
replayed metrics or tool calls. The estimated cost was 11,400 micro-USD under the versioned
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

Validate the committed synthetic bundle without any model credential:

```bash
uv run market-impact agent validate \
  --evidence-pack examples/agent/energy_supply/evidence-pack.json \
  --evidence-documents examples/agent/energy_supply/evidence-documents.json \
  --pattern-pack examples/agent/energy_supply/pattern-pack.json
```

Run a new private real-model judgment only after the three documented MiniMax environment
variables are present:

```bash
uv run market-impact agent run \
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

Current non-claims remain explicit: only the MiniMax adapter passed, so Provider portability
is not established; the synthetic energy case is pipeline evidence, not event-family
calibration; Skills are installed, updated, or removed only through explicit user-authorized
repository/filesystem changes, never by the model; and the current research runtime exposes
read-only tools only.

The bounded local runtime gate is satisfied. It does not override the failed Phase 2
trading-calibration gate and grants no paper or live capability.
