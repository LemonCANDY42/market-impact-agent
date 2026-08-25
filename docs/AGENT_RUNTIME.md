# Agent Runtime Acceptance Boundary

## Status

The Agent runtime is planned and not implemented. The first local model acceptance may use
MiniMax M3 through the China endpoint, but a working model response alone is not Harness
acceptance. No model, Skill, MCP server, or tool receives broker credentials or a path around
hard policy.

The machine-local environment names are:

- `MINIMAX_API_KEY`: secret, loaded from the user's Keychain-backed environment;
- `MINIMAX_BASE_URL=https://api.minimaxi.com`: China API origin; the adapter appends the
  versioned API path;
- `MINIMAX_MODEL=MiniMax-M3`: explicit model identity with no silent substitution.

MiniMax's official [M3 model page](https://www.minimax.io/models/text/m3),
[M3 launch note](https://www.minimax.io/blog/minimax-m3), and
[China/international endpoint guide](https://platform.minimax.io/docs/token-plan/cursor)
are the initial Provider references. The API key never enters prompts, run artifacts, model
history, tool arguments, logs, or committed configuration.

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
- secret injection at process/request boundary only, plus redacted errors and traces.

### Run lifecycle and durable state

- stable run, turn, message, tool-call, and artifact identities;
- append-only event journal plus content-identified checkpoints;
- crash-safe resume, cancellation, bounded retries, and idempotent tool-result replay;
- explicit terminal states for completed, failed, cancelled, budget-exhausted, and
  human-input-required runs;
- wall-time, token, cost, tool-call, and recursion budgets enforced by the Harness;
- no duplicate side effect after restart or an ambiguous transport outcome.

### Context and automatic compaction

- an inspectable context ledger, not only one mutable prompt string;
- deterministic inclusion priorities for system/policy, current mandate, task state, recent
  turns, unresolved tool calls, pinned evidence, and referenced artifacts;
- token measurement against the selected Provider and reserved output/tool budget;
- automatic compaction before overflow, with source-message ranges, compactor identity,
  summary hash, and retained facts/decisions/unknowns;
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

Codex, OpenCode, and Pi may be researched for reusable patterns and compatible licensed
components. Before adoption, the exact current implementation, security model, dependency
surface, and license must be reviewed. Their feature presence is prior art, not evidence that
this Harness satisfies its own contracts.

## Acceptance gate

The runtime is locally accepted only when:

1. a no-secret deterministic fixture and a real MiniMax M3 tool-calling fixture both finish;
2. forced compaction preserves policy, task state, citations, and unresolved tool continuity;
3. a Skill is discovered, loaded only when selected, traced by hash, and denied an
   undeclared capability;
4. an MCP server completes negotiation, success, timeout, cancellation, crash, and restart
   cases without duplicate side effects;
5. checkpoint/resume reproduces the same terminal artifact identity for a no-side-effect run;
6. injection and secret-exfiltration fixtures fail closed;
7. token/cost/latency and every tool side effect are auditable; and
8. no paper/live/account capability is reachable in the acceptance configuration.

This gate is a prerequisite for future Agent-driven event-family research. It does not
override the failed Phase 2 trading-calibration gate and grants no paper or live capability.
