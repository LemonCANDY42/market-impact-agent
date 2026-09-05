# ADR 0006: Use pi mechanisms behind the existing Harness authority

Status: accepted as the only current model path on 2026-09-03 for CPA/Luna max Responses
and MiniMax M3 Chat; market effectiveness and broker execution remain separate gates.

## Decision

Directly depend on `@earendil-works/pi-ai` and `@earendil-works/pi-agent-core` at
`0.84.4`, upstream commit `b79e4cc834970cca69daebffab7df1da7d1e52c4`.
`runtime/pi/package-lock.json` pins transitive integrity. Use only public exports:
`runAgentLoop`, `createModels`/`createProvider`, native API factories, `prepareCompaction`,
`compact`, and `buildSessionContext`. The new `AgentHarness` is not used: this revision's
implementation is incomplete. There is no fork, deep import, `/compat` dependency,
coding-assistant UI, shell/file tool, global extension discovery, or second session database.

Python `AgentEngine` keeps its business interface and delegates the generic loop to a reusable
Node child. Python owns capability selection, PIT, budget/concurrency, durable Journal, Usage,
Judgment validation and trading controls. Native pi messages remain intact in private artifacts;
the bridge extracts only the fields needed by those owners. Historical artifacts and old Profile
identities remain unchanged. Model Profile v2 configures the native route. Every new model entry
uses pi, including zero-tool business roles; none can fall back to another implementation.
The old generic code is deleted before qualification; new builds remain closed to production
dispatch until their exact qualification passes. Historical behavior is available through Git, not a current
compatibility runtime.

## Why a thin process bridge remains

| Local difference | Requirement / upstream limitation | Owner and removal condition |
| --- | --- | --- |
| Awaited stdio callbacks | Python already owns signed Journal, tools and finance; pi is TypeScript | One child per worker, memory only; replace this boundary only if an upstream interface preserves that authority with less code |
| Physical request admission, retry and OS request leases | Existing budgets and received-408 authority must cover summaries and ordinary calls equally | pi/SDK retries are zero; Harness owns retry, cancellation and a shared-root same-model maximum of three |
| Bounded original usage/model observation | pi zero-fills absent counters; Responses reports requested `model`, while actual returned identity may differ | Use the same pinned official OpenAI SDK SSE decoder to retain observed model/usage presence; remove if pi exposes them losslessly |
| Final payload budget assertion | pi Responses raises small output caps to its protocol minimum of 16 | Reject insufficient budget before dispatch and assert the generated payload does not exceed admission; do not patch upstream |
| MiniMax `reasoning_split` request option | Native Chat otherwise puts thinking and final answer in the same content field; observed in the first failed canary | Public `onPayload` hook selects the documented separated format; pi parses and replays reasoning metadata. No regex removal, effort reduction, or protocol fallback. Remove the hook when the selected upstream factory configures it equivalently |
| Canonical call IDs | Native call IDs may contain protocol-specific separators | Hash only business IDs, retaining original IDs and continuation metadata in native messages |

These exceptions do not justify custom Provider parsing, a second Agent loop, a custom summary
algorithm or autonomous finance in Node. Models emit proposals only. Summary calls use the same
admission and accounting path and cannot execute tools. Summaries are not original evidence.

## Lifecycle

1. Freeze exact Profile, local adapter/lock hashes, prompt, tools and Skill bindings.
2. Await request admission and durable dispatch before network I/O.
3. Await native response persistence and completed-turn accounting before tools.
4. Await each authorized tool result persistence before the next model request.
5. Persist upstream compaction output before replacing in-memory context; retain original history
   and fixed policy/evidence entrances. A failed/truncated/tool-calling summary cannot be committed.
   Apply the same semantic check to completed-summary replay; a crash between receipt persistence
   and validation cannot promote an empty/whitespace summary into replacement history.
6. Finalize Judgment on `agent_end`, not a preparation callback. A lost final IPC reply cannot
   invalidate an already committed terminal. Completed records replay without regeneration;
   started-without-completion remains unknown and stops.
7. Imported native history precedes the new user task, matching upstream prompt append semantics.
   A received empty assistant answer retains its artifact and usage and reaches bounded business
   correction; a transcript nonempty check must not suppress that recovery. Context contract code
   participates in the runtime build identity.

The 0.84.3-to-0.84.4 rehearsal temporarily installs the prior public package, observes the changed
terminal preparation callback, and verifies exactly one request and one terminal event in both
versions. It cleans up that installation; the project lock contains only the current runtime.

## Extension and upgrade contract

- Tools/queries: Python registrations generate the pi tool surface; no changes to the finance core.
- Agent/Judge/Skill composition: existing Profile, Skill and budget registrations.
- Models: configure a v2 Profile and accept the exact native route/model/capabilities. Initial
  qualification covers CPA/Luna max Responses and MiniMax M3 Chat. Compatible additional models
  require configuration and targeted acceptance, not changes to the finance core.
- Special protocol options: use public Provider factories/request hooks in the Node adapter, never
  allow untrusted model content to change endpoint, tool scope or budget.
- Upgrades: review official releases/deprecations, build candidate dependencies separately, run
  focused protocol/lifecycle/replay tests and separately authorized paid canaries as needed.
  Switch after current Runs end. Changed behavior creates a new runtime/experiment epoch.
  Rollback selects the prior accepted runtime for subsequent Runs only; no Journal/Usage/Intent
  or broker facts are rolled back. Do not hot-update packages beneath an active worker.
- Terminal-only canary replay may use the original frozen Profile/runtime identity without starting
  Node. An incomplete Run remains bound to the original accepted build; it cannot silently resume
  under an upgraded runtime. Rejected paid results are not rewritten by an adapter correction.
- Changed-build repair qualification requires independent review of the exact source delta before
  retained unaffected evidence is used. It verifies old immutable artifacts without loading the old
  runtime, runs new acceptance for affected behavior and carries forward cumulative authorization.
  An old failed report never becomes passing, and an unreviewed build never inherits admission.
- No private fork by default. A necessary patch must identify the exact upstream gap, smallest
  difference, acceptance evidence and deletion condition; remove it when upstream fixes the gap.

The Node child is source-checkout deployed. `market-impact runtime prepare` installs the locked
dependencies under an exclusive build lease; `runtime doctor` is a read-only readiness check.
Importing the Python package alone does not install Node or prove route acceptance. A portable
wheel deployment and Linux installation are not claimed without their own actual evidence.

## Brief migration history

The former Python loop and protocol transports were retired, rather than kept as fallback.
Two preliminary native pi stages on 2026-09-02 used 8 requests / USD .025153 in total: the first
failed on MiniMax thinking/answer separation, the authorized repair verified `reasoning_split`
and exposed a harmless whole-answer JSON fence. Both original outcomes and ledgers remain private
and immutable. Neither stage proved matched caching, compression or whole-runtime acceptance.
The final qualification reopens their exact Usage union before spending any remaining allowance.

The cleaned-build qualification closed on 2026-09-03 Singapore without admission: an additional
26 requests / USD .061409 produced 11 passing cases and one concurrent case stopped at its
two-request allowance after successful reads and searches, before a final Judgment. The remaining
cancellation case was not run. The failed terminal and passing sub-results are retained; follow-up
qualification requires explicit authority rather than changing the completed batch's limits.

The authorized focused follow-up used two further requests / USD .002426, reaching cumulative
36 / USD .088988. Real request overlap passed, but both native responses contained empty text;
the Python context projection failed before business correction, so neither case completed and
cancellation was not reached. The repaired source fixes imported-history chronology and permits
artifact-backed empty assistant turns to reach bounded correction. That new build required its
own qualification, not retrospective success. Exact failed-report replay passed using the original
source; private frozen source archives preserve that build without adding a production fallback.

Independent review identified a single-turn-role chronology regression. The lead fixed the role
adapter to split only trailing user prompts from ordered native history and verified three
successive invocations and replay. The newly authorized repair-v1 qualification then passed all
six cases with nine requests / USD .023354, including both-route history and compression
continuation, physical overlap, completion restart and cancellation. Cumulative usage was
45/48 requests / USD .112342, fully reconciled. Exact offline replay passed. Build `ad861366…`
was admitted as acceptance `5222b963…`; earlier failures and cache limitations remain visible.

## Verification and references

The owning acceptance matrix and status are in `../AGENT_RUNTIME.md`. Do not equate test counts,
a successful native request, or a synthetic Judgment with stable cache performance, market
effectiveness or broker readiness.

- [Pinned loop](https://github.com/earendil-works/pi/blob/v0.84.4/packages/agent/src/agent-loop.ts)
- [Public Provider API](https://github.com/earendil-works/pi/blob/v0.84.4/packages/ai/README.md)
- [Callback lifecycle change](https://github.com/earendil-works/pi/blob/v0.84.4/packages/agent/CHANGELOG.md)
