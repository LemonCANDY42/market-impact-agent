# Model Provider Reliability

This document owns the failure, retry, circuit, and operator-notice boundary for model-generation
Providers. It does not change Model Provider Profile identity, semantic evaluation, Judgment
authority, or any trading permission.

## Transport reuse — 2 September 2026

The bounded reuse slice replaces the urllib opener/redirect handler and `asyncio.to_thread`
model I/O with `httpx2==2.12.0`. The dependency was already present transitively; it is now pinned
as a direct runtime dependency. No external Runner, retry supervisor, session store or tool-loop
authority is added. Provider-specific payloads and complete response dictionaries remain lossless;
the Harness still validates model identity, tool arguments and usage before accepting a turn.

The reference decision was based on source inspection and a synthetic, zero-model-cost probe:

| Reference | Evidence and decision |
| --- | --- |
| [OpenAI Python 3.7.0](https://github.com/openai/openai-python/tree/ab76ab5c64b8d19761ce838891acc80743cd944a), Apache-2.0 | Supports custom endpoints and direct requests, but defaults to two retries. A `MockTransport` probe confirmed `OPENAI_CUSTOM_HEADERS` inheritance even with explicit endpoint/key/org/project. This is SDK configuration behavior, not an SDK defect. Its model resources are unnecessary for the existing lossless dictionary boundary; adopting it here would add configuration filtering without removing Harness policy. Not installed as a project dependency. |
| [HTTPX2 2.12.0](https://github.com/pydantic/httpx2/tree/71ae23be5448f859c2b4e21d9972ddfa7b8d759d), BSD-3-Clause | Also used by the inspected OpenAI SDK. Native async request/context-manager lifecycle and default zero connection retries fit this boundary. Adopted through its public API; no vendored source. |

`PinnedHttpxJsonTransport.request_json` is async. Both model adapters and CPA Usage Keeper reads
await it; synchronous CLI entry points own their `asyncio.run` boundary. Each physical attempt owns
one client context, which closes on success, failure or cancellation. This intentionally keeps the
previous request-scoped lifetime, without a cross-loop connection pool or a second lifecycle owner.
Every URL must match the pinned origin; redirects are disabled, and loopback requests ignore proxy
environment variables. HTTPX does not import unrelated `OPENAI_*` headers or credentials. Remote
HTTPS retains its existing environment-proxy policy. HTTPX transport retries remain zero; the
existing Harness retry loop alone emits dispatches and decides 429 backoff or explicitly authorized
one-time received-408 regeneration. An outer request deadline also bounds streaming/trickling reads.

Cancellation closes the local connection; it does **not** prove that the gateway or upstream model
stopped generation. A dispatched request without a durable result remains unknown, retains its
dispatch/accounting evidence and cannot be automatically resent on restart. The existing bounded
work scheduler still owns concurrency/dependency barriers; this transport starts no independent
tasks or queues. Gateway-internal attempts remain outside project proof.

Acceptance exercises the real provider/parser/observer/AgentEngine/Journal/Usage path with HTTP
responses for success, 429 exhaustion, legacy 408, authorized regeneration and repeated 408. A real
loopback TCP test proves cancellation closes the connection and restart does not redispatch.
Focused fault cases cover timeout/TLS, malformed/non-object JSON, origin rejection, redirects and
ambient proxy/header isolation. Existing work-scheduler and replay tests remain the concurrency
authority; no duplicate scheduler is tested or introduced.

This change does not alter frozen Profile, prompt, tool or artifact contracts, does not rewrite old
Runs, and does not grant a new model call. The previous nine empty tool queries are a **separate
input-access usability failure**: a transport library cannot turn a natural-language question into
a valid literal/AND filter. A new versioned default-read tool surface and a separately authorized
model-facing canary remain necessary before claiming that failure is resolved. No live gateway,
paid-model or broker acceptance is inferred from offline transport tests.

## Failure model

Observer-capable model adapters assign each project-to-gateway physical request one opaque
correlation ID and emit a separate Harness Journal dispatch event. The generic AgentEngine also
records a logical-call guard before invoking any Provider; a legacy non-observable Provider gets
that conservative guard, not fabricated physical-attempt evidence. A sanitized `ProviderFailure`
records:

- error class and diagnostic code;
- HTTP status when present;
- correlation ID and physical attempt number;
- elapsed latency and total attempts;
- generation state: `not_started`, `unknown`, or `response_received`;
- retry disposition: `safe`, `authorized_regeneration`, `forbidden`, or `terminal`;
- parsed `Retry-After` when present.

Error bodies, credentials, prompts, and Provider responses are not copied into the health store or
operator notice outbox.

For a model-generation `POST`, timeout, TLS/transport failure, and ambiguous 5xx all mean
`generation_state=unknown`; the project adapter must not retry them. An explicit HTTP 429 rejection
classified as `not_started` can retry with bounded exponential backoff plus `Retry-After`.
New explicitly opted-in CPA Profiles additionally permit the received-408 regeneration below;
this authorization does not turn unknown generation into `not_started`. Read-only `GET` health/model discovery may retry typed
transient failures. A terminal invalid response is `response_received` and fails the Run rather
than being mislabeled as an ambiguous dispatch.

## Durable health and notification state

`ProviderHealthStore` uses a private SQLite WAL/FULL database for sanitized incidents, provider
circuit state, and an operator-notice outbox. A single ambiguous failure enters cooldown; repeated
transient failures cross a frozen threshold and enqueue a persistent-failure notice. Authentication
unavailability, authentication failure, and quota exhaustion open the circuit immediately and
enqueue an action-required notice. Admission remains closed until cooldown ends, an explicit safe
health probe succeeds, or an operator resets an `open` circuit after remediation.

Notice creation and delivery acknowledgement are durable, but this repository does not install an
external notification destination. A host integration must deliver pending notices and acknowledge
them; absence of that adapter must not be reported as end-to-end alerting.
The specialized triage execution path integrates this health store. Generic AgentEngine attempt
tracing does not by itself open a shared circuit or deliver a notification; its caller must own
that admission/notification integration. Do not infer fleet-level protection from a single Run's
no-redispatch guard.

## 31 August 2026 incident diagnosis

The immutable 39-version triage batch contains 46 logical graph members: four work units times four
map roles, one partition member, and 29 classify members. Twenty-nine completed, one classify member
became ambiguous, and sixteen logical members were not started. Thirty members were attempted across
31 physical Provider attempts.

The failed logical member had this observed path:

1. the Harness dispatched one project request;
2. the local gateway's first upstream attempt failed with `tls: bad record MAC` and returned HTTP
   500;
3. the old project adapter treated that 500 as retryable and sent a second generation `POST` after
   250 ms;
4. the gateway rejected the second request locally with HTTP 503 `auth_unavailable` before an
   upstream response;
5. the old triage runner retained only a generic Provider exception, so the batch correctly stopped
   but did not preserve enough structured root-cause evidence.

The evidence supports a transient TLS-path integrity failure amplified by an unsafe project-level
retry and then gateway authentication-route unavailability. It does not show quota exhaustion,
HTTP 429 rate limiting, or a process restart. The exact physical cause below the TLS boundary is not
provable from project evidence.

The repaired adapter and structured health records apply prospectively. They do not rewrite or
upgrade the old batch. The local gateway may also perform internal retries under machine-global
configuration; because this repository neither owns nor changed that setting, one project dispatch
is not yet proof of one upstream attempt. A separately accepted project-dedicated gateway route
with internal retries disabled is required before making that stronger claim.

## Replacement-run boundary

No failed or ambiguous generation is silently resent outside the frozen retry policy. The accepted Replacement Grant is a separate
append-only Harness authority that reopens the exact old terminal artifact, Journal and Usage record,
then permits one distinct replacement Run identity. It never combines a possible late old response,
cannot run before its authorization time, cannot replace the replacement, counts both Usage records
against the original frozen unit/phase/aggregate ceilings, and grants no Judgment or execution
authority. Equivalent authorization reopens the same Grant instead of issuing another.
Provider reliability acceptance remains a prerequisite, not replacement authority by itself.

A fully received response with locally malformed JSON is not an ambiguous Provider failure. V7 and
later Work Plans parse through pinned `json-repair` and accept only strict JSON or one semantics-
preserving structural punctuation edit, with content-identified evidence. For an already terminal
pre-v7 member, one explicit Format Recovery Grant may bind the immutable failed terminal, Journal,
Usage and final response and create a separate zero-Provider recovery Run. It never resends the
request, creates no new Usage, leaves the failed source Usage charged, and cannot repair semantic
tokens or another recovery. This format path is distinct from Provider retry, circuit and Replacement
Grant authority.

Material-ingress v9-v11 preserve the same dispatch, ambiguity, parser, Usage and restart
evidence but have only one model phase: one coordinator request per bounded Work Unit. Partition and
classify are
deterministic Harness derivations, so they cannot create extra Provider ambiguity or receive separate
budgets. A failed blind comparison is terminalized before its active head is released; restart cannot
rerun the failed versions or reinterpret them as a semantic Decision. The terminalization authority
does not trust a caller-reconstructed failure: it first reopens the append-only Comparison
Registration and Report and replays both completed Run Journals and exact Usage sets. The ordinary
run path rejects comparison-governed material-ingress plans before Provider availability or
generation. If a process
crashes after Report or terminal commit, retry reopens the first durable identity and finishes head
release without constructing or probing a Provider; only a genuinely missing Work member can resolve
and check a Provider immediately before a new dispatch. A factory or availability failure at that
pre-dispatch boundary appends a `provider.preparation.failed` diagnostic with generation
`not_started`, retry disposition `safe` and zero Provider attempts. It leaves the Run nonterminal and
creates no Usage record, returns control once, and may be re-probed only by a later invocation; it is
never converted into an ambiguous dispatch or silently retried in place. V11 adds only a closed
registered checkpoint-rule projection to that existing coordinator request; it does not create
another Provider phase, retry path, budget, state owner or authority.

The real Grant was consumed for the ambiguous classify member above. Its replacement completed and
the v4 graph advanced to 39 completed logical members. A later classify member then failed normally
after three received responses violated the same output contract; it was not a network, quota,
authentication or ambiguous-ACK incident. The complete v4 Ledger now contains 41 Run Usage records, 44
physical attempts, 393,440 input and 205,023 output Tokens. Six logical members were not started and
no Proposal or Decision exists. Diagnosis found that each response copied the sole frozen evidence
Version ID with one missing or extra character. That is owned by the v5 ordinal-citation contract,
not by Provider retry or circuit logic, and the terminal v4 member is not replayed.

## V6 HTTP 408 evidence and compatibility boundary

The same-candidate v6 revalidation later stopped after eleven completed logical members when the
gateway returned HTTP 408 after about 190 seconds. Its exact gateway log says the upstream stream
disconnected before completion after streaming had begun. The old adapter persisted
`http_408 / not_started / terminal`; that historical record is immutable but its generation-state
classification is wrong. Prospectively the adapter records this exact failure as
`upstream_stream_incomplete / unknown / forbidden` in the legacy Profiles, so those frozen Runs
never retry it automatically. Other HTTP 408 model-generation failures likewise remain unknown.
The later opt-in received-408 policy below permits one regeneration without claiming pre-generation
rejection; a read-only GET 408 remains safely retryable.

One narrowly compatible Replacement Grant may accept only the exact legacy terminal pair whose
matching failed/rejected events contain HTTP 408, `error_class=http`, `http_408`, `not_started`,
`terminal`, and identical request identity bound to one real preceding dispatch event. Other
rejected, authentication, quota, pre-dispatch or failed Runs remain ineligible;
the replacement cannot be replaced. The real grant was consumed once. The replacement completed,
the Provider health state returned to healthy, and the full v6 graph completed 47 logical members.
The authoritative Ledger retains both physical dispatches: 48 Usage records / attempts, 396,709
input and 172,508 output Tokens. Mean per-record Usage was 8,264.77 input and 3,593.92 output Tokens;
mean recorded latency was 71.65 seconds and maximum recorded latency was 310.73 seconds. Cost stayed
unallocated at zero microusd because this local Provider profile has no price schedule; that is not
a claim of zero economic cost. A durable `provider_recovered` notice exists, but no external notice
delivery adapter is installed or claimed.

## 2 September 2026 historical v2 stream interruption

The opened-risk adjudication experiment's second analyst pair both returned HTTP 408. The two
retained gateway error logs now establish the more specific error: an upstream stream ended before
its completion marker. Each log records one upstream API request and the project's correlation
header, but the old generic AgentEngine terminal retained only the HTTP status summary, not that
correlation or typed diagnosis. Neither log establishes the lower-level network/server cause.
This is not evidence of a Harness 600-second deadline expiring, HTTP 429, credential failure or
quota exhaustion. The earlier failed Run/Usage artifacts are immutable and remain incomplete.

The installed CLIProxyAPI is 7.2.140. Reference inspection is pinned to upstream commit
`a7e3596b7e351d800e58ed29529fbca3d1c18737`: its
[Codex executor](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/runtime/executor/codex_executor_execute.go)
collects the upstream stream for non-streaming clients and needs a terminal response event;
the [incomplete-stream error](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/runtime/executor/codex_executor_terminal.go)
maps the missing-completion case to 408. Switching the downstream client to SSE or merely raising
its timeout does not by itself recover a missing upstream final answer. No service was restarted,
no machine-global retry setting was changed, and neither failed request was resent.

The actual redacted request-field projection also matches the pinned
[Chat-to-Codex translator](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/translator/codex/openai/chat-completions/codex_openai_request.go):
Luna and `reasoning.effort=max` survive translation, but `max_tokens=8192`, temperature and top-p
do not reach the upstream request. Therefore the Profile freezes requested settings, not proof
that every upstream control took effect. Token/cost preflight and post-response checks still
bound Harness admission and acceptance; they do not constitute an upstream generation cutoff or
an exact billing ceiling. A timeout can stop local waiting without proving upstream cancellation.
Do not silently rewrite the historical profiles or raise limits to conceal this capability gap.

The generic AgentEngine also discarded cumulative failed-call latency even though ProviderFailure
already carried it. The repaired path keeps completed ModelTurn/final failed-call metrics as the
single accounting owner. A typed `model.turn.failed` preserves the allowlisted safe fields and
cumulative logical-call latency; a legacy attempts-only event still contributes zero **recorded**
failure latency, never reconstructed or asserted zero elapsed time. The terminal message is fixed,
not an arbitrary Provider error body. Both normal and authoritative replay reconstruct the same
metrics, and Usage Ledger copies that result without another accounting state.

New generic-run events use `model.turn.started` and `model.attempt.*` names, signed when the Run
Journal is authoritative; ordinary research Journals retain their non-promotional hash-chain mode.
Existing specialized triage `model.request.*` history is unchanged. The existing per-run claim excludes a second
caller while a run is active. A guard without a durable ModelTurn cannot be resumed by another
generation request: it requires human input and records unknown usage rather than inventing token
totals from incomplete diagnostics. A durable failure-terminal event can finish its interrupted
artifact/run-status commit on reopen without calling the Provider. Physical-attempt diagnostics
are never added to cumulative logical-call latency a second time.

A recovered small canary can establish new request/tool/terminal wiring only; it cannot prove that
long max-effort calls never disconnect or retrospectively complete the failed historical case.

## Explicit received-408 regeneration — new CPA epochs

The user authorized discarding an incomplete model answer and retrying a returned HTTP 408 once,
stopping if it repeats. The separate `cliproxyapi-luna-max-cpa-retry408-v1` Profile therefore sets
`retry_received_408_once=true`. Existing Profiles omit the field and retain their exact identities
and no-408-retry behavior. The opted-in Profile hash also enters RuntimeConfig/Run binding, so this
policy cannot silently resume an old Run under the same configuration identity.

- Only a received model-generation HTTP 408 with `http_408` or `upstream_stream_incomplete`
  diagnosis qualifies. Authentication/quota diagnostics, local timeouts, TLS errors, other HTTP
  statuses, malformed received output, and an interrupted process do not inherit this permission.
- At most one regeneration per logical call, within the existing total attempt ceiling (two in
  the bundled Profile) and original time budget. Backoff is at least one second and honors the
  bounded `Retry-After`; if the remaining time cannot cover backoff, stop without another dispatch.
- The same frozen messages and tools are sent again. Only the complete successful attempt reaches
  the tool/decision loop; incomplete output is never merged, executed, or selected as an alternative.
  Each `(correlation ID, physical attempt)` remains in the Journal. No failed evidence is deleted.
- The first qualifying failure remains `generation_state=unknown`, with retry disposition
  `authorized_regeneration`, not `safe`. A second 408 is forbidden and terminates the call; it
  cannot start a third attempt even if another Profile has a three-attempt ceiling.
- Success accounts for all attempts and cumulative waiting, but token/cost totals remain known
  lower bounds when the failed generation supplied no usage. A completed answer does not recover
  those missing tokens or prove that the abandoned upstream computation was cancelled.

This is a narrow model-inference policy, not order submission, mutating-tool retry, automatic
replacement of a terminal historical Run, or a new trading permission. Consecutive failures stop
the experiment and preserve diagnostics; generic-run external alert delivery is still not installed.

Specialized Triage Work accepts a completed regeneration chain only when its exact frozen Profile
opted in, the dispatch/failure/request identities and consecutive attempt numbers match, the output
request is unchanged, and at most one qualifying 408 precedes the completed response within the
Profile's attempt ceiling. Completed authority/Usage replay and recovery after response validation
use that same proof. An unknown trailing failure without a durable successful response remains
non-resumable; this change does not broaden the existing safe pre-generation crash-retry path.

Acceptance on 2026-09-02 passed Ruff, formatting, Pyright and all 1,672 tests. Independent review
identified cancellation-task ownership and specialized Triage replay compatibility; both were
corrected with regression coverage. The built wheel includes the legacy max and new retry Profile.
One real synthetic CPA canary then completed two model POSTs and all five frozen read-only tool
reads: 6,614 input / 7,939 output Tokens, 147,174 ms recorded model latency and USD 0.010850
estimated usage. Both POSTs succeeded without 408; actual 408 behavior is supported by injected
transport tests, not falsely claimed as observed in this canary. Two terminal reopens made no
Provider call or Journal change, and one Usage Record reopened unchanged. Its research Journal
is non-promotional. Private registration, audit and Usage identities are respectively
`5446b6f9e1f4e94012deca09d2bd0c2b0f7994a72a11191ebb3c983cc95632c4`,
`e764349f4f4558e4cf9fc0a23d50f63750159e0e468ace943149f8804dab59da`, and
`a2d1dd1693d99b9865c65860b92a428b2c3f40f6789a5a5989af7a1f660c1f0f`.
This completes the bounded transport/wiring prerequisite, not the historical Judge or strategy gate.
