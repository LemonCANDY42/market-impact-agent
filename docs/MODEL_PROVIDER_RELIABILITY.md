# Model Provider Reliability

This document owns model-generation failure, retry, circuit and operator-notice
semantics. It does not grant a model call, semantic acceptance, Judgment authority
or trading permission. The current execution architecture and qualification
status are in [Agent Runtime](AGENT_RUNTIME.md).

## Runtime and physical request ownership

All new calls use pinned pi public Provider factories and the Agent loop.
Python callbacks own physical admission, parent-budget reservation, durable
response receipt and business projection. pi/SDK automatic retries are zero.
The thin adapter exceptions and upstream revision are recorded in
[ADR 0006](adr/0006-use-pi-agent-runtime.md).

Credential-bearing fetches are restricted to the exact frozen endpoint and
method; redirects and ambient Provider configuration cannot change that route.
The reusable child receives only its frozen credential reference and minimal
process environment. Error bodies, credentials and licensed inputs are not
copied into health records or public diagnostics. Raw native responses remain
private, content-addressed evidence.

Every request, including one-shot roles, Judge, child work and summaries, uses
the same request observer and shared logical-model leases. The existing parent
Run Journal holds in-flight budget reservations; Usage Ledger remains the
accounting projection. A crash releases OS locks, not budget evidence.

Cancellation closes local I/O and stops the child when necessary. It cannot
prove that a gateway/upstream generation stopped. Gateway-internal attempts
are outside project physical-dispatch proof.

A legitimate next reasoning/tool turn is not a failed-request retry. The
qualification runner checks its case allowance before admitting a fresh turn,
but permits replay of an already durable response after allowance exhaustion.
A local zero-dispatch cap stop must not be described as a provider outage or
missing answer. The original qualification's less precise terminal is retained
unchanged; the fixed diagnostic applies to new qualification Runs only.

## Failure model

Each project-to-gateway request has an opaque correlation identity, a physical
attempt number and a durable dispatch record. Sanitized diagnostics retain:

- error class/code and HTTP status when known;
- generation state: `not_started`, `unknown`, or `response_received`;
- disposition: `safe`, `authorized_regeneration`, `forbidden`, or `terminal`;
- elapsed time, attempt count and bounded `Retry-After`.

Timeout, TLS/transport failure, stream interruption and ambiguous 5xx on a
generation POST remain unknown and are not automatically resent. An explicit
classified 429 rejection may be retried with frozen attempt/time limits and
backoff. Quota exhaustion and authentication errors are not ordinary rate
limits. Unclassified 429 does not receive invented proof of non-generation.

A complete but invalid business response is `response_received`, not a
transport failure. Minor format repair uses the current JSON boundary; any
permitted model correction is a separately counted request under the original
budget. It cannot recover missing facts, grant authority or reset a failed Run.
An empty native assistant answer is recorded with its original artifact and
usage, then follows the same bounded Judgment correction path (at most two
corrections). It cannot fail earlier merely because its projected transcript
text is empty. Reasoning remains native metadata, never an inferred final answer.
Repeated empty answers stop as a business-output failure, not a network retry.

## Explicit received-408 regeneration

The current CPA pi Profile opts into the user-authorized one-time retry of an
explicitly received generation HTTP 408. This differs from a lost connection:

- Only received `http_408` / `upstream_stream_incomplete` qualifies, within
  the Profile's total attempt limit and the original deadline.
- Backoff is at least one second and respects bounded Retry-After. Insufficient
  remaining time stops the call.
- The identical frozen request is regenerated once. An incomplete answer is
  never merged, executed or selected as an alternative.
- The first failure remains `unknown / authorized_regeneration`, not
  `not_started / safe`. A second 408 terminates; no third attempt.
- Both physical attempts remain in Journal/Usage evidence. If failed-attempt
  usage is absent, successful usage is a known subtotal and the failed attempt
  retains its conservative budget reservation.

Local timeout, cancellation, process loss, malformed output, auth/quota and
other HTTP errors do not inherit this permission. This is model inference
policy, never broker-order or mutating-tool retry. It cannot reopen a terminal
historical Run.

## Completion and recovery

Native response receipt precedes business projection and tool execution.
Tool results precede dependent model continuation. Recovery reuses durable
native messages, projections and tool results; it does not invoke the model
again. A started request without a durable response becomes
`human_input_required`; terminal and ambiguity inspection need no credentials.

An unknown response is not made safe by a "request succeeded" diagnostic.
A business write failure after receipt is repaired by replaying that receipt.
The collector must avoid holding SQL write locks during slow artifact work;
see [Data Input Harness](DATA_INPUT_HARNESS.md).

## Durable health and notification state

`ProviderHealthStore` retains sanitized incidents, circuit state and a notice
outbox in its existing private SQLite store. The specialized Triage caller
integrates it: ambiguous/transient failures enter cooldown, repeated failures
raise a persistent notice, auth/quota opens the circuit immediately.
Admission reopens only through the defined cooldown, real healthy evidence
or an explicit operator remediation/reset.

A local `assert_model_available` check only verifies configuration and worker
startup. It is not remote health or proof that a circuit recovered. A received
valid native response supplies that evidence.

Notice creation and delivery acknowledgement are durable, but no external
notification destination is installed by this migration. Generic AgentEngine
attempt traces do not independently create a fleet-wide circuit or guarantee
external alert delivery; the calling workflow owns that integration.

## Historical recovery authorities

Existing append-only Replacement/Format Recovery grants remain financial
experiment authorities over their exact historical terminals, not runtime
fallbacks. They preserve original Run/Usage, bind one authorized successor and
do not combine late old responses or replace a replacement. Current code has
no old generic Provider loop; historical runtime behavior is reopened at its
Git revision in an isolated offline checkout.

Past failure classification mistakes and canary failures remain immutable.
Their former transport implementations and obsolete operating instructions are
not current architecture. A new runtime or retry rule creates a new epoch;
it does not retroactively reclassify previous evidence.

## Verification

Exercise the real pi loop/decoder with only external I/O substituted for
deterministic fault tests: classified429, one408, repeated408, unknown stream,
cancellation, missing response, durable-response crash and duplicate replay.
Cover parent-budget contention and shared cross-process leases at their actual
owners. Do not retest the whole upstream SDK or add redundant failure wrappers.

Real qualification separately measures model identity, evidence use, native
tool/summary continuation, cache counters, concurrency and completed-response
restart. Offline 408 injection is not evidence that a real canary experienced
408; successful runtime qualification is not investment or broker acceptance.

### Bounded reliability ablation

`uv run python tests/reliability_ablation.py OUTPUT_DIRECTORY` runs a fixed
offline control/mutation matrix. It copies Python source into a disposable
directory, reuses existing production-path tests and pinned pi modules, and
removes one protection at a time there. It never changes the active runtime,
installs an acceptance record, reads model credentials or reaches a broker.
The output directory must be new; failed experiments are not overwritten.

The 2026-09-03 matrix passed 11 distinct control scenarios and detected all
12 preselected mutations in 18.28 seconds, with zero model requests. The two
sizing mutations intentionally share one positive/negative test. Protected
behaviors include cache accounting, unknown budget reservations, transient 408
recovery, compaction validation on restart, tool authorization, account
freshness, raw fill prices, existing-position deltas, mandate notional limits,
kill-aware dispatch, unknown submissions and open-order reconciliation.

This is evidence that these tests detect those specific faults, not a failure
probability estimate, proof of all possible schedules, or evidence that any
production protection can be removed. Successful ordinary paths and targeted
fault/replay tests remain necessary. Financial effects and real-model evidence
sensitivity are separate from transport and execution safety.

The separate CPA/Luna max evidence-sensitivity batch (`7ee5dade…`) completed
four fixed synthetic Runs: plant-output and feedstock interruption, each with
and without target-exposure evidence. All four read the exact tool artifact
into a later native model context. Both complete-input answers retained the
observed duration and supported 60%/25% physical share; both ablated answers
reported exposure as unknown. None invented a monetary impact or used a
physical share as a portfolio weight. All four abstained, so this demonstrates
grounded reporting and missing-information handling, **not** positive signal
recall, the quality of abstention on real markets, or a benefit from any Skill.
These are two synthetic functional examples, not independent market samples;
their assigned clock is synthetic and establishes no actual-receipt/PIT claim.

Usage was **8 physical requests, 14,067 input / 11,145 output tokens,
USD .016191** against a frozen USD .80 / 16-request experiment cap (user ceiling
USD 10, desired spend below USD 1). MiniMax was not called. All four terminal
results replayed exactly in a new process without credentials or new requests;
Usage Ledger and parent reservations reconciled with zero unsettled requests.
The original experiment preflight omitted the selected Skill tool allowlist
and was correctly rejected with zero dispatch; its registration/script remain
closed and retained. The successor reused the existing operational reader Skill
and preflighted all four bindings. Production runtime and financial controls
were unchanged; no new protocol, fallback, service or execution permission.

Local verification: 137 focused tests, full 1,727-test regression, four Node
tests, Ruff/format, Pyright and TypeScript passed. Independent read-only review
confirmed the fixed mutation failures hit their intended safeguards; paid
artifact-consumption and semantic checks were subsequently performed by the
lead. Private registrations, source mutations, failure logs, model artifacts
and reports live under `.market-impact/reliability-ablation-20260903-*`.
Do not expand or reopen the closed paid batch to improve its outcome. Next
economic evidence belongs to the separately budgeted Earnings/historical lane.
