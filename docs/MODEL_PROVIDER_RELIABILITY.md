# Model Provider Reliability

This document owns the failure, retry, circuit, and operator-notice boundary for model-generation
Providers. It does not change Model Provider Profile identity, semantic evaluation, Judgment
authority, or any trading permission.

## Failure model

Every project-to-gateway physical request receives one opaque correlation ID and emits a separate
Harness Journal dispatch event. A sanitized `ProviderFailure` records:

- error class and diagnostic code;
- HTTP status when present;
- correlation ID and physical attempt number;
- elapsed latency and total attempts;
- generation state: `not_started`, `unknown`, or `response_received`;
- retry disposition: `safe`, `forbidden`, or `terminal`;
- parsed `Retry-After` when present.

Error bodies, credentials, prompts, and Provider responses are not copied into the health store or
operator notice outbox.

For a model-generation `POST`, timeout, TLS/transport failure, and ambiguous 5xx all mean
`generation_state=unknown`; the project adapter must not retry them. The only accepted automatic
`POST` retry is an explicit HTTP 429 rejection classified as `not_started`, and it respects bounded
exponential backoff plus `Retry-After`. Read-only `GET` health/model discovery may retry typed
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

No failed or ambiguous generation is silently resent. The accepted Replacement Grant is a separate
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

Material-ingress v9 preserves the same dispatch, ambiguity, parser, Usage and restart evidence but
has only one model phase: one coordinator request per bounded Work Unit. Partition and classify are
deterministic Harness derivations, so they cannot create extra Provider ambiguity or receive separate
budgets. A failed blind comparison is terminalized before its active head is released; restart cannot
rerun the failed versions or reinterpret them as a semantic Decision. The terminalization authority
does not trust a caller-reconstructed failure: it first reopens the append-only Comparison
Registration and Report and replays both completed Run Journals and exact Usage sets. The ordinary
run path rejects comparison-governed v9 before Provider availability or generation. If a process
crashes after Report or terminal commit, retry reopens the first durable identity and finishes head
release without constructing or probing a Provider; only a genuinely missing Work member can resolve
and check a Provider immediately before a new dispatch. A factory or availability failure at that
pre-dispatch boundary appends a `provider.preparation.failed` diagnostic with generation
`not_started`, retry disposition `safe` and zero Provider attempts. It leaves the Run nonterminal and
creates no Usage record, returns control once, and may be re-probed only by a later invocation; it is
never converted into an ambiguous dispatch or silently retried in place.

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
`upstream_stream_incomplete / unknown / forbidden`, so it is never retried automatically.
Any other HTTP 408 from a model-generation POST is also `unknown / forbidden` unless a future
Provider proves pre-generation rejection through a separately accepted diagnostic; a read-only GET
408 remains safely retryable.

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
