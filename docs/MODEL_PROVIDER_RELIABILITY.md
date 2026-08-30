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

The immutable 39-version triage batch contains 34 logical graph members: four map members, one
partition member, and 29 classify members. Twenty-nine completed, one classify member became
ambiguous, and four classify members were never started. Thirty members were attempted across 31
physical Provider attempts.

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

No failed or ambiguous generation is silently resent. A future replacement-run contract must keep
the original dispatch immutable, issue a distinct Run identity, prohibit combining a late original
response with the replacement, bound the number of replacements, and retain no Judgment or
execution authority. Provider reliability acceptance is a prerequisite, not replacement authority
by itself.
