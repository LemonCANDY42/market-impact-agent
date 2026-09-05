# Continuous decision study evidence, 2026-09-05

This report separates implemented behavior from source qualification, actual model
observations and investment acceptance. The owning registration and claim contract
is [CONTINUOUS_DECISION_STUDY.md](CONTINUOUS_DECISION_STUDY.md).

## Delivered engineering

Commit `6b4d40c` closes the reviewed pre-existing pi/runtime and portfolio changes.
Commit `e3cc86d` connects cached on-demand research, dynamic A-share admission,
continuous Nautilus accounts, scoped Recall, two-step rotation, study entry points,
Watch review and offline IBKR preparation. Both commits were pushed. Unrelated
research documents and private source evidence were excluded.

The engineering node passed Ruff, formatting, Pyright, TypeScript/Node checks and
1,956 Python tests, supplemented by targeted checks of the final source-cache and
coverage changes. Independent read-only reviews covered the implementation
boundaries. The subsequent recovery/qualification fixes passed 1,991 Python tests
and 30 targeted final evidence-scope/adoption checks, plus Ruff, formatting,
Pyright, TypeScript/Node and the zero-network runtime doctor. Independent reviews
covered recovery, source qualification and evidence-scope changes. These checks do
not establish completed real-model trajectories or broker acceptance.

The current source-qualified CNY/Recall node ran the complete 2,041-test Python
suite: 2,039 passed and two test-contract regressions were identified. The historical
Recall fixture still expected duplicate full text, and the source configuration
allowlist omitted the three newly added documented APIs. After correcting these
test expectations, all three initial-adoption tests and all 28 source-provider tests
passed. Production behavior was unchanged by those final corrections. Ruff,
formatting, Pyright, TypeScript, four Node tests, production CLI preparation and the
runtime doctor passed. Independent reviews covered source qualification, CNY account
and risk, Recall, source-derived fills, and reconstructed-process recovery; the
reported risk rollover and two recovery defects were fixed and re-reviewed.

## Source and coverage qualification

The frozen study has 18 coverage cases and eight deep cases, with 54 initial
model/case diagnostics and 72 intended account trajectories. Three ordinary windows
were selected from pre-start price features by the frozen hash order. News absence,
historical dispersion and liquidity remain explicit coverage gaps where unsupported.

The original reported-limit source preflight qualifies `cn-2024-policy-melt-up`
for its six registered sessions. The other 17 coverage windows remain blocked by
one or more source or executable-baseline gaps. Dominant gaps include dated trading
rules, missing or conflicting daily limits and unsupported corporate-action
entitlements/payment timing. The new opt-in qualified scenario separately binds dated exchange rules, issuer
identity and a normal-session assumption; it retains reported-limit disagreements
as diagnostics. It blocks unsupported corporate-action reference changes and
special sessions. Current receipts do not gain strict historical PIT authority.

Sixteen new rule artifacts cover both seed ETFs over eight effective periods. The
2016 circuit-breaker interval remains an explicit qualification gap. Frozen input
manifests retain all original research receipts and case identities. Matched
source/baseline preflight uses each deep case's registered horizon; the full original
price path remains a separate diagnostic. Qualification of an optional candidate
does not replace the required initial holding's execution gate.

The qualified preflight completed with eight ready coverage cases and 32 complete
matched baselines: 2015 deleveraging, 2016–2018 quality slow bull (registered 120
sessions), 2021 sector rotation (60), 2022 reopening, 2024 broad rebound, 2024 policy
melt-up, ordinary low volatility and ordinary mid volatility. Five are registered
deep cases, making 45 trajectories source-eligible before model/decision checks;
the denominator remains 72. Thirty-six baselines have source/engine-input gaps and
four have incomplete opening execution. In particular, the COVID path crosses a
rule-artifact version boundary that the immutable engine specification currently
rejects; it is not described as missing daily prices.

The conservative full-matrix planning envelope exceeds the USD 40 authorization.
It assumes a token per frozen frame byte, output reserves and daily review in every
arm; it is not a measured cost forecast. Dispatch remains bounded by the unchanged
parent and stage ledgers, with fixed-order partial completion reported explicitly.

Official SSE suspension responses are persisted and authenticated. Both seed ETFs
have bounded full-history queries; absence is projected only from complete verified
responses. A source reuse acceptance reopened one immutable source graph for all
18 windows. Raw licensed inputs remain in the private authority store.

## Actual prospective discovery

The frozen batch uses actual newly collected news receipts and three independent
model contexts. Its five physical requests cost USD 0.409447. Replay reproduced the
entire report without another request or fee.

| Model | Actual observation | Acceptance limit |
|---|---|---|
| Luna max | Completed a broad A-share research thesis; selected no concrete candidate through a native profile query. | No dynamic candidate or portfolio acceptance. |
| Terra high | Independently queried an outside-seed company, acquired admission inputs and entered candidate research. | Continuation aborted on conflicting price-limit date arguments after two successful tool reads. This is a tool-contract failure, not an economic conclusion. |
| Sol high | Independently queried the same outside-seed company, reused source facts and completed candidate research. | Dynamic trading admission refused because the production adapter supplied no accepted trading-rule authority; this was a Harness wiring gap, not proof of missing upstream data. No portfolio or order was produced. |

The shared candidate/source outcome did not arise from sharing another model's
ranking or thesis: initial contexts were equal, while native query receipts and
responses were independently bound. The company-specific response payloads remain
private. The original failed Terra attempt and its cost are retained. A later
corrective tool implementation cannot retroactively turn that attempt into success
or reset its frozen two-Run Episode allowance.

The original v1 discovery entry had no accepted current Mock account/trading-rule
authority. Its report remains unchanged. The new v2 source-qualified CNY composition
has passed native synthetic research, portfolio, ACK, partial-fill, reconciliation
and restart checks; actual fresh-receipt execution and natural Watch review remain
unaccepted. Synthetic engineering acceptance is reported separately.

## Recovery findings

The first historical experiment dispatch stopped before model invocation because
an immutable preflight changed on replay. A failed opening allocation had persisted
an account result; the old caller treated the presence of results as a successful
seed. Recovery now revalidates the exact original seed input and full fill before
using any subsequent curve. Already-appended invalid observations remain evidence,
not an accepted registered-account path. No historical model cost arose from this
failed preflight.

The prospective query failure is preserved separately from an admission refusal or
`hold`. Correctable domain-argument errors must return actionable tool feedback
through the existing durable pi loop, under the same Run and budget. Authority and
source-integrity failures must continue to stop execution.

The first eligible historical case completed three research theses. All three
original portfolio attempts stopped before dispatch because repeated provenance
hashes inflated the prompt to approximately 1.48 MB. The model projection now
retains all economic fields and replaces only the repeated hash arrays with
content-addressed references, reducing that input to approximately 15.4 KB.

A single fixed successor may repair a signed legacy pre-dispatch failure only after
verifying zero physical reservations and unchanged research, account, sources,
profile and budget. Unknown requests are ineligible. In the actual recovery batch,
two successors completed portfolio decisions; one received answer failed validation
because it cited genuine nested thesis evidence that the old portfolio vocabulary
did not accept. Fresh bindings now version that scope explicitly and allow only
validated supporting/counterevidence refs from reopened signed theses. Initial
adoption derives the same scope from its verified source Run. Legacy bindings and
projection-recovery successors retain their original scope; the failed answer is
not reclassified as a completed decision.
The original failures and all costs remain preserved. Rolling acceptance is still
reported from complete registered account paths, not these initial decisions.

## Continuous execution cost correction

The combined live-stream and verified-parsing candidate passed 2,005 Python tests,
Ruff, formatting, Pyright, TypeScript/Node and the production runtime doctor.
Independent read-only reviews found no concrete lifecycle or cache-authority issue.

The old daily-frontier caller restarted authoritative replay from the first session
on every step: a 120-session arm would invoke 7,260 account callbacks. The explicit
live stream retains generator-local scheduling state under one lifetime episode
claim and advances each session once; `run()` still performs full replay. Restart,
changed-validator, cancellation and concurrent-claim behavior have separate tests.

A measured 20-snapshot read reparsed 21,450 immutable observations. An instance-local
bounded parsed cache reduced the profiled second pass from a cold 9.375 seconds to
0.035 seconds, retaining 20 current-file SHA256 checks and identical snapshot
objects. Current SQLite mappings and regular-file identity are checked on every
lookup. These are component measurements, not end-to-end investment-run latency.

The legacy recovery process was interrupted during local account/source validation
with no additional physical requests or unknown reservations. Its signed decisions
and partial account prefix remain the recovery authority. At that checkpoint, the
cumulative ledger recorded 117 requests, USD 6.219792 known and USD 0.011769 reserved
for the one prior unknown request.

## Legacy six-session results and Recall correction

The completed legacy v6 batch retained the fixed 54 initial-diagnostic and 72
trajectory denominators. Initial diagnostics contain 2 completed, 1 incomplete
legacy portfolio and 51 pending. Trajectories contain 2 completed, 4 research
incomplete, 3 pending legacy portfolio and 63 pending window preparation. The two
completed trajectories are Terra scheduled and event review, each 6/6 sessions.
They validate the account path only; no complete expiry-control pair exists, so
all strategy comparisons remain incomplete and no investment improvement is claimed.

All four research interruptions exhausted the Harness's 12,000 UTF-8-byte Recall
context guard. Returning full current opinions already present in the initial
input caused redundant injection. This is a Harness integration failure, not a
model judgment failure. New Run bindings validate the original signed opinion and
return a compact reference when the exact opinion is already injected; other
historical opinions still reopen and count normally. Legacy bindings and failures
retain their old semantics. The byte guard remains a conservative bound, not an
exact token measurement. Chinese payload and native pi review tests passed, with
an independent review finding no concrete issue in this repair.

At this checkpoint the cumulative ledger records 163 physical requests,
USD 8.764493 known cost and USD 0.011769 reserved for one prior unknown request.
The expanded qualified paid matrix has not dispatched. Source-qualified zero-model
preflight and replay were byte-identical; five deep cases currently have eligible
source paths, representing 45 potential tracks within the unchanged denominator
of 72, not 45 completed tracks. Missing rules, company actions and execution gaps
remain explicit.

## Execution rule provenance and latest source recovery

The v1 qualified COVID preflight was blocked at the March 13 rule-source revision.
The frozen ETF execution parameters on both sides are identical; only provenance
and effective intervals differ. The compatibility correction retains exact source
and receipt bindings while allowing equivalent execution parameters in baseline,
engine, BUY and initial-adoption paths. Actual rule changes remain blocked. The
subsequent full suite passed 2,053 tests; final native runtime/adoption and ten
policy-version checks cover the additional admission and v2/v3 isolation boundaries.
Policy v3 preserves old reports and uses a distinct baseline-journal namespace.
A new real source preflight remains required before changing eligibility counts.

The actual-receipt v2 discovery batch incurred one completed Luna request costing
USD 0.006559 before local range projection failed. Cumulative known cost is now
USD 8.771052 across 164 requests, with the same USD 0.011769 prior unknown reserve.
The cached populated interval and later empty suffix were both saved; an offline
reconstruction reproduced the receipt-time mismatch without a source refetch.
The batch is paused for verified local recovery. This is a Harness cache defect,
not missing upstream data, a model judgment error or a completed account decision.

## Outstanding acceptance

Actual historical model/trajectory results will be reported against the unchanged
54/72 denominators after qualified execution. Unseen cases, long-window strategy
comparisons, volatility/industry executable baselines without qualified mappings,
natural prospective account reviews and real IBKR Paper acceptance remain pending.
IBKR preparation is offline only and cannot send an order.
