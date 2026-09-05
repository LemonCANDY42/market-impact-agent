# Account decision loop

This document owns the path from admitted research or a holdings review to a controlled account action. It does
not grant paper or live capability. The Harness remains the only orchestration, portfolio-policy,
approval, execution-state and reconciliation authority.

## Smallest complete loop

```text
Trigger Admission / independently admitted account review
  -> frozen research evidence and authoritative account view
  -> analytical view (facts, forecast, assumptions, risk)
  -> portfolio Agent (current holdings/cash/orders -> desired exposure)
  -> Harness-validated Portfolio Decision
  -> deterministic Order Sizing Decision
  -> Order Intent
  -> hard policy and Trading Mandate
  -> approval
  -> durable submit / cancel / replace request
  -> Nautilus / accepted Provider
  -> complete order / fill / position / cash reconciliation
  -> next admitted review
```

The two event entrances remain different only before `Trigger Admission`:

- pre-registered policy, earnings and macro checkpoints select the first event matching their
  frozen experimental rule;
- broad material events first require formal Event Impact Triage, an authoritative EventAssessment
  and the deterministic Materiality Gate.

After admission they use the same evidence, Judgment, portfolio, policy, approval and execution
boundaries. A missing optional input is visible degradation; missing authority for a trigger,
tradable target, raw price basis, account reconciliation, mandate, approval or execution state is a
hard blocker at that owning boundary.

A research Signal is an optional upstream expression of the market thesis, not a compulsory
fiction between a portfolio decision and its order. An independently admitted account review may
start without one. The new portfolio-origin order binds the actual completed review and target;
the legacy Signal-origin path keeps its old replay contract. Scheduled operation is a separate
activation gate: a callable review does not by itself establish a running scheduler.

## Continuous market and account sensing

Market monitoring and position monitoring are one operating loop, not two competing authorities.
In the target loop, the Harness continuously collects accepted source routes, while either a
new-evidence trigger or a scheduled portfolio review freezes one Authorized Decision View and
starts a bounded Agent run. The same view may include:

- news, official releases, macro vintages, expectations and positioning;
- raw tradable bars plus cutoff-correct adjusted research series, volume, turnover, liquidity and
  volatility;
- stock and index valuation/context fields such as PE, PB, market value and dividend yield;
- instrument status, limits and the then-effective index, industry and ETF constituent graph; and
- credential-free cash, positions, open orders, recent fills, concentration and account gaps.

A broad discovery run may find a new issuer, industry, ETF, constituent set or information aspect
and request a bounded Watch. A holdings review starts from every current position and open-order
conflict before considering new exposure. Both may request accepted local-first retrieval, but only
the Harness can collect, journal and freeze new data for a fresh Run.

The event path has concrete runtime owners. `PortfolioReviewAuthority.review_account` now provides
an explicit account-only invocation; a periodic scheduler or automatic event-route activation is
still a separate gate. Neither may be simulated by relabeling old news as newly received or
manufacturing a Watch Wake. The user-approved delayed-review policy
keeps the original missed-window sample and opens a separate current-time judgment. Its accumulated
subject evidence retains original receipt/authority times; fresh contextual inputs use the new
Harness cutoff. Subject age is not the same as a stale current price or account snapshot. A
read-only reassessment need not claim a trading-session deadline; exact calendar and tradability
evidence remain required wherever a session or executable action is claimed.
The first reassessment increment is deliberately Judgment-only; its
[input and terminal boundary](EVENT_IMPACT_TRIAGE.md#current-time-reassessment-initial-judgment-boundary)
does not implement the scheduled portfolio loop or inherit any account/Signal authority.

The minimum continuous-loop activation still has three unfinished bindings. A Wake callback must
reopen the parent Watch question, prior thesis, counterevidence and invalidation conditions; a
versioned scheduler must admit recurring portfolio reviews; and either trigger must reach the same
account-aware Portfolio Review and sizing composition. The current callable components and tested
recovery paths do not by themselves prove that this loop is operating continuously. The first real
acceptance must start from an actual new receipt or a due scheduled review and end in exactly one
reconciled `hold` or controlled Intent without duplicated model or Provider work.

The Agent keeps three conclusions distinct:

1. **Market/company thesis:** what changed, confirming and disconfirming evidence, catalyst and
   invalidation conditions.
2. **Security readiness:** whether the mapped stock or ETF is liquid, tradable, valued and timed well
   enough to consider.
3. **Portfolio action:** whether the exact reconciled account should hold, reduce, close,
   rotate or add exposure.

### Review triggers and action cadence

Continuous operation has two complementary decision triggers:

- **event-driven review:** a Watch receives a new material version, contradiction, confirmation or
  registered invalidation observation and wakes a fresh research Run; and
- **scheduled account review:** a versioned market-calendar schedule freezes the latest authorized
  news, market and account state even when no single new headline is material.

The same cutoff and same frozen inputs converge on one review identity; a restart or simultaneous
trigger cannot generate a second recommendation. Every changed cutoff creates a fresh Snapshot and
Run rather than appending mutable news or prices to an earlier decision. Collection may continue at
minute-level cadence, but the LLM is invoked only on an admitted event or scheduled review. Nautilus
may execute frozen intraday rules between reviews; tick data does not cause per-tick model calls.

“Review every trading day” does not mean “trade every trading day.” The Portfolio Agent must still
return an affirmative account recommendation, including `hold`, while the Harness creates an order
only when the deterministically sized target change clears the versioned tolerance, friction,
turnover, risk and Mandate gates. This permits a later review to correct an earlier thesis while
preventing small narrative changes from causing churn.

There is no requirement to trade every day and no universal model-authored score. Missing optional
context is visible; missing or stale account truth can still support a risk alert or reduction
proposal, but cannot authorize an order when position or open-order coverage is uncertain. This
separation prevents a valid macro thesis from silently becoming an unsuitable order for the current
account.

## Authorized Decision View

### Current Agent handoff and activation boundary

`PortfolioReviewAuthority` now supplies the account-aware model producer through the existing pi
single-invocation path. `run_portfolio_review_pipeline` connects its completed recommendation to
deterministic sizing and optional durable Mock admission. The Harness-configured input source
freezes the complete Account State, Position Snapshot, Authorized Decision View, signed exposure
view, Mandate, raw prices and trading rules. No second Agent framework or account ledger is added.

The producer has two explicit entrances:

- `review(..., research_run_ids=...)` reopens every selected, completed same-root research terminal,
  including its validated Judgment and cutoff. An uncertain research view is a legitimate input;
  it does not fabricate a directional Signal or suppress the account recommendation.
- `review_account(...)` starts an independently admitted holdings review with no research Run IDs.
  The supplied account facts still require complete, fresh authority before model dispatch.

The model sees a versioned, immutable `market-impact.portfolio-prompt-projection.v1`
and bound research in one pi invocation, without a second retrieval loop. The projection
preserves all account, risk, price, operational rule and other source metadata fields;
only `rule_set.source_documents[].source_record_hashes` becomes a digest, count and
full-input CAS artifact/JSON pointer. Complete inputs remain frozen in the signed binding
and reopenable CAS artifact as the validation authority. Replay reconstructs the projection
and checks its source artifact; older bindings remain replayable. This business-context
projection does not change the pi runtime or its route qualification.
Fresh reviews bind `market-impact.portfolio-evidence-scope.v2`: permitted references
also include only the evidence/counterevidence IDs of reopened, same-root signed
research theses. The projection lists these permitted IDs explicitly; arbitrary thesis
text cannot grant a reference. Initial adoption derives this scope from the signed
source portfolio Run and reopens its bound research at the destination cutoff.
Legacy bindings and projection-only recovery successors
retain their original reference scope and prompt. Existing failed terminals remain
failed; this change does not authorize another successor or model retry. New dynamic-horizon Runs use `AgentPortfolioProposalV4`; v3 remains replayable.
V4 contains the account recommendation, target exposure, selected thesis horizon, priced-in
assessment, transmission, review point, evidence, counterevidence and invalidation. Harness injects identities and
computes quantities. Completion, raw native response, JSON normalization, physical Usage and
signed terminal ancestry persist before execution admission. A completed Run replays rather than
regenerates; an interrupted unknown request remains blocked for reconciliation.
Cancellation after dispatch closes an incomplete/cancelled terminal and Usage before propagating
the cancellation; its unknown-generation reservation stays open. Signed-terminal-before-finish
recovery preserves the cancelled state without another request.

The first paid four-scenario/three-model portfolio ablation completed 5/12 Runs
under its frozen strict contract. All seven rejected raw answers proposed an
action within the preregistered reasonable set, but reused one frozen evidence
item as both support and counterevidence. That is a valid way to express competing
interpretations, so the current V4 contract permits overlap while still requiring
every reference to belong to the frozen input. Research Thesis and Portfolio V4
also trim only surrounding whitespace in bounded narrative fields and record the
normalization paths; evidence identities and Harness-owned fields remain strict.
The old terminals remain incomplete and no Mock action was produced from the
diagnostic replay.

The corresponding `ResearchThesisV1` is an analytical answer, not an order. It
chooses one of 1/3, 5/10 or 20/60 trading sessions; the Harness derives the
corresponding `immediate`/`tactical`/`swing` band rather than asking the model
to echo that redundant classification. It states `up`, `down` or
`rangebound`; distinguishes incremental information from what appears priced
in; and records a counter-scenario, observable invalidation and suggested review
offset. It has no `abstain` action. Missing optional evidence becomes a typed
unknown; missing PIT identity or required account authority makes the Run
incomplete. The Portfolio Agent still maps every completed thesis to
`hold/open/increase/reduce/close/rotate` for the exact account, while only
deterministic Harness sizing may produce order quantity.

`Decision Recall v1` is a rebuildable projection over signed Research Thesis
artifacts in the existing Journal, not another decision ledger. Its
zero-parameter current-thesis read and bounded search only navigate; a prior
thesis can influence a Run only after the original artifact is reopened,
hash-checked, tied to its signed completed source Run, and rebound to its own cutoff.
The projection cannot admit a standalone CAS object, even when that object has a
valid content hash and thesis-shaped JSON. Current account and portfolio state
still come from their dedicated authorities rather than this disposable index.
Search is capped at eight candidates; the implementation conservatively limits
cumulative historical injection to 12,000 UTF-8 bytes, rather than measuring actual
model tokens. New Runs return a verified input reference for an already injected
current opinion; older opinions remain available through bounded reopening. Recall enforces the
new Run's `as_of`, and excludes secrets, account identifiers, paid-news bodies
and hidden outcomes. pi summaries may navigate this history but never replace
its evidence.

Scheduled review must use exact trading-session offsets for the selected
horizon. Sparse historical checkpoints may be event-review candidates, but
cannot be relabeled as D1/D3/D5 scheduled reviews. The cadence study therefore
performs a zero-cost evidence preflight and refuses paid calls until each
required cutoff is reconstructable without future information.

The selected horizon is the duration of a complete trading experiment, not a
jump from its first session to its last. Every D1 through Dn session must have a
point-in-time market/account observation, mark-to-market portfolio state,
deterministic risk/stop evaluation and simulated execution/reconciliation
result. A one-shot thesis remains active across that daily path; “one-shot” only
means the research model is not called again. Scheduled and material-event arms
may replace the active thesis at their admitted review points, after which the
new target applies to subsequent daily states. Thus drawdown, stopped loss,
missed rebound, turnover and costs come from the full path even on days when the
LLM is not invoked.

The current four historical successor sequences do not yet meet that admission:
they have sparse event snapshots at offsets 20/35, 28/58, 4/5 and 30/59 rather
than complete daily decision-state packs. The existing daily outcome prices are
kept on the scoring side of the PIT boundary. The next implementation slice must
materialize each day's cutoff-correct market/news/account inputs before the paid
one-shot/scheduled/event comparison can run.

This is a callable composition, not silent activation of the existing experiment route.
`ProspectiveDecisionPipeline.run` retains its frozen Signal-origin behavior and caller-selected
action for old experiments. Automatically selecting a control/treatment terminal, creating a
scheduled review, or consuming a real news event requires an explicit route/selection registration.
Neither historical arms nor their answers may be silently combined. The conditional research Judge
resolves disagreement; it is not the portfolio Agent. Real-market portfolio effectiveness and
broker Paper execution have not been established by this implementation.

#### Required portfolio recommendation; no abstention action

The v3/v4 portfolio producers do not expose `abstain` or a standalone `observe` action.
Every completed, admitted portfolio review must explain what to do with the supplied
account: maintain exposure (including remaining in cash), open/increase, reduce/close, or rotate.
Uncertain research is an input to that decision, not permission to omit the recommendation.
`hold` is an affirmative decision to maintain the observed exposure, not a renamed refusal to
analyze. Its rationale must explain why changing exposure is less appropriate and state the next
review or invalidation condition. A Watch or a retrieval need may accompany any recommendation;
neither substitutes for one. Use existing rationale, horizon and invalidation fields where they
suffice rather than adding ceremonial fields or scores.

For example, the same uncertain outlook may justify retaining cash for a flat account, maintaining
a suitably diversified position, or reducing an excessive concentration. These are possible
outcomes to test, not hard-coded answers: compare opportunity cost, downside, existing orders and
the applicable mandate. A positive forecast alone does not require buying, and the absence of a
new positive forecast does not require selling.

Missing authoritative account state, model failure or exhausted runtime budget is an incomplete
review/operational blocker, not an accepted portfolio `hold`. The Harness may request missing
information and prevent execution; an Agent may describe a conditional risk response but cannot
invent holdings, cash or order status. A valid recommendation also does not authorize execution:
mandate, kill, sizing, approval and reconciliation gates remain independent.

V3 whole-account `hold` omits target fields and supports remaining in cash. Historical v1/v2
artifacts retain their original vocabulary and Signal binding; they are not silently reinterpreted.
Current research experiments also retain their registered up/down/abstain forecast contract.

`PortfolioOrderIntent` (order-intent v2) binds the genuine completed portfolio review and computed
delta, not a research-side equality test. Thus positive research can result in selling an
overconcentrated long holding without inventing bearish research. The existing
`AutonomousPaperExecutionServiceV2` owns this path under either its accepted autonomous mode or
explicit `manual_each`; this slice does not activate either mode against a broker.
Manual admission reserves the rounded executable notional but cannot dispatch. Explicit approval
reopens review/sizing/current-account authority; rejection or expiry releases unsent reservations.
Dispatch still verifies Mandate, kill, freshness, Provider identity and durable operation ownership.
Unknown acknowledgement cannot regenerate an order. The legacy manual service rejects the new
intent type instead of accidentally skipping its portfolio ancestry.

Account/review evidence binds directly to the frozen view. `risk_observation_ready` alone is not
enough: a completed v3 review currently requires fresh cash, positions, open orders and fills,
matching projections and no account observation gaps. Missing facts can support a preliminary
risk alert, not a completed recommendation or an executable order. Rotation, bearish openings,
short-position execution and manual cancel/replace through this new entry remain blocked until
their specific acceptance; the existing legacy lifecycle does not automatically accept them.

The production-shaped integration tests use the real pi modules with only model network responses
replaced, then the real portfolio/sizing/manual execution/Mock owners. They exercise cash hold,
positive research with concentrated holdings and reduction, uncertain research with an independent
account recommendation, restart/explicit approval, partial and duplicate fills, final account
reconciliation, changed-account refusal, expiry, unknown generation/ACK and forged-terminal refusal.
These demonstrate wiring and safety, not spontaneous model allocation quality. The 2026-09-03
acceptance has 18 focused portfolio cases; the full checkout passed 1,766 Python tests, four Node
tests, Ruff/format, Pyright and TypeScript. Independent review identified cancelled-request Usage
omission and a mark-unit ambiguity; both are fixed and exercised through the owning production
paths, including a cancelled terminal-before-finish recovery. No paid portfolio-model request or
broker operation was made in this acceptance.

Mock account truth is derived from one immutable synthetic opening configuration and its durable
order/fill facts, with explicit current raw per-share marks; lot or unknown units are rejected
before computing marked concentration. It is not a second mutable account authority.
Simulated fills are explicitly recorded and duplicate-checked, not inferred from an ACK. This
first fixture uses immediate USD settlement and no fees; it is not a historical execution/settlement
model, broker fact, or investment P&L claim. Market and portfolio ablations must register the
appropriate costs, settlement and fill assumptions separately.

Measure each layer's contribution separately. First assess research direction, horizon, factual
support and invalidation under a fixed portfolio rule. Then compare that same frozen research and
account view with a simple deterministic allocation versus the portfolio Agent; keep mandate,
costs, fill rules and execution identical. The extra model must justify its cost through better
net outcomes or lower downside without excessive forgone upside, not merely produce a longer
explanation. Finally audit execution against the admitted target: rejected, delayed, partial and
unfilled orders are execution outcomes, not rewritten research forecasts. These are sequential
ablation questions, not a large all-combinations experiment or new permission to trade.

This adopts the separation of signal, portfolio target, risk and execution found in maintained
systems ([LEAN portfolio construction](https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/portfolio-construction/key-concepts)),
without adopting a second trading engine. Execution quality includes fees, slippage and delay,
separately from the quality of the investment view ([CFA Trade Strategy and Execution](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/trade-strategy-execution)).
For LLM-specific role separation, [TradingAgents v3](https://arxiv.org/html/2412.20138v3)
is a secondary reference for analyst-to-trader synthesis, not a reason to copy every debate role.
Its reported January–March 2024 simulation does not establish this project's multi-regime or
prospective effectiveness. Here the portfolio Agent remains a proposal producer; neither a model
acting as risk manager nor a fund-manager persona can replace Harness hard controls.

### Current read-only tool view

An Agent run may discover and call every registered read-only capability authorized for that task:

- frozen event, expectation, market, industry, macro, positioning and tradability inputs;
- a credential-free Position Snapshot projected from an accepted Account State Snapshot;
- open-order and recent-fill summaries needed to avoid duplicate or contradictory actions;
- registered historical analogies and Research Method Skills with their evidence lane intact; and
- local-first Monitoring Scope / Retrieval Resolution for missing information.

The view is broad but not omniscient. It contains exact Snapshot and tool identities, cutoff,
freshness, coverage, licensing and cost limits. When frozen local state is insufficient, the Agent
may request a bounded retrieval need. The Harness selects an accepted route, journals the result and
starts a fresh run over the new Snapshot. A transient HTTP response cannot be appended to the active
decision context.

No research or decision Agent receives a broker session, credential, arbitrary URL fetcher, raw
submission capability or mutable account ledger.

## Portfolio actions and authority

The v3 account-aware Agent proposes hold/open/increase/reduce/close/rotate. The table also shows
separate operational controls, not additional actions in that proposal schema. Persisted v1/v2
parsers retain historical vocabulary; do not reinterpret old abstention as an account recommendation.

| Proposal | Meaning | Mutation authority |
| --- | --- | --- |
| `hold` | maintain current exposure, including cash; explain why and when to review | no order; Harness may admit a bounded Watch |
| `open` / `increase` | add exposure | Harness sizing, mandate, policy and approval |
| `reduce` / `close` | lower existing exposure | Harness sizing, holdings and approval |
| `rotate` | linked reduce plus open candidates | Harness evaluates each leg; no atomicity claim |
| `cancel` | request cancellation of an open order | execution service after reconciliation |
| `replace` | cancel an exact order and create a new idempotent intent | execution service; never in-place mutation |
| `kill` | stop new dispatch and request operational escalation | Harness kill-switch authority |

The Agent may suggest target, direction, horizon, urgency and desired exposure. It cannot choose the
final executable quantity. The Harness computes or rejects quantity from the admitted portfolio
review (or exact Signal for a legacy origin),
unadjusted Price Basis, side-applicable lot/tick/tradability rules, trusted Account State Snapshot,
Trading Mandate and versioned sizing policy. Model confidence remains observational and cannot size
a position.

The additive paper v2 proposal makes that boundary explicit: the Agent may supply target instrument,
long or bearish direction, horizon, target gross-exposure ratio, rationale, counterevidence and
invalidation, but no quantity or account authority. The ratio denominator is the Trading Mandate
v2 gross-exposure limit, never inferred account NAV. The Harness computes the signed target delta
from the exact raw execution price and current marked position, then enforces the mandate's gross,
net, single-position, position-count, turnover, submission and cash limits. A bearish ordinary ETF
requires a current account-bound permission and borrow proof; an allowlisted exactly non-levered
inverse ETF is expressed as a long buy. Missing, stale, mismatched or insufficient proof fails
closed, and the Harness cannot substitute an instrument after the proposal is bound.
The binding's self-description is not proof: a Harness-owned borrow/Instrument Master authority
must reopen the exact content before Portfolio Decision admission and again at sizing, where expiry
is rechecked. Likewise, sizing accepts a Portfolio Exposure View only through the Harness-owned
exposure/reconciliation-ledger authority; a caller-created view with plausible balances or lower
turnover is not admissible. Every actionable leg binds its exact current Position Snapshot entry,
and each sized leg plus the sizing decision identity records the exact raw Price Basis hash.

Deposits, withdrawals, credential access, account-profile or permission changes, data-entitlement
purchases and broker-session administration are outside the Agent tool surface.

## State ownership

`Account State Snapshot` is a content-identified, cutoff-bound, credential-free reconciliation of
cash, positions, open orders, recent fills and explicit gaps for one opaque account reference and
environment. The Provider reports broker facts; the Harness normalizes, persists and decides
whether the snapshot is complete. An incomplete or stale snapshot may support risk notification,
but an order requires authoritative position and open-order coverage; exposure increase additionally
requires a gap-free view.

The serialized account reference is a Harness-keyed pseudonym, not an unkeyed hash of a broker
identifier. The pseudonymization key is atomically published into private state before any reader can
use it, and neither the key nor the raw reference enters the Snapshot or Agent view. A Position
Snapshot cannot be evaluated before its reconciliation timestamp.

The first real read-only adapter now connects only to a local IB Gateway Paper session with a
nonzero client identity and exposes no submit, cancel or replace method. It waits for API readiness,
then independently closes the account-download, account-summary, all-open-orders and executions
barriers before the Harness can mint an Account State Snapshot. Closing the fourth barrier freezes
one immutable callback state under the reader lock; later broker callbacks cannot drift content
beneath the earlier reconciliation time. Cash, positions, open orders and recent fills use typed
absence when a section cannot be proven complete; broker account and
order/execution identifiers are keyed pseudonyms. Its 2026-09-01 local acceptance observed one cash
record and empty position, API-open-order and recent-fill sets. IBKR requires client 0 binding to
observe manually submitted TWS orders, which is outside this non-mutating adapter; the snapshot
therefore retains `manual_tws_open_orders_not_observed`, remains usable for position-risk review and
blocks every order mutation, including an apparently risk-reducing sell: an unseen manual order may
already close or reverse the position. Exact amounts, identifiers and artifacts remain in ignored
private state.
The separate `ibkr-nautilus-paper` candidate probe has also completed one real mutation-free local
run through pinned NautilusTrader `1.231.0` and the official IB adapter. It waits until Nautilus has
connected, reconciled execution state, initialized its portfolio and started a zero-Strategy Trader,
then compares its account/open-order/open-position counts with the direct reader. That proves the
selected engine can observe the same bounded Paper account state; it neither removes the manual-TWS
coverage gap nor exercises an order command. The report and logs remain in ignored private state.

### Offline IBKR Paper preparation

`ibkr_paper_preparation` turns an exact Trading Mandate v2 source file and its
complete instrument-route map into one content-addressed offline plan. It accepts
only the existing loopback Paper configuration (`127.0.0.1`, port `4002`, client
ID `0`, `fetch_all_open_orders`, and `DAY`) and requires the current exact
per-order manual-approval mode. The plan binds both the source-byte SHA-256 and
canonical mandate/risk identities, without serializing its account scope. It
lists the receipts needed before each future order and the required handling for
incomplete coverage, manual TWS orders, client-ID collision, ambiguous
acknowledgement, disconnect/restart, partial or duplicate fill, and
cancel/replace faults.

Use the following command to print the plan and, when requested, write a new
private artifact with exclusive creation. The command accepts no broker
credentials or account identifier.

```bash
market-impact ibkr-paper prepare --mandate <private-mandate.json> --instrument-route <instrument>=<market> [--output <private-plan.json>]
```

Preparation does not import or construct the command runtime, connect to a
gateway, access credentials, bind manual orders, submit/cancel/replace orders,
or claim a fill. It always reports `pending_real_ibkr_paper_acceptance`: the
concrete driver and Provider Acceptance remain unaccepted until sealed real
Paper scenario evidence covers submit, cancel, replace, account reconciliation,
ambiguous acknowledgement, disconnect, external order, partial/duplicate fill,
gateway/process restart, and the exact accepted scope.
The Authorized Decision View recomputes freshness at its own cutoff instead of copying an earlier
readiness result, rejects exposure-increase readiness whenever any account observation gap remains,
and mints the read tool only for the exact content-identified Position Snapshot it names. A
caller-supplied lookalike tool is not part of this authority boundary. Cutoff and freeze instants are
canonical UTC in serialized content identity, so equivalent aware timestamps cannot fork replay IDs.

`Portfolio Decision` v3 binds a same-root portfolio review, its selected completed research (which
may be empty for account-only review), the exact Account/Position/Authorized Decision View and a
disposition. Account and exposure lineage is direct, not copied into a synthetic Signal. The
portfolio-origin intent reopens that review before deterministic sizing, approval and dispatch.

For the retained Signal-origin contract, Portfolio Decision binds one Judgment/Signal candidate,
the exact Position Snapshot, open-order conflicts and a disposition. Decision Admission v2 also
binds its parent Account State Snapshot. The paper composition root reprojects it from that trusted
parent, rebuilds the Authorized Decision View, and checks venue/class against a trusted Instrument
Master projection. `Portfolio Decision` is a proposal-admission boundary, not an order. `Order
Sizing Decision` is deterministic Harness output and is the only path that may create the quantity
used by an Agent-originated `OrderIntent`.

Portfolio Decision v2 represents rotation as two linked non-atomic legs. The source reduce/close leg
is sized first. The destination open/increase leg is recorded as
`blocked_pending_source_reconciliation`; it cannot create an Order Intent until a later exact
reconciliation proves the source transition and the Harness rebuilds current account and exposure
views. The contract does not claim an atomic broker combo or let the Agent pre-authorize both legs.

The additive autonomous Paper v2 execution owner consumes only this v2 chain. Its daily Trading
Mandate is Paper-only and `autonomous`, denominated in USD, and capped at $10,000 gross exposure,
the -$10,000 to +$10,000 net band, ten positions, $50,000 daily turnover, fifty submissions, a $300
daily-loss kill and a $1,000 strategy peak-drawdown kill. `account_id` is the opaque
`account-ref-...` identity from Account State, not a raw broker account identifier. Its content and
identity include the canonical `LocalDataSnapshotStore.harness_authority_id`; the same mandate
cannot be recreated as authority in another root. `PaperExecutionService` owns the durable Provider
acceptance, and autonomous admission and dispatch can obtain a lease only by reopening that exact
content-addressed acceptance in the same Harness authority root. The lease binds
Provider version, opaque account, exact Trading Mandate content hash, instrument-route hash, Paper
environment, accepted market, market-order capability, DAY time-in-force and validity. That mandate
hash participates in the lease identity and is rechecked on service open and every Provider mutation;
a lease cannot be reopened under a more permissive same-account mandate. Legacy lease payloads
without this binding fail closed and are not upgraded in place. An unsealed acceptance-shaped object
or a caller-provided equality verifier has no authorization role. The lease, mandate-day risk state,
outbox and reconciliation tables share the store's persisted `harness_authority_id`; a fresh root
cannot mint or reopen the original root's lease or reset its baseline, even if it records a new
acceptance for identical Provider facts. The same Harness composition root supplies historical,
mock and IBKR providers; none receives a separate authority path.

No per-order human approval is introduced on this path. The Harness still persists the exact Policy
Evaluation, Trading Mandate binding and policy approval before creating a durable outbox lease.
At Mandate-day activation the canonical Harness authority transactionally initializes day-start and peak
equity from the exact authoritative reconciled Account State and Exposure View. It never accepts a
baseline, peak, cash-flow adjustment, measurement, source hash or kill label from a caller. Each
subsequent authoritative observation updates the persisted peak monotonically and internally
recomputes daily P&L and drawdown from mandate-currency settled cash plus marked net exposure.
External cash flow is zero unless a future broker-ledger authority is introduced; a caller cannot
adjust it. A stale risk observation activates a durable kill and blocks increases, while exact
reduce/close, cancel and reconciliation remain available. Each
admitted operation atomically reserves its
submission, turnover, signed/gross exposure delta, cash and position-count effects. A consumed
Exposure View cannot authorize a second distinct decision, and a newer view is evaluated together
with every still-active reservation plus the durable daily submission and turnover ledger.
Unknown acknowledgement, incomplete coverage, reconciliation difference, stale/incomplete account
state, daily loss, peak drawdown or Provider loss activates a durable kill. A kill blocks new or
increased exposure but leaves already admitted exact reduce/close, cancel and reconciliation
operations available. Interrupted submitting leases become `unknown` on restart and are never
blindly retried. Provider reconciliation alone cannot clear coverage or release a reservation: the
Harness reconciliation authority itself rebuilds a newer Account State and Exposure View from that
exact Provider reconciliation snapshot. Release additionally proves terminal orders are absent from
open orders, exact receipt fill IDs and quantities appear in recent fills, and the resulting signed
positions reflect those fills; merely copying a pre-fill view under a new snapshot hash fails. An
ambiguous submit or cancel remains killed until the order has
an authoritative terminal state; an `accepted` or `unknown` receipt is insufficient. Clean terminal
reconciliation is the only route that can clear acknowledgement gaps; strategy risk kills remain
active. Before cancellation, one short canonical Harness transaction atomically claims the exact
lease and cancellation attempt. Provider-side validation reopens only that durable claim, and the
Provider call runs without holding the SQLite write lock. Revocation is an explicit durable request:
it immediately blocks new mutation claims, remains pending while an already-authorized call is in
flight, and becomes final when that call records its receipt or unknown acknowledgement. Direct
deletion of an active claim is rejected. Restart converts a stranded claim to `unknown`, applies any
pending revocation and never retries it. A root-specific `0600` OS advisory lock permits exactly one
active autonomous service for each canonical Harness root and is acquired before any recovery. A
second service fails without changing the durable claim; clean `close()` or context-manager exit
releases the lock, while an OS process exit releases it automatically so a successor can recover.
The lease records its acquiring process and is close-on-exec: a forked child cannot use the copied
service, and closing or destroying its copied descriptor never unlocks the parent's lease.
Rotation destination admission remains impossible until a new post-source-reconciliation v2 decision
and sizing chain is built.

Agent-directed admission, human approval and dispatch each re-read the trusted current Account State
source. The exact snapshot must still match the admitted parent and remain inside the Harness
composition-root maximum age; a caller-provided Position Snapshot cannot widen it. Dispatch repeats
the check after the durable claim and the Provider capability validator repeats it with all hard
expiries immediately before acceptance. Otherwise the pending approval or claimed outbox row expires
without a Provider call and a fresh decision is required. A manual approval never extends
account-state validity. Legacy Decision Admission v1 is replay-only and cannot mutate paper state.
Close quantities also obey the applicable lot rule; the Harness does not silently submit a
nonconforming full-position quantity.

Submit and cancel each have stable request identity, a durable attempt lease and an `unknown`
outcome for ambiguous transport. No ambiguous operation is retried automatically. A replacement is
never an in-place mutation: it durably links one exact cancellation to a new Order Intent identity,
and that new intent cannot be admitted until complete reconciliation proves the old provider order
canceled. An Agent-directed replacement additionally requires a fresh Decision Admission for the
new quantity, price and account state.

Cancellation uses an optional Provider port and a sealed Harness capability bound to the exact
request, manual approval, Provider identity/version, provider order and current durable attempt. A
Provider command receipt is only acknowledgement; a globally complete reconciliation must
explicitly report `canceled` before the Harness marks either the cancellation or original order
terminal. Pending cancellation work has priority over new submissions. Every later complete
reconciliation also rechecks each durable accepted open order rather than treating its first
reconciliation as permanent truth.

The durable kill switch blocks every new submission claim while leaving exact cancellation and
reconciliation available. It does not silently mass-cancel orders, and it cannot be cleared until a
new complete reconciliation has occurred after activation. Reconciliation v2 binds the kill-switch
generation observed before the Provider snapshot call, so a pre-activation snapshot cannot race and
clear a later kill. Restart with any durable accepted open order also blocks new execution until a
fresh reconciliation, including legacy rows that cannot prove their Provider binding. Transport
success never establishes broker order, fill, position or cash state.

## Delivery gates

1. **Read-only portfolio context.** Complete. Normalized Account State and full Position Snapshot
   contracts, fixture acceptance, one real local IB Gateway Paper read and the frozen
   `AuthorizedDecisionView`/`read_position_snapshot` Agent tool are accepted. No credential or
   mutation capability is exposed. This is bounded `ACCOUNT` read acceptance only; its explicit
   manual-order coverage gap keeps all order mutations closed.
2. **Decision and sizing.** The Signal-origin `manual_each` Mock path remains accepted. Its content-
   identified Portfolio Decision and Order Sizing Decision bind the exact Signal, parent
   Account State, Authorized Decision View, Position Snapshot, Trading Mandate, raw Price Basis,
   trusted Instrument Master identity, side-applicable venue/class rule and versioned sizing policy.
   Exposure increase requires a gap-free view and no conflicting open order. Reduction and close
   require authoritative open-order coverage; the current accepted A-share rule artifact only claims
   ordinary buy-order scope, so long-position sells remain fail-closed until an accepted sell rule is
   added. The Agent cannot write quantity and confidence never sizes an order. Decision Admission v2
   reopens this full chain after restart before approval or dispatch; both stages also require the
   bound Account State to remain current and unchanged. Open, blocked increase, blocked uncertain
   reduction, non-lot close rejection, hold, rotate rejection, adjusted-price rejection and the
   complete manual approval→mock accepted→reconciliation path pass locally. This is still synthetic
   account/mock execution acceptance, not external broker-paper evidence. The added v3 pi producer
   and portfolio-origin intent use the same v2 sizing/risk owner, complete account/research ancestry
   and explicit manual approval. Its synthetic USD-denominated ETF case demonstrates positive research plus
   reduction, native-response replay, Mock partial/duplicate/final fills and account reconciliation;
   it does not inherit A-share sell rules or broker acceptance. See the
   [current handoff](#current-agent-handoff-and-activation-boundary) for activation limits.
3. **Operation lifecycle.** The legacy Signal-origin Mock cancel/replace boundary is accepted. Cancel has exact manual
   approval, sealed capability, durable attempt state, restart recovery, ambiguous-ACK handling and
   reconciliation-established terminal state. Replace is cancel plus a new intent and cannot submit
   before the old order is reconciled canceled; cancellation creation and replacement linkage share
   one SQLite transaction. This does not accept an external Provider or yet
   prove close/rotate portfolio behavior against a broker. The v3 manual portfolio entry does not
   yet accept cancel/replace; do not route its orders through a legacy service to bypass that gate.
4. **Operational control.** The durable kill switch is complete for the mock boundary: it blocks new
   submissions, preserves cancel/reconciliation, survives restart and requires a post-activation
   complete reconciliation before clearing. Durable approval inbox/notifications and reconciliation
   escalation remain open. Wake remains separately gated.
5. **IBKR paper execution.** Implement and independently accept `ibkr-nautilus-paper` for submit,
   cancel, replace, fill, ambiguous acknowledgement, restart and complete account reconciliation.
   The accepted direct account read, mutation-free Nautilus readiness probe and mock evidence cannot
   satisfy this gate. The disabled adapter candidate, durable identity/no-redispatch boundary,
   provider-neutral partial/full-fill reconciliation and external Provider Acceptance artifact are
   now implemented and tested with an injected runtime. Acceptance is validity-bounded and binds
   configuration, anonymous account scope, Instrument routes, markets and order types; durable
   bindings prevent cross-account reuse. Fill state is cumulative and monotonic, while partially
   filled orders retain an exact cancel path for the remainder. The concrete long-lived Nautilus command
   runtime and real broker fault evidence remain open; therefore no external Paper execution
   capability is advertised.
6. **Live.** Require explicit authorization, versioned live mandate and limits, credential
   isolation, tested kill switch and separate live Provider Acceptance.

Backtest and prospective work must share research, portfolio-policy and order semantics.
Backtests use cutoff-bound simulated Account State and deterministic fills; paper/live use actual
reconciled state. Neither lane upgrades the other's evidence authority.

## Decision-path parity and replay

Backtest, paper and live must use the same decision path up to the execution Provider. They must bind
the same frozen evidence/tool view, Agent runtime and Skill surface, Judgment/portfolio validation,
deterministic sizing, Order Intent, hard policy and mandate semantics. Signal-origin legacy and
portfolio-origin v3 ancestry remain distinguishable; neither is silently converted into the other.
Environment-specific code may supply only the owning facts it must own:

- a backtest supplies cutoff-bound simulated account state and later deterministic fills;
- paper supplies reconciled paper-account state and broker events; and
- live supplies separately authorized live-account state and broker events.

This is decision-path equivalence plus replay-safe idempotence, not a claim that a nondeterministic
model will independently emit byte-identical text. The first physical model call is journaled under
one Run identity; restart or audit reopens its frozen response and tool results rather than calling
the model again. A materially different Snapshot, retrieval result, account state or policy creates
a new Run. Research features may use cutoff-correct adjusted/total-return prices, while fills, fees,
limits and order prices always use the raw tradable basis.

## Effectiveness evidence

The zero-cost [reliability ablation](MODEL_PROVIDER_RELIABILITY.md#bounded-reliability-ablation)
exercises the real financial control owners with synthetic account/Provider
fixtures. Removing freshness, raw-price, position-delta, notional, kill,
unknown-submission or open-order reconciliation protections causes the owning
tests to fail. This establishes targeted fault sensitivity, not IBKR Paper
acceptance or profitable/risk-reducing investment decisions. Production risk
controls are never disabled for an ablation.

Engineering acceptance and investment effectiveness are separate. The common loop must first prove
that it can replay the same frozen inputs, account state, policies and costs across historical,
mock-paper and broker-paper environments. Strategy or Skill promotion then requires independent
chronological cases in its declared market, event family and horizon, with later outcomes opened only
after the decision is sealed.

Reports compare after-cost results against the relevant cash/no-action, scheduled investment,
index/ETF buy-and-hold and simple trend or volatility baselines. They report return together with
maximum drawdown, tail loss/CVaR, Sharpe and Sortino, adverse excursion, downside capture, turnover,
liquidity and the opportunity cost of avoided exposure. A claimed risk-avoidance decision must show
the loss reduced relative to holding and also disclose rallies it missed. One lucky event, one market
regime or a lower drawdown obtained only by staying in cash cannot promote a strategy.

The exact registration, denominator, balanced return/risk gate, concentration checks and stopping
rules are owned by `AGENT_EFFECTIVENESS_ACCEPTANCE.md`. This document owns the account and decision
loop only; it does not independently promote a strategy or Skill.

A reusable general or domain-specific Skill remains a candidate until it has more than one
independent validation, no unresolved material counterexample or conflict with an existing Skill,
and an observational trace showing when it was offered, loaded, reported as used and influenced the
proposal. Promotion is limited to the demonstrated domain; with/without-Skill comparisons test
incremental value rather than attributing the whole Agent result to the Skill.

### Continuous historical A-share account

`HistoricalStreamingAccount` in `streaming_nautilus_account.py` owns one pinned
NautilusTrader `1.231.0` engine and one CNY settlement account per historical arm.
The [upstream streaming lifecycle](https://nautilustrader.io/docs/latest/concepts/backtesting/apis-and-runs/)
is used directly: add a session batch, `run(streaming=True)`, `clear_data()`, and
`end()` only when the arm closes. The pinned local `backtest/engine.pyx` documents
this same lifecycle. A shared synthetic `HIST` venue supports one cash account
across XSHG/XSHE; source symbols and exchange identities remain in the Account State.
It is a historical BACKTEST provider, separate from the USD and CNY local Mock Paper mandates,
IBKR and live execution.

The caller supplies source-backed instrument rules and raw, unadjusted session bars,
and owns Instrument Master/PIT verification and policy admission. `register_instrument`
adds a newly admitted equity or `exchange_traded_fund` to the running engine; there is
no requirement to trade the initial instrument universe forever. `advance_session`
consumes already admitted `ExecutableOrder` records and returns actual engine fills,
explicit no-fill/partial-fill reasons, cash, daily NAV and the existing canonical
`AccountStateSnapshot`. Every held instrument needs a raw close for daily valuation.
The adapter never generates research, changes a mandate or admits its own Agent orders.

A validated effective rule may change its source reference while every execution
parameter remains identical. `execution_rules_compatible` excludes only that
reference at registration, baseline readiness, BUY bounds and initial adoption;
identity, venue, class, tick, lot, price limits and all fees must still match.
Ordinary spec equality, journal opening configuration and exact signed adoption
receipts retain their provenance. Continuous experiment policy v3 gives the new
comparison a distinct batch identity and isolates baseline journals from v2;
previous reports and account prefixes are preserved.

The default starting cash is CNY 100,000. `bootstrap_half_hs300` takes an explicit
prior-session raw 510300 bar and buys approximately half the capital through the same
engine. Fees reduce opening NAV; a missing, suspended or incompletely filled seed
cannot be claimed as the required overnight allocation. Recovery validates the exact
persisted opening input and complete fill before accepting that allocation; an existing
journal alone is not readiness. Reopening an unsuccessful seed neither submits it again
nor promotes later observations to an accepted seeded-account curve. Cash and positions then persist
across all sessions. Overnight long inventory limits sales (including same-session
buy/sell attempts), with full residual odd-lot liquidation supported. Suspension,
zero volume, absent bid/ask liquidity and side-applicable daily limits yield explicit
no-fill evidence. IOC market execution uses the available raw opening quote, with
zero configured slippage; it does not invent intraday liquidity from daily OHLC.
The existing A-share commission model is reused per instrument, with explicit ETF
stamp-tax exemption. Partial fills and fees come from Nautilus, not a parallel ledger.

Session inputs, corporate actions, registrations and admitted order identities are
atomically published and fsynced before engine mutation. A single-writer journal lock
prevents competing engines for one arm. On interruption the adapter reopens and replays
that exact durable input prefix into one replacement engine; no Agent/model call is
part of recovery. Any failed execution instance must be discarded before continuation.
Source-backed net cash dividends use the upstream `SimulationModule` exchange account
adjustment and prior-session share entitlement at the effective open. Split/bonus-share
transitions remain explicitly unsupported and block the session before persistence:
no public pinned position-adjustment transition has passed acceptance, and adjusted
price factors must never impersonate corporate-action cash or shares. The data owner
must supply the actual payable per-share cash and effective date; this adapter does
not infer tax or payment dates from price adjustments.

### Offline IBKR Paper preparation

`market-impact ibkr-paper prepare` freezes the exact private Paper Mandate source,
instrument routes, pinned runtime, per-order checklist and fault matrix without
constructing a broker runtime or connecting a socket. The current static adapter
scope is loopback Gateway port 4002, client ID 0, all-open-order coverage and DAY;
the existing HK/US routes are preparation capabilities, not new trading permission.
The CNY historical account does not activate this adapter. Preparation always
reports `execution_accepted: false` until separately authorized real Paper evidence
passes its own acceptance gate.

For each future approved Paper order, the generated checklist requires, in order:

1. Fresh complete account, positions, open orders and execution reconciliation.
2. Same-root exposure view and raw executable-side price evidence.
3. Exact order intent, policy result and unchanged versioned Mandate binding.
4. Human approval for that exact order.
5. Current sealed Provider acceptance, capability and exclusive session lease.
6. Complete reconciliation after any transport or broker response.

The preparation artifact owns the machine-readable fault matrix:

| Failure | Required response and recovery evidence |
| --- | --- |
| Stale or incomplete account coverage | Block new orders; obtain complete fresh reconciliation. |
| Uncovered manual TWS order | Prove client-ID-0 external-order coverage before mutation. |
| Client-ID-0 collision | Do not start the command runtime or submit. |
| Ambiguous submit or ACK | Preserve unknown state, stop new submissions and reconcile. |
| Disconnect or Gateway restart | Invalidate the session and reconcile under a fresh scope. |
| Partial or duplicate fill | Preserve monotonic cumulative fills and reconcile. |
| Process restart | Reopen the exact durable operation without redispatch. |
| Cancel or replace failure | Retain the original state until reconciliation proves the change. |

Submit, cancel, replace, account reconciliation, ambiguous ACK, partial/duplicate
fills, external orders, disconnect, Gateway restart and process restart each need
real sealed acceptance receipts. Offline tests and local Mock evidence do not
satisfy those broker scenarios.

### Current CNY local Mock authority

The durable `MockExecutionProvider` also supports the explicit
`cny-local-mock.v1` opening authority. CNY cash and overnight opening inventory
are immutable, including the source reference and opening timestamp. Later
source-qualified instruments append admission evidence without rewriting the
opening account; venue/class identity cannot change across qualification
revisions. The projection remains the existing Mock order/fill journal, not a
second account ledger or execution engine.

An accepted order is only an ACK. Only explicit simulated fills change cash or
positions. CNY fills require explicit fees; purchases also require a future
sellability timestamp. The Harness must derive fees and the next trading-session
open from accepted rules/calendar before recording these facts. The provider
does not infer a trading calendar or commission schedule. It deducts fees,
rejects cash overdrafts, and enforces settled sellable inventory at both order
submission (including outstanding sells) and fill recording. Repeated fill IDs
must retain exact quantity, price, fee and sellability authority across restart.
`simulated_sellable_quantity` exposes that same journal projection for sizing;
canonical Account State continues to report total held quantity. Reconciliation may
supply its exact Provider snapshot to the projection; current durable receipts
must match before the Account State adopts that snapshot timestamp and identity.

Only the CNY `TradingMandateV3` `local_mock` scope accepts the registered study
risk envelope: CNY 100,000 gross/maximum net exposure, zero minimum net exposure,
five positions, CNY 100,000 daily turnover, ten daily submissions, CNY 10,000
daily-loss kill and CNY 20,000 peak-drawdown kill. Universe-only renewals retain
account/policy/day risk authority, prior peak equity and account-day activity.
CNY loss-risk sessions roll at UTC midnight. Fresh authoritative equity initializes
a new session atomically during evaluation; stale evidence cannot establish a new
baseline. Same-day renewals retain the starting equity and historical peak. USD
retains its legacy mandate-bound risk key and baseline semantics.
An unresolved operation prevents switching away from its original mandate;
reopen that mandate and reconcile first. The USD envelope remains available,
and CNY Mock evidence grants no IBKR or live capability.

For that exact CNY envelope, an autonomous mandate may admit a completed,
same-root Portfolio Review directly under `harness_policy` approval. The service
reopens the completed review and recomputed sizing during admission, dispatch
and Provider capability validation. It does not create a human approval record,
and a raw Agent proposal without a completed review cannot use this CNY path.
USD portfolio-origin orders retain their existing `manual_each` boundary.

The source-derived local Mock fill adapter requires a traded minute strictly after
submission and an unexpired accepted market order. It derives commission and sell
stamp tax from the qualified rule and BUY sellability from a contiguous calendar
through the next open session. Its full-order simulation policy is recorded in an
immutable artifact; it does not claim broker liquidity or fills. Missing quote,
company-action or calendar authority returns typed gaps without moving funds.

Prospective review capture freezes account-day ledger facts with the original
account and cutoff. Recovery reopens the persisted operation and lease before
attempting fresh admission, including after ACK, partial fill or quote expiry.
An expired quote cannot erase a submitted order or authorize a new one. The
reconciliation owner remains accessible and reports
`fresh_account_exposure_rebuild_required` until a genuinely new frozen
`reconciliation_input` supplies current marks. `reconcile_prospective_mock_review`
uses that source input for the explicit fill adapter and existing reconciliation
owner, preserving the original review and order identities.
