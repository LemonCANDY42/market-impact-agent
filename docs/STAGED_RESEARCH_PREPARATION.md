# Staged research preparation

This entry freezes three independent development studies without invoking a model,
opening a broker, constructing a trading account, or creating a `ModelBudget`. A
preparation is an auditable input package. It is never paid authorization or evidence
that a model route, study result, or investment capability has been accepted.

## Fixed stages

Each stage has two preselected development windows. The JSON specification owns the
ordered session list and therefore the denominator; missing manifests, calendar rows,
research records, system results, or budget capacity become explicit gaps and never
remove a registered opportunity.

The original Stage 1 specification includes a known reversal development case.
That label describes its selection, not a requirement for subsequent panels.
Source-first follow-ups freeze their selection policy before inspecting target
outcomes and need not classify any window as a reversal. They still remain
development samples and retain all source, calendar, horizon and budget gates.

If an otherwise present window references an unavailable research, reference-source,
raw-observation, source-config, calendar, suspension, band, or seed CAS object, the
whole source-backed window falls back to its registered placeholder opportunities with
a typed `window_source_artifact_missing:*` gap. A present object whose bytes, content
hash, source identity, cutoff, or account binding disagree remains a hard failure.

| Stage | Arms | Registered opportunities | Research horizon | Predecessor gate |
| --- | --- | ---: | --- | --- |
| 1, event evidence | `price_only`, `price_and_event` | 2, one per window | H5 primary, H1 auxiliary | None |
| 2, target selection | `fixed_seed`, `dynamic_candidates` | 2, one per window | H5 primary, H1 auxiliary | Complete comparable Stage 1 result |
| 3, continuous review | `expiry_only`, `scheduled`, `event` | 20, exactly ten sessions per window | Remaining registered H1/H3/H5/H10 | Complete comparable Stage 2 result |

Every paired arm receives the same research question, target, cutoff, profile, prompt
hashes, and common-pairing hash. Stage 1 changes only the evidence projection. Its
price-only package lists exact completed-price references; its event package adds only
references with concrete pre-cutoff publisher records and source artifact proof. Stage
2 and Stage 3 packages reopen the same reference list for each arm.

The frozen profile is `gpt-5.6-terra` at `high` effort. Preparation hashes the current
`RESEARCH_THESIS_V2_PROMPT`, `PORTFOLIO_REVIEW_PROMPT_V3`, complete provider profile,
and local pi runtime identity. Opportunity-scoped tool descriptors remain pending the
separate execution registration because their Recall and acquisition authority depends
on the accepted predecessor and exact account scope.

## Offline commands

The source and Harness authority roots are explicit. These commands only read the
private source roots and write immutable files beneath `--state-root`.

```bash
uv run market-impact agent continuous-study prepare \
  --study-spec examples/research/staged-study-1-v1.json \
  --state-root .market-impact/staged-research-20260906/final-v5/stage-1 \
  --research-inputs-root .market-impact/continuous-20260905/study/research-inputs-cash-only-inception-v4 \
  --authority-root ~/.local/state/market-impact-agent/model-runtime/continuous-study-20260905-usd40 \
  --provider-profile examples/providers/pi-cpa-terra-high-v2.json

uv run market-impact agent continuous-study preflight \
  --study-spec examples/research/staged-study-1-v1.json \
  --state-root .market-impact/staged-research-20260906/final-v5/stage-1 \
  --research-inputs-root .market-impact/continuous-20260905/study/research-inputs-cash-only-inception-v4 \
  --authority-root ~/.local/state/market-impact-agent/model-runtime/continuous-study-20260905-usd40 \
  --provider-profile examples/providers/pi-cpa-terra-high-v2.json

uv run market-impact agent continuous-study report \
  --study-spec examples/research/staged-study-1-v1.json \
  --state-root .market-impact/staged-research-20260906/final-v5/stage-1
```

Change the stage number in both the specification and output root for Stage 2 or 3.
`run` with `--study-spec` is refused: paid execution requires a result-based execution
registration, complete-group admission, current runtime-route acceptance, and separate
authorization.

The formal retrospective is also read-only. It stores run/source-level private detail
under `--state-root` and prints aggregate cost, thesis, proxy outcome, and matched
momentum summaries:

```bash
uv run market-impact agent continuous-study retrospective \
  --state-root .market-impact/continuous-20260905/retrospective-formal-v1 \
  --report-path .market-impact/continuous-20260905/continuous-experiment-execution-report-cash-only-v4-resumed-generation-v1.json \
  --preflight-hash 0cc7079515ec99b0fa24a1db7cfe50d9f5d0df5d9b51526d06c5e3fc68101ba1 \
  --authority-root ~/.local/state/market-impact-agent/model-runtime/continuous-study-20260905-usd40
```

## Readiness and cost interpretation

`preparation_complete` means the immutable preparation and report template were
written. `source_ready` means all registered source and predecessor gates passed.
`ready` remains false because this entry grants no execution authority. Runtime-route
acceptance and paid authorization appear separately in `execution_gaps`.

The cost graph is deliberately conservative. One role Run can admit at most eight
logical turns and two physical attempts per turn. The physical token envelope is
263,808 input plus 8,192 output tokens, or 625,920 microusd at the frozen price; all 16
attempt slots yield a 10,014,720-microusd per-Run token envelope. Complete-group
admission must use that envelope because an unknown physical attempt can retain its
reservation while a received-408 regeneration uses another slot. The full two-window
funding bounds are 120,176,640 microusd for Stage 1, 240,353,280 for Stage 2, and
3,605,299,200 for Stage 3. These include two research successors per review and, where
applicable, one portfolio projection recovery and one post-reconciliation rotation
destination review. They are bounds, not allocations.

For a successful response, the pi boundary reduces affordable output against the
300,000-microusd profile usage cap. That number excludes unresolved failed-attempt
reservations and therefore cannot fund a complete group.

The historical Terra figures in the preparation are attribution-only references:
4,968,002 microusd over 165 rolling requests and a 25,785-microusd median paid Run.
Those heterogeneous prior Runs are not a predicted complete-group cost.

## Current private preparation gaps

The 2026-09-06 offline preparation found qualified dated event records for the 2020
closure-shock window. The 2024 broad-rebound source contains price evidence but no
qualifying event record, so Stage 1 remains source-incomplete. Stage 2 retains the
genuine historical industry/company exposure gap in both windows and the missing Stage
1 predecessor result. Stage 3 must retain its Stage 2 predecessor gate. Its initial
account package reopens the source-backed prior-session half-510300 seed, its matching
engine-journal prefix, and a signed `portfolio.review.frozen` binding with a complete
reconciled fill, cash, position and exposure view. It does not mutate that account.

The original preparation reported `runtime_route_qualification_pending_current_build`
after the runtime budget authority changed. A later zero-call preparation/reopen on
2026-09-06 found the current route accepted: all three original specifications now
report only `separate_paid_authorization_required` in execution gaps. Their source
and predecessor gaps remain, so none is ready to execute. This recheck creates no
paid authority and does not alter the original preparation. The bounded
[readiness receipt](research/efficient-progress-20260906/staged-followthrough-readiness.json)
keeps this original-specification check separate from later source-first panels.

Direction scoring uses an inclusive rangebound band of plus or minus 0.5 times the
sample standard deviation of the last 20 completed daily returns, scaled by the square
root of the horizon. `unknown` counts in response completion but is neither direction
covered nor scored. Both completion and direction coverage divide by every registered
opportunity, including source-insufficient, system-failed, and budget-stopped rows;
conditional hit rate divides only scored rows.

## Authorized execution extension

The later [Stage 1 model pilot](STAGE1_MODEL_PILOT.md) registers verified source
supplements and a separately authorized budget without rewriting this offline
preparation. Its execution results, qualification and model selection gates are
separate from the preparation readiness described above.
