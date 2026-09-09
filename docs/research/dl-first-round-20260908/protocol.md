# DL first-round development protocol

Frozen before fitting on 2026-09-08. This independent, zero-paid-API study does not
change prior registrations, terminals, source gates or broker authority.

## Price experiment

Use the existing source-verified 510300.SH fund_daily/fund_adj history (2,676 rows,
2014-01-02 to 2024-12-31), reopened from the existing read-only authority. Private
export SHA256: fd9a3d418565994f3e500c821b872c20810c13d3434b3325bb5a755d572aefd1.
Current retrieved historical versions are Modeled-PIT development data, not strict
historical availability evidence. One surviving, preselected ETF cannot establish
cross-sectional selection effectiveness.

Use native pyqlib 0.9.7 Alpha158 features (all 158), and the earlier nine-feature
ROC/MA/STD 5/20/60 subset as an ablation. OHLC use raw price times same-day factor;
VWAP = amount(thousand CNY)*10/volume(lots), times factor; volume is shares/factor.
Factors are applied per date, with no future endpoint normalization. Data version
and dividend/adjustment limitations remain explicit. Warm up 60 trading sessions.

The three existing development windows remain 2015 deleveraging (52 sessions),
2018 bear (120), 2024 policy rally (6). Features at previous completed close predict
next executable open to fifth-session adjusted close return. Only labels matured
before each training/validation boundary are eligible. Last 60 pre-test feature dates
form validation; purge training labels overlapping validation and validation labels
not matured before first test open. No parameter selection from validation or test.
Training expands from first eligible 2014 observation; each test model stays frozen.

Models per feature set: sklearn Ridge(alpha=1), training-only median imputation
and StandardScaler; LightGBM regressor, 100 trees, learning_rate=.05, num_leaves=7,
min_child_samples=20, deterministic=true, force_col_wise=true, 16 threads, seed=17.
No search, early stopping or repeat seeds in this round. Compare zero-return,
training-mean and SMA5/20 mechanical signals. Retain every model/window result.

Report validation/test error, time-series Spearman correlation (not cross-sectional
RankIC), sign coverage/accuracy, and sample size. Primary prediction scoring includes
only H5 labels matured within the registered window; later labels are not silently
added to short windows. Daily sign(prediction)>0 targets long, else cash, through the
existing native account research path. Common overnight half-position initial state,
fees, lot rules, T+1, corporate actions and raw-price execution; also double commission
rate/minimum. Compare the four existing mechanical baselines and SMA on the same
conditions. Preserve full account paths, costs, drawdown, exposure and replay evidence.
A favorable short-window curve does not authorize promotion. Stop algorithm/search
expansion if signs are inconsistent or sources/replay fail.

## GPU experiment

First public candidate Qwen/Qwen3.5-9B, official revision
c202236235762e1c871ad0ccb60c8ee5ba337b9a, Apache-2.0, BF16; vLLM 0.28.0 isolated
upstream backend, single GPU/concurrency, 16K total context, bounded output, no tools.
Fixed synthetic Chinese evidence extraction cases, with expected answers frozen
before generation, compare deterministic mechanical extraction and thinking off/on.
Measure JSON validity, source faithfulness, missingness, contradictions, injection
handling, repeat stability, latency/token counts and GPU memory. Synthetic task
quality is not investment effectiveness or pi runtime admission.

Qwen3.8-27B GGUF Q5 is a conditional stronger comparator, at most one additional
large download after the first candidate. QUASAR NVFP4 remains a feasibility candidate;
its card has no declared license field/standalone license in the inspected metadata,
so clarify derivative permissions before adopting it. Do not assume GGUF compatibility.
Do not launch RL/LLM training without useful preceding evidence. Retain caches and
failure logs. Limit numerical tasks to 16 CPU threads and one GPU task. No service,
second account engine, paid API, broker operation or automation is introduced.

## Bounded risk follow-up (frozen after return fits, before risk fits)

The first return fits are unstable, so do not expand return factor search. Test a
narrow risk-description module: forecast next five completed daily squared returns'
mean, using close-to-close adjusted log returns. This includes overnight risk and
is not an executable-open return or intraday realized variance. Volatility persistence
is a research motivation, not an assumed local result. [Corsi's HAR-RV paper](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1365738)
motivates multiple timescales, but this daily-price experiment is not HAR-RV replication.
[Moreira and Muir](https://www.nber.org/papers/w22208) motivate separating risk management
from mean prediction; their portfolio results are not transferred here.

Same development folds and label-maturity purge; fixed 1/5/20-session means of squared
returns, log transformed with floor 1e-10. Compare trailing 20-session mean, training
mean, Ridge(alpha=1, train-only scaling), and identical small LightGBM settings on
log target. No tuning or new data. Report variance MSE, QLIKE log(pred)+target/pred,
underprediction frequency and sample count. Budget <=60 CPU seconds/16 threads;
no account sizing change, statistical significance, alpha or risk-policy claim.

## Context capacity canary (before any extraction outputs)

After the 48 short extraction responses, run four separate non-thinking retrieval
canaries: approximately 4K and 14K input tokens, relevant synthetic fact at beginning
or end among explicitly irrelevant filler. Same 16K total context, 2K output ceiling,
single concurrency and model/backend. These are capacity/needle retrieval checks,
not real long-document comprehension, general long-context accuracy or throughput
optimization. Keep them outside the 12-case extraction denominator. Sample GPU memory
at 0.5-second intervals during this later run; label sampled maxima as such.

## Cutoff-owner diagnostic (after non-thinking failure)

The original non-thinking future-source case selected the post-cutoff correction
twice. Preserve both failures. In the later context-canary process, make two extra
non-thinking calls with the exact same case after a deterministic timestamp filter
removes the post-cutoff record. This isolates input eligibility from prompting or
model choice. Do not change the original benchmark denominator, claim repaired model
reasoning, or delegate source admission to the model. No production code is changed.

## Sampling correction and operational stop

After repeated 2,048-token exhaustion in the initial greedy thinking run, stop that
run operationally, preserve every completed response plus the cancelled/incomplete
planned denominator, and do not reinterpret it as a completed model-quality study.
The original temperature=0 configuration is a bounded deterministic stress trial.
The official card recommends different sampling for general tasks; it recommends a
much larger usual output budget. The present 2K ceiling deliberately tests local
extraction cost feasibility, not unconstrained benchmark capability.

Before the replacement calls, freeze one pass over all 12 unchanged cases in each
mode (24 responses): thinking temperature=1.0/top_p=.95; non-thinking temperature=.7/
top_p=.8; both top_k=20, min_p=0, presence_penalty=1.5, repetition_penalty=1,
seed=17, max_tokens=2048. Use vLLM's maintained native sampler via
VLLM_USE_FLASHINFER_SAMPLER=0 after the observed FlashInfer CUDA-header failure.
No third-party source patches. Do not select cases by prior success, repair answers,
or mix the two sampling configurations into one accuracy denominator. No repeated
seed sweep. Record original and replacement calls/costs separately.

Source: [official model card sampling and output recommendations](https://huggingface.co/Qwen/Qwen3.5-9B#best-practices).
The two filtered-cutoff diagnostic calls and four context canaries retain the original
greedy non-thinking setting to isolate their respective input changes.
