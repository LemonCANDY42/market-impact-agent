# Reproduction and evidence index

This directory contains aggregate research receipts and synthetic extraction inputs.
The price export, account key, raw market records, complete native journals and model
weights are deliberately absent. The study is development-only and grants no provider,
strategy or execution acceptance.

- `protocol.md`: initial choices and explicitly dated-in-sequence follow-ups/stops.
- `training-results.json`: all return-model configurations, split counts, errors, time.
- `model-audit.json`: model/prediction hashes and descriptive feature importance.
- `account-results.json`: all 54 native account paths including incomplete targets,
  both fee cases, common economic hashes and byte-stable reopen evidence.
- `fee-stress-diagnostic.json`: the discrete-lot reason one doubled-fee result improves.
- `risk-results.json`: all small risk-description comparisons.
- `prefix-results.json`: exact native Alpha158 values under prefix/future perturbation.
- `qlib-source.json`: upstream dump script byte match to its fixed revision.
- `model-integrity.json`: mirror downloads verified against independently fetched
  original-Hub file hashes. No credentials were needed or sent to the mirror.
- `extraction-cases.json`: synthetic questions and expected fields frozen before calls.
- `mechanical-extraction-results.json`: the simple non-LLM comparator.
- `*-requirements.lock`: resolved package versions for separate Linux Python 3.12 envs.
- `script-manifest.json`: SHA256 of retained experiment scripts; private artifacts
  are required for the native account reproduction.
- `verification.json`: current workspace required checks; the 212 existing warnings
  are recorded, not hidden.

## Existing local reproduction locations

Mac working directory: `/Users/kennymccormick/github/market-impact-agent`.
Private scripts/outputs: `.market-impact/dl-research-20260908/`.
The account executor imports the previous isolated research executor at
`.market-impact/investment-assessment-20260906/native-rule/run.py` without editing it.
It uses the existing local source authority and private account key in place; do not
copy those credentials to DL. It is not a portable synthetic account replacement.

```sh
uv run --with TA-Lib==0.6.8 python .market-impact/dl-research-20260908/export.py
# Copy only the private price export and secret-free manifest to the user's DL.
scp .market-impact/dl-research-20260908/etf.csv \
  .market-impact/dl-research-20260908/data-manifest.json \
  moirix-5090:projects/market-impact-research-20260908/
```

On DL, project root is `/home/dl/projects/market-impact-research-20260908`.
Create fresh environments from the retained locks if needed; do not mix frameworks
or change the system Python/driver. Qlib's source exporter is the byte-verified
`dump_bin.py` alongside the scripts. Run training only in a fresh output directory
when creating a new scientific attempt; never overwrite the original evidence.

```sh
qlib-env/bin/python train.py
qlib-env/bin/python prefix_check.py
qlib-env/bin/python risk.py
qlib-env/bin/python model_audit.py
```

Bring the frozen `predictions.csv` back to the Mac before native replay:

```sh
scp moirix-5090:projects/market-impact-research-20260908/predictions.csv \
  .market-impact/dl-research-20260908/
uv run --with TA-Lib==0.6.8 python .market-impact/dl-research-20260908/accounts.py
```

The model download was bounded to the public, fixed Qwen3.5-9B revision. Original
Hub metadata was captured independently on the Mac. The DL uses an isolated HF_HOME
with implicit tokens disabled; a token is neither required nor part of these commands.

```sh
HF_ENDPOINT=https://hf-mirror.com HF_HUB_DISABLE_XET=1 \
HF_HUB_DISABLE_IMPLICIT_TOKEN=1 HF_HOME=/home/dl/models/hf-research \
hf-env/bin/hf download Qwen/Qwen3.5-9B \
  --revision c202236235762e1c871ad0ccb60c8ee5ba337b9a \
  --local-dir /home/dl/models/Qwen3.5-9B --max-workers 2
hf-env/bin/python verify_model.py
```

For a new, separately recorded model attempt, the working upstream configuration is
`VLLM_USE_FLASHINFER_SAMPLER=0`; scripts set offline HF mode and 16 CPU threads.
Use the existing vLLM environment's `bin` in PATH. `gpu_probe_recommended.py` refuses
an existing response file, preserving original model evidence. `long_probe.py` is the
separate cutoff-owner and context-capacity diagnostic. No HTTP service is started.

```sh
VLLM_USE_FLASHINFER_SAMPLER=0 MAX_JOBS=16 NVCC_THREADS=4 \
  vllm-env/bin/python gpu_probe_recommended.py
VLLM_USE_FLASHINFER_SAMPLER=0 MAX_JOBS=16 NVCC_THREADS=4 \
  vllm-env/bin/python long_probe.py
```

The greedy trial was stopped and remains incomplete, not retroactively successful.
The replacement uses official general-task sampling with the same bounded 2K output
ceiling, which is deliberately smaller than the model card's usual recommendation.
Any whole-code-fence normalization reported later is a secondary offline diagnostic,
not an edited answer or production parser change.
