# Round 2 protocol — frozen before inference and fitting

2026-09-08. Authorized: bounded local/DL experiments, public model/data downloads; no paid APIs, broker actions, production provider, persistent service, or new tasks/automations. Preserve all existing work. Raw third-party documents/data stay in ignored storage.

## Questions and sequence

1. Can current practical local candidates reliably extract and reconcile Chinese financial evidence with adequate reasoning output?
2. Can the pinned upstream Qlib CSI300 Alpha158/LightGBM benchmark be reproduced on its available public example data, before transferring any claim to the native account?
3. Only then identify a concrete trainable error and determine whether LoRA/AlphaGen has a justified follow-up. This round does not promise a profitable strategy or an RL training run.

## Language evaluation

- Fixed candidates: existing Qwen3.5-9B BF16/vLLM baseline; Qwen3.8-27B GGUF Q5; Gemma 4 12B IT GGUF Q5. Use maintained upstream runtimes, pinned model hashes. Quantization/runtime differences are part of deployment comparison, not isolated architecture effects.
- Candidate screening also checks recent primary leaderboard methodology and small/medium open models. Add no fourth model unless one primary result provides a concrete reason; capture missing or conflicting evidence.
- Build 30–50 source-grounded Chinese financial questions before inference, from 6 or more official issuer documents. Target numeric extraction, units/periods, accounting basis, derived arithmetic, missing information, source citation and conflicting claims. Do not relabel constructed adversarial questions as naturally occurring corrections.
- Split by issuer: approximately two thirds development, one third confirmation; freeze source/question/answer hashes. No confirmation feedback used to select model, prompt, sampling or budget. Report clustered sample limitations and possible pretraining exposure. No historical investment claim.
- The reference key is grounded in verbatim source locations and reviewed before running; never use a tested model's answer as truth. Raw source text stays private.
- Use upstream recommended sampling and templates. Non-thinking output max 2K; reasoning pilot 8K/16K/32K on frozen development subset, with truncation counted as incomplete rather than factual error. Select lowest budget with complete acceptable answers; if 32K remains inadequate, report the limitation. Context must fit input plus output. Record actual tokens/latency/memory and cold load separately.
- One fixed seed for broad screening; repeat ambiguous development comparisons only. Confirm finalists on untouched issuer split once. Report strict parse validity separately from semantic content and citation accuracy. No silent repair or scoring changes.

## Qlib reproduction

- Pin official source configuration and version; use full Alpha158 CSI300, native upstream labels/processors, train 2008–2014, validation 2015–2016, test 2017–2020-08-01 if downloadable data cover them. If not, explicitly report incomplete reproduction rather than silently replacing periods.
- Upstream LightGBM published configuration and linear baseline; freeze thread cap and random seed before fit. Do not tune to test. Record native RankIC, prediction coverage and upstream simulation metrics as method reproduction only. Its simulator is an isolated benchmark tool, not a second project execution engine or accepted native account.
- Record example dataset provenance, hash, historical constituent coverage, price adjustment limitations and licensing uncertainty; no raw data redistribution or use for live decisions. Upstream software license does not license market data.
- If reproduction is viable, document mapping gaps to native execution; do not claim native fee/T+1 acceptance from upstream simulation. Training/validation boundary label overlap is preserved for literal reproduction, quantified, and must be corrected in a separately named stricter follow-up.

## Exit

Save all failures, versions, outputs and resource costs. Stop transient inference processes. Run required repository checks at completion. Update owning research docs with actual results, not forecasted successes. Model training and native strategy promotion remain decisions requiring the evidence above.
