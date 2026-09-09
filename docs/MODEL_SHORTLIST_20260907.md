# 模型候选与最新公开资料核验

核验时间：2026-09-07（Asia/Singapore）。本轮直接 fetch 官方 Hugging Face API、模型卡、厂商模型目录及评测站点；没有下载模型权重、连接本地服务器、启动推理/训练或调用付费模型。用途建议是研究判断，不是金融任务实测排名。

## 建议第一批候选

| 角色 | 候选 | 单卡 5090 使用建议 |
|---|---|---|
| 本地较强研究模型 | [Qwen3.8-27B](https://huggingface.co/Qwen/Qwen3.8-27B) | 因子提议、代码、较复杂证据分析。先用经过来源核验的 4/5-bit 量化；不直接使用 BF16 |
| 本地批处理对照 | [Qwen3.5-9B](https://huggingface.co/Qwen/Qwen3.5-9B) | 中文事件抽取、实体/日期/方向与反证字段。先测试非思考模式；复杂任务再独立比较思考模式 |
| 异模型对照 | [Gemma 4 12B IT](https://huggingface.co/google/gemma-4-12B-it) | 文档与图表、证据抽取和交叉检查。不同模型不是独立真值，分歧需返回原文核验 |
| 后续小模型微调 | [Qwen3.5-4B](https://huggingface.co/Qwen/Qwen3.5-4B) | 有合格训练集后再 profile LoRA/QLoRA；单卡 GRPO 仍取决于上下文、采样数及后端 |

以上四个官方模型卡/API 当前均标示 Apache-2.0。Qwen3.8-27B 是本轮发现的较新本地候选；9B/4B 选择基于资源与角色，不声称是最新发布系列。模型格式和后端支持需实际固定版本核验，不能只依赖自动生成的部署示例。

本轮核验的 [Unsloth 第三方量化文件](https://huggingface.co/unsloth/Qwen3.8-27B-GGUF/tree/main)为 UD-Q4_K_M 16.46 GB、UD-Q5_K_M 19.77 GB、UD-Q6_K 21.98 GB。它们是文件大小，不是运行显存。建议从 8K–16K 输入/输出总上下文、单并发开始实测，再按质量和峰值显存扩展；官方 262K 原生上下文不等于单卡可承载的上下文。Qwen 的 FP8 版本也不能默认优于 4/5-bit 量化的单卡容量安排。

## 用户补充：QUASAR NVFP4

[QUASAR-QAT/Qwen3.8-27B-QUASAR-NVFP4](https://huggingface.co/QUASAR-QAT/Qwen3.8-27B-QUASAR-NVFP4) 应列入 5090 第一批量化候选，与 GGUF Q5 对照。2026-09-07 核验 revision `d8e6fbfa3e3a78899b440222b827430045a05b44`：采用 QUASAR 量化感知蒸馏，教师为冻结的原始 BF16 模型，目标是保留原模型能力，不是金融领域微调。配置为 compressed-tensors NVFP4 W4A4；视觉、lm_head、MTP 等模块有高精度例外，因此不能称整个模型所有权重都是 4-bit。

作者模型卡要求 vLLM >=0.27，明确给出 RTX 5090/32GB 使用 65536 上下文的建议。那是作者部署建议，不是本机验收；实际先用 16K/单并发测质量与峰值显存，再扩长上下文。NVFP4 与 GGUF 是不同部署路径，不能直接把此 checkpoint 当 GGUF 加载到原 llama.cpp 服务，也不承诺 NVFP4 一定更快。

模型卡宣称大小 19.7GB；本轮 API 汇总五个 safetensors 文件为 20.559GB，报告保留差异，不以任一值作为完整运行显存。作者的 GPQA/AIME 比较接近 BF16，但没有本项目金融任务或对 GGUF Q5 的同条件结果。[QUASAR 方法论文](https://arxiv.org/abs/2608.13966)与该具体 checkpoint 的模型卡实验分开看待。本轮未看到独立 LICENSE 文件或 card license 字段，不能把前述四个官方模型的 Apache-2.0 核验自动扩张到这个第三方工件。未下载权重或改变服务。

## API 候选

| 用途 | 当前官方候选 | 选择依据与限制 |
|---|---|---|
| 少量复杂研究参考 | GPT-6 Astra；GPT-5.6 Sol | [OpenAI Docs](https://developers.openai.com/api/docs/models) 当前列出这两者；前者属于最新旗舰与分阶段开放范围，未核验用户账户可调用性。参考答案也需核验，不能直接作为 ground truth |
| 批量成本对照 | GPT-5.6 Luna；DeepSeek-V4-Flash-0731 | [OpenAI](https://developers.openai.com/api/docs/models)、[DeepSeek 当前模型目录](https://api-docs.deepseek.com/quick_start/pricing/)；首轮二选一，避免无必要模型矩阵 |
| 独立较强厂商对照 | Claude Opus 5／Sonnet 5 | [Opus 5 官方发布](https://www.anthropic.com/news/claude-opus-5)、[Sonnet 5 官方发布](https://www.anthropic.com/research/claude-sonnet-5)；有具体失败案例再加入 |
| 多模态/长文档对照 | Gemini 3.8 Flash | [当前稳定版模型页](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)；官方总目录已更新到 3.8，搜索缓存中的 3.7 不再是最新 Flash |

上述是公开供应商可用性信息，不是现有项目 Provider 准入、用户账户权限或训练授权。首次调用前按既有运行时和预算流程选择确切模型/版本，不沿用旧实验预算。当前价格不另做静态对比，避免忽略缓存、思考 token、峰谷和工具费用。

## 最新但不适合作为单卡主力的模型

[Qwen3.8-Flash-Next](https://huggingface.co/Qwen/Qwen3.8-Flash-Next) 为 125B 主模型、另有 51B n-gram embedding 和 4B MTP，模型卡标注 6B 活跃参数；[GLM-5.3-Flash](https://huggingface.co/zai-org/GLM-5.3-Flash) 为约 320B 总参数、18B 活跃参数。[DeepSeek-V4-Flash-0731](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-0731) 同样是大型 MoE。少量活跃参数不意味着全部权重能够放进 32 GB。多 CPU/RAM offload 可以另行探索，但本轮未取得吞吐证据，不推荐作为第一轮默认路线。

## 数值预测与金融专用候选

Qlib 第一轮继续采用线性模型与 LightGBM；LLM 负责生成或解读信息，价格预测仍需独立对照。[Chronos-2](https://huggingface.co/amazon/chronos-2) 为 120M 时序基础模型，支持多变量/协变量及 CPU/GPU，可以作为后续小成本对照。[Kronos-base](https://huggingface.co/NeoQuasar/Kronos-base) 是金融时序候选；使用前核验预训练市场及日期覆盖，避免测试集已出现在预训练中。本轮没有复现两者金融收益。

本次新发现 [TimesFM 3.0](https://huggingface.co/google/timesfm-3.0-pytorch)，但官方当前使用 TimesFM Non-Commercial License v1.0；不能将旧版授权印象带到新版或自动用于商业交易系统。暂列观察，不因此替换基线。金融名称、领域微调或金融问答分数也不能替代费用后交易收益证据。

## 如何判断是否适合本项目

先用现有开发材料建立约 30 个固定任务的便宜筛选：新闻结构化/证据引用、因子表达式/代码执行、冲突证据下的研究结论。相同来源、上下文预算、工具权限及最大重试；统计事实/数值/日期错误、反证覆盖、可执行率、延迟、显存和成本。该数量只用于筛除明显不适合者。先测试两个本地候选与一个已具备权限的 API 参考，明确失败后再加第三个本地模型，不一次启动全部候选。

胜出模型随后参加[框架研究方案](FRAMEWORK_RESEARCH_20260907.md)的同条件增量实验；研究质量、因子分数与完整账户费用后收益分别报告。最新模型可能已经知道历史行情，即使输入文档遵守 PIT，也不能自动成为严格历史盲测；历史用作开发，金融有效性仍需新的冻结/前瞻样本。

按既有偏好访问了 [DENG](https://deng.codexradar.com/en)、[DeepSWE](https://deepswe.datacurve.ai/)、[CodexRadar](https://codexradar.com/) 和 [LiveBench](https://livebench.ai/)。部分动态榜单正文未返回完整数值，本报告不杜撰排名；软件工程、通用推理与行情预测属于不同评价任务。Qwen/GLM 模型卡的 DeepSWE 成绩还使用不同 harness/预算，不能直接混排。

## 官方模型快照

以下时间为 Hugging Face 仓库创建时间（UTC 日期），不冒充正式发布日期；SHA 为本轮 API 返回的 revision。权重未下载。

| 模型 | 创建日期 | revision |
|---|---|---|
| Qwen3.8-27B | 2026-08-05 | `1d4bf0f2ff6012fd82039f2fa52739d0dd7c60c0` |
| Qwen3.5-9B | 2026-02-27 | `c202236235762e1c871ad0ccb60c8ee5ba337b9a` |
| Qwen3.5-4B | 2026-02-27 | `851bf6e806efd8d0a36b00ddf55e13ccb7b8cd0a` |
| Gemma 4 12B IT | 2026-05-23 | `707f0a3b8a3c7ad586ed01e27eafbad8a27dd0f7` |
| Qwen3.8-Flash-Next | 2026-08-24 | `de4b8e4d43b917e7706784d8bb445c9af86a3540` |
| GLM-5.3-Flash | 2026-08-25 | `690b705278a3a58e538fcb37c2ca8b5f9511213c` |
| DeepSeek-V4-Flash-0731 | 2026-07-31 | `7872f01b1d1fe23eabc4c98b48bffcef5a386062` |
| TimesFM 3.0 | 2026-08-24 | `43046b85ec22d584a13f8098c2ed39c889e129c2` |


后续实测：[DL 首轮模型与算法对照（2026-09-08）](DL_FIRST_ROUND_20260908.md)。已实际训练完整 Alpha158 的单 ETF 开发基线、完成费用一致的原生账户路径，并测试本地 Qwen3.5-9B；没有发现稳定投资增量，具体失败、采样配置与单卡边界见该报告。

第二轮实测更新（2026-09-08）：[DL 第二轮报告](DL_SECOND_ROUND_20260908.md)已完成上述 9B、27B Q5 与 Gemma 4 12B Q5 的真实文档开发／确认对照。固定确认材料中，27B 思考模式答案值 14/14 正确，值／单位／状态 12/14、全部检查 10/14；它成为后续文档提取的优先候选，仍需确定性数字与来源核验。Gemma 的围栏输出与答案内容分别评分，不把格式失败等同于事实错误。该结果不证明新闻投资增量，也不提供立即做 LoRA 的依据。全市场数值方法的扩展实验仍在进行，以第二轮报告的独立结论为准。
