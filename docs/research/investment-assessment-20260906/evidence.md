# 来源、候选证据卡与集成依据

访问/核验日：2026-09-06。配套主报告：[投资能力评估](../../INVESTMENT_CAPABILITY_ASSESSMENT_20260906.md)。这是有界检索，作者报告、实现可检查、独立复现与可信前瞻记录分别对待。

## 来源索引

| ID | 原始来源 | 本轮采用的证据 / 局限 |
|---|---|---|
| S1 | Vanguard, [Cost averaging: Invest now or temporarily hold your cash?](https://corporate.vanguard.com/content/dam/corp/research/pdf/cost_averaging_invest_now_or_temporarily_hold_your_cash.pdf), 2023 | 已有整笔资金的投入时机研究；不等同工资等未来现金流定投，不能直接推导本项目收益 |
| S2 | Moskowitz, Ooi, Pedersen, [Time Series Momentum](https://www.aqr.com/Insights/Research/Journal-Article/Time-Series-Momentum), 2012 | 多资产期货/远期的作者研究；不是 SMA5/20 或 A 股受限长仓的直接验证 |
| S3 | Zhu et al., [Profitability of simple technical trading rules of Chinese stock exchange indexes](https://arxiv.org/abs/1504.04254), Physica A, 2015 | MA/TRB、历史上证/深证指数、Reality Check；作者报告加入费用后优势消失。核对摘要，未重做其样本 |
| S4 | Wang et al., [Testing the performance of technical trading rules in the Chinese market](https://arxiv.org/abs/1504.06397), Physica A, 2015 | 7,000 多规则、SPA、时期差异；CSI300 与较早上证样本不同。与 S3 作者重叠，非独立复制 |
| S5 | [TA-Lib Python](https://github.com/TA-Lib/ta-lib-python/tree/290dd191d1bd8bc07ce7da65b8b70c38f174124b) | 官方算法实现和安装约束；本轮 native 包 0.6.8，内置 C 库 0.6.4；不提供策略业绩保证 |
| S6 | Chen et al., [Dynamic Grid Trading Strategy](https://arxiv.org/abs/2506.11921), 2025 | 作者 BTC/ETH 分钟数据回测，2021-01 至 2024-07；假设与 A 股不同。零期望分析有模型前提，不能概括所有网格 |
| S7 | Kenneth French, [Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html) | 因子与组合构造及历史档案入口；不是本项目 PIT 股票仓位策略或交易数据授权 |
| S8 | Kakushadze, [101 Formulaic Alphas](https://arxiv.org/abs/1601.00991), Wilmott, 2016 | 明确公式与作者统计；公式开放不证明当前可交易净收益，亦不自动许可第三方实现和数据 |
| S9 | Davis Funds, [How Much Should We Pay?](https://davisfunds.com/about/discipline/pay) | 盈利、估值与安全边际的原始投资方法说明；不把基金回报归因于“戴维斯双击”单一规则 |
| S10 | Moreira & Muir, [Volatility-Managed Portfolios](https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12513), Journal of Finance, 2017 | 作者对波动率管理给出的因子研究。使用公开摘要；没有把其杠杆/多空结果直接迁移到长仓账户 |
| S11 | [Qlib 官方固定版本源码](https://github.com/microsoft/qlib/tree/79633dd9506ea689e5400dea0197717b5b3d74b7)、[官方基准](https://github.com/microsoft/qlib/blob/79633dd9506ea689e5400dea0197717b5b3d74b7/examples/benchmarks/README.md) | Alpha158、LightGBM 配置与作者/项目基准；本轮只实测九个 feature，不复现基准收益 |
| S12 | Liu et al., [FinRL](https://arxiv.org/abs/2011.09607), 2020；[官方源码](https://github.com/AI4Finance-Foundation/FinRL/tree/2334a5fe6d30629157f13c3b0319e1637e15e123) | 教学/研究框架与算法适配；论文市场覆盖不代表当前所有入口具有对应源/规则 |
| S13 | [FinRL-Meta 原始论文](https://arxiv.org/abs/2211.03107), NeurIPS 2022；[后续论文](https://arxiv.org/abs/2304.13174), 2023；[源码](https://github.com/AI4Finance-Foundation/FinRL-Meta/tree/15405db81ef46790d430341166700a61ba51b70d) | 市场环境与 train/test/trade 设计；保留过拟合、低信噪比及环境简化限制 |
| S14 | [ElegantRL 官方源码](https://github.com/AI4Finance-Foundation/ElegantRL/tree/24228304867bdc80165de435a598ef90b1893598) | 通用 RL 算法、actor 与训练边界；无本轮合格 A 股 checkpoint |
| S15 | [FinRL-X 论文](https://arxiv.org/abs/2603.21330), 2026；[FinRL-Trading 源码及业绩表](https://github.com/AI4Finance-Foundation/FinRL-Trading/blob/e65d6f0483ead7d2ef4a5fc940cdf960392a25c1/README.md#L142-L177) | 作者历史及 Alpaca paper 结果，不是独立券商审计；官方 FinRL-X 指向此库 |
| S16 | Daniel & Moskowitz, [Momentum Crashes](https://www.aqr.com/Insights/Research/Journal-Article/Momentum-Crashes), 2016 | 动量尾部风险研究；多空输家空头的机制不能直接套用长仓 ETF |
| S17 | Bailey et al., [The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf), 2015 | 重复试验/选优与样本外失败风险；支持保留试验分母和冻结选择规则 |
| S18 | Wang et al., [FinRL Contests](https://ietresearch.onlinelibrary.wiley.com/doi/10.1049/aie2.12004), Artificial Intelligence for Engineering, 2025-09-05 | 已读公开全文，社区标准化和赛后短样本证据；作者与 FinRL 重叠，不能当独立外部验证 |
| S19 | Grądzki, [Unstable Gains](https://doi.org/10.1016/j.jfds.2026.100205), 2026-08-19；[出版社目录](https://www.keaipublishing.com/en/journals/the-journal-of-finance-and-data-science/recent-articles/) | 元数据可核；全文直取受限，方法/结果仅有出版社搜索索引片段。本报告不采用其精确收益数字作为已复核结论 |
| S20 | [On the performance of volatility-managed portfolios](https://www.sciencedirect.com/science/article/pii/S0304405X2030132X) | 原始研究的公开索引摘要对系统性优越性提出限制；全文 403，仅作反证线索，不据此推导新的交易规则 |

S18 公布竞赛前后不同窗口的结果，赛后窗口很短，部分排名优势消失；可以支持增加独立验证，不能得出 RL 普遍无效。S19 检索片段讨论多随机种子、选优偏差和配置不稳定性，但无法完整核对全文，本轮不采纳其具体百分比。后续若训练 RL，需要预先固定 seed/模型选择政策、成本模型和多重比较处理，报告跨 seed 仓位与换手稳定性；这属于评估方法，不是降低已有投资门槛。

## 统一候选卡

“运行成本”是算法级资源需求判断，除本轮复现外未作性能测量；“角色”不赋予生产权限。每个候选的参数、模型、数据授权仍需单独冻结。

| 候选 | 输入与适用任务 | 训练/推理成本 | 来源/版本与许可 | 证据缺口/主要反例 | 集成难度与角色 |
|---|---|---|---|---|---|
| 保持初态/一直持有 | 现有持仓、源支持的 ETF/股票、现金流 | 无训练；低计算 | 当前项目四个 baseline；仓库代码许可 | 暴露不同；不能把现金当收益有效基准的唯一对手 | 低；必要对照 |
| 定投/均线定投 | 同一外部现金流时间表；后者加截止点均线与投入规则 | 无训练；低计算 | S1 方法资料；尚未选择具体新实现 | 整笔已有现金与未来收入不同；少投入的回撤低不说明风险技能 | 中；公平现金流基准/建议工具 |
| 双均线 SMA5/20 | 20 个连续已完成调整价；长仓/现金 | 无训练；低计算 | TA-Lib Python 0.6.8，BSD-2；S2–S5 | 本轮为开发复现；费用和反转会损害收益，无当前独立 alpha 证据 | 低；对照与指标工具 |
| MACD | EMA 及信号线所需历史；还需明确 entry/exit/投入比例 | 无训练；低计算 | TA-Lib，尚未冻结策略参数 | 指标不等于完整策略；本轮无净收益复现 | 低至中；工具或后续对照 |
| 动量/波动管理 | 严格滞后的收益、风险估计、敞口上限 | 无训练或有限估计；低至中 | 项目三日动量；S2/S10/S16 方法 | 期货多空和长仓股票不同；杠杆、换手和参数估计需重验 | 中；基准/风险建议 |
| 网格 | 分钟或更细的可成交路径、有限资金、库存和退出 | 无训练；路径计算中等 | S6 论文/作者代码入口；代码许可证尚未逐项接受 | 加密经验不能直接迁移；日线、多次同日卖出、单边库存风险 | 高；暂不进入可执行库 |
| 价值/质量/戴维斯双击 | 财务发布时间、历史 EPS/估值、预期版本、公司行动 | 无训练或估计模型；数据准备高 | S7/S9；具体实现未选定 | 负盈利、摊薄和事后估值扩张标签；数据未证明 PIT | 高；研究工具/未来候选，不是现成交易算法 |
| Alpha101 | 公式要求的 OHLCV、VWAP、行业与横截面 | 特征低至中；搜索高 | S8 原文；第三方代码许可未接受 | 数据/行业历史、算子语义、搜索次数；公布公式不保证收益 | 中至高；候选特征 |
| Qlib feature-only | 注册特征、明确 cutoff、完整历史及证券映射 | 无训练；本轮含进程初始化/导出约一分钟 | pyqlib 0.9.7 / MIT；主分支源码 S11 | 只实测九个特征，未实测全 Alpha158/OHLCV/横截面；环境约 738 MiB | 中；隔离只读特征，可与轻量实现比较成本 |
| Qlib LightGBM/其他预测器 | PIT 横截面、训练/验证/测试切分、fit 截止 | 训练中至高，推理较低；未测 | S11 官方配置，模型/依赖各自许可 | 本轮不训练、不复现排行榜；归一化和标签不可泄漏 | 中至高；未来分数建议器 |
| FinRL PPO/A2C/DDPG/SAC/TD3 | 绑定的 MDP、状态、动作、奖励、训练数据 | 训练高、多 seed 放大；推理需实际测量 | S12 MIT；底层依赖独立许可 | 缺本轮合格 A 股 policy；自带环境简化 | 高；仅算法研究 |
| FinRL-Meta | 数据/环境与算法连接 | 环境构建中至高 | S13 MIT | 100 股取整不等于 T+1；限价/停牌/公司行动与时点证据不足 | 高；参考环境，不作为执行权威 |
| ElegantRL | 自定义环境与 actor 工件 | 训练高；推理未测 | S14 Apache-2.0（LICENSE 明文，API 为 NOASSERTION） | 无合格 A 股 checkpoint；调整价/简单费用环境不等同交易 | 高；未来隔离 actor |
| FinRL-X/Trading | 美股数据与权重策略、Alpaca 路径 | 数据/回测/训练依策略而定，未测 | S15 Apache-2.0 | 作者 paper 非独立验收；引入整栈会重复状态/账户/执行所有权 | 只取接口参考；整体不采用 |

## 关键源码定位

- [Qlib Alpha158 handler](https://github.com/microsoft/qlib/blob/79633dd9506ea689e5400dea0197717b5b3d74b7/qlib/contrib/data/handler.py#L98-L152)：默认未来 label 必须与工具 feature 分离。
- [Qlib Alpha158 loader](https://github.com/microsoft/qlib/blob/79633dd9506ea689e5400dea0197717b5b3d74b7/qlib/contrib/data/loader.py)：本轮冻结 ROC/MA/STD 5、20、60 日九字段。
- [Qlib LightGBM 配置](https://github.com/microsoft/qlib/blob/79633dd9506ea689e5400dea0197717b5b3d74b7/examples/benchmarks/LightGBM/workflow_config_lightgbm_Alpha158.yaml)：时间切分、50 只股票 Top-k、close 成交与固定费用不能直接冒充本项目条件。
- [Qlib Exchange](https://github.com/microsoft/qlib/blob/79633dd9506ea689e5400dea0197717b5b3d74b7/qlib/backtest/exchange.py#L28-L190)、[Top-k 策略](https://github.com/microsoft/qlib/blob/79633dd9506ea689e5400dea0197717b5b3d74b7/qlib/contrib/strategy/signal_strategy.py#L138-L295)：检查交易规则和订单所有权。
- [FinRL-Meta China A-share 环境](https://github.com/AI4Finance-Foundation/FinRL-Meta/blob/15405db81ef46790d430341166700a61ba51b70d/meta/env_stock_trading/env_stocktrading_China_A_shares.py#L84-L176)：买入与卖出按当前持股执行，受检路径没有 T+1 可卖库存状态。
- [FinRL stock environment](https://github.com/AI4Finance-Foundation/FinRL/blob/2334a5fe6d30629157f13c3b0319e1637e15e123/finrl/meta/env_stock_trading/env_stocktrading.py)：通用动作与成本，不是本项目 A 股 engine。
- [FinRL-Trading requirements](https://github.com/AI4Finance-Foundation/FinRL-Trading/blob/e65d6f0483ead7d2ef4a5fc940cdf960392a25c1/requirements.txt)：Yahoo/Alpaca/bt 等路径不能作为本项目数据、账户和执行接入的替代。

## 检索与停止记录

第一轮按已有项目对照、价格规则/因子/RL 分组寻找原始来源；第二轮核对六个官方仓库的默认分支、许可证、环境/特征代码及模型工件；第三轮专门寻找成本、时期变化、竞赛赛后表现和随机种子选择的反证。新闻诊断使用原始已保存字节，未重采集。

没有使用社区盈利截图作收益结论；没有把同生态论文算独立复制。全文不可访问的 S19/S20 保留为有限证据/反证线索。没有合格 checkpoint 的 RL 名额保持空缺；这个决定来自有界官方代码与 release 检查，不代表全球不存在合格算法。完成全部候选类别覆盖后，不再为了收集更多框架名称扩大检索。
