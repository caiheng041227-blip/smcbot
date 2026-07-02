# smcbot — 一个"防止自己骗自己"的 ETH 永续 ICT 信号 bot

[English](README.md) · **中文**

一个单品种(ETHUSDT 永续)的交易**信号** bot,实现了一小组 ICT / Smart-Money-Concepts
套路。它盯盘,套路触发时把信号推到 Telegram —— **由你手动下单**(纯建议,永不自动执行)。

这个仓库值得一看的地方,不是策略(策略故意做得很简单),而是包在它外面的**评估纪律** ——
恰恰是大多数散户交易仓库跳过的那部分。

> ⚠️ **非投资建议。** 这是个人研究项目。回测过往表现说明不了未来;而且一个被公开的
> edge 往往会衰减。风险自负。

---

## 诚实的数字

回测跨度 **1095 天(2023 → 2026 年中,约 3.5 年样本外)**,按我**实际的交易方式**汇报 ——
**同一时间只持一单**(不靠叠加相关信号来虚增收益):

| | 毛(Gross) | 净 @8bp | 净 @12bp | 净 @16bp |
|---|---:|---:|---:|---:|
| ΣR(单仓口径) | +49.9R | +38.7R | **+33.1R** | +27.5R |

- **成本已扣。** `--cost-bp` 按每单扣除往返手续费 + 滑点(8–16bp)。edge 扛得住,
  但比毛数字薄 —— 本该如此。
- **逐年(净 @12bp):** `2023 +6.8 · 2024 −7.6 · 2025 +4.0 · 2026 +29.8`。
  它**依赖 regime** —— 2024 是负的,2026 扛起大头。我不藏这个。
- **每单 Sharpe ≈ 0.12,p ≈ 0.07** —— *边缘显著*,未被确认。请把它当成一个
  有小幅正期望的研究系统,**不是印钞机**。

自己复现:

```bash
python3 scripts/backtest.py --days 1095 --cost-bp 12
```

---

## 为什么看方法论,而不是信号

这个项目的整个历史就是一句话:*"大多数想法都会在样本外失败。"* 这套工具的存在,是为了在
**上线前**抓住这件事,而不是等某个烂年之后:

- **单仓口径记账** —— 头条数字假设你同一时间只持一单。无限叠仓的求和(这里 +82R)
  看着漂亮但会误导;单仓(净 +33R)才是一个单仓交易者真正能拿到的。(样本唯一性
  ≈ 1.0,而叠仓是 0.79。)
- **逐年样本外(OOS)作硬门槛** —— 一个改动只有在**任何一年都不恶化**(2023/24/25/26)
  时才允许上线。就这一条规则,毙掉了我试过的绝大多数"改进"。
- **Deflated Sharpe(DSR)+ 回测过拟合概率(PBO)** —— 把"我试过几十组参数"这件事
  计入代价。选择偏差是被扣掉的,不是被无视的。
- **最小可信样本量(MinTRL)** —— 一个 edge 要多少单才能在统计上被确认。逐检测器:
  OTE `p=0.03`(已显著)、liquidity-raid `p=0.15`(接近)、order-block `p=0.89`
  (统计上是死的 —— 已移除)。

参考文献:López de Prado(《Advances in Financial Machine Learning》)、Bailey &
López de Prado(《Deflated Sharpe Ratio》;《Sharpe Ratio Efficient Frontier / MinTRL》)、
Lo(《Statistics of Sharpe Ratios》)、Kaminski & Lo(《When Do Stop-Loss Rules Stop Losses?》)。

---

## 套路

| 检测器 | 思路 | 状态 |
|---|---|---|
| **liquidity_raid** | 扫荡 equal-high/low 流动性池 + 教科书位移确认,然后反转 | 真正的 edge(毛 +24R) |
| **OTE** | 62–79% 回撤入场,加了"必须顺 30 天动量趋势"的门,定倍 2.5R 止盈 | 主要盈利来源 |
| **order_block** | premium/discount 处的 OB | 统计上是死的,已移除 |

HTF 门:日线 MSS bias + premium/discount(dealing range)+ OTE 的 30 天动量趋势门。

## 架构

```
ict_signal/state_machine.py   ICT 状态机(4h/1h 驱动,HTF 门,去重)
engine/ict_pois.py            POI 检测器
engine/ict.py                 HTF 门原语(bias / dealing range / zone)
scripts/backtest.py           回测 harness(成本、单仓口径、回撤/盈利因子、逐年)
scripts/_eval_robustness.py   DSR / PBO / PSR / MinTRL
notify/telegram_bot.py        Telegram 推送 + 命令
```

Python 3.11 + asyncio · OKX WebSocket(实盘)· SQLite · python-telegram-bot。

## 快速开始

```bash
cp .env.example .env          # 填你的 Telegram bot token + chat id
pip install -r requirements.txt
python3 scripts/backtest.py --days 365 --cost-bp 12   # 先跑回测
python3 main.py                                        # 实盘运行(推送到 Telegram)
```

密钥只存在 `.env` 里(已 git-ignore),不提交任何 key。

---

*在公开环境里构建并压力测试。如果你能在方法论里找到漏洞,请开 issue —— 那是对这个项目
最有用的贡献。*
