# ICT 交易信号 Bot — Claude Code 工作手册

## ⚠️ 最重要的规则(适用所有改动)

**任何代码改动都必须遵守:先汇报 → 等用户同意 → 再 commit + 部署。**

具体流程:
1. **先做本地修改**(只 `Edit` / `Write`,不动 git)
2. **必要时跑验证**(回测、syntax check、跑测试等)
3. **TG 报告**:具体改了什么文件、什么逻辑、回测对比数据(如果跑了)
4. **等用户回复**类似"同意 / 好 / 部署吧"再继续
5. **同意后**才 `git add` + `git commit` + `git push` + `ssh 部署`

**禁止:**
- ❌ 不汇报直接 commit
- ❌ commit 之后才告诉用户(已 commit 的撤销成本更高)
- ❌ 在用户没有明确"同意 / 部署 / push"等指令时擅自 push 到 GitHub
- ❌ 在用户没有明确指令时擅自 ssh 进 Lightsail 重启 service

**例外(可不报备直接做):**
- ✅ 纯查询(读 DB、读 journal、读代码、跑回测看数据,不改任何文件)
- ✅ 用户明确说"直接做"或"不用问我"时

**用户改主意了想撤销:** 用户回复"撤销 / 不要这个改动",立刻 `git checkout <file>` 或 `git revert HEAD` 恢复。


## 项目速查

- **品种**:ETHUSDT(OKX 数据)。BTC 参考流已随 SMC 管线删除(2026-06-10)
- **Repo**:GitHub private https://github.com/caiheng041227-blip/smcbot
- **本地根路径**:`/Users/nnnn/Desktop/交易计划agent/`
- **生产环境**:AWS Lightsail,`ubuntu@51.24.19.178:/home/ubuntu/smcbot/`
- **systemd 服务名**:`smcbot.service`
- **TG Bot**:`@Baicai1227_bot`(单向推送 + 双向命令)

## 部署流程(每次改完代码)

在 Mac 提交后,一条命令更新 Lightsail:

```bash
# 1. 本地提交 + 推 GitHub
cd /Users/nnnn/Desktop/交易计划agent
git add -A && git commit -m "描述改动"
git push

# 2. 同步 Lightsail + 重启(一条命令完成)
ssh -i ~/Downloads/LightsailDefaultKey-eu-west-2.pem ubuntu@51.24.19.178 \
  'cd ~/smcbot && git pull && sudo systemctl restart smcbot && sleep 5 && sudo journalctl -u smcbot -n 30 --no-pager'
```

**Lightsail 的 git 认证**:已配置 SSH deploy key(`~/.ssh/id_ed25519_smcbot` + `~/.ssh/config` 的 `Host github-smcbot`),remote URL 是 `git@github-smcbot:caiheng041227-blip/smcbot.git`。**不要改回 HTTPS**(没缓存密码)。

### 验证部署成功的标志

`journalctl` 输出里出现:
- `replay 完成: 处理 N close 事件 (cutoff=24h)` — replay 功能在跑
- `TelegramNotifier 已连接: @Baicai1227_bot  (命令接收: 已启用)` — TG 双向命令活着
- `OKX public 已连接` + `OKX business 已连接` — 数据流正常

## TG Bot 手机端命令

在 `@Baicai1227_bot` 对话里,输入框下方有 6 个按钮(点即发):

- `📊 近 6h` / `📊 近 24h` / `📊 近 3 天` — 查 notified 信号
- `⏳ in-flight` — 当前推进中的信号
- `🔧 状态` — active_signals 分布 + 最近 K 线 close
- `❓ 帮助` — 命令列表

文本命令:`/signals [小时]`(上限 168)、`/active`、`/status`、`/help`。

**授权**:只有 `TELEGRAM_CHAT_ID` 环境变量匹配的 chat 才能触发,别人发命令沉默忽略。

## 核心工作流

### 修改 ICT 参数(config.yaml 的 `ict.*` 或 engine/ict_pois.py 检测器)

标准流程:

1. 编辑目标参数/检测逻辑,注释里写**日期 + 数据依据**
2. 跑回测验证:`python scripts/backtest.py --days 365 --export-csv /tmp/bt.csv`
3. **跨 ≥2 年验证**(`--days 730` 切两年):权重/gate 对单年回测过拟合是已知坑,
   新改动必须在前后两年都不恶化才合并(教训:failed_sweep_reentry 前一年 −31.6R)
4. 重要变更写进 gbrain(slug 如 `backtest_comparison_{date}_{what}`)

### 跑回测

```bash
# 基础 365 天回测(ICT engine,需 config ict.engine_enabled: true)
python scripts/backtest.py --days 365

# 带 hybrid trail 对比
python scripts/backtest.py --days 365 --trail-atr 1.5

# 导出 CSV 给后续分析 + 两年 OOS 切分
python scripts/backtest.py --days 730 --export-csv /tmp/bt.csv
```

### 重启 Lightsail(不拉新代码,只是重启)

```bash
ssh -i ~/Downloads/LightsailDefaultKey-eu-west-2.pem ubuntu@51.24.19.178 \
  'sudo systemctl restart smcbot && sudo journalctl -u smcbot -n 30 --no-pager'
```

### 查当前走势的历史相似走势(本地工具,不影响 live)

```bash
# 默认 ETHUSDT,从本地 GB 读 8 年历史 + OKX 补最近,~4 秒出结果
python scripts/_find_similar_now.py

# 切 BTCUSDT
python scripts/_find_similar_now.py --symbol BTCUSDT

# 跳过 OKX 补齐,纯 GB(更快但 anchor 不是最新)
python scripts/_find_similar_now.py --offline
```

输出:当前 anchor 宏观(MA30/MA90/年内位置/MA90 趋势/180d Fib 档位)+ Top 3 跨时段历史相似窗口(72/120/168h 综合 cosine,5 维宏观 gate)+ 每个 Top 后续 48h 实际走向。

**注意**:本地 [data/market.db](data/market.db) 是工具用 GB(8 年 Binance 1h),跟 live 完全隔离。**这些文件不要 push 到 GitHub**,Lightsail 内存(911MB)装不下 8 年滑窗矩阵会爆 OOM,**纯本地工具**:

- [scripts/_find_similar_now.py](scripts/_find_similar_now.py)
- [scripts/backfill_history_binance.py](scripts/backfill_history_binance.py)(刷新 GB 用)

启动后 `replay_recent_state` 会自动回放近 24h K 线重建 active_signals(config.yaml 的 `replay_hours: 24` 控制),不再每次部署丢失 in-flight 信号。

## 关键文件速查

```
ict_signal/state_machine.py   — ICT 主状态机(4h/1h 驱动 + HTF gates + 去重)
ict_signal/signal_state.py    — SignalState / SignalStep 数据结构(DB/TG/tracker 共用)
engine/ict_pois.py            — 4 个 ICT POI 检测器(ob/ote/liquidity_raid/mss_retest)
engine/ict.py                 — HTF gates 原语(daily bias / dealing range / price zone)
engine/poi.py                 — OB/FVG/equal-H-L 底层检测原语(被 ict_pois 复用)
engine/market_structure.py    — swing / ATR pivot / classify_structure / detect_choch
scripts/backtest.py           — 回测 harness(含 simulate_outcome_hybrid)
notify/telegram_bot.py        — TG 推送 + 按钮 + 命令 handler
notify/formatter.py           — 信号文本格式化
notify/signal_tracker.py      — advisory TP1/SL/trail 仓位跟踪
storage/database.py           — SQLite 信号/仓位落盘 + recent_signals 查询
main.py                       — 入口 + replay_recent_state 函数
config.yaml                   — 参数配置(symbol / TF / replay_hours / ict.*)
```

(SMC 管线 `smc_signal/` 已于 2026-06-10 删除:365d −16.9R / 730d −146.8R 全 source 为负。
代码在 git 历史里,恢复用 `git log --diff-filter=D -- smc_signal/`。)

## 重要常数 / 参数

| 位置 | 参数 | 值 | 含义 |
|---|---|---|---|
| `config.yaml` | `ict.engine_enabled` | true | ICT 主开关(false = bot 不推任何信号) |
| `config.yaml` | `signal.signal_ttl_seconds` | 129600 | 信号 TTL = 36h |
| `config.yaml` | `replay_hours` | 24 | 重启时回放窗口 |
| `config.yaml` | `ict.require_displacement` | true | 反转类 POI 的教科书位移确认 gate |
| `config.yaml` | `ict.displacement_min_height_atr` | 0.0 | 位移 FVG 最小高度(勿上调到 0.5,会崩) |
| `ict_pois.py` | OB/OTE/raid `sl_atr_mult` | 0.3 | SL = 结构极值 ± 0.3×ATR |
| `ict_pois.py` | `_calc_tp_from_dealing_range` | 3.0 | TP = dealing range 对侧,兜底 3R |
| `state_machine.py` | `_dedupe_cross_type` 窗口 | 6h | 跨 POI 类型去重(拉长会亏 R,勿动) |

## 禁止踩的坑

- **不要改 `main` 以外的分支然后推 GitHub** — Lightsail 只 pull main
- **不要在 Lightsail 上直接改代码** — 会被下次 `git reset/pull` 覆盖(现在是 git repo 了,本地改动必须 commit)
- **不要 push 到 `main` 之前跳过本地回测** — 参数/gate 调整必须 365d + 730d 双窗口验证
- **不要同时在本地和 Lightsail 跑同一个 bot token** — Telegram long-polling 会冲突,两边抢消息
- **不要把 `.env` 提交上去** — `.gitignore` 已排除,但小心别被 `git add -A` 误抓
- **不要 `sudo systemctl restart smcbot --no-block`** — 必须等 stop 完成再 start,否则可能数据库锁冲突

## 当前策略版本

**v4.0 — ICT-only**(2026-06-10):删除 SMC 管线,ICT engine 转正为唯一信号源。

关键里程碑:
- `09f67c9` — ICT engine 接入(4 POI + HTF gates,与 SMC 并行)
- `4193f27` — 教科书 ICT 确认 gate(liquidity_raid 接 MSS 位移确认,365d -8.9R→+16.4R)
- 本次 — SMC 管线删除(365d -16.9R / 730d -146.8R 全负)+ ICT engine_enabled 默认 true

ICT 回测基准(2026-06-10):365d ~+37R / 730d +22.5R(前一年 -20R,regime 依赖)。
历史 SMC 资料在 gbrain 的 `smcbot_v3.2_strategy_notes_20260423`。
