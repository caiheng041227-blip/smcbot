# SMC 交易信号 Bot — Claude Code 工作手册

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

- **品种**:ETHUSDT(OKX 数据)+ BTCUSDT(参考,correlation gate 已禁用)
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
- `⏳ in-flight` — 当前推进中的信号(Step1~6)
- `🔧 状态` — active_signals 分布 + 最近 K 线 close
- `❓ 帮助` — 命令列表

文本命令:`/signals [小时]`(上限 168)、`/active`、`/status`、`/help`。

**授权**:只有 `TELEGRAM_CHAT_ID` 环境变量匹配的 chat 才能触发,别人发命令沉默忽略。

## 核心工作流

### 修改 POI 权重(`smc_signal/scorer.py` 的 `_POI_TIER_WEIGHTS`)

标准流程:

1. 编辑 `_POI_TIER_WEIGHTS` 的目标 key
2. 注释里写**日期 + 数据依据**,例如 `# 2026-04-23 升 1.0→3.0:187d 49 笔 49% WR +51.49R`
3. 跑回测验证:`python scripts/backtest.py --days 187 --export-csv /tmp/bt.csv`
4. 对比 NOTIFIED ΣR 的 delta,新 ΣR 必须 ≥ 旧的(净正)才合并
5. 重要变更写进 gbrain(slug 如 `backtest_comparison_{date}_{what}`)

### 跑回测

```bash
# 基础 187 天回测
python scripts/backtest.py --days 187

# 带 hybrid trail 对比(推荐做权重变更后的验证)
python scripts/backtest.py --days 187 --trail-atr 1.5

# 导出 CSV 给后续分析
python scripts/backtest.py --days 187 --export-csv /tmp/bt.csv

# 诊断特定日期前后的状态机事件
python scripts/backtest.py --days 30 --inspect 2026-04-20
```

### 重启 Lightsail(不拉新代码,只是重启)

```bash
ssh -i ~/Downloads/LightsailDefaultKey-eu-west-2.pem ubuntu@51.24.19.178 \
  'sudo systemctl restart smcbot && sudo journalctl -u smcbot -n 30 --no-pager'
```

启动后 `replay_recent_state` 会自动回放近 24h K 线重建 active_signals(config.yaml 的 `replay_hours: 24` 控制),不再每次部署丢失 in-flight 信号。

## 关键文件速查

```
smc_signal/scorer.py          — POI tier 权重 + 5 维度打分 (threshold 3.0)
smc_signal/state_machine.py   — 主状态机 Step1~6(~1500 行)
engine/poi.py                 — POI 检测器全集
engine/market_structure.py    — swing / ATR pivot / classify_structure
scripts/backtest.py           — 回测 harness(含 simulate_outcome_hybrid)
notify/telegram_bot.py        — TG 推送 + 按钮 + 命令 handler
notify/formatter.py           — 信号文本格式化
storage/database.py           — SQLite 信号/仓位落盘 + recent_signals 查询
main.py                       — 入口 + replay_recent_state 函数
config.yaml                   — 参数配置(symbol / TF / VP / replay_hours / signal)
```

## 重要常数 / 参数

| 位置 | 参数 | 值 | 含义 |
|---|---|---|---|
| `config.yaml` | `signal.score_threshold` | 3.0 | 过通知门闸最低分 |
| `config.yaml` | `signal.signal_ttl_seconds` | 129600 | Step1 TTL = 36h |
| `config.yaml` | `replay_hours` | 24 | 重启时回放窗口 |
| `state_machine.py` | `_SL_BUFFER_ATR_MULT` | 0.15 | SL = swing ± 0.15×ATR_4h |
| `state_machine.py` | `_SL_MIN_DIST_ATR_MULT` | 0.5 | SL 距 entry 下限 0.5×ATR_1h |
| `state_machine.py` | `_TP_FALLBACK_RR` | 2.0 | 结构 TP 找不到时用 2R 兜底 |
| `state_machine.py` | `MIN_STRUCTURAL_RR` | 1.5 | 结构 TP < 1.5R 直接 INVALIDATE |

## 禁止踩的坑

- **不要改 `main` 以外的分支然后推 GitHub** — Lightsail 只 pull main
- **不要在 Lightsail 上直接改代码** — 会被下次 `git reset/pull` 覆盖(现在是 git repo 了,本地改动必须 commit)
- **不要 push 到 `main` 之前跳过本地回测** — 权重调整必须 187d 回测验证
- **不要同时在本地和 Lightsail 跑同一个 bot token** — Telegram long-polling 会冲突,两边抢消息
- **不要把 `.env` 提交上去** — `.gitignore` 已排除,但小心别被 `git add -A` 误抓
- **不要 `sudo systemctl restart smcbot --no-block`** — 必须等 stop 完成再 start,否则可能数据库锁冲突

## 当前策略版本

**v3.2**(tag 在 commit `bd88287`),之后的增强:

- `5df0085` — scorer: failed_sweep_reentry 1.0→3.0 + hybrid trail + 24h replay
- `b9c4d1e` — TG 双向命令 /signals /active /status
- `e002cca` — TG ReplyKeyboard 按钮(手机端)

详细 v3.2 改动清单在 gbrain 的 `smcbot_v3.2_strategy_notes_20260423`。
