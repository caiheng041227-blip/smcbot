# GBrain Integration for SMC Signal Bot

## 为什么有这个目录

给 SMC 信号 bot 装一层**外部记忆**:把回测 CSV 和 live 信号都汇聚到一个可
语义检索的知识图谱(GBrain)里,让后续的 LLM 分析/复盘/参数调优能直接问
"亚盘 bullish OB 的历史胜率如何"这种问题。Bot 本身**零依赖** GBrain —— 我
们只产出 JSONL + markdown,GBrain 挂了 bot 照跑。

---

## 前置依赖

GBrain(https://github.com/garrytan/gbrain)基于 Bun 运行:

```bash
# 装 Bun
curl -fsSL https://bun.sh/install | bash

# 装 GBrain(按 GBrain README)
# …这里跟随上游 README 即可
```

本目录的 Python 脚本**只用 stdlib**,不引入额外依赖。项目本体 Python ≥ 3.11。

---

## 数据流

```
┌──────────────┐          ┌────────────────────────┐
│ Bot runtime  │─log_signal─▶ jsonl/live_YYYY-MM.jsonl │
└──────────────┘          └────────────────────────┘
                                      │
  ┌──────────────┐                    ▼
  │ backtest CSV │─import_backtest_csv─▶ jsonl/backtest_<run>.jsonl
  └──────────────┘                    │
                                      ▼
                           jsonl_to_markdown.py
                                      │
                                      ▼
                          out_markdown/*.md
                                      │
                                      ▼
                    gbrain init && gbrain import
                                      │
                                      ▼
                           gbrain query "..."
```

---

## 命令顺序

```bash
# 1) 回测 CSV -> JSONL
python gbrain_integration/import_backtest_csv.py \
    --csv data/analysis/signals_187d_v2.csv \
    --run-id smc_v2_187d_20260420

# 2) JSONL -> markdown(幂等,可重跑)
python gbrain_integration/jsonl_to_markdown.py

# 3) 初始化 GBrain 索引(首次)
gbrain init

# 4) 把 markdown 灌进 GBrain
gbrain import gbrain_integration/out_markdown/

# 5) 查询
gbrain query "亚盘 bullish OB 胜率"
```

Live 信号在 bot 运行中由 `logger.log_signal(signal_state)` 持续追加到
`jsonl/live_YYYY-MM.jsonl`。周期性(每天/每周)跑一次步骤 2 和 4 即可刷新
GBrain。

---

## 注意事项

- **默认查询都应 filter `source: live`**。回测数据是参考,不是"历史真相"。
  只在参数敏感性分析或明确要做 live-vs-backtest gap 对比时才纳入
  `source: backtest` 的记录。
- **JSONL 是真源,markdown 是派生物**。schema 变了?删掉 `out_markdown/`
  重跑 `jsonl_to_markdown.py`。**不要**手改 md 文件 —— 改了下一次重跑就被覆盖。
- **logger 永不 raise**。任何失败都只 `logging.warning`,保证 bot 主线程
  永远不被记忆层拖垮。
- Bot 的 `main.py` 本身不引用这个目录。集成点是在信号触发后手动调
  `gbrain_integration.logger.log_signal(signal_state)`;补丁由用户自己打到
  合适的位置(通常是 `state_machine.py` 里信号生成/关闭的回调处)。

---

## Schema 变更 migration

TradeRecord 字段新增(加字段 + 默认 None):
- 无需 migrate。老 JSONL 反序列化时 `jsonl_to_markdown.py` 里的
  `_dict_to_record` 会用 field default 填新字段。
- 重新跑 `jsonl_to_markdown.py` 覆盖所有 md。

TradeRecord 字段重命名 / 语义变更:
1. 先在 schema.py 加新字段,保留老字段一段时间。
2. 写一次性迁移脚本把老 JSONL 里的老字段映射成新字段,输出到新 JSONL。
3. 确认 GBrain 索引 ok 再删老字段。
4. **不要**直接改动 `out_markdown/` 再 `gbrain import` —— GBrain 的向量索引
   和原始 md 双向对齐,绕过重新生成会导致漂移。

---

## 已知局限 / TODO

1. **Outcome 回填未做 reducer**。`update_outcome` 会追加一条 outcome_update
   事件到 JSONL,但 `jsonl_to_markdown.py` 目前直接跳过这些事件。需要补一个
   reducer:按 `signal_id` 把 outcome 合并回原始 signal 行,再渲染 md。
   短期 workaround:回测 CSV 里 outcome/pnl_r/win 已经齐全;live 信号的
   outcome 暂时靠手动等 signal 彻底 close 后再调 `log_signal` 快照。
2. **SignalState 字段偏移容忍度有限**。logger 用 `getattr`,字段改名不会
   raise 但会静默丢数据。建议在 `state_machine.py` 改字段时同步更新
   schema.py 的字段列表,并做一次 live JSONL 抽样校验。
3. **没有 schema validation 层**。TradeRecord 只靠类型注解,没有 pydantic
   也没有 JSON Schema。如果下游被垃圾数据污染会很难定位。后续可考虑加
   dbt/great_expectations 的 gold-layer 质量检查。
4. **markdown 文件单目录平铺**。534 条 + 未来 live 流入,几年后会有数万个
   文件同目录。GBrain import 性能 / 文件系统遍历可能变慢;后续按
   `source/YYYY-MM/` 分层。
5. **时区处理只认 UTC/ET**。hour_et 直接从 CSV 来;live 端依赖
   SignalState 是否已经填好 hour_et。夏令时切换日可能产生一两条错位记录。
