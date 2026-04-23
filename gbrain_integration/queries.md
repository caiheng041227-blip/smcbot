# GBrain 查询示例

每条查询都是 **"自然语言问题 + 预期命中路径"** 的组合。
默认所有查询都应该带 `source: live` 过滤,除非显式要做回测对比或参数敏感性分析。

---

## 1. Setup 胜率(按切片)

### Q1.1 亚盘 bullish OB 的历史胜率如何?
- **预期命中**:frontmatter 字段过滤 `direction: long` + `poi_type` 含 "ob" + 正文 "Asia session" 语义命中 → GBrain 返回的相关记录聚合 `win` 字段得到胜率。
- **检索类型**:keyword + field filter(`hour_et` in 2..7)

### Q1.2 在 h4 结构为 bearish 时,short setup 的表现?
- **预期命中**:字段过滤 `h4_structure: "bearish"` + `direction: "short"`;vector 可以忽略,纯字段查询即可。
- **检索类型**:field filter

### Q1.3 `1h_breaker` 和 `4h_ob` 哪个 poi_source 胜率更高?
- **预期命中**:字段过滤 `poi_source in {"1h_breaker","4h_ob"}`,分组聚合。
- **检索类型**:field filter + group-by

---

## 2. 评分维度判别力

### Q2.1 score_total >= 12 的信号胜率是否显著高于全样本?
- **预期命中**:字段过滤 `score_total >= 12`,对比 baseline 胜率。
- **检索类型**:field filter(数值范围)

### Q2.2 哪些单项评分(C1..C9)在 win=1 子集里出现频率最高?
- **预期命中**:在 win=1 子集统计 C1..C9 的均值/非零率。
- **检索类型**:field filter + numeric aggregate

### Q2.3 C5(sweep 质量)为 0 的交易输得更多吗?
- **预期命中**:字段过滤 `C5 == 0`,对比 `C5 > 0` 的子集 pnl_r 分布。
- **检索类型**:field filter

---

## 3. 参数敏感性(多 backtest_run_id 对比)

### Q3.1 对比 run_id `smc_v2_187d_20260420` 和将来的 `smc_v3_*` 在 bullish_breaker 上的胜率差异?
- **预期命中**:分别按 `backtest_run_id` 过滤,得到两组胜率。
- **检索类型**:field filter + side-by-side aggregate

### Q3.2 同样的回测版本下,加上 CHoCH 收盘价确认之后过滤掉了哪些交易?
- **预期命中**:两次回测 run_id 的记录 diff(按 `time_ms` 做 set difference)。
- **检索类型**:field filter + set-diff(需要外部脚本辅助 GBrain 结果)

---

## 4. 时段效应

### Q4.1 NY 开盘(hour_et 9-11)的 long setup 胜率比伦敦盘高吗?
- **预期命中**:字段过滤 `hour_et in [9,10,11]` + `direction: "long"`,对比 `hour_et in [3,4,5,6,7]`。
- **检索类型**:field filter + numeric bucket

### Q4.2 周五(weekday=4)的交易是否更容易在 SL 出局?
- **预期命中**:字段过滤 `weekday == 4`,聚合 `outcome == "sl"` 比例。
- **检索类型**:field filter + group-by

---

## 5. Live vs Backtest gap

### Q5.1 `source: live` 和 `source: backtest` 在同类 setup(poi_type=bullish_ob)上的胜率差多少?
- **预期命中**:按 `source` 分组,对比 win rate。能暴露策略的样本外衰减。
- **检索类型**:field filter + group-by

### Q5.2 实盘里出现过 `real_slippage_pct > 0.05%` 的交易有哪些特征?
- **预期命中**:字段过滤 `source: "live"` + `real_slippage_pct > 0.0005`。
- **检索类型**:field filter(live-only 字段)

---

## 使用提示

- GBrain 对 markdown 做 vector+keyword 混合检索。**字段精确过滤**(score_total、outcome、hour_et 等)走 frontmatter;**语义探索**(比如 "sweep 很强的 setup")走 body 正文。
- 永远先做 `source` 过滤 —— 不要让一个回测 run 的噪声污染 live 推理。
- 任何"哪些交易 …"类的查询,拿到 GBrain 返回列表后,再用原始 JSONL/CSV 做精确聚合(md 是给 LLM 读的,精确数字从 JSONL 来)。
