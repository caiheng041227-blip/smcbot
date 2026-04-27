"""历史回测 harness:用 Binance 历史 K 线 replay state_machine,统计信号命中率。

局限:
- 不依赖 aggTrade,VP 用 1h K 线 close 价 + volume 近似(用于 C7 / OB VP 支撑)
- Delta 用 (close>open)? +vol*0.6 : -vol*0.6 近似,C3/C5 打分参考但不精确
- 不模拟 Telegram 通知 / DB 持久化,只打印结果

使用:
  python scripts/backtest.py --days 30
  python scripts/backtest.py --days 90 --symbol ETHUSDT --rr-min 2.0

输出:每条通知级信号 + 前瞻 72h 的 TP/SL 模拟结果,末尾 win-rate 汇总。
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.candle_manager import CandleManager
from data.rest_backfill import fetch_klines
from engine.delta import DeltaTracker
from engine.key_levels import KeyLevels
from engine.market_structure import classify_structure
from engine.volume_profile import VPResult
from smc_signal import SignalEngine, score as signal_score
from utils.logger import logger, setup_logger


# --- VP / Delta 近似 -------------------------------------------------------

class StubWeeklyVP:
    """用 1h K 线近似累计周 VP(周日 17:00 ET 锁定上周)。"""

    def __init__(self, tz_name="America/New_York", bucket_count=200, value_area_pct=0.70):
        from engine.volume_profile import compute_profile, _bucket_idx
        self._cp = compute_profile
        self._bi = _bucket_idx
        self.tz = pytz.timezone(tz_name)
        self.bucket_count = bucket_count
        self.value_area_pct = value_area_pct
        self._bucket_size: Optional[float] = None
        self._buckets: Dict[int, float] = {}
        self._week_key: Optional[str] = None
        self.prev_vp: Optional[VPResult] = None

    def _week_key_of(self, ts: datetime) -> str:
        local = ts.astimezone(self.tz)
        shifted = local - timedelta(hours=17)
        dsunday = (shifted.weekday() + 1) % 7
        return (shifted - timedelta(days=dsunday)).date().isoformat()

    def add_kline(self, ts: datetime, price: float, vol: float) -> Optional[VPResult]:
        key = self._week_key_of(ts)
        locked = None
        if self._week_key is None:
            self._week_key = key
        elif key != self._week_key:
            if self._bucket_size is not None:
                self.prev_vp = self._cp(self._buckets, self._bucket_size, self.value_area_pct)
                locked = self.prev_vp
            self._buckets = {}
            self._week_key = key
        if self._bucket_size is None:
            self._bucket_size = price * 0.4 / self.bucket_count
        idx = self._bi(price, self._bucket_size)
        self._buckets[idx] = self._buckets.get(idx, 0.0) + vol
        return locked

    def current(self) -> Optional[VPResult]:
        if self._bucket_size is None:
            return None
        return self._cp(self._buckets, self._bucket_size, self.value_area_pct)


# --- 前瞻 TP/SL 模拟 -------------------------------------------------------

def simulate_outcome(
    sig_time_ms: int,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    future_bars: List[dict],
    max_hours: int = 72,
) -> Dict[str, object]:
    """从 sig_time_ms 之后的 K 线前瞻,找先触及 SL 还是 TP。
    max_hours 现在是"信号后多少根 bar"(不管 bar 粒度)。
    """
    bars_after = 0
    for k in future_bars:
        if int(k["t"]) <= sig_time_ms:
            continue
        bars_after += 1
        if bars_after > max_hours:
            break
        hi, lo = float(k["h"]), float(k["l"])
        if direction == "long":
            sl_hit = lo <= sl
            tp_hit = hi >= tp
        else:
            sl_hit = hi >= sl
            tp_hit = lo <= tp
        # 保守口径:同一根 K 内先触 SL(避免高估 TP 命中)
        if sl_hit:
            return {"outcome": "sl", "pnl_r": -1.0, "bars": bars_after}
        if tp_hit:
            r = abs(tp - entry) / abs(entry - sl) if entry != sl else 0.0
            return {"outcome": "tp", "pnl_r": round(r, 2), "bars": bars_after}
    return {"outcome": "pending", "pnl_r": 0.0, "bars": bars_after}


def simulate_outcome_hybrid(
    sig_time_ms: int,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    atr_4h: float,
    future_bars: List[dict],
    max_hours: int = 168,
    tp1_portion: float = 0.5,
    trail_atr_mult: float = 1.5,
) -> Dict[str, object]:
    """混合出场:SL 全仓 / TP1 平 `tp1_portion`(结构 TP)/ 剩余用 peak - `trail_atr_mult` × ATR_4h 追踪止盈。

    仓位单位化:初始 1.0,按比例减仓后更新 remaining。
    返回 {outcome, pnl_r, bars, tp1_hit, trail_price(若触发)}。
    - outcome:'sl' / 'tp1_only'(TP1 但 trail 未闭)/ 'tp1_trail' / 'pending'
    - pnl_r:合计 R(以 entry 到 SL 的全仓风险为 1 单位)
    """
    if entry is None or sl is None or tp is None or entry == sl:
        return {"outcome": "pending", "pnl_r": 0.0, "bars": 0, "tp1_hit": False}
    risk = abs(entry - sl)
    if risk <= 0:
        return {"outcome": "pending", "pnl_r": 0.0, "bars": 0, "tp1_hit": False}
    # 安全兜底:ATR 缺失时 trail 距离用 0.5R(就近跟)
    trail_dist = trail_atr_mult * atr_4h if atr_4h and atr_4h > 0 else 0.5 * risk

    peak = entry
    tp1_hit = False
    realized_r = 0.0
    remaining = 1.0
    bars_after = 0

    for k in future_bars:
        if int(k["t"]) <= sig_time_ms:
            continue
        bars_after += 1
        if bars_after > max_hours:
            break
        hi, lo = float(k["h"]), float(k["l"])

        # 同一根 K 内:先判 SL,再判 TP1,最后判 trail —— 保守低估胜率
        if direction == "long":
            if lo <= sl:
                realized_r += remaining * (sl - entry) / risk  # = -remaining
                return {"outcome": "sl", "pnl_r": round(realized_r, 3),
                        "bars": bars_after, "tp1_hit": tp1_hit}
            if not tp1_hit and hi >= tp:
                realized_r += tp1_portion * (tp - entry) / risk
                remaining -= tp1_portion
                tp1_hit = True
                peak = max(peak, hi)
            if hi > peak:
                peak = hi
            if tp1_hit:
                trail_stop = peak - trail_dist
                if lo <= trail_stop and trail_stop > entry:
                    realized_r += remaining * (trail_stop - entry) / risk
                    return {"outcome": "tp1_trail", "pnl_r": round(realized_r, 3),
                            "bars": bars_after, "tp1_hit": True, "trail_price": trail_stop}
                # trail_stop 仍在 entry 下方时,兜底让 SL 继续起作用
        else:
            if hi >= sl:
                realized_r += remaining * (entry - sl) / risk  # = -remaining(short: entry<sl)
                return {"outcome": "sl", "pnl_r": round(realized_r, 3),
                        "bars": bars_after, "tp1_hit": tp1_hit}
            if not tp1_hit and lo <= tp:
                realized_r += tp1_portion * (entry - tp) / risk
                remaining -= tp1_portion
                tp1_hit = True
                peak = min(peak, lo)
            if lo < peak:
                peak = lo
            if tp1_hit:
                trail_stop = peak + trail_dist
                if hi >= trail_stop and trail_stop < entry:
                    realized_r += remaining * (entry - trail_stop) / risk
                    return {"outcome": "tp1_trail", "pnl_r": round(realized_r, 3),
                            "bars": bars_after, "tp1_hit": True, "trail_price": trail_stop}

    # 时间窗到期或数据用尽
    if tp1_hit:
        # TP1 已落袋,余仓未闭 → 按最后一根 close 近似清算余仓(保守估)
        last_close = float(future_bars[-1]["c"]) if future_bars else entry
        if direction == "long":
            realized_r += remaining * (last_close - entry) / risk
        else:
            realized_r += remaining * (entry - last_close) / risk
        return {"outcome": "tp1_only", "pnl_r": round(realized_r, 3),
                "bars": bars_after, "tp1_hit": True}
    return {"outcome": "pending", "pnl_r": 0.0, "bars": bars_after, "tp1_hit": False}


# --- 主回测流程 ------------------------------------------------------------

def _enrich_signal(
    s: Any,
    engine: Any,
    symbol: str,
    close_time_ms: int,
) -> Dict[str, Any]:
    """在信号落地时快照所有特征(供 CSV 导出 / ML 分析)。
    只从 engine 当前状态取数,不引入未来信息。
    """
    # ATR
    atr_4h = engine._atr(symbol, "4h")
    atr_1h = engine._atr(symbol, "1h")
    atr_15m = engine._atr(symbol, "15m")
    # 4h 结构(signal 时刻)
    h4_win = engine.candles.window(symbol, "4h", 50)
    lb = engine.swing_lookback
    h4_struct = classify_structure(h4_win, lb) if len(h4_win) >= 2 * lb + 3 else "neutral"
    # POI 衍生特征
    poi_mid = None
    poi_height_pct = None
    poi_distance_pct = None
    poi_age_hours = None
    if s.poi_low is not None and s.poi_high is not None and s.entry_price:
        poi_mid = (s.poi_low + s.poi_high) / 2
        poi_height_pct = (s.poi_high - s.poi_low) / s.entry_price * 100
        poi_distance_pct = (poi_mid - s.entry_price) / s.entry_price * 100  # 有符号
    if s.poi_origin_time is not None:
        poi_age_hours = (close_time_ms - int(s.poi_origin_time)) / 3600000.0
    # 时间特征(NY)
    ny = pytz.timezone("America/New_York")
    dt_et = datetime.fromtimestamp(close_time_ms / 1000.0, tz=timezone.utc).astimezone(ny)
    # 评分 C1-C9 展开
    scores = dict(s.scores or {})
    feat = {
        "time_ms": close_time_ms,
        "time_iso": dt_et.strftime("%Y-%m-%d %H:%M:%S"),
        "hour_et": dt_et.hour,
        "weekday": dt_et.weekday(),
        "direction": s.direction,
        "poi_source": s.poi_source,
        "poi_type": s.poi_type,
        "entry": s.entry_price,
        "sl": s.stop_loss,
        "tp": s.take_profit,
        "rr": s.risk_reward,
        "score_total": s.total_score,
        "atr_4h": round(atr_4h, 4),
        "atr_1h": round(atr_1h, 4),
        "atr_15m": round(atr_15m, 4),
        "atr_ratio_4h_1h": round(atr_4h / atr_1h, 3) if atr_1h > 0 else None,
        "poi_height_pct": round(poi_height_pct, 4) if poi_height_pct is not None else None,
        "poi_distance_pct": round(poi_distance_pct, 4) if poi_distance_pct is not None else None,
        "poi_age_hours": round(poi_age_hours, 2) if poi_age_hours is not None else None,
        "poi_displacement_mult": s.poi_displacement_mult,
        "sweep_bar_delta": s.sweep_bar_delta,
        "sweep_wick_strong": int(s.sweep_wick_strong) if s.sweep_wick_strong is not None else None,
        "h4_structure": h4_struct,
    }
    for i in range(1, 10):
        feat[f"C{i}"] = scores.get(f"C{i}", 0.0)
    return feat


async def run_backtest(symbol: str, ref_symbol: Optional[str], days: int, cfg: dict, inspect_date: Optional[str] = None, csv_path: Optional[str] = None, trail_atr_mult: float = 0.0, tp1_portion: float = 0.5) -> None:
    setup_logger("INFO")
    import os
    proxy = cfg.get("binance", {}).get("proxy") or os.getenv("HTTP_PROXY") or None
    logger.info(f"回测:{symbol} ref={ref_symbol} days={days}  proxy={proxy or '无'}")

    # 拉历史 K 线(1d/4h/1h/15m)。OKX 两段拉取 (`/candles` + `/history-candles`)
    # 已能翻到 ~1 年前,各 TF 按天数直接算即可。
    needs = {
        "1d": days + 30,
        "4h": days * 6 + 60,
        "1h": days * 24 + 200,
        "15m": days * 96 + 300,
    }
    klines: Dict[str, List[dict]] = {}
    for tf, limit in needs.items():
        klines[tf] = await fetch_klines(symbol, tf, limit=limit, proxy=proxy)
        logger.info(f"  拉取 {symbol} {tf}: {len(klines[tf])} 根")

    ref_klines: List[dict] = []
    if ref_symbol:
        ref_klines = await fetch_klines(ref_symbol, "4h", limit=days * 6 + 60, proxy=proxy)
        logger.info(f"  拉取 {ref_symbol} 4h: {len(ref_klines)} 根")

    # 构造 state_machine 栈
    candles = CandleManager(maxlen=3000)
    levels = KeyLevels()
    weekly_vp = StubWeeklyVP()
    delta = DeltaTracker()
    for tf in needs.keys():
        delta.ensure_tf(tf)

    engine = SignalEngine(candle_manager=candles, key_levels=levels, scorer=signal_score, config=cfg)
    engine.set_vp_provider(lambda: weekly_vp.current())
    if ref_symbol:
        swing_lb = int(cfg.get("signal", {}).get("swing_lookback", 5))
        def _cg(direction):
            rc = candles.window(ref_symbol, "4h", 50)
            if len(rc) < 2 * swing_lb + 3:
                return True
            rs = classify_structure(rc, swing_lb)
            if rs == "neutral":
                return True
            return rs == ("bullish" if direction == "long" else "bearish")
        engine.set_correlation_gate(_cg)

    # 合并所有 close 事件到单一时间轴:(close_time_ms, tf, sym, payload)
    events: List[tuple] = []
    for tf, rows in klines.items():
        for r in rows:
            events.append((int(r["T"]), tf, symbol, r))
    for r in ref_klines:
        events.append((int(r["T"]), "4h", ref_symbol, r))
    events.sort(key=lambda x: x[0])

    collected_signals: List[dict] = []
    step_counts = {"created": 0, "step2": 0, "step3": 0, "step4": 0, "step5": 0, "step6": 0, "scored": 0}
    # 初始快照
    prev_steps: Dict[str, str] = {}
    # 全量信号事件记录(用于 --inspect 诊断)
    signal_events: List[dict] = []  # 每条 {close_time, tf, sid, old_step, new_step, note}

    # 订阅 close 回调:更新 VP/delta + 驱动 engine
    def on_close(sym, tf, c):
        if sym != symbol:
            return
        ts = datetime.fromtimestamp(c.close_time / 1000.0, tz=timezone.utc)
        # VP / key levels 用 15m 累加(原来用 1h,现在主 TF 已换)
        if tf == "15m":
            locked = weekly_vp.add_kline(ts, c.close, c.volume)
            if locked is not None:
                levels.update_prev_week(locked)
            levels.update_weekly(c.close, ts)
            levels.update_monthly(c.close, ts)
        # delta 近似(各 TF 独立)
        d_approx = c.volume * (0.6 if c.close > c.open else -0.6 if c.close < c.open else 0.0)
        engine.set_bar_delta(tf, d_approx)
    candles.subscribe_close(on_close)

    # 主回放循环
    diag_struct_log = []
    for close_time, tf, sym, payload in events:
        candles.on_kline(sym, tf, payload)
        # 诊断:每次 1d/4h 收盘记录 ETH 与 BTC 结构
        if tf in ("1d", "4h") and sym == symbol:
            ew = candles.window(symbol, tf, 50)
            if len(ew) >= 13:
                es = classify_structure(ew, 5)
                bw = candles.window(ref_symbol, "4h", 50) if ref_symbol else []
                bs = classify_structure(bw, 5) if len(bw) >= 13 else "na"
                diag_struct_log.append((close_time, tf, es, bs))
        # 仅对主品种(ETH)推进状态机;参考品种(BTC)的 K 线仅存入 CandleManager 供 correlation gate 读取
        if sym == symbol:
            # 推进前快照:(sid -> step)
            pre_state = {sid: s.step.value for sid, s in engine.active_signals.items()}
            await engine.on_candle_close(sym, tf, candles.last(sym, tf))
            # 推进后对比:记录所有变化
            for sid, s in engine.active_signals.items():
                new_step = s.step.value
                old_step = pre_state.get(sid, "(new)")
                if old_step != new_step:
                    note = ""
                    if new_step == "STEP1_PASSED":
                        note = f"trigger={s.step1_trigger_price:.2f}"
                    elif new_step == "STEP3_PASSED":
                        note = f"sweep level={s.triggered_level}={s.triggered_level_value}"
                    elif new_step == "STEP4_PASSED":
                        note = f"OB [{s.poi_low}-{s.poi_high}]"
                    elif new_step in ("EXPIRED", "INVALIDATED"):
                        note = "(see log)"
                    elif new_step == "SCORED":
                        note = f"score={s.total_score}"
                    signal_events.append({
                        "close_time": close_time, "tf": tf, "sid": sid[:8],
                        "old_step": old_step, "new_step": new_step, "note": note,
                    })
        # 统计各步骤到达数(首次到达时计一次)
        for sid, st in engine.active_signals.items():
            key = st.step.value
            if prev_steps.get(sid) != key:
                prev_steps[sid] = key
                if key == "STEP1_PASSED":
                    step_counts["created"] += 1
                elif key == "STEP2_PASSED":
                    step_counts["step2"] += 1
                elif key == "STEP3_PASSED":
                    step_counts["step3"] += 1
                elif key == "STEP4_PASSED":
                    step_counts["step4"] += 1
                elif key == "STEP5_PASSED":
                    step_counts["step5"] += 1
                elif key == "STEP6_PASSED":
                    step_counts["step6"] += 1
                elif key == "SCORED":
                    step_counts["scored"] += 1
        # Step1 去重只拦 STEP1_PASSED,下游可能有多条并行 SignalState
        # 同刻锁到同一 cluster → 相同 entry/sl/tp。按内容指纹去重,避免:
        #  (a) 同一 signal 在后续每根 15m 收盘都被重写(同 sid 多 tick)
        #  (b) 不同 sid 的并行 signal 写出相同内容(Step1 下游并行 bug)
        # 指纹**不含** close_time:只要内容相同就视为同一事件,无论首次出现在哪根 K。
        def _content_key(s_):
            return (
                s_.direction, s_.poi_source,
                round(s_.entry_price or 0, 4),
                round(s_.stop_loss or 0, 4),
                round(s_.take_profit or 0, 4),
            )
        # 收割通知级信号
        for s in engine.get_notification_ready():
            ck = _content_key(s)
            if any(c.get("_content_key") == ck for c in collected_signals):
                continue
            base = {
                "time": close_time,
                "level": s.triggered_level,
                "direction": s.direction,
                "entry": s.entry_price,
                "sl": s.stop_loss,
                "tp": s.take_profit,
                "rr": s.risk_reward,
                "score": s.total_score,
                "poi": s.poi_type,
                "source": s.poi_source,
                "notified": True,
                "scores": dict(s.scores or {}),
                "_content_key": ck,
                "_features": _enrich_signal(s, engine, symbol, close_time),
            }
            collected_signals.append(base)
        # 额外收 scored 但未通知(评分不够)的信号,仅诊断
        from smc_signal.state_machine import SignalStep as _SS
        for sid, s in engine.active_signals.items():
            if s.step == _SS.SCORED and not s.notification_sent:
                ck = _content_key(s)
                if any(c.get("_content_key") == ck for c in collected_signals):
                    continue
                collected_signals.append({
                    "time": close_time,
                    "level": s.triggered_level,
                    "direction": s.direction,
                    "entry": s.entry_price,
                    "sl": s.stop_loss,
                    "tp": s.take_profit,
                    "rr": s.risk_reward,
                    "score": s.total_score,
                    "poi": s.poi_type,
                    "source": s.poi_source,
                    "notified": False,
                    "scores": dict(s.scores or {}),
                    "_content_key": ck,
                    "_features": _enrich_signal(s, engine, symbol, close_time),
                })

    logger.info(f"回放完成,捕获通知级信号 {len(collected_signals)} 条")
    logger.info(f"步骤漏斗: {step_counts}")
    # 详细 gate 诊断
    diag = engine.diagnostics()
    print(f"\n{'='*60}")
    print(f"详细 Gate 诊断({days} 天)")
    print(f"{'='*60}")
    print(f"Step1 候选数:            {diag.get('step1_created', 0)}")
    print(f"  - Step1 TIME_EXPIRED(36h):  {diag.get('expired_step1_ttl', 0)}")
    print(f"Step2 尝试次数:          {diag.get('step2_attempts', 0)}")
    print(f"  - Step2 通过:              {diag.get('step2_passed', 0)}")
    print(f"  - 死因: 找不到 OB:         {diag.get('step2_fail_no_ob', 0)}")
    print(f"  - 死因: OB 已被 dedupe:     {diag.get('step2_fail_dedupe', 0)}")
    print(f"Step3 尝试次数:          {diag.get('step3_attempts', 0)}")
    print(f"  - Step3 通过:              {diag.get('step3_passed', 0)}")
    print(f"  - 死因: wick 未到 POI:     {diag.get('step3_fail_wick_miss', 0)}")
    print(f"  - 死因: close 穿透 POI:    {diag.get('step3_fail_close_penetrated', 0)}")
    print(f"  - 死因: wick/delta 都失败: {diag.get('step3_fail_gate', 0)}")
    print(f"Step5 尝试次数:          {diag.get('step5_attempts', 0)}")
    print(f"  - Step5 通过:              {diag.get('step5_passed', 0)}")
    print(f"  - 死因: 15m CHoCH 未出:    {diag.get('step5_fail_no_choch', 0)}")
    print(f"  - 死因: CHoCH 方向不符:    {diag.get('step5_fail_wrong_dir', 0)}")
    print(f"SCORED:                  {diag.get('scored', 0)}")
    print(f"  - 评分 < 阈值 未通知:      {diag.get('scored_below_threshold', 0)}")
    print(f"NOTIFIED(最终通知):     {diag.get('notified', 0)}")
    print(f"  - 被通知去重跳过:        {diag.get('notified_dedupe_skipped', 0)}")
    print("-" * 60)
    print(f"INVALIDATED 分布:")
    print(f"  - B 维度(4h 翻转):       {diag.get('invalidated_b_structure', 0)} 条")
    print(f"  - C 维度(POI 破坏):      {diag.get('invalidated_c_poi_break', 0)} 条")
    print(f"  - Step1 TIME_EXPIRED:     {diag.get('expired_step1_ttl', 0)} 条")
    print()
    # 诊断:统计 ETH 各 TF 的结构分布 + 与 BTC 一致的 4h 收盘数
    dist = defaultdict(int)
    agree_count = 0
    conflict_count = 0
    for t, tf, es, bs in diag_struct_log:
        dist[(tf, es)] += 1
        if tf == "4h" and es != "neutral" and bs != "neutral":
            if es == bs:
                agree_count += 1
            else:
                conflict_count += 1
    logger.info("结构分布:")
    for k in sorted(dist.keys()):
        logger.info(f"  {k}: {dist[k]}")
    logger.info(f"ETH4h 与 BTC4h 方向明确时:一致 {agree_count} / 冲突 {conflict_count}")

    # 前瞻 TP/SL 模拟(用 1h K 线拿更长历史;15m 数据源被限在 ~15 天,太少)
    # 窗口 7 天 = 168 根 1h,覆盖典型 SMC 持仓周期
    future_1h = klines["1h"]
    results = []
    SYN_SL_PCT = 0.03  # 兜底合成 SL(极少触发,bot 现在自动算 SL/TP)
    for sig in collected_signals:
        sl_sim = sig["sl"]
        tp_sim = sig["tp"]
        if sl_sim is None and sig["entry"] is not None:
            sl_sim = sig["entry"] * (1 - SYN_SL_PCT) if sig["direction"] == "long" else sig["entry"] * (1 + SYN_SL_PCT)
        if tp_sim is None and sl_sim is not None and sig["entry"] is not None:
            r = abs(sig["entry"] - sl_sim)
            tp_sim = sig["entry"] + 2 * r if sig["direction"] == "long" else sig["entry"] - 2 * r
        out = simulate_outcome(
            sig["time"], sig["direction"], sig["entry"], sl_sim, tp_sim, future_1h,
            max_hours=168,
        )
        merged = {**sig, **out}
        # 并行跑 hybrid(仅当启用),与 baseline 对比,不替换
        if trail_atr_mult > 0:
            atr_4h_at_sig = (sig.get("_features") or {}).get("atr_4h") or 0.0
            hy = simulate_outcome_hybrid(
                sig["time"], sig["direction"], sig["entry"], sl_sim, tp_sim,
                float(atr_4h_at_sig), future_1h, max_hours=168,
                tp1_portion=tp1_portion, trail_atr_mult=trail_atr_mult,
            )
            merged["hybrid_outcome"] = hy.get("outcome")
            merged["hybrid_pnl_r"] = hy.get("pnl_r")
            merged["hybrid_tp1_hit"] = hy.get("tp1_hit")
        results.append(merged)

    # 打印表格
    print(f"\n{'='*100}")
    print(f"{'Time (ET)':<18} {'Dir':<5} {'Level':<10} {'Entry':>9} {'SL':>9} {'TP':>9} {'RR':>5} {'Score':>6} {'Outcome':<8} {'R':>6}")
    print("-" * 100)
    ny = pytz.timezone("America/New_York")
    def _fmt_num(v, width=9, decimals=2):
        if v is None:
            return f"{'-':>{width}}"
        return f"{v:>{width}.{decimals}f}"
    for r in results:
        t_et = datetime.fromtimestamp(r["time"] / 1000.0, tz=timezone.utc).astimezone(ny).strftime("%m-%d %H:%M")
        notified_tag = "✅" if r.get("notified") else "🚫"
        print(
            f"{t_et:<18} {r['direction']:<5} {(r['level'] or '-'):<10} "
            f"{_fmt_num(r['entry'])} {_fmt_num(r['sl'])} {_fmt_num(r['tp'])} "
            f"{_fmt_num(r['rr'], 5)} {_fmt_num(r['score'], 6, 1)} "
            f"{r['outcome']:<8} {_fmt_num(r['pnl_r'], 6)}  {notified_tag}"
        )
        sc = r.get("scores") or {}
        if sc:
            hit = [f"{k}={v:g}" for k, v in sorted(sc.items()) if float(v or 0) > 0]
            print(
                f"{'  └ scores:':<18} {' '.join(hit) if hit else '(无命中)'}  "
                f"source={r.get('source','?')}  type={r.get('poi','?')}"
            )

    # 诊断模式:dump 指定日期前后 1 天内的所有信号事件
    if inspect_date:
        from datetime import date as _date
        try:
            y, m, d = map(int, inspect_date.split("-"))
            center = datetime(y, m, d, tzinfo=pytz.timezone("America/New_York"))
            lo = int((center - timedelta(days=1)).timestamp() * 1000)
            hi = int((center + timedelta(days=2)).timestamp() * 1000)
        except Exception:
            lo, hi = 0, 10**16
        ny = pytz.timezone("America/New_York")
        print(f"\n{'='*100}")
        print(f"INSPECT {inspect_date}:±1 天内所有信号事件 ({len(signal_events)} 总事件)")
        print("-" * 100)
        print(f"{'Time (ET)':<18} {'TF':<4} {'SID':<10} {'FROM':<16} → {'TO':<16} {'NOTE'}")
        print("-" * 100)
        shown = 0
        for ev in signal_events:
            if ev["close_time"] < lo or ev["close_time"] > hi:
                continue
            t_et = datetime.fromtimestamp(ev["close_time"] / 1000.0, tz=timezone.utc).astimezone(ny).strftime("%m-%d %H:%M")
            print(f"{t_et:<18} {ev['tf']:<4} {ev['sid']:<10} {ev['old_step']:<16} → {ev['new_step']:<16} {ev['note']}")
            shown += 1
        print(f"(筛选后显示 {shown} 事件)")

    # 汇总
    closed = [r for r in results if r["outcome"] in ("tp", "sl")]
    wins = [r for r in closed if r["outcome"] == "tp"]
    total_r = sum(r["pnl_r"] for r in closed)
    print("-" * 100)
    print(f"信号: {len(results)}  已闭合: {len(closed)}  胜: {len(wins)}  "
          f"胜率: {(len(wins)/max(1,len(closed))):.1%}  累计 R: {total_r:+.2f}")

    # Hybrid trail 对比汇总
    if trail_atr_mult > 0:
        hy_rows = [r for r in results if r.get("hybrid_outcome") is not None]
        hy_closed = [r for r in hy_rows if r["hybrid_outcome"] != "pending"]
        hy_pos = [r for r in hy_closed if (r.get("hybrid_pnl_r") or 0) > 0]
        hy_r = sum(r.get("hybrid_pnl_r") or 0 for r in hy_closed)
        # 子集对比:同一样本(baseline 与 hybrid 都闭合)
        both_closed = [r for r in results if r["outcome"] in ("tp","sl") and r.get("hybrid_outcome") not in (None, "pending")]
        base_r_on_both = sum(r["pnl_r"] for r in both_closed)
        hy_r_on_both = sum((r.get("hybrid_pnl_r") or 0) for r in both_closed)
        print("-" * 100)
        print(f"[HYBRID TRAIL] TP1={tp1_portion*100:.0f}% + 剩余 peak − {trail_atr_mult}×ATR_4h 追踪")
        print(f"  全部 n={len(hy_rows)}  闭合={len(hy_closed)}  净正={len(hy_pos)}  ΣR={hy_r:+.2f}")
        print(f"  同样本对比(两种模式均闭合, n={len(both_closed)}):  Baseline ΣR={base_r_on_both:+.2f}  Hybrid ΣR={hy_r_on_both:+.2f}  Δ={hy_r_on_both - base_r_on_both:+.2f}")
        # outcome 分布
        oc = defaultdict(int)
        for r in hy_rows:
            oc[r["hybrid_outcome"]] += 1
        print(f"  Outcome 分布: {dict(oc)}")
        # NOTIFIED only 对比
        ntf = [r for r in results if r.get("notified") and r.get("hybrid_outcome") not in (None, "pending")]
        ntf_base = [r for r in results if r.get("notified") and r["outcome"] in ("tp","sl")]
        if ntf and ntf_base:
            b_r = sum(r["pnl_r"] for r in ntf_base)
            h_r = sum((r.get("hybrid_pnl_r") or 0) for r in ntf)
            print(f"  NOTIFIED only:  Baseline ΣR={b_r:+.2f} (n={len(ntf_base)})  Hybrid ΣR={h_r:+.2f} (n={len(ntf)})  Δ={h_r - b_r:+.2f}")
    by_dir = defaultdict(lambda: {"n": 0, "w": 0})
    for r in closed:
        by_dir[r["direction"]]["n"] += 1
        if r["outcome"] == "tp":
            by_dir[r["direction"]]["w"] += 1
    for d, v in by_dir.items():
        print(f"  {d}: {v['n']} 单  胜 {v['w']}  胜率 {(v['w']/max(1,v['n'])):.1%}")

    # CSV 导出(特征 + outcome,供 ML 分析)
    if csv_path:
        rows = []
        for r in results:
            feat = r.get("_features") or {}
            row = dict(feat)
            row["outcome"] = r.get("outcome")
            row["pnl_r"] = r.get("pnl_r")
            # 闭合的 1=win, 0=loss, pending=None
            if r.get("outcome") == "tp":
                row["win"] = 1
            elif r.get("outcome") == "sl":
                row["win"] = 0
            else:
                row["win"] = None
            row["notified"] = int(bool(r.get("notified")))
            row["hybrid_outcome"] = r.get("hybrid_outcome")
            row["hybrid_pnl_r"] = r.get("hybrid_pnl_r")
            row["hybrid_tp1_hit"] = int(bool(r.get("hybrid_tp1_hit"))) if r.get("hybrid_outcome") else None
            rows.append(row)
        if rows:
            # 固定列顺序,便于后续分析
            cols = [
                "time_iso", "time_ms", "hour_et", "weekday",
                "direction", "poi_source", "poi_type", "h4_structure",
                "entry", "sl", "tp", "rr",
                "atr_4h", "atr_1h", "atr_15m", "atr_ratio_4h_1h",
                "poi_height_pct", "poi_distance_pct", "poi_age_hours",
                "poi_displacement_mult", "sweep_bar_delta", "sweep_wick_strong",
                "score_total",
                "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9",
                "notified", "outcome", "pnl_r", "win",
                "hybrid_outcome", "hybrid_pnl_r", "hybrid_tp1_hit",
            ]
            # 补齐缺失列
            for r in rows:
                for c in cols:
                    r.setdefault(c, None)
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cols)
                writer.writeheader()
                writer.writerows(rows)
            print(f"\n[CSV] 已导出 {len(rows)} 条信号到 {csv_path}")

    # 按 POI source 聚合:全量(含 pending)的 R:R + 已闭合的胜率
    by_src = defaultdict(lambda: {
        "n_all": 0, "rr_sum": 0.0, "rr_n": 0,
        "n_closed": 0, "n_win": 0, "r_sum": 0.0,
    })
    for r in results:
        src = r.get("source") or "?"
        by_src[src]["n_all"] += 1
        if r.get("rr") is not None:
            by_src[src]["rr_sum"] += float(r["rr"])
            by_src[src]["rr_n"] += 1
        if r["outcome"] in ("tp", "sl"):
            by_src[src]["n_closed"] += 1
            by_src[src]["r_sum"] += float(r["pnl_r"] or 0.0)
            if r["outcome"] == "tp":
                by_src[src]["n_win"] += 1
    print(f"\n{'='*100}")
    print(f"按 POI 来源分组统计")
    print("-" * 100)
    print(f"{'Source':<14} {'信号':>5} {'平均R:R':>9} {'闭合':>5} {'胜':>4} {'胜率':>7} {'累计R':>8}")
    print("-" * 100)
    for src in sorted(by_src.keys(), key=lambda s: (-by_src[s]["n_all"], s)):
        v = by_src[src]
        avg_rr = v["rr_sum"] / v["rr_n"] if v["rr_n"] else 0.0
        win_rate = v["n_win"] / v["n_closed"] if v["n_closed"] else 0.0
        print(
            f"{src:<14} {v['n_all']:>5} {avg_rr:>9.2f} {v['n_closed']:>5} "
            f"{v['n_win']:>4} {win_rate:>6.1%} {v['r_sum']:>+8.2f}"
        )


def main() -> None:
    ROOT = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="回测天数(默认 30)")
    ap.add_argument("--symbol", default=None, help="覆盖 config.yaml 的 symbol")
    ap.add_argument("--rr-min", type=float, default=None, help="覆盖 risk_reward_min")
    ap.add_argument("--inspect", default=None, help="诊断模式:聚焦指定日期 (YYYY-MM-DD),打印该日前后所有信号状态转换")
    ap.add_argument("--export-csv", dest="csv", default=None, help="信号特征 + outcome 导出到 CSV(供 ML 分析)")
    ap.add_argument("--trail-atr", type=float, default=0.0,
                    help="启用 hybrid trail:设成 >0(如 1.5),TP1 平仓后剩余按 peak − N×ATR_4h 追踪。0=仅跑 baseline")
    ap.add_argument("--tp1-portion", type=float, default=0.5, help="hybrid 模式 TP1 平仓比例(0-1)")
    args = ap.parse_args()

    with open(ROOT / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if args.rr_min is not None:
        cfg.setdefault("signal", {})["risk_reward_min"] = args.rr_min

    symbol = args.symbol or cfg["symbol"]
    ref = (cfg.get("reference") or {}).get("symbol")

    asyncio.run(run_backtest(symbol, ref, args.days, cfg, inspect_date=args.inspect, csv_path=args.csv,
                             trail_atr_mult=args.trail_atr, tp1_portion=args.tp1_portion))


if __name__ == "__main__":
    main()
