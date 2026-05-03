"""IFVG(Inversion FVG)入场回测分析。

思路:对历史所有 SL'd 信号,模拟"价格回到 POI 区间时第二次入场":
  1. 原信号 SL 命中 → 仓位关闭
  2. 等待价格反弹冲到新高/低后,**回踩到原 POI 区间**
  3. 用回踩时的 1h 收盘做 entry,SL = 期间最高/低点 + buffer,TP = 原 TP
  4. 模拟二次入场结果

不修改 bot,只跑历史数据看是否值得实装。

用法:
  python scripts/ifvg_analysis.py --days 187
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml

from scripts.backtest import (
    StubWeeklyVP,
    simulate_outcome,
    run_backtest,
)
from data.candle_manager import CandleManager
from data.rest_backfill import fetch_klines, _fetch_klines_okx


async def fetch_long(symbol, tf, limit, proxy):
    """187d 1h 需要 4688 根,超过 Binance 1500 上限,直接走 OKX 分页。"""
    if limit > 1500:
        return await _fetch_klines_okx(symbol, tf, limit, proxy, timeout_s=30)
    return await fetch_klines(symbol, tf, limit=limit, proxy=proxy)
from engine.delta import DeltaTracker
from engine.key_levels import KeyLevels
from engine.market_structure import classify_structure
from smc_signal import SignalEngine, score as signal_score
from utils.logger import logger, setup_logger


def simulate_ifvg_reentry(
    sl_hit_time_ms: int,
    direction: str,
    poi_low: float,
    poi_high: float,
    original_tp: float,
    future_bars: List[dict],
    max_hours: int = 24,
    sl_buffer_pct: float = 0.5,
) -> Dict[str, Any]:
    """SL 后等价格回到 POI 区间,模拟二次入场。

    Args:
      sl_hit_time_ms: 原信号 SL 命中时刻(用 1h K 的 close_time 近似)
      direction: 'long' / 'short'
      poi_low / poi_high: 原 POI 区间
      original_tp: 用原信号的 TP
      future_bars: 1h K 列表(close_time 单位 ms)
      max_hours: SL 后最多等多少根 1h K 寻找 re-entry
      sl_buffer_pct: 二次入场 SL 离 recent_extreme 加百分比 buffer
                     (例 0.5 = 0.5% of POI height,小数值更敏感)

    返回:
      {
        "reentered": bool,
        "entry_time_ms": int,
        "entry": float,
        "sl": float,
        "tp": float,
        "rr": float,
        "outcome": 'tp' / 'sl' / 'pending',
        "pnl_r": float,
        "reason": str (未入场原因)
      }
    """
    bars_after = 0
    recent_extreme = float("-inf") if direction == "short" else float("inf")
    poi_height = poi_high - poi_low
    sl_buf = poi_height * sl_buffer_pct

    for k in future_bars:
        if int(k["t"]) <= sl_hit_time_ms:
            continue
        bars_after += 1
        if bars_after > max_hours:
            return {"reentered": False, "reason": f"no reentry within {max_hours}h"}

        hi = float(k["h"])
        lo = float(k["l"])

        if direction == "short":
            recent_extreme = max(recent_extreme, hi)
            # re-entry condition: bar low touches the upper edge of POI from above
            # (price came back down into FVG zone)
            if lo <= poi_high:
                entry = float(k["c"])
                # 新 SL = recent_extreme + buffer(更宽的止损,容忍二次冲高)
                new_sl = recent_extreme + sl_buf
                if entry >= new_sl:
                    # 入场后已经在 SL 上方,无效
                    return {"reentered": False, "reason": "entry above new SL"}
                if entry <= original_tp:
                    return {"reentered": False, "reason": "entry already below TP"}
                risk = new_sl - entry
                reward = entry - original_tp
                rr = reward / risk if risk > 0 else 0
                # 模拟 outcome,从这根 K 之后
                future_after = [b for b in future_bars if int(b["t"]) > int(k["t"])]
                out = simulate_outcome(
                    int(k["t"]), direction, entry, new_sl, original_tp,
                    future_after, max_hours=168,
                )
                return {
                    "reentered": True,
                    "entry_time_ms": int(k["t"]),
                    "entry": entry,
                    "sl": new_sl,
                    "tp": original_tp,
                    "rr": round(rr, 2),
                    "outcome": out["outcome"],
                    "pnl_r": out["pnl_r"],
                    "wait_bars": bars_after,
                }
        else:  # long
            recent_extreme = min(recent_extreme, lo)
            if hi >= poi_low:
                entry = float(k["c"])
                new_sl = recent_extreme - sl_buf
                if entry <= new_sl:
                    return {"reentered": False, "reason": "entry below new SL"}
                if entry >= original_tp:
                    return {"reentered": False, "reason": "entry already above TP"}
                risk = entry - new_sl
                reward = original_tp - entry
                rr = reward / risk if risk > 0 else 0
                future_after = [b for b in future_bars if int(b["t"]) > int(k["t"])]
                out = simulate_outcome(
                    int(k["t"]), direction, entry, new_sl, original_tp,
                    future_after, max_hours=168,
                )
                return {
                    "reentered": True,
                    "entry_time_ms": int(k["t"]),
                    "entry": entry,
                    "sl": new_sl,
                    "tp": original_tp,
                    "rr": round(rr, 2),
                    "outcome": out["outcome"],
                    "pnl_r": out["pnl_r"],
                    "wait_bars": bars_after,
                }

    return {"reentered": False, "reason": f"no reentry within {max_hours}h"}


async def main_async(symbol: str, ref_symbol: Optional[str], days: int, cfg: dict) -> None:
    setup_logger("WARNING")
    import os
    proxy = cfg.get("binance", {}).get("proxy") or os.getenv("HTTP_PROXY") or None

    # 拉数据
    needs = {
        "1d": days + 30,
        "4h": days * 6 + 60,
        "1h": days * 24 + 200,
        "15m": days * 96 + 300,
    }
    klines: Dict[str, List[dict]] = {}
    for tf, limit in needs.items():
        klines[tf] = await fetch_long(symbol, tf, limit, proxy)

    ref_klines: List[dict] = []
    if ref_symbol:
        ref_klines = await fetch_long(ref_symbol, "4h", days * 6 + 60, proxy)

    # 跑标准回测获取所有信号(复用 backtest.py 的 run_backtest 输出 stdout 太多,
    # 这里手动运行简化版)
    candles = CandleManager(maxlen=3000)
    levels = KeyLevels()
    weekly_vp = StubWeeklyVP()
    delta = DeltaTracker()
    for tf in needs.keys():
        delta.ensure_tf(tf)

    engine = SignalEngine(candle_manager=candles, key_levels=levels, scorer=signal_score, config=cfg)
    engine.set_vp_provider(lambda: weekly_vp.current())

    # 合并所有 close 事件
    events: List[tuple] = []
    for tf, rows in klines.items():
        for r in rows:
            events.append((int(r["T"]), tf, symbol, r))
    for r in ref_klines:
        events.append((int(r["T"]), "4h", ref_symbol, r))
    events.sort(key=lambda x: x[0])

    collected: List[dict] = []

    def on_close(sym, tf, c):
        if sym != symbol:
            return
        from datetime import datetime, timezone
        ts = datetime.fromtimestamp(c.close_time / 1000.0, tz=timezone.utc)
        if tf == "15m":
            locked = weekly_vp.add_kline(ts, c.close, c.volume)
            if locked is not None:
                levels.update_prev_week(locked)
            levels.update_weekly(c.close, ts)
            levels.update_monthly(c.close, ts)
        d_approx = c.volume * (0.6 if c.close > c.open else -0.6 if c.close < c.open else 0.0)
        engine.set_bar_delta(tf, d_approx)

    candles.subscribe_close(on_close)

    for close_time, tf, sym, payload in events:
        candles.on_kline(sym, tf, payload)
        if sym == symbol:
            await engine.on_candle_close(sym, tf, candles.last(sym, tf))
        for s in engine.get_notification_ready():
            if s.entry_price is None or s.stop_loss is None or s.take_profit is None:
                continue
            collected.append({
                "time": close_time,
                "direction": s.direction,
                "entry": s.entry_price,
                "sl": s.stop_loss,
                "tp": s.take_profit,
                "rr": s.risk_reward,
                "score": s.total_score,
                "poi_low": s.poi_low,
                "poi_high": s.poi_high,
                "poi_source": s.poi_source,
            })

    # 模拟原始信号 outcome
    future_1h = klines["1h"]
    for sig in collected:
        out = simulate_outcome(
            sig["time"], sig["direction"], sig["entry"], sig["sl"], sig["tp"],
            future_1h, max_hours=168,
        )
        sig["outcome"] = out["outcome"]
        sig["pnl_r"] = out["pnl_r"]
        sig["bars_to_close"] = out["bars"]

    # 仅看 NOTIFIED + SL 的信号
    sl_signals = [s for s in collected if s["outcome"] == "sl"]
    print(f"\n{'='*80}")
    print(f"IFVG 二次入场分析  共 {len(collected)} NOTIFIED 信号,{len(sl_signals)} 条 SL")
    print(f"{'='*80}")

    # 对每条 SL'd 信号尝试 IFVG 重入
    # 注意:SL 命中时刻不是原始信号时间,而是 outcome 模拟里 SL 触达的 K 时刻
    # 这里用近似:原信号 time + bars_to_close * 1h
    HOUR_MS = 3600 * 1000
    ifvg_results = []
    for sig in sl_signals:
        if sig["poi_low"] is None or sig["poi_high"] is None:
            continue
        sl_hit_time = sig["time"] + sig["bars_to_close"] * HOUR_MS
        ifvg = simulate_ifvg_reentry(
            sl_hit_time_ms=sl_hit_time,
            direction=sig["direction"],
            poi_low=sig["poi_low"],
            poi_high=sig["poi_high"],
            original_tp=sig["tp"],
            future_bars=future_1h,
            max_hours=24,
            sl_buffer_pct=0.5,
        )
        ifvg_results.append({**sig, "ifvg": ifvg})

    # 汇总
    n_reentered = sum(1 for r in ifvg_results if r["ifvg"].get("reentered"))
    n_skipped = len(ifvg_results) - n_reentered
    closed_ifvg = [r for r in ifvg_results if r["ifvg"].get("outcome") in ("tp", "sl")]
    wins = [r for r in closed_ifvg if r["ifvg"]["outcome"] == "tp"]
    total_orig_loss = sum(r["pnl_r"] for r in sl_signals)  # 全是 -1
    total_ifvg_r = sum(r["ifvg"]["pnl_r"] for r in closed_ifvg)
    net_change = total_ifvg_r  # 二次入场新增 R(原 SL 的 -1 已计)

    print()
    print(f"原 SL 信号数:                {len(sl_signals)}")
    print(f"  原始累计亏损:               {total_orig_loss:+.2f} R")
    print(f"二次入场触发:                {n_reentered} ({100*n_reentered/max(1,len(sl_signals)):.0f}%)")
    print(f"  - 闭合:                    {len(closed_ifvg)}")
    print(f"  - 胜:                      {len(wins)}")
    print(f"  - 二次入场胜率:             {100*len(wins)/max(1,len(closed_ifvg)):.1f}%")
    print(f"  - 二次入场累计 R:           {total_ifvg_r:+.2f} R")
    print(f"未触发二次入场:              {n_skipped}")
    print()
    print(f"{'='*80}")
    print(f"📊 净效果(原 SL 不变 + 加入 IFVG 二次入场)")
    print(f"{'='*80}")
    print(f"原始 SL 累计:                {total_orig_loss:+.2f} R")
    print(f"+ IFVG 二次入场累计:         {total_ifvg_r:+.2f} R")
    print(f"= 加入 IFVG 后总 R:          {total_orig_loss + total_ifvg_r:+.2f} R")
    print(f"  净改善:                    {total_ifvg_r:+.2f} R")
    print()

    # 按 POI 源分组
    by_src = defaultdict(lambda: {"n": 0, "reentered": 0, "wins": 0, "r": 0.0})
    for r in ifvg_results:
        src = r["poi_source"] or "?"
        by_src[src]["n"] += 1
        if r["ifvg"].get("reentered"):
            by_src[src]["reentered"] += 1
            if r["ifvg"]["outcome"] == "tp":
                by_src[src]["wins"] += 1
            by_src[src]["r"] += r["ifvg"].get("pnl_r", 0)
    print(f"按 POI 源分组(SL 信号 + IFVG):")
    print(f"{'Source':<25} {'SL数':>5} {'重入':>5} {'胜':>4} {'IFVG R':>8}")
    for src, v in sorted(by_src.items(), key=lambda kv: -kv[1]["n"]):
        print(f"{src:<25} {v['n']:>5} {v['reentered']:>5} {v['wins']:>4} {v['r']:>+8.2f}")

    # 详细 dump 触发了 IFVG 的信号
    print()
    print(f"IFVG 触发详情(按时间序):")
    print(f"{'OrigTime':<12} {'Dir':<6} {'POI':<22} {'OrigSL':<8} {'IFVGEntry':<10} {'IFVGSL':<10} {'IFVGOutcome':<12} {'IFVG R':<8}")
    for r in ifvg_results:
        if not r["ifvg"].get("reentered"):
            continue
        from datetime import datetime, timezone
        import pytz
        ny = pytz.timezone("America/New_York")
        t_et = datetime.fromtimestamp(r["time"]/1000, tz=timezone.utc).astimezone(ny).strftime("%m-%d %H:%M")
        poi = f"[{r['poi_low']:.0f}-{r['poi_high']:.0f}]"
        i = r["ifvg"]
        print(
            f"{t_et:<12} {r['direction']:<6} {poi:<22} "
            f"{r['sl']:<8.2f} {i['entry']:<10.2f} {i['sl']:<10.2f} "
            f"{i['outcome']:<12} {i['pnl_r']:>+8.2f}"
        )


def main():
    ROOT = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=187)
    args = ap.parse_args()

    with open(ROOT / "config.yaml") as f:
        cfg = yaml.safe_load(f)

    symbol = cfg["symbol"]
    ref = (cfg.get("reference") or {}).get("symbol")

    asyncio.run(main_async(symbol, ref, args.days, cfg))


if __name__ == "__main__":
    main()
