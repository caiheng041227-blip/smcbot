"""#1 regime 依赖诊断(纯事后分析,不改 bot 行为)。

目标:730d 是 +43R / −20R(顺境年赚、逆境年回吐,主因 OTE)。本脚本给每条 ICT 信号
打上"信号产生时"的 regime 标签(只用过去 1h K 线,无未来函数),再按 年份 × regime × POI
交叉统计期望 R,定位 −20R 到底来自哪种市况 —— 为"正交于 bias 的 regime 过滤 / 降仓"找证据。

用法:
  python3 scripts/_regime_diag.py            # 默认 730d,读缓存
  python3 scripts/_regime_diag.py --days 1460

铁律:任何由此得出的过滤/降仓规则,必须在 ≥2 年各自不恶化才算数(见 gbrain 过拟合教训)。
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pytz
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.candle_manager import CandleManager
from ict_signal import ICTSignalEngine, SignalStep
from scripts.backtest import load_klines, simulate_outcome, compute_metrics
from utils.logger import setup_logger

NY = pytz.timezone("America/New_York")


async def collect_signals(symbol: str, days: int, cfg: dict):
    """跑一遍 ICT engine,收割 SCORED 信号 + 前瞻 168 根 1h 模拟 outcome。"""
    klines = await load_klines(symbol, days, cfg, use_cache=True)
    candles = CandleManager(maxlen=3000)
    engine = ICTSignalEngine(candle_manager=candles, config=cfg)

    events = []
    for tf, rows in klines.items():
        for r in rows:
            events.append((int(r["T"]), tf, r))
    events.sort(key=lambda x: x[0])
    for close_time, tf, payload in events:
        candles.on_kline(symbol, tf, payload)
        await engine.on_candle_close(symbol, tf, candles.last(symbol, tf), db=None)

    sigs = []
    for sid, s in list(engine.active_signals.items()):
        if s.step != SignalStep.SCORED:
            continue
        sigs.append({
            "time": int((s.created_at or 0) * 1000),
            "direction": s.direction,
            "entry": s.entry_price,
            "sl": s.stop_loss,
            "tp": s.take_profit,
            "source": s.poi_source,
            "poi": s.poi_type,
            "entry_mode": getattr(s, "entry_mode", "immediate"),
        })

    future_1h = klines["1h"]
    for sig in sigs:
        out = simulate_outcome(
            sig["time"], sig["direction"], sig["entry"], sig["sl"], sig["tp"],
            future_1h, max_hours=168, entry_mode=sig.get("entry_mode", "limit"),
            limit_ttl_hours=24,
        )
        sig.update(out)
    return sigs, klines["1h"]


# --- regime 特征(只用信号时刻之前的 1h 数据)---------------------------------

def _build_index(bars_1h):
    """预算 1h 数组:升序 close_time(T)/close/high/low,供二分定位。"""
    rows = sorted(bars_1h, key=lambda k: int(k["T"]))
    ts = [int(k["T"]) for k in rows]
    close = [float(k["c"]) for k in rows]
    high = [float(k["h"]) for k in rows]
    low = [float(k["l"]) for k in rows]
    return ts, close, high, low


def _idx_at(ts, sig_ms):
    """信号时刻最后一根已收 1h bar 的下标(close_time <= sig_ms)。"""
    import bisect
    i = bisect.bisect_right(ts, sig_ms) - 1
    return i


def _atr_pct(high, low, close, i, n=14):
    if i < n:
        return None
    trs = []
    for j in range(i - n + 1, i + 1):
        tr = max(high[j] - low[j], abs(high[j] - close[j - 1]), abs(low[j] - close[j - 1]))
        trs.append(tr)
    atr = sum(trs) / len(trs)
    return atr / close[i] if close[i] else None


def regime_features(ts, close, high, low, sig_ms, direction):
    """返回 dict:trend / vol / align / 以及原始数值。无足够历史返回 None。"""
    i = _idx_at(ts, sig_ms)
    if i < 720:  # 需要 ~30d 1h 历史
        return None
    c = close[i]
    ma_fast = sum(close[i - 200 + 1:i + 1]) / 200          # ~8d
    ma_slow = sum(close[i - 720 + 1:i + 1]) / 720           # ~30d
    ma_slow_prev = sum(close[i - 720 - 168 + 1:i - 168 + 1]) / 720  # 168 bar 前的慢线
    ret_30d = c / close[i - 720] - 1.0
    ret_7d = c / close[i - 168] - 1.0
    atr_pct = _atr_pct(high, low, close, i)

    # 趋势 regime:慢线斜率 + 价格位置
    slope_up = ma_slow > ma_slow_prev
    if c > ma_slow and slope_up:
        trend = "up"
    elif c < ma_slow and not slope_up:
        trend = "down"
    else:
        trend = "range"

    # 波动 regime:atr_pct 相对过去 90d(2160 bar)分位
    vol = "?"
    if atr_pct is not None and i >= 2160:
        window = [_atr_pct(high, low, close, j) for j in range(i - 2160, i + 1, 24)]  # 每天采一点
        window = [x for x in window if x is not None]
        if window:
            rank = sum(1 for x in window if x <= atr_pct) / len(window)
            vol = "hi" if rank >= 0.66 else ("lo" if rank <= 0.33 else "mid")

    # 方向是否顺势(相对慢线趋势)
    if trend == "range":
        align = "range"
    elif (direction == "long" and trend == "up") or (direction == "short" and trend == "down"):
        align = "with"
    else:
        align = "counter"

    return {
        "trend": trend, "vol": vol, "align": align,
        "ret_30d": ret_30d, "ret_7d": ret_7d, "atr_pct": atr_pct,
    }


def _year(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(NY).year


def _print_bucket(title, groups):
    """groups: {label: [pnl_r,...]} → 打印 n / 胜率 / ΣR / 期望。"""
    print(f"\n[{title}]")
    print(f"  {'bucket':<14}{'n':>4}{'胜率':>8}{'ΣR':>9}{'期望R':>9}")
    for label in sorted(groups.keys()):
        rs = groups[label]
        n = len(rs)
        wins = sum(1 for x in rs if x > 0)
        sr = sum(rs)
        print(f"  {str(label):<14}{n:>4}{wins/max(1,n):>7.1%}{sr:>+9.2f}{sr/max(1,n):>+9.3f}")


async def load_tagged(symbol: str, days: int, cfg: dict, source: str | None = None):
    """跑 engine + 给每条已闭合信号打 regime 标签,返回 (closed_list)。供本脚本与过滤测试复用。"""
    sigs, bars_1h = await collect_signals(symbol, days, cfg)
    ts, close, high, low = _build_index(bars_1h)
    closed = []
    for s in sigs:
        if s["outcome"] not in ("tp", "sl"):
            continue
        if source and s["source"] != source:
            continue
        feat = regime_features(ts, close, high, low, s["time"], s["direction"])
        if feat is None:
            continue
        s.update(feat)
        s["year"] = _year(s["time"])
        closed.append(s)
    return closed


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--source", default=None, help="只看某个 POI source(如 ote/raid/ob)")
    args = ap.parse_args()
    setup_logger("WARNING")

    ROOT = Path(__file__).resolve().parent.parent
    with open(ROOT / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    symbol = args.symbol or cfg["symbol"]

    closed = await load_tagged(symbol, args.days, cfg, args.source)

    print("=" * 78)
    print(f"regime 诊断:{symbol} {args.days}d  闭合信号 {len(closed)} 条"
          + (f"  (source={args.source})" if args.source else ""))
    m = compute_metrics(sorted(closed, key=lambda r: r["time"]))
    pf = m["profit_factor"]
    print(f"  总:胜率 {m['win_rate']:.1%}  ΣR {m['total_r']:+.2f}  期望 {m['expectancy']:+.3f}  "
          f"最大回撤 {m['max_drawdown_r']:.1f}R")

    # 1) 按年
    by_year = defaultdict(list)
    for s in closed:
        by_year[s["year"]].append(s["pnl_r"])
    _print_bucket("按年份", by_year)

    # 2) 按 trend regime
    by_trend = defaultdict(list)
    for s in closed:
        by_trend[s["trend"]].append(s["pnl_r"])
    _print_bucket("按趋势 regime(30d 慢线)", by_trend)

    # 3) 按 align(顺势/逆势)
    by_align = defaultdict(list)
    for s in closed:
        by_align[s["align"]].append(s["pnl_r"])
    _print_bucket("按方向对齐", by_align)

    # 4) 按 vol regime
    by_vol = defaultdict(list)
    for s in closed:
        by_vol[s["vol"]].append(s["pnl_r"])
    _print_bucket("按波动 regime(90d ATR 分位)", by_vol)

    # 5) 年份 × trend 交叉(定位 −20R 来源)
    print("\n[年份 × 趋势 regime 交叉(ΣR / n)]")
    years = sorted(set(s["year"] for s in closed))
    trends = ["up", "range", "down"]
    print(f"  {'':<8}" + "".join(f"{t:>14}" for t in trends))
    for y in years:
        cells = []
        for t in trends:
            rs = [s["pnl_r"] for s in closed if s["year"] == y and s["trend"] == t]
            cells.append(f"{sum(rs):+.1f}/{len(rs)}")
        print(f"  {y:<8}" + "".join(f"{c:>14}" for c in cells))

    # 6) POI × align 交叉
    print("\n[POI source × 方向对齐(ΣR / n)]")
    srcs = sorted(set(s["source"] for s in closed))
    aligns = ["with", "counter", "range"]
    print(f"  {'':<18}" + "".join(f"{a:>14}" for a in aligns))
    for src in srcs:
        cells = []
        for a in aligns:
            rs = [s["pnl_r"] for s in closed if s["source"] == src and s["align"] == a]
            cells.append(f"{sum(rs):+.1f}/{len(rs)}")
        print(f"  {src:<18}" + "".join(f"{c:>14}" for c in cells))


if __name__ == "__main__":
    asyncio.run(main())
