"""历史回测 harness:用历史 K 线 replay ICT engine,统计信号命中率。

- 不模拟 Telegram 通知 / DB 持久化,只打印结果
- ICT engine 读 config.yaml 的 ict.* 配置(注意 engine_enabled 须为 true 才出信号)

使用:
  python scripts/backtest.py --days 30
  python scripts/backtest.py --days 365 --export-csv /tmp/bt.csv
  python scripts/backtest.py --days 187 --trail-atr 1.5

输出:每条信号 + 前瞻 168 根 1h 的 TP/SL 模拟结果,末尾 win-rate + per-POI 汇总。
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import pickle
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
from ict_signal import ICTSignalEngine, SignalStep
from utils.logger import logger, setup_logger

_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "bt_cache"


async def load_klines(
    symbol: str,
    days: int,
    cfg: dict,
    use_cache: bool = True,
    refresh: bool = False,
) -> Dict[str, List[dict]]:
    """拉取(或从本地缓存读取)回测所需的 1d/4h/1h/15m K 线。

    缓存:`data/bt_cache/{symbol}_{days}d.pkl`。一次拉取多次复用 —— 让参数扫描 / regime
    诊断 / TP A/B 等迭代变成秒级离线操作(也让 workflow 多 agent 并行回测可行)。
    `refresh=True` 强制重新联网拉取并覆盖缓存。研究期固定快照 = 可复现。
    """
    cache_path = _CACHE_DIR / f"{symbol}_{days}d.pkl"
    if use_cache and not refresh and cache_path.exists():
        with open(cache_path, "rb") as f:
            klines = pickle.load(f)
        n = {tf: len(v) for tf, v in klines.items()}
        logger.info(f"[CACHE] 命中 {cache_path.name}: {n}")
        return klines

    proxy = cfg.get("binance", {}).get("proxy") or os.getenv("HTTP_PROXY") or None
    from data.rest_backfill import _fetch_klines_okx

    async def _fetch(sym, tf, lim):
        if lim > 1500:
            return await _fetch_klines_okx(sym, tf, lim, proxy, timeout_s=30)
        return await fetch_klines(sym, tf, limit=lim, proxy=proxy)

    # 15m 不参与 ICT 信号生成(engine 只在 4h/1h 上动作,daily bias 读 1d),且前瞻模拟用 1h。
    # 不再拉 15m(原占 fetch 量 ~80%),回测提速数倍。
    needs = {
        "1d": days + 30,
        "4h": days * 6 + 60,
        "1h": days * 24 + 200,
    }
    klines: Dict[str, List[dict]] = {}
    for tf, limit in needs.items():
        klines[tf] = await _fetch(symbol, tf, limit)
        logger.info(f"  拉取 {symbol} {tf}: {len(klines[tf])} 根")

    if use_cache:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "wb") as f:
            pickle.dump(klines, f)
        logger.info(f"[CACHE] 已写入 {cache_path}")
    return klines


# --- 绩效度量(#4:回测度量加强)----------------------------------------------

def compute_metrics(closed: List[dict], pnl_key: str = "pnl_r") -> Dict[str, float]:
    """从已闭合交易序列(按时间升序)算一组绩效度量。

    - max_drawdown_r:R 累计曲线的最大回撤(峰值到谷值,单位 R)
    - profit_factor:毛盈 / 毛亏
    - expectancy:每单期望 R
    - avg_win / avg_loss / payoff:平均盈亏 R 与盈亏比
    - max_consec_loss:最长连亏
    - win_rate / total_r / n
    """
    rs = [float(r.get(pnl_key) or 0.0) for r in closed]
    n = len(rs)
    if n == 0:
        return {"n": 0, "total_r": 0.0, "win_rate": 0.0, "expectancy": 0.0,
                "profit_factor": 0.0, "max_drawdown_r": 0.0, "avg_win": 0.0,
                "avg_loss": 0.0, "payoff": 0.0, "max_consec_loss": 0}
    wins = [x for x in rs if x > 0]
    losses = [x for x in rs if x < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    # 最大回撤(R 曲线)
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for x in rs:
        cum += x
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    # 最长连亏
    consec = 0
    max_consec = 0
    for x in rs:
        if x < 0:
            consec += 1
            max_consec = max(max_consec, consec)
        else:
            consec = 0
    avg_win = gross_win / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    return {
        "n": n,
        "total_r": sum(rs),
        "win_rate": len(wins) / n,
        "expectancy": sum(rs) / n,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else float("inf"),
        "max_drawdown_r": max_dd,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff": (avg_win / avg_loss) if avg_loss > 0 else float("inf"),
        "max_consec_loss": max_consec,
    }


def single_position_r(closed: List[dict], ny, bar_ms: int = 3_600_000):
    """单仓口径 ΣR:同一时间只允许 1 个持仓,在仓时新信号被跳过(贪心,按入场时间)。

    匹配"实盘同一时间只能开一单":自动剔除所有重叠 / 近似重复单。
    入场时间 = 信号时间 + fill_bars 根(limit 等回踩);出场 = 入场 + bars 根。
    返回 (ΣR, 取用笔数, {year: ΣR})。
    """
    trades = []
    for r in closed:
        fb = r.get("fill_bars", 0) or 0
        nb = r.get("bars", 1) or 1
        entry_ms = int(r["time"]) + fb * bar_ms
        exit_ms = entry_ms + max(1, nb) * bar_ms
        trades.append((entry_ms, exit_ms, float(r.get("pnl_r") or 0.0)))
    trades.sort(key=lambda x: x[0])
    busy_until = 0
    total = 0.0
    taken = 0
    per_year: Dict[int, float] = defaultdict(float)
    for entry_ms, exit_ms, pnl in trades:
        if entry_ms < busy_until:
            continue  # 仍在仓 → 跳过这条信号
        total += pnl
        taken += 1
        yr = datetime.fromtimestamp(entry_ms / 1000.0, tz=timezone.utc).astimezone(ny).year
        per_year[yr] += pnl
        busy_until = exit_ms
    return total, taken, dict(per_year)


def _fmt_metrics(m: Dict[str, float]) -> str:
    if m["n"] == 0:
        return "  (无闭合交易)"
    pf = m["profit_factor"]
    pf_s = "∞" if pf == float("inf") else f"{pf:.2f}"
    po = m["payoff"]
    po_s = "∞" if po == float("inf") else f"{po:.2f}"
    return (
        f"  闭合 {m['n']}  胜率 {m['win_rate']:.1%}  ΣR {m['total_r']:+.2f}  "
        f"期望 {m['expectancy']:+.3f}R/单\n"
        f"  盈亏比 {po_s}(均盈 {m['avg_win']:+.2f} / 均亏 {m['avg_loss']:-.2f})  "
        f"盈利因子 {pf_s}  最大回撤 {m['max_drawdown_r']:.2f}R  最长连亏 {m['max_consec_loss']}"
    )


# --- 前瞻 TP/SL 模拟 -------------------------------------------------------

def simulate_outcome(
    sig_time_ms: int,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    future_bars: List[dict],
    max_hours: int = 72,
    entry_mode: str = "limit",
    limit_ttl_hours: int = 24,
) -> Dict[str, object]:
    """从 sig_time_ms 之后的 K 线前瞻,找先触及 SL 还是 TP。
    max_hours 现在是"信号后多少根 bar"(不管 bar 粒度)。

    2026-05-24:支持 limit 挂单
      - entry_mode='limit':先等回测到 entry(TTL=limit_ttl_hours h),否则 expired_unfilled
      - entry_mode='immediate':立即按 entry 起算(IFVG / 回退路径)
    """
    bars_after = 0
    # ---- limit 模式:先等 fill ----
    fill_idx = -1
    fill_bars = 0
    if entry_mode == "limit":
        for idx, k in enumerate(future_bars):
            if int(k["t"]) <= sig_time_ms:
                continue
            fill_bars += 1
            if fill_bars > limit_ttl_hours:
                return {"outcome": "expired_unfilled", "pnl_r": 0.0,
                        "bars": fill_bars, "fill_bars": fill_bars}
            hi, lo = float(k["h"]), float(k["l"])
            if direction == "short" and hi >= entry:
                fill_idx = idx
                break
            if direction == "long" and lo <= entry:
                fill_idx = idx
                break
        if fill_idx < 0:
            return {"outcome": "expired_unfilled", "pnl_r": 0.0,
                    "bars": fill_bars, "fill_bars": fill_bars}
        bars_iter = future_bars[fill_idx + 1:]
    else:
        bars_iter = future_bars

    for k in bars_iter:
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
            return {"outcome": "sl", "pnl_r": -1.0, "bars": bars_after, "fill_bars": fill_bars}
        if tp_hit:
            r = abs(tp - entry) / abs(entry - sl) if entry != sl else 0.0
            return {"outcome": "tp", "pnl_r": round(r, 2), "bars": bars_after, "fill_bars": fill_bars}
    return {"outcome": "pending", "pnl_r": 0.0, "bars": bars_after, "fill_bars": fill_bars}


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
    entry_mode: str = "limit",
    limit_ttl_hours: int = 24,
) -> Dict[str, object]:
    """混合出场:SL 全仓 / TP1 平 `tp1_portion`(结构 TP)/ 剩余用 peak - `trail_atr_mult` × ATR_4h 追踪止盈。

    2026-05-24:支持 limit 挂单模式
      - entry_mode='limit':先等价格回测到 entry(SHORT: hi≥entry; LONG: lo≤entry)
        才算成交;超过 limit_ttl_hours 未触达 → 'expired_unfilled'
      - entry_mode='immediate':立即按 entry 起算(IFVG / 回退路径)

    仓位单位化:初始 1.0,按比例减仓后更新 remaining。
    返回 {outcome, pnl_r, bars, tp1_hit, trail_price(若触发), fill_bars}。
    - outcome:'sl' / 'tp1_only' / 'tp1_trail' / 'pending' / 'expired_unfilled'
    - pnl_r:合计 R(以 entry 到 SL 的全仓风险为 1 单位);未成交时 0
    """
    if entry is None or sl is None or tp is None or entry == sl:
        return {"outcome": "pending", "pnl_r": 0.0, "bars": 0, "tp1_hit": False, "fill_bars": 0}
    risk = abs(entry - sl)
    if risk <= 0:
        return {"outcome": "pending", "pnl_r": 0.0, "bars": 0, "tp1_hit": False, "fill_bars": 0}
    # 安全兜底:ATR 缺失时 trail 距离用 0.5R(就近跟)
    trail_dist = trail_atr_mult * atr_4h if atr_4h and atr_4h > 0 else 0.5 * risk

    # ---- limit 模式:先等 fill,再跑 TP/SL ----
    fill_idx = -1
    fill_bars = 0
    if entry_mode == "limit":
        for idx, k in enumerate(future_bars):
            if int(k["t"]) <= sig_time_ms:
                continue
            fill_bars += 1
            if fill_bars > limit_ttl_hours:
                # 超过 TTL 未成交,信号作废
                return {"outcome": "expired_unfilled", "pnl_r": 0.0,
                        "bars": fill_bars, "tp1_hit": False, "fill_bars": fill_bars}
            hi, lo = float(k["h"]), float(k["l"])
            # SHORT: 价格上探到 entry → 卖单成交
            # LONG:  价格下探到 entry → 买单成交
            if direction == "short" and hi >= entry:
                fill_idx = idx
                break
            if direction == "long" and lo <= entry:
                fill_idx = idx
                break
        else:
            # future_bars 用尽仍未成交
            return {"outcome": "expired_unfilled", "pnl_r": 0.0,
                    "bars": fill_bars, "tp1_hit": False, "fill_bars": fill_bars}
        if fill_idx < 0:
            return {"outcome": "expired_unfilled", "pnl_r": 0.0,
                    "bars": fill_bars, "tp1_hit": False, "fill_bars": fill_bars}
        # 从 fill 这根 K 之后开始跑 TP/SL(本根不在同一根里判,避免一根 K 内 fill+SL 同时触发)
        bars_iter = future_bars[fill_idx + 1:]
    else:
        # immediate 模式:从 sig_time 之后立即开始
        bars_iter = future_bars

    peak = entry
    tp1_hit = False
    realized_r = 0.0
    remaining = 1.0
    bars_after = 0

    for k in bars_iter:
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
                        "bars": bars_after, "tp1_hit": tp1_hit, "fill_bars": fill_bars}
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
                            "bars": bars_after, "tp1_hit": True, "trail_price": trail_stop,
                            "fill_bars": fill_bars}
                # trail_stop 仍在 entry 下方时,兜底让 SL 继续起作用
        else:
            if hi >= sl:
                realized_r += remaining * (entry - sl) / risk  # = -remaining(short: entry<sl)
                return {"outcome": "sl", "pnl_r": round(realized_r, 3),
                        "bars": bars_after, "tp1_hit": tp1_hit, "fill_bars": fill_bars}
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
                            "bars": bars_after, "tp1_hit": True, "trail_price": trail_stop,
                            "fill_bars": fill_bars}

    # 时间窗到期或数据用尽
    if tp1_hit:
        # TP1 已落袋,余仓未闭 → 按最后一根 close 近似清算余仓(保守估)
        last_close = float(future_bars[-1]["c"]) if future_bars else entry
        if direction == "long":
            realized_r += remaining * (last_close - entry) / risk
        else:
            realized_r += remaining * (entry - last_close) / risk
        return {"outcome": "tp1_only", "pnl_r": round(realized_r, 3),
                "bars": bars_after, "tp1_hit": True, "fill_bars": fill_bars}
    return {"outcome": "pending", "pnl_r": 0.0, "bars": bars_after, "tp1_hit": False,
            "fill_bars": fill_bars}


# --- 主回测流程 ------------------------------------------------------------

async def run_backtest(symbol: str, days: int, cfg: dict, csv_path: Optional[str] = None, trail_atr_mult: float = 0.0, tp1_portion: float = 0.5, use_cache: bool = True, refresh_cache: bool = False, cost_bp: float = 0.0) -> None:
    setup_logger("INFO")
    logger.info(f"回测:{symbol} days={days}")

    # 拉历史 K 线(1d/4h/1h/15m),命中本地缓存则秒级返回(见 load_klines)
    klines = await load_klines(symbol, days, cfg, use_cache=use_cache, refresh=refresh_cache)

    # 构造 ICT engine(项目唯一信号 pipeline;SMC 管线 2026-06-10 删除)
    candles = CandleManager(maxlen=3000)
    ict_engine = ICTSignalEngine(candle_manager=candles, config=cfg)

    # 合并所有 close 事件到单一时间轴:(close_time_ms, tf, sym, payload)
    events: List[tuple] = []
    for tf, rows in klines.items():
        for r in rows:
            events.append((int(r["T"]), tf, symbol, r))
    events.sort(key=lambda x: x[0])

    collected_signals: List[dict] = []

    # 主回放循环:喂 K 线 → 驱动 ICT engine
    for close_time, tf, sym, payload in events:
        candles.on_kline(sym, tf, payload)
        if sym == symbol:
            await ict_engine.on_candle_close(sym, tf, candles.last(sym, tf), db=None)

    # ----- 收割 ICT engine 的 SCORED 信号 -----
    _SS = SignalStep
    ict_collected = 0
    for sid, s in list(ict_engine.active_signals.items()):
        if s.step != _SS.SCORED:
            continue
        # 这些 setup 已经在 _emit_setup 里 dedup 过,直接接受
        collected_signals.append({
            "time": int((s.created_at or 0) * 1000),  # ms 给 simulate_outcome 用
            "level": s.triggered_level,
            "direction": s.direction,
            "entry": s.entry_price,
            "sl": s.stop_loss,
            "tp": s.take_profit,
            "rr": s.risk_reward,
            "score": s.total_score,
            "poi": s.poi_type,
            "source": s.poi_source,
            "source_engine": "ict",
            "poi_low": s.poi_low,
            "poi_high": s.poi_high,
            "entry_mode": getattr(s, "entry_mode", "immediate"),
            "notified": True,
            "scores": dict(s.scores or {}),
            "_features": {
                "time_ms": int((s.created_at or 0) * 1000),
                "direction": s.direction,
                "poi_source": s.poi_source,
                "poi_type": s.poi_type,
                "entry": s.entry_price,
                "sl": s.stop_loss,
                "tp": s.take_profit,
                "rr": s.risk_reward,
                "score_total": s.total_score,
            },
        })
        ict_collected += 1
    logger.info(f"[ICT] engine 收割 {ict_collected} 条 SCORED 信号(诊断: {dict(ict_engine.diagnostics())})")

    logger.info(f"回放完成,捕获信号 {len(collected_signals)} 条")

    # 前瞻 TP/SL 模拟(用 1h K 线拿更长历史;15m 数据源被限在 ~15 天,太少)
    # 窗口 7 天 = 168 根 1h,覆盖典型持仓周期
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
        # 统一用 simulate_outcome(固定 TP/SL;live 由 SignalTracker hybrid trail 接管)
        out = simulate_outcome(
            sig["time"], sig["direction"], sig["entry"], sl_sim, tp_sim, future_1h,
            max_hours=168,
            entry_mode=sig.get("entry_mode", "limit"),
            limit_ttl_hours=24,
        )
        merged = {**sig, **out}
        # 并行跑 hybrid(仅当启用),与 baseline 对比,不替换
        if trail_atr_mult > 0:
            atr_4h_at_sig = (sig.get("_features") or {}).get("atr_4h") or 0.0
            hy = simulate_outcome_hybrid(
                sig["time"], sig["direction"], sig["entry"], sl_sim, tp_sim,
                float(atr_4h_at_sig), future_1h, max_hours=168,
                tp1_portion=tp1_portion, trail_atr_mult=trail_atr_mult,
                entry_mode=sig.get("entry_mode", "limit"),
                limit_ttl_hours=24,
            )
            merged["hybrid_outcome"] = hy.get("outcome")
            merged["hybrid_pnl_r"] = hy.get("pnl_r")
            merged["hybrid_tp1_hit"] = hy.get("tp1_hit")
            merged["hybrid_fill_bars"] = hy.get("fill_bars")
        results.append(merged)

    ny = pytz.timezone("America/New_York")

    for rec in results:
        rec.setdefault("source_engine", "ict")

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

    # 汇总
    closed = [r for r in results if r["outcome"] in ("tp", "sl")]
    closed.sort(key=lambda r: r["time"])  # 时间序:让回撤 / 连亏度量有意义
    wins = [r for r in closed if r["outcome"] == "tp"]
    total_r = sum(r["pnl_r"] for r in closed)
    print("-" * 100)
    print(f"信号: {len(results)}  已闭合: {len(closed)}  胜: {len(wins)}  "
          f"胜率: {(len(wins)/max(1,len(closed))):.1%}  累计 R: {total_r:+.2f}")

    # ---- 绩效度量(#4)----
    print("-" * 100)
    print("[绩效度量]")
    print(_fmt_metrics(compute_metrics(closed)))
    # 未成交占比(limit 单的执行风险)
    n_unfilled = sum(1 for r in results if r["outcome"] == "expired_unfilled")
    n_pending = sum(1 for r in results if r["outcome"] == "pending")
    print(f"  未成交(expired_unfilled): {n_unfilled}  到期未平(pending): {n_pending}  "
          f"/ 总信号 {len(results)}")

    # ---- 单仓口径(#4):同一时间只持 1 仓,在仓则跳过新信号 ----
    # 这是匹配"实盘只能开一单"的真实账目 —— 自动剔除所有重叠/近似重复单,
    # 比按价格判重更彻底。无限叠仓的 ΣR 对单仓交易者是虚高。
    sp_total, sp_taken, sp_year = single_position_r(closed, ny)
    print("-" * 100)
    print(f"[单仓口径(同一时间最多 1 仓)]  取用 {sp_taken}/{len(closed)} 笔  ΣR {sp_total:+.2f}"
          f"  (无限叠仓口径 {total_r:+.2f})")
    if sp_year:
        print("  各年: " + "  ".join(f"{y}:{sp_year[y]:+.1f}" for y in sorted(sp_year)))

    # ---- 扣真实交易成本(#1:手续费+滑点,往返 cost_bp 个基点)----
    # 每单成本换算成 R:cost_R = (cost_bp/1e4 × entry) / |entry−sl|。宽 SL 的单成本占 R 更小。
    if cost_bp and cost_bp > 0:
        net_closed = []
        for r in closed:
            e, sl = r.get("entry"), r.get("sl")
            cost_r = (cost_bp / 10000.0 * e) / abs(e - sl) if (e and sl and e != sl) else 0.0
            net_closed.append({**r, "pnl_r": (r["pnl_r"] or 0.0) - cost_r, "_cost_r": cost_r})
        net_total = sum(x["pnl_r"] for x in net_closed)
        avg_cost = sum(x["_cost_r"] for x in net_closed) / max(1, len(net_closed))
        nsp_total, nsp_taken, nsp_year = single_position_r(net_closed, ny)
        print("-" * 100)
        print(f"[扣真实成本:往返 {cost_bp:.0f}bp ≈ 每单 −{avg_cost:.3f}R]")
        print(f"  无限叠仓: 毛 {total_r:+.2f} → 净 {net_total:+.2f}R  (成本 {net_total - total_r:+.2f})")
        print(f"  单仓口径: 毛 {sp_total:+.2f} → 净 {nsp_total:+.2f}R  (取用 {nsp_taken})")
        if nsp_year:
            print("  单仓各年(净): " + "  ".join(f"{y}:{nsp_year[y]:+.1f}" for y in sorted(nsp_year)))

    # ---- 按年份切分(#4 / #1:regime 依赖体检,样本外铁律)----
    by_year = defaultdict(list)
    for r in closed:
        yr = datetime.fromtimestamp(r["time"] / 1000.0, tz=timezone.utc).astimezone(ny).year
        by_year[yr].append(r)
    if len(by_year) > 1:
        print("-" * 100)
        print("[按年份切分(ET),无限叠仓口径]")
        for yr in sorted(by_year.keys()):
            m = compute_metrics(sorted(by_year[yr], key=lambda r: r["time"]))
            pf = m["profit_factor"]
            pf_s = "∞" if pf == float("inf") else f"{pf:.2f}"
            print(f"  {yr}:  闭合 {m['n']:3d}  胜率 {m['win_rate']:>5.1%}  "
                  f"ΣR {m['total_r']:+7.2f}  期望 {m['expectancy']:+.3f}  "
                  f"PF {pf_s}  最大回撤 {m['max_drawdown_r']:.1f}R")

    # 按 source_engine 分组
    by_engine = defaultdict(list)
    for r in results:
        by_engine[r.get("source_engine") or "ict"].append(r)
    if len(by_engine) > 1 or "ict" in by_engine:
        print("-" * 100)
        for eng in sorted(by_engine.keys()):
            rs = by_engine.get(eng) or []
            if not rs:
                continue
            cs = [r for r in rs if r["outcome"] in ("tp", "sl")]
            ws = [r for r in cs if r["outcome"] == "tp"]
            tr = sum(r["pnl_r"] for r in cs)
            print(f"  [{eng.upper():>3}]  信号: {len(rs):3d}  闭合: {len(cs):3d}  胜: {len(ws):2d}  "
                  f"胜率: {(len(ws)/max(1,len(cs))):>5.1%}  ΣR: {tr:+.2f}")

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
    ap.add_argument("--export-csv", dest="csv", default=None, help="信号特征 + outcome 导出到 CSV(供 ML 分析)")
    ap.add_argument("--trail-atr", type=float, default=0.0,
                    help="启用 hybrid trail:设成 >0(如 1.5),TP1 平仓后剩余按 peak − N×ATR_4h 追踪。0=仅跑 baseline")
    ap.add_argument("--tp1-portion", type=float, default=0.5, help="hybrid 模式 TP1 平仓比例(0-1)")
    ap.add_argument("--no-cache", action="store_true", help="禁用 K 线缓存,强制每次联网拉取")
    ap.add_argument("--refresh-cache", action="store_true", help="刷新缓存(联网重拉并覆盖本地快照)")
    ap.add_argument("--cost-bp", type=float, default=0.0, help="扣真实交易成本:往返基点(如 12 = 手续费+滑点往返 0.12%)")
    args = ap.parse_args()

    with open(ROOT / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    symbol = args.symbol or cfg["symbol"]

    asyncio.run(run_backtest(symbol, args.days, cfg, csv_path=args.csv,
                             trail_atr_mult=args.trail_atr, tp1_portion=args.tp1_portion,
                             use_cache=not args.no_cache, refresh_cache=args.refresh_cache,
                             cost_bp=args.cost_bp))


if __name__ == "__main__":
    main()
