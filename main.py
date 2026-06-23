"""入口：这一期只验证数据管道。

运行后：
  - 订阅 ETHUSDT 的 aggTrade + 1m/15m/4h/1d kline
  - 实时累加 VP、CVD
  - 每分钟打印一次当前 session VP
  - K 线 close 时打印 OHLCV + 该 bar delta
  - 跨过 NY 17:00 时锁定并打印 pdVAH/pdPOC/pdVAL
"""
from __future__ import annotations

import asyncio
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pytz
import yaml
from dotenv import load_dotenv

from data.binance_ws import BinanceWS
from data.okx_ws import OkxWS
from data.candle_manager import CandleManager, Candle
from data.rest_backfill import backfill_to_manager, fetch_klines
from datetime import timedelta
from engine.delta import DeltaTracker
from engine.key_levels import KeyLevels
from engine.volume_profile import VolumeProfileSession, WeeklyVolumeProfileSession
from gbrain_integration.logger import log_signal as gbrain_log_signal
from notify import TelegramNotifier
from notify.signal_tracker import SignalTracker
from ict_signal import ICTSignalEngine, SignalStep
from storage.database import Database
from utils.logger import logger, setup_logger


async def replay_recent_state(
    symbol: str,
    timeframes: list,
    hours: int,
    candles: CandleManager,
    ict_engine: ICTSignalEngine,
    weekly_vp: WeeklyVolumeProfileSession,
    levels: KeyLevels,
    proxy: Optional[str] = None,
    db: Optional[Any] = None,
    exchange: str = "binance",
) -> None:
    """重启后回放近 N 小时历史,重建 ICT engine 的 active_signals + VP/levels。

    逻辑:
      - 拉每个 TF 足够历史(warmup + 回放窗口)
      - 按 close_time 合并排序,按时间轴重放
      - close_time < cutoff:仅喂 CandleManager(为后续 engine.window 预热)
      - close_time >= cutoff:额外驱动 engine + 更新 VP/levels
      - 回放结束后,所有在回放中推进到 SCORED 的信号标为 notification_sent=True
        → 防止 live notify_loop 启动后把过期信号误推送出去
    """
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)
    tfs = [t for t in timeframes if t != "1m"]  # 1m 不驱动 state_machine
    # OTE 趋势门(#1)需 ~720(MA)+168(斜率) 根 1h 才能激活;否则 fail-open 永不生效。
    # 从 engine 读门配置,自动同步 ote_trend_ma_bars(不开门时为 0,退回原 200)。
    ote_need = (getattr(ict_engine, "ote_trend_ma_bars", 720) + 180) \
        if getattr(ict_engine, "ote_trend_gate_enabled", False) else 0
    bar_counts = {
        "1d": 120,  # daily bias 只需 30,但盘面快照的日线 S/R 要更深的 swing 历史
        "4h": 200,  # ICT _scan_4h 用 window(100) + dealing_range lookback 60 + 快照 4H S/R
        "1h": max(200, hours + 100, ote_need),
        "15m": max(300, hours * 4 + 200),
    }

    klines_by_tf: dict = {}
    for tf in tfs:
        try:
            rows = await fetch_klines(symbol, tf, limit=bar_counts.get(tf, 200), proxy=proxy, exchange=exchange)
            klines_by_tf[tf] = rows
            logger.info(f"replay 预取 {symbol} {tf}: {len(rows)} 根")
        except Exception as e:  # noqa: BLE001
            logger.error(f"replay 预取 {symbol} {tf} 失败: {e}")
            klines_by_tf[tf] = []

    events = []
    for tf, rows in klines_by_tf.items():
        for r in rows:
            events.append((int(r["T"]), tf, r))
    events.sort(key=lambda x: x[0])

    replayed = 0
    for close_time, tf, payload in events:
        candles.on_kline(symbol, tf, payload)
        if close_time < cutoff_ms:
            continue
        c = candles.last(symbol, tf)
        if c is None:
            continue
        if tf == "15m":
            ts = datetime.fromtimestamp(c.close_time / 1000, tz=timezone.utc)
            locked = weekly_vp.on_trade(c.close, c.volume, ts)
            if locked is not None:
                levels.update_prev_week(locked)
            levels.update_weekly(c.close, ts)
            levels.update_monthly(c.close, ts)
        try:
            await ict_engine.on_candle_close(symbol, tf, c, db)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"replay on_candle_close 异常 {tf}@{close_time}: {e}")
        replayed += 1

    suppressed = 0
    alive_by_step: dict = {}
    for sid, s in list(ict_engine.active_signals.items()):
        if s.step == SignalStep.SCORED:
            # 抑制:标 NOTIFIED 防止 live notify_loop 把回放期信号推送出去。
            # 2026-06-23:**不再落盘**。replay 引擎是冷启动(_emitted_origins 为空、
            # 窗口预热不同),会重新推导出 live 当时去重掉/未产的"幽灵"信号;落盘它们
            # 会在 signals 表留下无 tracker、outcome 永空的孤儿(/signals 误显示"进行中")。
            # 真实信号在 live 评分时已由 notify_loop 落盘 + 建 tracker,replay 落盘纯属冗余。
            s.notification_sent = True
            s.step = SignalStep.NOTIFIED
            suppressed += 1
        else:
            alive_by_step[s.step.value] = alive_by_step.get(s.step.value, 0) + 1

    logger.info(
        f"replay 完成: 处理 {replayed} close 事件 (cutoff={hours}h), "
        f"抑制 {suppressed} 条回放期 SCORED(不落盘,防幽灵), 保留 in-flight: {alive_by_step}"
    )


ROOT = Path(__file__).parent


def load_config() -> dict:
    with open(ROOT / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


async def periodic_vp_print(vp: VolumeProfileSession, levels: KeyLevels, interval: float = 60.0):
    while True:
        await asyncio.sleep(interval)
        cur = vp.current()
        if cur is None:
            logger.info("VP 尚未累计到数据")
            continue
        logger.info(
            f"[VP] POC={cur.poc:.2f}  VAH={cur.vah:.2f}  VAL={cur.val:.2f}  "
            f"TotalVol={cur.total_volume:.2f}  buckets={len(cur.buckets)}"
        )
        snap = levels.snapshot()
        logger.info(
            f"[LEVELS] wVAH={snap['wVAH']}  wPOC={snap['wPOC']}  wVAL={snap['wVAL']}  "
            f"wHigh={snap['wHigh']}  wLow={snap['wLow']}  "
            f"mHigh={snap['mHigh']}  mLow={snap['mLow']}"
        )


async def main_async() -> None:
    load_dotenv(ROOT / ".env")
    cfg = load_config()
    setup_logger(cfg.get("logging", {}).get("level", "INFO"))

    symbol = cfg["symbol"]
    timeframes = cfg["timeframes"]
    tz_name = cfg["timezone"]
    reset_hour = int(cfg["daily_reset_hour"])
    # REST 回填/replay 预取必须与 live WS 同交易所(否则日线对齐错配,见 fetch_klines)
    exchange = (cfg.get("exchange") or "binance").lower()

    vp_session = VolumeProfileSession(
        timezone=tz_name,
        daily_reset_hour=reset_hour,
        bucket_count=int(cfg["volume_profile"]["buckets"]),
        value_area_pct=float(cfg["volume_profile"]["value_area_pct"]),
    )
    weekly_vp = WeeklyVolumeProfileSession(
        timezone=tz_name,
        bucket_count=int(cfg["volume_profile"]["buckets"]),
        value_area_pct=float(cfg["volume_profile"]["value_area_pct"]),
    )
    delta = DeltaTracker()
    for tf in timeframes:
        delta.ensure_tf(tf)
    levels = KeyLevels()
    candles = CandleManager()

    # 数据库（DB 路径相对项目根）
    db_path = ROOT / cfg["storage"]["db_path"]
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = Database(str(db_path))
    await db.connect()

    # ICT 信号引擎(项目唯一信号 pipeline;SMC 管线 2026-06-10 删除)
    ict_engine: ICTSignalEngine = ICTSignalEngine(
        candle_manager=candles,
        config=cfg,
    )
    if ict_engine.engine_enabled:
        logger.info("ICT engine 已启用")
    else:
        logger.info("ICT engine 已加载但未启用(config.ict.engine_enabled=false)")

    # Telegram 通知器(token/chat_id 从环境变量读取,缺失则降级为仅日志)
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    tg_proxy = os.getenv("TELEGRAM_PROXY") or None
    notifier: Optional[TelegramNotifier] = None

    # Signal tracker(advisory TP1/TP2/SL 监视,不下单)
    tracker = SignalTracker(db=db, notifier=None, symbol=symbol,
                            trail_mult=1.5, tp1_portion=0.5)
    await tracker.start()

    if tg_token and tg_chat:
        notifier = TelegramNotifier(
            token=tg_token, chat_id=tg_chat, proxy=tg_proxy,
            db=db, engine=ict_engine, tracker=tracker, tz_name=tz_name,
        )
        await notifier.start()
        tracker.notifier = notifier  # 双向绑定,让 tracker 能发 TG 提醒
        await notifier.send_text(
            "🤖 ICT Bot 已启动，开始监控 ETHUSDT\n\n"
            "👇 点下方按钮查询(心跳 / 盘面 / 状态 / 近24h / 近3天)",
            with_keyboard=True,
        )
    else:
        logger.warning("未配置 TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID，通知将只输出到日志")

    # K 线 close 回调：打印 + 落盘 + 切换 per-bar delta + 驱动状态机
    def on_candle_close(sym: str, tf: str, c: Candle) -> None:
        is_main = (sym == symbol)
        if is_main:
            bar_delta = delta.reset_bar(tf)
            logger.info(
                f"[KLINE {sym} {tf}] O={c.open} H={c.high} L={c.low} C={c.close} "
                f"V={c.volume:.2f}  barDelta={bar_delta:+.2f}  CVD={delta.cvd:+.2f}"
            )
        else:
            logger.info(f"[KLINE {sym} {tf}] close={c.close} (ref only)")
        asyncio.create_task(
            db.upsert_candle(sym, tf, c.open_time, c.close_time,
                             c.open, c.high, c.low, c.close, c.volume)
        )
        if is_main:
            # ICT engine 推进(项目唯一信号 pipeline)
            asyncio.create_task(ict_engine.on_candle_close(sym, tf, c, db))
            # 仓位追踪:在 15m 收盘扫描所有 open tracker 的 TP1/SL/Trail 条件
            if tf == "15m":
                asyncio.create_task(tracker.on_candle(sym, c))

    # REST 回填 + 状态回放:在订阅 close 回调之前灌入历史数据,并用近 N 小时
    # 的 K 线重放驱动 state_machine,恢复 active_signals(Step1/2/3/5 in-flight)。
    # 这样每次重启/部署不再丢失进行中的信号 —— replay 产生的 SCORED 会被抑制,
    # 避免重启后把过期信号推送出去。
    bf_proxy = cfg["binance"].get("proxy") or os.getenv("HTTP_PROXY") or None
    replay_hours = int(cfg.get("replay_hours", 24))
    try:
        kline_tfs = [tf for tf in timeframes if tf != "1m"]
        if replay_hours > 0:
            await replay_recent_state(
                symbol=symbol,
                timeframes=kline_tfs,
                hours=replay_hours,
                candles=candles,
                ict_engine=ict_engine,
                weekly_vp=weekly_vp,
                levels=levels,
                proxy=bf_proxy,
                db=db,
                exchange=exchange,
            )
        else:
            # 回放关闭时退回原纯回填行为(1h 同样要够 OTE 趋势门激活)
            ote_need = (getattr(ict_engine, "ote_trend_ma_bars", 720) + 180) \
                if getattr(ict_engine, "ote_trend_gate_enabled", False) else 200
            backfill_counts = {"1d": 120, "4h": 200, "1h": max(200, ote_need), "15m": 200}
            await backfill_to_manager(candles, symbol, kline_tfs, backfill_counts, proxy=bf_proxy, exchange=exchange)
    except Exception as e:  # noqa: BLE001
        logger.error(f"REST 回填/状态回放失败(继续用 WS 冷启动): {e}")

    candles.subscribe_close(on_candle_close)

    # 心跳文本组装(被 heartbeat_loop 定时调用,也被 TG /heartbeat 按钮手动触发)
    from engine.market_structure import classify_structure, classify_structure_atr
    from datetime import datetime, timezone
    import pytz as _pytz_hb
    _hb_ny = _pytz_hb.timezone(tz_name)

    async def build_heartbeat_text(hours: Optional[float] = None) -> str:
        """组装一条心跳文本,供定时推送和 TG 按钮手动触发共用。
        2026-05-07:简化 — 只显示 4 TF 结构 + 阻塞原因,不再展示统计计数。"""
        # ---- 当前 4 TF 结构(对齐 Step1 窗口数 + lookback)----
        _HB_WIN = {"15m": 100, "1h": 100, "4h": 50, "1d": 30}
        _HB_LB = 3
        struct: Dict[str, str] = {}
        close_prices: Dict[str, float] = {}
        for tf in ("15m", "1h", "4h", "1d"):
            win = candles.window(symbol, tf, _HB_WIN[tf])
            if len(win) >= 13:
                if tf in ("1h", "4h", "1d"):
                    atr_v = ict_engine._atr(symbol, tf)
                    struct[tf] = (
                        classify_structure_atr(win, atr_v, 1.0)
                        if atr_v > 0 else classify_structure(win, _HB_LB)
                    )
                else:
                    struct[tf] = classify_structure(win, _HB_LB)
            else:
                struct[tf] = "?"
            last = candles.last(symbol, tf, closed_only=True)
            if last:
                close_prices[tf] = last.close

        # ---- ICT gate 状态(daily bias 决定方向准入)----
        try:
            daily_bias = ict_engine._check_daily_bias(symbol) or "neutral"
        except Exception:  # noqa: BLE001
            daily_bias = "?"

        # ---- 组装文本 ----
        now_et = datetime.now(timezone.utc).astimezone(_hb_ny).strftime("%m-%d %H:%M ET")
        emoji_map = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪", "?": "❓"}
        lines = [
            f"🫀 ICT Bot 心跳 [{now_et}]",
            f"━━━━━━━━━━━━━━━",
            f"🔍 4 TF 结构:",
        ]
        for tf, st in struct.items():
            px = close_prices.get(tf)
            px_str = f"  C={px:.2f}" if px else ""
            lines.append(f"  {emoji_map.get(st, '?')} {tf:<4} {st}{px_str}")
        bias_note = {"bullish": "只放行 long", "bearish": "只放行 short",
                     "neutral": "两向均可", "?": "数据不足"}.get(daily_bias, "")
        lines.extend(["", f"🧭 ICT daily bias: {emoji_map.get(daily_bias, '❓')} {daily_bias}({bias_note})"])
        return "\n".join(lines)

    def _cluster_levels(prices: list, tol: float, maxwidth: float) -> list:
        """把临近的 swing 价位聚成区间。返回 [(lo, hi, n), ...],n=区间内被测试次数。
        tol:相邻两点合并的最大间距;maxwidth:单个区间封顶宽度(防一串 swing 链成大坨)。"""
        if not prices:
            return []
        ps = sorted(prices)
        clusters = [[ps[0], ps[0], 1]]
        for p in ps[1:]:
            lo, hi, n = clusters[-1]
            if p - hi <= tol and p - lo <= maxwidth:
                clusters[-1][1] = p
                clusters[-1][2] = n + 1
            else:
                clusters.append([p, p, 1])
        return [(lo, hi, n) for lo, hi, n in clusters]

    def _fmt_level(lo: float, hi: float) -> str:
        """单点显示一个数,成区间显示 lo-hi。"""
        if hi - lo < max(lo * 0.0015, 1e-9):
            return f"{lo:.0f}"
        return f"{lo:.0f}-{hi:.0f}"

    def _atr_of(bars: list, n: int = 14) -> float:
        """对任意 dict/Candle 列表算 ATR(快照各级别共用)。"""
        if len(bars) < n + 2:
            return 0.0
        def _h(c): return c["high"] if isinstance(c, dict) else c.high
        def _l(c): return c["low"] if isinstance(c, dict) else c.low
        def _c(c): return c["close"] if isinstance(c, dict) else c.close
        trs = [max(_h(x) - _l(x), abs(_h(x) - _c(bars[i-1])), abs(_l(x) - _c(bars[i-1])))
               for i, x in enumerate(bars[1:], 1)]
        s = sum(trs[:n]) / n
        for t in trs[n:]:
            s = (s * (n - 1) + t) / n
        return s

    def _vol_profile(bars: list, nbuckets: int = 80) -> Optional[tuple]:
        """从 OHLCV 建价格→成交量直方图。每根成交量记入其典型价(hlc/3)所在桶。
        返回 (buckets dict, bucket_size, lo) 或 None。"""
        def g(c, k): return c[k] if isinstance(c, dict) else getattr(c, k)
        if not bars:
            return None
        lo = min(g(c, "low") for c in bars)
        hi = max(g(c, "high") for c in bars)
        if hi <= lo:
            return None
        bsize = (hi - lo) / nbuckets
        buckets: dict = {}
        for c in bars:
            tp = (g(c, "high") + g(c, "low") + g(c, "close")) / 3
            idx = int((tp - lo) / bsize)
            buckets[idx] = buckets.get(idx, 0.0) + float(g(c, "volume"))
        return buckets, bsize, lo

    def _vol_confirmed(price: float, vp: Optional[tuple]) -> bool:
        """price 所在桶(±1)的成交量是否落在 top 30%(= 成交密集区)。"""
        if not vp:
            return False
        buckets, bsize, lo = vp
        if len(buckets) < 5 or bsize <= 0:
            return False
        vols = sorted(buckets.values())
        thr = vols[int(len(vols) * 0.7)]   # 70 分位线
        idx = int((price - lo) / bsize)
        local = sum(buckets.get(idx + d, 0.0) for d in (-1, 0, 1))
        return local >= thr and thr > 0

    async def build_snapshot_text() -> str:
        """像博主那样列各级别支撑/阻力位 —— 用结构性 swing 高低点(客观 S/R),
        临近的自动聚成区间。周线现拉 OKX(bot 不订阅周线);日线/4H 用内存。不预测。"""
        from engine.market_structure import find_swing_points_atr

        emoji_map = {"bullish": "🟢 偏多", "bearish": "🔴 偏空", "neutral": "⚪ 震荡", "?": "❓"}

        last_1h = candles.last(symbol, "1h", closed_only=True)
        px = last_1h.close if last_1h else None
        if px is None:
            return "📊 盘面快照:数据预热中,稍后再试"

        lines = [
            f"📊 {symbol} 支撑/阻力 "
            f"[{datetime.now(timezone.utc).astimezone(_hb_ny).strftime('%m-%d %H:%M ET')}]",
            f"💲 现价 {px:.2f}",
            "━━━━━━━━━━━━━━━",
        ]

        def _best(clusters: list) -> Optional[tuple]:
            """挑最靠谱的一档:离现价最近(=下一个会碰到的位),测试次数仅作可靠度标注。
            (纯按测试次数排会surface 离现价很远、短期碰不到的位,反而没用。)"""
            if not clusters:
                return None
            return sorted(clusters, key=lambda c: abs((c[0] + c[1]) / 2 - px))[0]

        def _fmt_best(c: Optional[tuple], empty: str) -> str:
            if c is None:
                return empty
            lo, hi, n = c
            touch = f" ({n}次测试)" if n >= 2 else ""
            return _fmt_level(lo, hi) + touch

        def _sr_block(label: str, bars: list, atr_v: float, min_move: float) -> None:
            """算一个级别**最靠谱的 1 档** S/R 并追加到 lines。bars: dict/Candle 列表。

            min_move 调大(4h/日线 2.0、周线 1.5)只留主结构 swing,滤掉小波动;
            sep 剔除贴现价(0.8% 内)的位 —— 那些不是有效 S/R,价格已经在那。
            """
            if len(bars) < 13 or atr_v <= 0:
                return
            highs, lows = find_swing_points_atr(bars, atr_v, min_move)
            trend = classify_structure_atr(bars, atr_v, min_move)
            # 阻力 = 现价上方的 swing **高点**(峰);支撑 = 下方的 swing **低点**(谷)。
            # 不混入"被跌破的旧支撑/旧阻力"(那是极性翻转,概念上更绕,且常贴现价)。
            high_p = [float(h["price"]) for h in highs]
            low_p = [float(l["price"]) for l in lows]
            sep = px * 0.008                     # 最小间距:剔除贴现价 0.8% 内的微小波动
            tol = max(px * 0.003, atr_v * 0.3)   # 相邻合并间距
            mw = px * 0.012                      # 单区间封顶宽度 ≈1.2%
            res = _best(_cluster_levels([p for p in high_p if p > px + sep], tol, mw))
            sup = _best(_cluster_levels([p for p in low_p if p < px - sep], tol, mw))
            if res is None and sup is None:
                return
            vp = _vol_profile(bars)   # 成交量确认:落在成交密集区的位标 ⭐

            def _star(c) -> str:
                return " ⭐成交密集" if c is not None and _vol_confirmed((c[0] + c[1]) / 2, vp) else ""

            lines.append(f"【{label}级别】 {emoji_map.get(trend, '❓')}")
            lines.append("  🔴 阻力: " + _fmt_best(res, "—(上方无结构)") + _star(res))
            lines.append("  🟢 支撑: " + _fmt_best(sup, "—(下方无结构)") + _star(sup))
            lines.append("")

        # 周线:bot 不订阅,现拉 OKX 深历史(~2 年)
        try:
            wk_rows = await fetch_klines(symbol, "1w", 104, proxy=bf_proxy, exchange=exchange)
            wk = [{"open": float(r["o"]), "high": float(r["h"]), "low": float(r["l"]),
                   "close": float(r["c"]), "volume": float(r["v"]), "open_time": int(r["t"])}
                  for r in wk_rows]
            wk.sort(key=lambda c: c["open_time"])
            _sr_block("周线", wk, _atr_of(wk), 1.5)   # 周线 bar 少,1.5 已够主结构
        except Exception as e:  # noqa: BLE001
            logger.warning(f"snapshot 周线拉取失败: {e}")

        # 日线 / 4H:用内存(closed_only,剔除当日未收盘 bar);min_move=2.0 只留主结构位
        for tf, label, win_n in [("1d", "日线", 120), ("4h", "4H", 200)]:
            win = candles.window(symbol, tf, win_n, closed_only=True)
            _sr_block(label, list(win), ict_engine._atr(symbol, tf, closed_only=True), 2.0)

        # ICT bias(决定 bot 当前只发哪个方向的信号)
        try:
            bias = ict_engine._check_daily_bias(symbol) or "neutral"
        except Exception:  # noqa: BLE001
            bias = "?"
        bias_note = {"bullish": "bot 当前只发 long", "bearish": "bot 当前只发 short",
                     "neutral": "bot 两向均可", "?": "数据不足"}.get(bias, "")
        lines.append(f"🧭 ICT bias: {emoji_map.get(bias, '❓')} — {bias_note}")
        lines += ["", "⚠️ 客观 swing 高低点;⭐=落在成交密集区(更强);非预测,接近时才是真位"]
        return "\n".join(lines)

    async def heartbeat_loop():
        """定时心跳推送(每 N 小时一次)。"""
        hours = float(cfg.get("heartbeat_hours", 6))
        if hours <= 0:
            return  # 关闭定时心跳
        await asyncio.sleep(60)  # 启动后 1 分钟开始(等 replay + 数据充足)
        while True:
            try:
                text = await build_heartbeat_text(hours)
                if notifier:
                    await notifier.send_text(text, parse_mode=None)
                else:
                    logger.info("[heartbeat] " + text.replace("\n", " | "))
            except Exception as e:  # noqa: BLE001
                logger.error(f"heartbeat 异常: {e}")
            await asyncio.sleep(hours * 3600)

    async def notify_loop():
        _SS = SignalStep

        async def _process_engine_signals(eng, eng_name: str):
            """处理单个 engine 的 SCORED 信号:落盘 + 推送 + tracker.add。"""
            # 1. 先把所有 SCORED 信号(无论是否过 threshold)落盘 → /signals 命令能查到
            for sid, s in list(eng.active_signals.items()):
                if s.step == _SS.SCORED and not getattr(s, "scored_persisted", False):
                    try:
                        await db.upsert_signal_scored(s)
                        s.scored_persisted = True
                    except Exception as e:  # noqa: BLE001
                        logger.error(f"[{eng_name}] 落盘 SCORED 信号失败 {sid[:8]}: {e}")
            # 2. 推送过 threshold 的信号
            for s in eng.get_notification_ready():
                logger.success(
                    f"[{eng_name} READY] {s.symbol} {s.direction} "
                    f"entry={s.entry_price} SL={s.stop_loss} TP={s.take_profit} "
                    f"RR={s.risk_reward:.2f} score={s.total_score}"
                )
                await db.insert_signal(s)
                gbrain_log_signal(s, source="live")
                if notifier:
                    await notifier.send_signal(s, with_actions=False)
                try:
                    atr_4h = eng._atr(s.symbol, "4h")
                    await tracker.add(s, atr_4h)
                except Exception as e:  # noqa: BLE001
                    logger.error(f"[{eng_name}] tracker.add 失败 {s.signal_id[:8]}: {e}")

        while True:
            try:
                if ict_engine.engine_enabled:
                    await _process_engine_signals(ict_engine, "ICT")
            except Exception as e:
                logger.error(f"notify_loop 异常: {e}")
            await asyncio.sleep(5)

    # ----- WS handlers --------------------------------------------------

    def handle_agg(stream: str, data: dict) -> None:
        price = float(data["p"])
        qty = float(data["q"])
        is_bm = bool(data["m"])
        ts = datetime.fromtimestamp(int(data["T"]) / 1000, tz=timezone.utc)

        delta.on_trade(qty, is_bm)
        levels.update_weekly(price, ts, tz_name)
        levels.update_monthly(price, ts, tz_name)

        locked_daily = vp_session.on_trade(price, qty, ts)
        if locked_daily is not None:
            # pd* 不再进 Step2 闸口,但 daily_vp 表继续留存为历史数据
            ny = ts.astimezone(pytz.timezone(tz_name))
            date_str = (ny.date()).isoformat()
            logger.success(
                f"[昨日 VP 锁定] date={date_str}  "
                f"POC={locked_daily.poc:.2f}  VAH={locked_daily.vah:.2f}  VAL={locked_daily.val:.2f}"
            )
            asyncio.create_task(
                db.save_daily_vp(symbol, date_str, locked_daily.poc,
                                 locked_daily.vah, locked_daily.val, locked_daily.total_volume)
            )

        locked_weekly = weekly_vp.on_trade(price, qty, ts)
        if locked_weekly is not None:
            levels.update_prev_week(locked_weekly)
            logger.success(
                f"[上周 VP 锁定] wPOC={locked_weekly.poc:.2f}  "
                f"wVAH={locked_weekly.vah:.2f}  wVAL={locked_weekly.val:.2f}"
            )

    def handle_kline(stream: str, data: dict) -> None:
        k = data["k"]
        tf = k["i"]
        # stream 形如 'ethusdt@kline_4h' / 'btcusdt@kline_4h',取符号以区分
        sym = stream.split("@", 1)[0].upper()
        candles.on_kline(sym, tf, k)

    # ----- Wire up -------------------------------------------------------

    extra_streams: list[str] = []

    # 交易所选择:exchange=binance / okx(默认 binance,Lightsail/美东部署用 okx 避开 Binance geo 限制)
    exchange = (cfg.get("exchange") or "binance").lower()
    if exchange == "okx":
        ws = OkxWS(
            symbol=symbol,
            timeframes=timeframes,
            proxy=cfg["binance"].get("proxy") or None,
            reconnect_min=float(cfg["binance"]["reconnect_min"]),
            reconnect_max=float(cfg["binance"]["reconnect_max"]),
            extra_streams=extra_streams,
        )
    else:
        ws = BinanceWS(
            symbol=symbol,
            timeframes=timeframes,
            ws_base=cfg["binance"]["ws_base"],
            proxy=cfg["binance"].get("proxy") or None,
            reconnect_min=float(cfg["binance"]["reconnect_min"]),
            reconnect_max=float(cfg["binance"]["reconnect_max"]),
            extra_streams=extra_streams,
        )
    ws.on(f"{symbol.lower()}@aggTrade", handle_agg)
    for tf in timeframes:
        ws.on(f"{symbol.lower()}@kline_{tf}", handle_kline)
    for s in extra_streams:
        ws.on(s, handle_kline)

    stop_event = asyncio.Event()

    def _sig_handler(*_):
        logger.warning("收到退出信号，正在关闭...")
        stop_event.set()
        ws.stop()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass  # Windows 兼容，本项目 macOS 不触发

    logger.info(f"启动数据采集: {symbol}  tf={timeframes}  tz={tz_name}")
    # 把心跳 / 盘面快照文本组装函数注入 notifier,让 TG 按钮能手动触发
    if notifier is not None:
        notifier.heartbeat_text_builder = build_heartbeat_text  # type: ignore[attr-defined]
        notifier.snapshot_text_builder = build_snapshot_text  # type: ignore[attr-defined]

    tasks = [
        asyncio.create_task(ws.run()),
        asyncio.create_task(periodic_vp_print(vp_session, levels, 60.0)),
        asyncio.create_task(notify_loop()),
        asyncio.create_task(heartbeat_loop()),
    ]
    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if notifier:
            await notifier.stop()
        await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
