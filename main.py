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
    bar_counts = {
        "1d": 30,
        "4h": 130,  # ICT _scan_4h 用 window(100) + dealing_range lookback 60,预热给足
        "1h": max(200, hours + 100),
        "15m": max(300, hours * 4 + 200),
    }

    klines_by_tf: dict = {}
    for tf in tfs:
        try:
            rows = await fetch_klines(symbol, tf, limit=bar_counts.get(tf, 200), proxy=proxy)
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
    persisted = 0
    for sid, s in list(ict_engine.active_signals.items()):
        if s.step == SignalStep.SCORED:
            # 落盘(在抑制 notification 之前):/signals 命令能查到回放期 SCORED 信号
            if db is not None:
                try:
                    await db.upsert_signal_scored(s)
                    s.scored_persisted = True
                    persisted += 1
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"replay 落盘 SCORED 信号失败 {sid[:8]}: {e}")
            s.notification_sent = True
            s.step = SignalStep.NOTIFIED
            suppressed += 1
        else:
            alive_by_step[s.step.value] = alive_by_step.get(s.step.value, 0) + 1

    logger.info(
        f"replay 完成: 处理 {replayed} close 事件 (cutoff={hours}h), "
        f"抑制 {suppressed} 条过期 SCORED(落盘 {persisted}), 保留 in-flight: {alive_by_step}"
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
            "👇 点下方按钮查询,或输入 /help 看命令",
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
            )
        else:
            # 回放关闭时退回原纯回填行为
            backfill_counts = {"1d": 30, "4h": 130, "1h": 200, "15m": 200}
            await backfill_to_manager(candles, symbol, kline_tfs, backfill_counts, proxy=bf_proxy)
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
    # 把心跳文本组装函数注入 notifier,让 TG 按钮能手动触发
    if notifier is not None:
        notifier.heartbeat_text_builder = build_heartbeat_text  # type: ignore[attr-defined]

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
