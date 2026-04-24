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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
from engine.market_structure import classify_structure
from engine.volume_profile import VolumeProfileSession, WeeklyVolumeProfileSession
from gbrain_integration.logger import log_signal as gbrain_log_signal
from notify import TelegramNotifier
from smc_signal import SignalEngine, score as signal_score
from smc_signal.state_machine import SignalStep
from storage.database import Database
from utils.logger import logger, setup_logger


async def replay_recent_state(
    symbol: str,
    timeframes: list,
    hours: int,
    candles: CandleManager,
    engine: SignalEngine,
    weekly_vp: WeeklyVolumeProfileSession,
    levels: KeyLevels,
    proxy: Optional[str] = None,
) -> None:
    """重启后回放近 N 小时历史,重建 state_machine 的 active_signals + VP/levels。

    逻辑:
      - 拉每个 TF 足够历史(warmup + 回放窗口)
      - 按 close_time 合并排序,按时间轴重放
      - close_time < cutoff:仅喂 CandleManager(为后续 engine.window 预热)
      - close_time >= cutoff:额外驱动 engine + 更新 VP/levels + delta 近似
      - 回放结束后,所有在回放中推进到 SCORED 的信号标为 notification_sent=True
        → 防止 live notify_loop 启动后把过期信号误推送出去
    """
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)
    tfs = [t for t in timeframes if t != "1m"]  # 1m 不驱动 state_machine
    bar_counts = {
        "1d": 30,
        "4h": 80,
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
        d_approx = c.volume * (0.6 if c.close > c.open else -0.6 if c.close < c.open else 0.0)
        engine.set_bar_delta(tf, d_approx)
        try:
            await engine.on_candle_close(symbol, tf, c)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"replay on_candle_close 异常 {tf}@{close_time}: {e}")
        replayed += 1

    suppressed = 0
    alive_by_step: dict = {}
    for sid, s in list(engine.active_signals.items()):
        if s.step == SignalStep.SCORED:
            s.notification_sent = True
            s.step = SignalStep.NOTIFIED
            suppressed += 1
        else:
            alive_by_step[s.step.value] = alive_by_step.get(s.step.value, 0) + 1

    logger.info(
        f"replay 完成: 处理 {replayed} close 事件 (cutoff={hours}h), "
        f"抑制 {suppressed} 条过期 SCORED, 保留 in-flight: {alive_by_step}"
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

    # 信号引擎
    engine = SignalEngine(
        candle_manager=candles,
        key_levels=levels,
        scorer=signal_score,
        config=cfg,
    )
    engine.set_vp_provider(lambda: vp_session.current())

    # BTC 联动过滤:参考品种结构与 ETH 方向冲突 → 否决 Step1
    ref_cfg = cfg.get("reference") or {}
    ref_symbol = (ref_cfg.get("symbol") or "").upper() or None
    ref_tf = ref_cfg.get("timeframe") or "4h"
    if ref_symbol:
        swing_lb = int(cfg.get("signal", {}).get("swing_lookback", 5))
        def _correlation_gate(direction: str) -> bool:
            ref_candles = candles.window(ref_symbol, ref_tf, 50)
            if len(ref_candles) < 2 * swing_lb + 3:
                return True  # 参考数据不足时不卡
            ref_struct = classify_structure(ref_candles, swing_lb)
            if ref_struct == "neutral":
                return True  # BTC 方向不明时不卡
            want = "bullish" if direction == "long" else "bearish"
            return ref_struct == want
        engine.set_correlation_gate(_correlation_gate)

    # Telegram 通知器(token/chat_id 从环境变量读取,缺失则降级为仅日志)
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    tg_proxy = os.getenv("TELEGRAM_PROXY") or None
    notifier: Optional[TelegramNotifier] = None

    if tg_token and tg_chat:
        notifier = TelegramNotifier(token=tg_token, chat_id=tg_chat, proxy=tg_proxy)
        await notifier.start()
        await notifier.send_text("🤖 SMC Bot 已启动，开始监控 ETHUSDT")
    else:
        logger.warning("未配置 TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID，通知将只输出到日志")

    # K 线 close 回调：打印 + 落盘 + 切换 per-bar delta + 驱动状态机
    def on_candle_close(sym: str, tf: str, c: Candle) -> None:
        # 主品种才切 delta bar + 驱状态机;ref_symbol(BTC)仅存 candle 供 correlation gate 读
        is_main = (sym == symbol)
        if is_main:
            bar_delta = delta.reset_bar(tf)
            engine.set_bar_delta(tf, bar_delta)
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
            asyncio.create_task(engine.on_candle_close(sym, tf, c))

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
                engine=engine,
                weekly_vp=weekly_vp,
                levels=levels,
                proxy=bf_proxy,
            )
        else:
            # 回放关闭时退回原纯回填行为
            backfill_counts = {"1d": 30, "4h": 50, "1h": 200, "15m": 200}
            await backfill_to_manager(candles, symbol, kline_tfs, backfill_counts, proxy=bf_proxy)
        if ref_symbol:
            await backfill_to_manager(candles, ref_symbol, [ref_tf], {ref_tf: 50}, proxy=bf_proxy)
    except Exception as e:  # noqa: BLE001
        logger.error(f"REST 回填/状态回放失败(继续用 WS 冷启动): {e}")

    candles.subscribe_close(on_candle_close)

    async def notify_loop():
        while True:
            try:
                for s in engine.get_notification_ready():
                    logger.success(
                        f"[SIGNAL READY] {s.symbol} {s.direction} "
                        f"entry={s.entry_price} SL={s.stop_loss} TP={s.take_profit} "
                        f"RR={s.risk_reward:.2f} score={s.total_score}"
                    )
                    # 先落盘,再发通知 —— 保证按钮回调永远能查到 signal 行
                    await db.insert_signal(s)
                    # GBrain 旁路记录(sync, 永不 raise, 失败只 warn)
                    gbrain_log_signal(s, source="live")
                    if notifier:
                        await notifier.send_signal(s, with_actions=False)
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
    if ref_symbol:
        extra_streams.append(f"{ref_symbol.lower()}@kline_{ref_tf}")

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
    tasks = [
        asyncio.create_task(ws.run()),
        asyncio.create_task(periodic_vp_print(vp_session, levels, 60.0)),
        asyncio.create_task(notify_loop()),
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
