"""Advisory 仓位跟踪器。

不下单、不改 SL/TP,只是盯价格。当 15m K 线的 H/L 触发条件时,推 TG 通知:
  - TP1 命中 → 用户手动平 tp1_portion(默认 50%)
  - TP2 trail 命中 → 用户手动平剩余仓位
  - SL 命中 → 用户手动全仓止损

假设:每条 NOTIFIED 信号 = 用户在 Entry 价建仓一笔。未进场的信号,用户可
`/close <sid>` 停止跟踪。

重启持久化:trackers 落 DB,systemd 重启后 start() 自动恢复。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from utils.logger import logger


@dataclass
class Tracker:
    signal_id: str
    symbol: str
    direction: str           # 'long' | 'short'
    entry: float
    sl: float
    tp1: float
    atr_4h: float
    trail_mult: float = 1.5
    tp1_portion: float = 0.5
    created_at: int = 0
    tp1_hit_at: Optional[int] = None
    tp1_hit_price: Optional[float] = None
    peak: Optional[float] = None              # high since TP1 (long) / low since TP1 (short)
    closed_at: Optional[int] = None
    close_reason: Optional[str] = None        # 'sl' | 'tp2' | 'tp1_then_sl' | 'manual'


class SignalTracker:
    """Notified signal 的 TP1 / TP2-trail / SL 监视器。"""

    def __init__(
        self,
        db: Any = None,
        notifier: Any = None,
        symbol: Optional[str] = None,
        trail_mult: float = 1.5,
        tp1_portion: float = 0.5,
    ) -> None:
        self.db = db
        self.notifier = notifier
        self.symbol = symbol
        self.default_trail_mult = trail_mult
        self.default_tp1_portion = tp1_portion
        self.trackers: Dict[str, Tracker] = {}

    async def start(self) -> None:
        """重启时从 DB 恢复 open trackers。"""
        if self.db is None:
            return
        try:
            rows = await self.db.load_open_trackers()
            for row in rows:
                t = Tracker(
                    signal_id=row["signal_id"],
                    symbol=row["symbol"],
                    direction=row["direction"],
                    entry=float(row["entry"]),
                    sl=float(row["sl"]),
                    tp1=float(row["tp1"]),
                    atr_4h=float(row.get("atr_4h") or 0.0),
                    trail_mult=float(row.get("trail_mult") or self.default_trail_mult),
                    tp1_portion=float(row.get("tp1_portion") or self.default_tp1_portion),
                    created_at=int(row.get("created_at") or 0),
                    tp1_hit_at=row.get("tp1_hit_at"),
                    tp1_hit_price=row.get("tp1_hit_price"),
                    peak=row.get("peak"),
                )
                self.trackers[t.signal_id] = t
            logger.info(f"SignalTracker 加载 {len(self.trackers)} 条 open tracker")
        except Exception as e:  # noqa: BLE001
            logger.error(f"SignalTracker.start 加载失败: {e}")

    async def add(self, s: Any, atr_4h: float) -> None:
        """NOTIFIED 信号进入跟踪。"""
        entry = getattr(s, "entry_price", None)
        sl = getattr(s, "stop_loss", None)
        tp = getattr(s, "take_profit", None)
        if entry is None or sl is None or tp is None:
            logger.warning(f"Tracker.add 跳过 {getattr(s,'signal_id','?')}: entry/sl/tp 缺失")
            return
        t = Tracker(
            signal_id=s.signal_id,
            symbol=s.symbol,
            direction=s.direction,
            entry=float(entry),
            sl=float(sl),
            tp1=float(tp),
            atr_4h=float(atr_4h) if atr_4h and atr_4h > 0 else 0.0,
            trail_mult=self.default_trail_mult,
            tp1_portion=self.default_tp1_portion,
            created_at=int(time.time()),
        )
        self.trackers[t.signal_id] = t
        await self._persist(t)
        logger.info(
            f"Tracker 接管 {t.signal_id[:8]} {t.direction} entry={t.entry:.2f} "
            f"SL={t.sl:.2f} TP1={t.tp1:.2f} ATR4h={t.atr_4h:.2f}"
        )

    async def close_manual(self, sid_prefix: str) -> Optional[Tracker]:
        """/close 命令用。按 SID 前缀匹配第一条 open tracker 关闭。"""
        for sid, t in self.trackers.items():
            if t.closed_at is None and sid.startswith(sid_prefix):
                t.closed_at = int(time.time())
                t.close_reason = "manual"
                await self._persist(t)
                return t
        return None

    async def on_candle(self, symbol: str, candle: Any) -> None:
        """在 15m K 线收盘时调用。扫描所有 open tracker 判定条件。"""
        if self.symbol and symbol != self.symbol:
            return
        if not self.trackers:
            return
        hi = float(getattr(candle, "high", 0))
        lo = float(getattr(candle, "low", 0))
        ct_ms = getattr(candle, "close_time", None)
        now = int(ct_ms / 1000) if ct_ms else int(time.time())
        for sid in list(self.trackers.keys()):
            t = self.trackers[sid]
            if t.closed_at is not None or t.symbol != symbol:
                continue
            try:
                await self._check_one(t, hi, lo, now)
            except Exception as e:  # noqa: BLE001
                logger.error(f"tracker check 失败 {sid[:8]}: {e}")

    async def _check_one(self, t: Tracker, hi: float, lo: float, now: int) -> None:
        """单条 tracker 条件判定。保守:同一根 K 先 SL,再 TP1,最后 trail。"""
        if t.direction == "long":
            # 1. SL 全仓止损(优先,避免在同根 K 内误报 TP)
            if lo <= t.sl:
                reason = "tp1_then_sl" if t.tp1_hit_at else "sl"
                await self._close(t, reason=reason, price=t.sl, now=now)
                return
            # 2. TP1 首次触达
            if t.tp1_hit_at is None and hi >= t.tp1:
                t.tp1_hit_at = now
                t.tp1_hit_price = t.tp1
                t.peak = max(t.tp1, hi)
                await self._persist(t)
                await self._alert_tp1(t)
                return
            # 3. TP1 后:更新 peak + 检查 trail
            if t.tp1_hit_at is not None:
                if t.peak is None or hi > t.peak:
                    t.peak = hi
                trail_stop = t.peak - t.trail_mult * t.atr_4h
                # 保护:trail_stop 必须高于 entry,否则已退回 SL 范围,交给 SL 处理
                if trail_stop > t.entry and lo <= trail_stop:
                    await self._close(t, reason="tp2", price=trail_stop, now=now)
                else:
                    await self._persist(t)
        else:  # short - 对称
            if hi >= t.sl:
                reason = "tp1_then_sl" if t.tp1_hit_at else "sl"
                await self._close(t, reason=reason, price=t.sl, now=now)
                return
            if t.tp1_hit_at is None and lo <= t.tp1:
                t.tp1_hit_at = now
                t.tp1_hit_price = t.tp1
                t.peak = min(t.tp1, lo)
                await self._persist(t)
                await self._alert_tp1(t)
                return
            if t.tp1_hit_at is not None:
                if t.peak is None or lo < t.peak:
                    t.peak = lo
                trail_stop = t.peak + t.trail_mult * t.atr_4h
                if trail_stop < t.entry and hi >= trail_stop:
                    await self._close(t, reason="tp2", price=trail_stop, now=now)
                else:
                    await self._persist(t)

    async def _close(self, t: Tracker, reason: str, price: float, now: int) -> None:
        t.closed_at = now
        t.close_reason = reason
        await self._persist(t)
        if reason == "tp2":
            await self._alert_tp2(t, price)
        elif reason == "sl":
            await self._alert_sl(t, price, after_tp1=False)
        elif reason == "tp1_then_sl":
            await self._alert_sl(t, price, after_tp1=True)

    async def _persist(self, t: Tracker) -> None:
        if self.db is None:
            return
        try:
            await self.db.upsert_tracker(t)
        except Exception as e:  # noqa: BLE001
            logger.error(f"tracker 落盘失败 {t.signal_id[:8]}: {e}")

    # --- Telegram 通知 ------------------------------------------------------

    async def _notify(self, text: str) -> None:
        """发纯文本(不用 Markdown,避免 _ / * / ` 解析踩坑)。"""
        if self.notifier is None:
            return
        try:
            # send_text 默认 Markdown,我们给个 plain(没 markdown 特殊字符,不依赖 parse 模式)
            await self.notifier.send_text(text, parse_mode=None)
        except TypeError:
            # 旧版 send_text 不支持 parse_mode 参数,回退
            try:
                await self.notifier.send_text(text)
            except Exception as e:  # noqa: BLE001
                logger.error(f"tracker send_text 失败: {e}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"tracker send_text 失败: {e}")

    async def _alert_tp1(self, t: Tracker) -> None:
        trail_dist = t.trail_mult * t.atr_4h
        body = (
            f"🎯 TP1 命中 [{t.signal_id[:8]}]\n"
            f"{t.direction.upper()} entry={t.entry:.2f} TP1={t.tp1:.2f}\n"
            f"→ 建议手动平 {int(t.tp1_portion*100)}%\n"
            f"→ 剩余启用追踪止盈:peak ± {t.trail_mult}×ATR_4h ≈ {trail_dist:.2f} 回撤"
        )
        await self._notify(body)

    async def _alert_tp2(self, t: Tracker, price: float) -> None:
        peak_val = t.peak if t.peak is not None else 0.0
        body = (
            f"🎯 TP2 追踪止盈触发 [{t.signal_id[:8]}]\n"
            f"{t.direction.upper()} peak={peak_val:.2f}\n"
            f"出场价 ≈ {price:.2f}\n"
            f"→ 建议手动平剩余仓位"
        )
        await self._notify(body)

    async def _alert_sl(self, t: Tracker, price: float, after_tp1: bool) -> None:
        tag = "TP1 后回撤到 SL" if after_tp1 else "全仓 SL 命中"
        body = (
            f"⛔ {tag} [{t.signal_id[:8]}]\n"
            f"{t.direction.upper()} entry={t.entry:.2f} SL={t.sl:.2f}\n"
            f"→ {'剩余' if after_tp1 else '全仓'}已止损"
        )
        await self._notify(body)


__all__ = ["SignalTracker", "Tracker"]
