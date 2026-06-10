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

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from utils.logger import logger


@dataclass
class Tracker:
    signal_id: str
    symbol: str
    direction: str           # 'long' | 'short'
    entry: float             # limit 模式:挂单价(POI 外侧);immediate 模式:即时入场价
    sl: float
    tp1: float
    atr_4h: float
    trail_mult: float = 1.5
    tp1_portion: float = 0.5
    created_at: int = 0
    # 2026-05-24:limit 挂单状态
    #   entry_pending=True  → 等价格回测到 entry 才算入场,期间不判 TP/SL
    #   entry_pending=False → 已成交(或 immediate 模式),正常跑 TP/SL 监控
    entry_pending: bool = False
    entry_filled_at: Optional[int] = None
    tp1_hit_at: Optional[int] = None
    tp1_hit_price: Optional[float] = None
    peak: Optional[float] = None              # high since TP1 (long) / low since TP1 (short)
    closed_at: Optional[int] = None
    close_reason: Optional[str] = None        # 'sl' | 'tp2' | 'tp1_then_sl' | 'manual' | 'expired_unfilled' | 'htf_choch' | 'tp'
    poi_source: Optional[str] = None          # POI 来源(ict_ob / ict_ote / ...)
    poi_low: Optional[float] = None
    poi_high: Optional[float] = None
    tp_target: Optional[float] = None         # 原始 TP,IFVG 二次入场复用


# limit 挂单 TTL:未成交超过这个时间则作废(默认 24h)
_LIMIT_TTL_HOURS = 24


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
                    entry_pending=bool(row.get("entry_pending") or 0),
                    entry_filled_at=row.get("entry_filled_at"),
                    tp1_hit_at=row.get("tp1_hit_at"),
                    tp1_hit_price=row.get("tp1_hit_price"),
                    peak=row.get("peak"),
                )
                self.trackers[t.signal_id] = t
            logger.info(f"SignalTracker 加载 {len(self.trackers)} 条 open tracker")
        except Exception as e:  # noqa: BLE001
            logger.error(f"SignalTracker.start 加载失败: {e}")

    async def add(self, s: Any, atr_4h: float) -> None:
        """NOTIFIED 信号进入跟踪。

        2026-05-24:支持 limit 挂单
          - entry_mode='limit'(默认):创建时 entry_pending=True,等价格回测到 entry 才算入场
          - entry_mode='immediate':IFVG 二次入场,立即按 entry 起算 TP/SL
        """
        entry = getattr(s, "entry_price", None)
        sl = getattr(s, "stop_loss", None)
        tp = getattr(s, "take_profit", None)
        if entry is None or sl is None or tp is None:
            logger.warning(f"Tracker.add 跳过 {getattr(s,'signal_id','?')}: entry/sl/tp 缺失")
            return
        entry_mode = getattr(s, "entry_mode", "limit")
        is_pending = (entry_mode == "limit")
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
            entry_pending=is_pending,
            poi_source=getattr(s, "poi_source", None),
            poi_low=getattr(s, "poi_low", None),
            poi_high=getattr(s, "poi_high", None),
            tp_target=float(tp),
        )
        self.trackers[t.signal_id] = t
        await self._persist(t)
        if is_pending:
            logger.info(
                f"Tracker 挂单等待 {t.signal_id[:8]} {t.direction} limit={t.entry:.2f} "
                f"SL={t.sl:.2f} TP1={t.tp1:.2f} TTL={_LIMIT_TTL_HOURS}h"
            )
        else:
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
        """单条 tracker 条件判定。保守:同一根 K 先 SL,再 TP1,最后 trail。

        2026-05-24 改造:如果 entry_pending=True,先判 limit 成交,未成交直接返回(不判 TP/SL)。
        """
        # ---- limit 挂单未成交 ----
        if t.entry_pending:
            # SHORT: 价格上探至 limit_entry → 卖单成交
            # LONG:  价格下探至 limit_entry → 买单成交
            filled = False
            if t.direction == "short" and hi >= t.entry:
                filled = True
            elif t.direction == "long" and lo <= t.entry:
                filled = True
            if filled:
                t.entry_pending = False
                t.entry_filled_at = now
                await self._persist(t)
                await self._alert_entry_filled(t)
                return  # 这根 K 已触发 fill,本根不再判 TP/SL
            # TTL 检查:超过 _LIMIT_TTL_HOURS 未成交 → 作废
            if t.created_at and (now - t.created_at) > _LIMIT_TTL_HOURS * 3600:
                await self._close(t, reason="expired_unfilled", price=t.entry, now=now)
            return

        # ---- 已入场,跑 SL/TP/Trail 监控 ----
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
        # Tier 1:回写 outcome / pnl_r 到 signals 表(供 /signals 查询和后续统计)
        pnl_r = self._compute_pnl_r(t, price, reason)
        if self.db is not None:
            try:
                await self.db.update_signal_outcome(t.signal_id, reason, pnl_r)
            except Exception as e:  # noqa: BLE001
                logger.error(f"回写 signals.outcome 失败 {t.signal_id[:8]}: {e}")
        # 推送 + Tier 2 (SL 自动验尸)
        if reason == "tp2":
            await self._alert_tp2(t, price)
        elif reason == "sl":
            await self._alert_sl(t, price, after_tp1=False)
            asyncio.create_task(self._auto_postmortem(t, "sl"))
        elif reason == "tp1_then_sl":
            await self._alert_sl(t, price, after_tp1=True)
            asyncio.create_task(self._auto_postmortem(t, "tp1_then_sl"))
        elif reason == "expired_unfilled":
            await self._alert_limit_expired(t)

    def _compute_pnl_r(self, t: Tracker, exit_price: float, reason: str) -> Optional[float]:
        """以 entry→SL 距离为 1 R,算总 PnL。仓位单位化:tp1_portion + (1-tp1_portion)。"""
        if t.entry == t.sl:
            return None
        risk = abs(t.entry - t.sl)
        if reason == "sl":
            return -1.0  # 全仓止损
        if reason == "tp1_then_sl":
            # tp1 落袋 + 剩余仓 SL
            r_tp1 = (t.tp1 - t.entry) / risk if t.direction == "long" else (t.entry - t.tp1) / risk
            return round(t.tp1_portion * r_tp1 - (1 - t.tp1_portion), 3)
        if reason == "tp2":
            # tp1 落袋 + 剩余仓追踪止盈出场
            r_tp1 = (t.tp1 - t.entry) / risk if t.direction == "long" else (t.entry - t.tp1) / risk
            r_trail = (exit_price - t.entry) / risk if t.direction == "long" else (t.entry - exit_price) / risk
            return round(t.tp1_portion * r_tp1 + (1 - t.tp1_portion) * r_trail, 3)
        # manual / unknown
        return None

    @staticmethod
    def build_postmortem_prompt(
        signal_id: str,
        direction: Optional[str],
        entry: Optional[float],
        sl: Optional[float],
        tp1: Optional[float],
        atr_4h: Optional[float],
        reason: str,
    ) -> str:
        """构造验尸 prompt(SL 自动验尸 + 回填共用)。"""
        reason_cn = "tp1 后剩余仓 SL" if reason == "tp1_then_sl" else "全仓 SL"
        return (
            f"信号 {signal_id} 命中 SL,验尸分析:\n"
            f"- 方向: {direction}\n"
            f"- entry: {entry}, SL: {sl}, TP1: {tp1}\n"
            f"- ATR_4h at entry: {atr_4h}\n"
            f"- 出场原因: {reason}({reason_cn})\n"
            f"\n请你做以下分析:\n"
            f"1. 查 data/market.db signals 表找这条信号的 SCORED breakdown / POI source / triggered_level\n"
            f"2. 查 systemd journal 看持仓期间(从 created_at 到 closed_at)的 STEP 推进 / invalidate / 4h 翻转事件\n"
            f"3. 输出直接原因分析(总长 < 300 字,中文,准确精炼,不要废话,不要 markdown 标题符号):\n"
            f"   具体哪根 K 线触发 SL / 哪个条件在入场前就已失效 / 持仓期间量价结构如何变化。\n"
            f"4. ⚠️ 严格遵守 CLAUDE.md 规则:不要 commit / push / 部署,等用户在 TG 回 '执行' 才动手\n"
        )

    async def run_postmortem(
        self, signal_id: str, prompt: str, reason: str, timeout_s: int = 600
    ) -> Dict[str, Any]:
        """执行一次 Claude 验尸,落盘到 postmortems 表,返回结果字典。
        共用入口:自动验尸 + /backfill_postmortems 都走这里。

        返回 {"status", "output", "error_msg", "duration_ms"}.
        status: 'success' / 'empty' / 'timeout' / 'error'
        """
        started_ms = int(time.time() * 1000)
        full_output: Optional[str] = None
        status = "error"
        error_msg: Optional[str] = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "-p", prompt,
                "--dangerously-skip-permissions",
                cwd="/home/ubuntu/smcbot",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=8 * 1024 * 1024,
                env={**os.environ},
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
            full_output = stdout.decode("utf-8", errors="ignore").strip()
            if not full_output:
                status = "empty"
                error_msg = (stderr.decode("utf-8", errors="ignore")[:500] or None)
            else:
                status = "success"
        except asyncio.TimeoutError:
            status = "timeout"
            error_msg = f"claude -p {timeout_s}s 超时"
        except Exception as e:  # noqa: BLE001
            status = "error"
            error_msg = repr(e)[:500]
            logger.error(f"run_postmortem 失败 {signal_id[:8]}: {e}")
        duration_ms = int(time.time() * 1000) - started_ms
        # 不论成功失败都落盘
        if self.db is not None:
            try:
                await self.db.save_postmortem(
                    signal_id=signal_id,
                    reason=reason,
                    prompt=prompt,
                    output=full_output,
                    status=status,
                    error_msg=error_msg,
                    duration_ms=duration_ms,
                )
            except Exception as e:  # noqa: BLE001
                logger.error(f"save_postmortem 失败 {signal_id[:8]}: {e}")
        return {
            "status": status,
            "output": full_output,
            "error_msg": error_msg,
            "duration_ms": duration_ms,
        }

    async def _auto_postmortem(self, t: Tracker, reason: str) -> None:
        """Tier 2:SL 命中后,自动跑 Claude 验尸,推 TG + 落盘。"""
        if self.notifier is None:
            return
        # 等 5 秒,让前面的 ⛔ SL 提醒先送达
        await asyncio.sleep(5)
        try:
            await self.notifier.send_text(
                f"🔬 自动验尸中 [{t.signal_id[:8]}] (≤5min,Claude 在 Lightsail 跑)...",
                parse_mode=None,
            )
        except Exception:  # noqa: BLE001
            pass

        prompt = self.build_postmortem_prompt(
            signal_id=t.signal_id, direction=t.direction,
            entry=t.entry, sl=t.sl, tp1=t.tp1, atr_4h=t.atr_4h,
            reason=reason,
        )
        result = await self.run_postmortem(t.signal_id, prompt, reason)
        status = result["status"]
        full_output = result["output"]

        if status == "success" and full_output:
            tg_text = full_output if len(full_output) <= 3600 else full_output[:3600] + "\n...(truncated)"
            await self.notifier.send_text(
                f"🧠 验尸报告 [{t.signal_id[:8]}]:\n\n{tg_text}",
                parse_mode=None,
            )
        elif status == "empty":
            await self.notifier.send_text(f"⚠️ 验尸无输出 [{t.signal_id[:8]}]")
        elif status == "timeout":
            await self.notifier.send_text(f"⏱️ 验尸超时 [{t.signal_id[:8]}]")
        else:
            await self.notifier.send_text(
                f"❌ 验尸异常 [{t.signal_id[:8]}]: {result.get('error_msg') or '未知错误'}"
            )

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

    async def _alert_entry_filled(self, t: Tracker) -> None:
        """limit 挂单成交通知:tracker 从此切到 SL/TP 监控。"""
        body = (
            f"✅ Limit 已成交 [{t.signal_id[:8]}]\n"
            f"{t.direction.upper()} entry={t.entry:.2f} SL={t.sl:.2f} TP1={t.tp1:.2f}\n"
            f"→ Tracker 开始监控 SL / TP1 / TP2-trail"
        )
        await self._notify(body)

    async def _alert_limit_expired(self, t: Tracker) -> None:
        """limit 挂单 TTL 超时未成交。"""
        body = (
            f"⏱️ Limit 挂单超时 [{t.signal_id[:8]}]\n"
            f"{t.direction.upper()} limit={t.entry:.2f}\n"
            f"→ {_LIMIT_TTL_HOURS}h 内价格未回测,信号作废"
        )
        await self._notify(body)


__all__ = ["SignalTracker", "Tracker"]
