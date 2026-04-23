"""6 步状态机 —— SMC/ICT 信号生命周期。

职责：把"每根 K 线收盘"事件转化为信号状态推进。评分与通知是旁路流程，
只在 STEP6 完成后跑一次。允许同一 symbol 并存多条 in-flight 信号
（例如日线多头 + 多个 POI），各自独立前进或超时。
"""
from __future__ import annotations

import math
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence

from engine.market_structure import (
    classify_structure,
    classify_structure_atr,
    detect_choch,
    find_swing_points,
    find_swing_points_atr,
    _close as _c,
    _h,
    _l,
    _t,
)
from engine.poi import (
    find_order_blocks, find_fvgs, find_breaker_blocks, find_equal_lows_highs_pois,
    find_failed_sweep_reentry_pois, find_failed_fvg_reentry_pois, find_double_bottom_pois,
)
from utils.logger import logger


class SignalStep(str, Enum):
    IDLE = "IDLE"
    STEP1_PASSED = "STEP1_PASSED"   # 4H 方向共识(D1 不冲突)
    STEP2_PASSED = "STEP2_PASSED"   # POI 已锁定(找到方向匹配的未回测 OB)
    STEP3_PASSED = "STEP3_PASSED"   # 扫荡发生在 POI 区间内 + wick/delta 至少一个确认
    STEP4_PASSED = "STEP4_PASSED"   # [已弃用] 旧流程的"找 POI"步骤,新版合并到 Step2
    STEP5_PASSED = "STEP5_PASSED"   # POI 内 15m CHoCH 确认
    STEP6_PASSED = "STEP6_PASSED"   # 入场(SL+TP 用户手动)
    SCORED = "SCORED"
    NOTIFIED = "NOTIFIED"
    EXPIRED = "EXPIRED"              # 时间 TTL 超时
    INVALIDATED = "INVALIDATED"      # 场景前提失效(结构翻转 / POI 破坏 / 重复 POI)


@dataclass
class SignalState:
    signal_id: str
    symbol: str
    direction: str                          # "long" / "short"
    step: SignalStep
    triggered_level: Optional[str] = None
    triggered_level_value: Optional[float] = None    # 触发价位的实际价
    step1_trigger_price: Optional[float] = None      # Step1 创建时的 4h 收盘价,用于漂移失效检查
    step1_override_by_sweep: bool = False            # Step1 是否通过 1h 冲突 + deep sweep override 放行(用于 C10 评分)
    liquidity_sweep_candle: Optional[dict] = None
    sweep_bar_delta: Optional[float] = None          # Step3 2h bar delta(证据)
    sweep_wick_strong: Optional[bool] = None         # Step3 长影 >= 实体 × sweep_wick_ratio
    poi_type: Optional[str] = None
    poi_source: Optional[str] = None                 # "4h_ob" / "1h_ob" / "sweep_based"
    poi_high: Optional[float] = None
    poi_low: Optional[float] = None
    poi_origin_time: Optional[int] = None            # POI 候选 K 线 open_time
    sweep_poi_break_count: int = 0                   # sweep POI 连续 1h close 穿破计数
    poi_displacement_time: Optional[int] = None      # OB 位移 K 线 open_time
    poi_displacement_mult: Optional[float] = None    # OB 位移 K 线 body / OB body
    poi_vp_poc: Optional[float] = None               # OB 检测时的 VP POC
    choch_break_price: Optional[float] = None        # Step5 突破确认收盘价
    choch_break_time: Optional[int] = None           # Step5 突破 K 线 open_time
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward: Optional[float] = None
    scores: dict = field(default_factory=dict)
    total_score: float = 0.0
    created_at: int = field(default_factory=lambda: int(time.time()))
    updated_at: int = field(default_factory=lambda: int(time.time()))
    expires_at: int = 0
    notification_sent: bool = False


# -------- engine -------------------------------------------------------------

class SignalEngine:
    """多时间周期驱动的信号状态机。

    外部只需要：
      engine.on_candle_close(symbol, tf, candle)  —— 在每根 K 线收盘时调用
      engine.get_notification_ready()            —— 取出待通知信号
    """

    def __init__(
        self,
        candle_manager,
        key_levels,
        scorer: Callable[..., Any],
        config: Dict[str, Any],
    ):
        self.candles = candle_manager
        self.levels = key_levels
        self.scorer = scorer
        self.cfg = config or {}
        sig_cfg = self.cfg.get("signal", {}) if isinstance(self.cfg, dict) else {}
        self.swing_lookback = int(sig_cfg.get("swing_lookback", 5))
        self.ob_lookback = int(sig_cfg.get("ob_lookback", 10))
        self.score_threshold = float(sig_cfg.get("score_threshold", 4.0))
        self.signal_ttl = int(sig_cfg.get("signal_ttl_seconds", 14400))
        self.rr_min = float(sig_cfg.get("risk_reward_min", 2.0))
        self.prox_pct = float(sig_cfg.get("level_proximity_pct", 0.005))
        # 场景失效阈值
        self.poi_break_pct = float(sig_cfg.get("poi_break_pct", 0.01))
        self.sweep_wick_ratio = float(sig_cfg.get("sweep_wick_body_ratio", 1.5))

        self.active_signals: Dict[str, SignalState] = {}
        # "当前时间"以最近一根处理过的 K 线 close_time 为准(秒)。生产 WS 模式下
        # 等于 wall-clock,回测 replay 模式下等于模拟时间 —— 让 TTL 判定正确。
        self._current_time_sec: Optional[int] = None
        # 诊断计数器:每个 gate / 每次失效都自增,调用 diagnostics() 取快照
        self._diag: Dict[str, int] = defaultdict(int)
        # POI 状态机注册表(替代原 dedupe-by-origin_time):
        # key=(symbol, direction, origin_time) → {"state": "IDLE|TRIGGERED|INVALIDATED",
        #                                          "poi_low": ..., "poi_high": ..., "updated": ts}
        # 重置规则:TRIGGERED POI 的价格距中线 > 0.5×ATR 时 → 重回 IDLE
        self._poi_registry: Dict[tuple, Dict[str, Any]] = {}
        # 旁路依赖（由 main.py 注入）：bar_delta_provider / vp_provider
        self._bar_delta: Dict[str, float] = {}    # tf -> last closed bar delta
        self._vp_provider: Optional[Callable[[], Any]] = None
        # BTC 联动过滤:fn(direction) -> True=放行
        self._correlation_gate: Optional[Callable[[str], bool]] = None

    # ---- injection points --------------------------------------------------

    def set_bar_delta(self, timeframe: str, delta_value: float) -> None:
        """main.py 在 K 线 close 时把本 bar delta 喂进来，供打分用。"""
        self._bar_delta[timeframe] = float(delta_value)

    def set_vp_provider(self, fn: Callable[[], Any]) -> None:
        self._vp_provider = fn

    def set_correlation_gate(self, fn: Callable[[str], bool]) -> None:
        """注入参考品种(BTC)方向一致性检查:fn(direction) -> True=放行。"""
        self._correlation_gate = fn

    def _now(self) -> int:
        """返回"当前时间"(秒):优先用最近 K 线 close_time,落空才回退 wall-clock。
        回测 replay 时,该值随 K 线推进;生产 WS 模式下两者一致。
        """
        if self._current_time_sec is not None:
            return self._current_time_sec
        return int(time.time())

    def diagnostics(self) -> Dict[str, int]:
        """返回从启动到现在的 gate 计数快照(调试/回测用)。"""
        return dict(self._diag)

    # ---- public -----------------------------------------------------------

    async def on_candle_close(self, symbol: str, timeframe: str, candle: Any) -> None:
        """驱动入口：按 timeframe 分发 Step + 场景失效检查。

        分工:
        - 1d / 4h 收盘 → Step1 方向共识 + B 结构翻转检查
        - 1h   收盘   → Step2+3(扫荡事件) + Step4 POI
        - 15m  收盘   → Step5 CHoCH、Step6 入场 + 评分
        - 1m           → 不驱动状态机

        顺序:清理终态 → 场景失效(A/B/C) → 推进。失效必须在推进之前,
        避免同根 K 线既失效又推进同一信号。
        """
        # 先更新"当前时间"锚点(秒),后续所有 TTL / prune 比较都用这个
        if candle is not None:
            close_ms = candle["close_time"] if isinstance(candle, dict) else getattr(candle, "close_time", None)
            if close_ms is not None:
                self._current_time_sec = int(close_ms) // 1000

        self._prune_terminals()
        self._expire_stale()

        # --- 场景失效(前提检查,不消耗 level 快照) --------------------
        # A 价格漂移已删除:漂移本身不是 setup 失效的有效信号,误杀太多。
        cur_price = _c(candle) if candle is not None else None
        if timeframe == "4h":
            self._invalidate_by_structure(symbol)
        if timeframe in ("15m", "1h") and cur_price is not None:
            self._invalidate_by_poi_break(symbol, cur_price, timeframe)

        # 1h 收盘顺便跑 POI "离开重置":TRIGGERED POI 价格远离中线 > 0.5×ATR → 回 IDLE
        if timeframe == "1h" and cur_price is not None:
            atr_4h = self._atr(symbol, "4h")
            self._reset_triggered_pois(symbol, cur_price, atr_4h)

        # --- 步骤推进 -------------------------------------------------
        # Step1 改用 15m 收盘触发(15m 结构 swing_lookback=3 主驱动);
        # Step2/3 仍在 1h 收盘(POI 锁定和扫荡用 1h 粒度更稳)
        if timeframe == "1h":
            self._advance_on_1h(symbol, candle)
        elif timeframe == "15m":
            self._check_step1(symbol)
            self._advance_on_15m(symbol, candle)
        return None

    # 通知去重阈值:同 (sym, dir) 的信号,entry 距离 ≤ 0.3% 且时间差 ≤ 30min → 视为重复
    _NOTIFY_DEDUPE_PCT = 0.003
    _NOTIFY_DEDUPE_WINDOW_SEC = 30 * 60

    def get_notification_ready(self) -> List[SignalState]:
        out: List[SignalState] = []
        for s in self.active_signals.values():
            if s.step == SignalStep.SCORED and not s.notification_sent and s.total_score >= self.score_threshold:
                # 通知去重:同时间 + 同 entry 区域的重复信号只发一条
                if self._is_duplicate_of_recent_notification(s):
                    s.notification_sent = True
                    s.step = SignalStep.NOTIFIED
                    self._diag["notified_dedupe_skipped"] += 1
                    continue
                s.notification_sent = True
                s.step = SignalStep.NOTIFIED
                self._diag["notified"] += 1
                out.append(s)
        return out

    def _is_duplicate_of_recent_notification(self, s: "SignalState") -> bool:
        """判定是否与已 NOTIFIED 且仍在内存的信号近似重复。"""
        now = self._now()
        entry = s.entry_price
        if entry is None or entry <= 0:
            return False
        for other in self.active_signals.values():
            if other is s:
                continue
            if other.symbol != s.symbol or other.direction != s.direction:
                continue
            if other.step != SignalStep.NOTIFIED:
                continue
            if not other.notification_sent:
                continue
            if other.entry_price is None or other.entry_price <= 0:
                continue
            # 时间差(秒)
            if abs(now - other.updated_at) > self._NOTIFY_DEDUPE_WINDOW_SEC:
                continue
            # 价格距离(相对)
            if abs(entry - other.entry_price) / entry > self._NOTIFY_DEDUPE_PCT:
                continue
            return True
        return False

    # ---- internals --------------------------------------------------------

    def _expire_stale(self) -> None:
        """**仅对 STEP1_PASSED 做时间 TTL**。已进 Step2 及之后(POI 锁定)的信号
        只靠场景失效杀掉(POI 被破坏 / 4h 结构翻转),不受时间影响 —— POI 占坑
        直到被真实场景证伪,价格能等多久就等多久。
        """
        now = self._now()
        for sid, s in list(self.active_signals.items()):
            if s.step != SignalStep.STEP1_PASSED:
                continue
            if s.expires_at and now >= s.expires_at:
                s.step = SignalStep.EXPIRED
                s.updated_at = now
                self._diag["expired_step1_ttl"] += 1
                logger.info(f"[SIGNAL {sid[:8]}] expired (Step1 TTL)")

    # 终态超过 1 小时从 active_signals 移除,防止无界累积(架构师建议)
    _TERMINAL_LINGER_SEC = 3600

    def _prune_terminals(self) -> None:
        now = self._now()
        for sid in list(self.active_signals.keys()):
            s = self.active_signals[sid]
            if s.step in (SignalStep.NOTIFIED, SignalStep.EXPIRED, SignalStep.INVALIDATED):
                if now - s.updated_at > self._TERMINAL_LINGER_SEC:
                    del self.active_signals[sid]

    # ---- 场景失效(A/B/C)-----------------------------------------------

    def _invalidate(self, sid: str, s: "SignalState", reason: str) -> None:
        s.step = SignalStep.INVALIDATED
        s.updated_at = self._now()
        logger.info(f"[SIGNAL {sid[:8]}] invalidated: {reason}")

    def _invalidate_by_structure(self, symbol: str) -> None:
        """B: 4h 结构翻转到反方向 → 信号前提没了。neutral 不算翻转,继续等。

        2026-04-23:改用 ATR pivot 1.0 × ATR_4h,与 Step1 4h 层统一。
        (之前 Step1 用 ATR pivot、这里用 fractal lookback=5,同一个 4h 数据两种判定
        经常打架 → Step1 放行的 short 立刻被 fractal invalidate 杀掉。今修复。)
        """
        h4 = self.candles.window(symbol, "4h", 50)
        if len(h4) < 10:
            return
        atr_4h_val = self._atr(symbol, "4h")
        if atr_4h_val > 0:
            new_struct = classify_structure_atr(h4, atr_4h_val, min_move_mult=1.0)
        else:
            new_struct = classify_structure(h4, self.swing_lookback)
        if new_struct == "neutral":
            return
        for sid, s in list(self.active_signals.items()):
            if s.symbol != symbol:
                continue
            if s.step in (SignalStep.EXPIRED, SignalStep.NOTIFIED, SignalStep.INVALIDATED, SignalStep.SCORED):
                continue
            if (s.direction == "long" and new_struct == "bearish") or \
               (s.direction == "short" and new_struct == "bullish"):
                self._diag["invalidated_b_structure"] += 1
                self._invalidate(sid, s, f"4h 翻转为 {new_struct}")

    def _invalidate_by_poi_break(self, symbol: str, cur_price: float, timeframe: str = "1h") -> None:
        """C: **反方向 leave** —— close 越过 POI 中线 > 0.5×ATR_4h,连续 2 根 1h close 坐实 → INVALIDATED。

        与 _reset_triggered_pois 统一口径(都以 POI 中线 ± 0.5×ATR_4h 为参考线):
          - 反方向 leave(long 信号价格跌破中线下方 / short 信号价格突破中线上方)
              → 本函数:signal + POI 一起永久 INVALIDATED(2 根 1h close 确认)
          - 同方向 leave → _reset_triggered_pois 处理:POI 回 IDLE,signal 不动

        阈值:0.5×ATR_4h(ATR=0 回退 `midpoint × poi_break_pct`)
        2 根连续 1h close 确认,防单根插针误杀。
        """
        if timeframe != "1h":
            return
        if not math.isfinite(cur_price):
            return
        atr_4h = self._atr(symbol, "4h")
        poi_locked_steps = (SignalStep.STEP2_PASSED, SignalStep.STEP3_PASSED, SignalStep.STEP5_PASSED)
        for sid, s in list(self.active_signals.items()):
            if s.symbol != symbol:
                continue
            if s.step not in poi_locked_steps:
                continue
            if s.poi_low is None or s.poi_high is None:
                continue
            if not (math.isfinite(s.poi_low) and math.isfinite(s.poi_high)):
                continue

            midpoint = (s.poi_low + s.poi_high) / 2
            dist = 0.5 * atr_4h if atr_4h > 0 else midpoint * self.poi_break_pct

            # 仅反方向 leave 才计入失效:long 信号看价格是否跌破中线 - dist
            if s.direction == "long":
                broken = cur_price < midpoint - dist
            else:
                broken = cur_price > midpoint + dist

            if broken:
                s.sweep_poi_break_count += 1
                if s.sweep_poi_break_count >= 2:
                    self._diag["invalidated_c_poi_break"] += 1
                    self._mark_poi(
                        self._poi_key(symbol, s.direction, s.poi_origin_time),
                        "INVALIDATED", s.poi_low, s.poi_high,
                    )
                    side = "下方" if s.direction == "long" else "上方"
                    self._invalidate(
                        sid, s,
                        f"close {cur_price:.2f} 连续 2 根 1h 位于 POI 中线 {midpoint:.2f} {side} >0.5×ATR"
                    )
            else:
                s.sweep_poi_break_count = 0

    # Step1 快速 swing_lookback:15m / 1h 共用(~90min~3h 形成 HH+HL)
    # 高 TF 4h/D1 保持默认 5,防误判大方向
    _STEP1_SWING_LOOKBACK = 3

    _DEEP_SWEEP_LOOKBACK = 3          # 最近 N 根 1h 内找 deep sweep
    _DEEP_SWEEP_WICK_ATR_MULT = 0.5   # wick 深度必须 ≥ 0.5 × ATR_1h

    def _has_deep_sweep_override(self, symbol: str, direction: str) -> bool:
        """最近 3 根 1h 内是否有 deep sweep + rejection 形态。
        用来 override "1h 结构冲突" 的 Step1 否决 —— 反转早期 1h 结构滞后,
        但底部/顶部的 deep sweep 往往领先,值得放行。

        long:wick 下刺(min(open,close) - low)≥ 0.5×ATR_1h + 阳线收盘
        short:wick 上刺(high - max(open,close))≥ 0.5×ATR_1h + 阴线收盘
        """
        h1 = self.candles.window(symbol, "1h", 20)
        if len(h1) < self._DEEP_SWEEP_LOOKBACK + 1:
            return False
        atr_1h = self._atr(symbol, "1h")
        if atr_1h <= 0:
            return False
        min_wick = self._DEEP_SWEEP_WICK_ATR_MULT * atr_1h
        for c in h1[-self._DEEP_SWEEP_LOOKBACK:]:
            o = c["open"] if isinstance(c, dict) else c.open
            cls = _c(c); hi = _h(c); lo = _l(c)
            if direction == "long":
                wick_below = min(o, cls) - lo
                if wick_below >= min_wick and cls > o:
                    return True
            else:
                wick_above = hi - max(o, cls)
                if wick_above >= min_wick and cls < o:
                    return True
        return False

    def _check_step1(self, symbol: str) -> None:
        """Step1 方向共识(15m 主驱动,1h / 4h / D1 高 TF 校验):
        - **15m 结构**(swing_lookback=3)必须明确
        - 1h 结构(swing_lookback=3)明确时要求同方向(neutral 放行)
        - 4h 结构(swing_lookback=5)明确时要求同方向(neutral 放行)
        - D1 结构(swing_lookback=5)明确时要求同方向(neutral 放行)

        (2026-04-20 实验过 4h strict + ADX>20,187 天回测证明是 adverse selection:
         过滤后只留下"趋势末段回踩",胜率 48%→25%,累计 +9.83R→-10.34R,已回滚。)
        """
        m15 = self.candles.window(symbol, "15m", 100)
        if len(m15) < 2 * self._STEP1_SWING_LOOKBACK + 3:
            return
        m15_struct = classify_structure(m15, self._STEP1_SWING_LOOKBACK)
        if m15_struct == "neutral":
            return

        override_by_sweep = False
        h1 = self.candles.window(symbol, "1h", 100)
        if len(h1) >= 2 * self._STEP1_SWING_LOOKBACK + 3:
            # 2026-04-22 改用 ATR pivot 判 1h 结构(只影响 Step1 的 1h 层,其它环节仍用 fractal)
            # 原因: fractal(swing_lookback=3) 对 ETH 急涨急跌相邻 K 高度接近时会漏掉显著摆动,
            # 100 根 1h 窗口内累积的老 HH+HL 会让反转早期的 1h 结构一直判 bullish。
            # ATR pivot(ZigZag)按波动率自适应识别显著摆动,"视觉明显"的峰谷必然被捕获。
            atr_1h_val = self._atr(symbol, "1h")
            if atr_1h_val > 0:
                h1_struct = classify_structure_atr(h1, atr_1h_val, min_move_mult=1.0)
            else:
                # ATR 缺失回退到 fractal(避免启动阶段数据不足时异常)
                h1_struct = classify_structure(h1, self._STEP1_SWING_LOOKBACK)
            if h1_struct in ("bullish", "bearish") and h1_struct != m15_struct:
                # 2026-04-20:底部/顶部反转早期 1h 结构滞后,若最近 3 根 1h 有 deep sweep
                # (wick ≥ 0.5×ATR_1h + 收盘带方向 body)→ 认为市场已完成 stop-hunt,放行
                candidate_dir = "long" if m15_struct == "bullish" else "short"
                if self._has_deep_sweep_override(symbol, candidate_dir):
                    self._diag["step1_h1_override_by_sweep"] += 1
                    override_by_sweep = True
                    logger.info(
                        f"[STEP1] {symbol} 15m={m15_struct} 被 1h={h1_struct} 冲突,"
                        f"但 deep sweep override 放行"
                    )
                else:
                    logger.info(f"[STEP1] {symbol} 15m={m15_struct} 被 1h={h1_struct} 否决")
                    return

        # 2026-04-22:4h / D1 也改用 ATR pivot(与 1h 层一致,统一 ATR 加权识别)
        # 刚试过 lookback=3 在 4h 下反指(暴露 2423 小峰让结构翻 bullish),
        # ATR pivot 1.0 × ATR 下 4h 和 D1 都是 neutral,不误拦反转早期信号。
        h4 = self.candles.window(symbol, "4h", 50)
        if len(h4) >= 10:
            atr_4h_val = self._atr(symbol, "4h")
            if atr_4h_val > 0:
                h4_struct = classify_structure_atr(h4, atr_4h_val, min_move_mult=1.0)
            else:
                h4_struct = classify_structure(h4, self._STEP1_SWING_LOOKBACK)
            if h4_struct in ("bullish", "bearish") and h4_struct != m15_struct:
                logger.info(f"[STEP1] {symbol} 15m={m15_struct} 被 4h={h4_struct} 否决")
                return

        d1 = self.candles.window(symbol, "1d", 30)
        if len(d1) >= 10:
            atr_1d_val = self._atr(symbol, "1d")
            if atr_1d_val > 0:
                d1_struct = classify_structure_atr(d1, atr_1d_val, min_move_mult=1.0)
            else:
                d1_struct = classify_structure(d1, self._STEP1_SWING_LOOKBACK)
            if d1_struct in ("bullish", "bearish") and d1_struct != m15_struct:
                logger.info(f"[STEP1] {symbol} 15m={m15_struct} 被 D1={d1_struct} 否决")
                return

        direction = "long" if m15_struct == "bullish" else "short"

        # 2026-04-23:BTC 相关性过滤已禁用(保留 set_correlation_gate 钩子以便未来重启用)
        # 原因:ETH 独立行情时 BTC 方向经常滞后或不一致,强行要求一致会误杀正确信号。
        # 方向共识已靠 15m/1h/4h/D1 多 TF ATR pivot 保障,不再需要 BTC 作为第五层过滤。
        # if self._correlation_gate is not None:
        #     try:
        #         if not self._correlation_gate(direction):
        #             logger.info(f"[STEP1] {symbol} {direction} 被参考品种(BTC)结构冲突否决")
        #             return
        #     except Exception as e:  # noqa: BLE001
        #         logger.warning(f"correlation_gate 异常,按通过处理: {e}")

        # Step1 去重(带"陈旧让位"机制):
        # - 同 (sym, dir) 已有 STEP1_PASSED + 创建不足 12h → 跳过新建
        # - 已有 STEP1_PASSED 但已活超过 12h 未推进 → 标记老信号 EXPIRED,新建接管
        now = self._now()
        STALE_THRESHOLD_SEC = 12 * 3600
        for s in list(self.active_signals.values()):
            if s.symbol == symbol and s.direction == direction and s.step == SignalStep.STEP1_PASSED:
                age = now - s.created_at
                if age < STALE_THRESHOLD_SEC:
                    return
                s.step = SignalStep.EXPIRED
                s.updated_at = now
                self._diag["step1_stale_replaced"] += 1
                logger.info(f"[SIGNAL {s.signal_id[:8]}] 被新 Step1 覆盖(>12h 未推进)")
                break
        trigger_px = _c(m15[-1])
        sid = str(uuid.uuid4())
        self.active_signals[sid] = SignalState(
            signal_id=sid,
            symbol=symbol,
            direction=direction,
            step=SignalStep.STEP1_PASSED,
            step1_trigger_price=trigger_px,
            step1_override_by_sweep=override_by_sweep,
            created_at=now,
            updated_at=now,
            expires_at=now + self.signal_ttl,
        )
        self._diag["step1_created"] += 1
        logger.info(f"[SIGNAL {sid[:8]}] STEP1 direction={direction}  trigger={trigger_px:.2f}")

    def _advance_on_1h(self, symbol: str, candle: Any) -> None:
        """1h 收盘:Step2(POI 锁定) → Step3(扫荡进 POI)。

        新流程(2026-04-19 重构): **先 POI,后 sweep**
          Step2 - POI 锁定:找方向匹配、未回测的 4h OB,锁定为 signal.poi。价格此时可能
                            离 POI 很远,先锁定并持续等价格过来。
          Step3 - 扫荡进 POI:1h K 线的 wick 必须真的**进入 POI 区间**,且满足:
                   ① long: low ≤ poi_high(wick 进到 POI 区间) AND close ≥ poi_low(没跌穿 POI)
                      short: high ≥ poi_low AND close ≤ poi_high
                   ② 长影 >= 实体 × sweep_wick_ratio **OR** 1h bar delta 方向一致
                      (双过评分 C9 +2)
                   ③ 同 (sym, dir, poi_origin_time) 无其它 ≥Step3 的活跃信号
        """
        price = _c(candle)
        low = _l(candle)
        high = _h(candle)

        for sid, s in list(self.active_signals.items()):
            if s.symbol != symbol:
                continue
            if s.step in (SignalStep.EXPIRED, SignalStep.NOTIFIED, SignalStep.INVALIDATED, SignalStep.SCORED):
                continue

            # ---- Step2:POI 锁定(多类型候选 + 优先级选择)----------------
            if s.step == SignalStep.STEP1_PASSED:
                self._diag["step2_attempts"] += 1
                want = "bullish" if s.direction == "long" else "bearish"
                now_sec = self._now()
                candles_4h = self.candles.window(symbol, "4h", 80)
                candles_1h_win = self.candles.window(symbol, "1h", 100)
                vp_snap = self._vp_provider() if self._vp_provider else None

                candidates: List[tuple] = []  # (priority, poi_dict)
                atr_4h = self._atr(symbol, "4h")
                atr_1h = self._atr(symbol, "1h")

                # 候选 1:4h FVG(tier1,最高优先级)
                # 单独看 -10.77R(117 条,31.6% 胜率),但**剔除后反而更差**:下级 POI
                # (sweep/1h_ob/4h_breaker)被暴露出来,累积 -20R。4h_fvg 本身是"次优但
                # 遮盖了更差的候选",保留作为 Step2 的主力 POI。
                # (2026-04-20 尝试过用连续 final_score 打分替代 tier 硬切,187 天
                #  从 +21.83 → -3.96R,tier 系统的隐式 confluence 过滤效应丢失,已回滚。)
                fvgs_4h = find_fvgs(
                    candles_4h, lookback=40,
                    min_height=0.3 * atr_4h if atr_4h > 0 else 0.0,
                    timeframe_tag="4h",
                )
                for p in fvgs_4h:
                    if p["direction"].startswith(want):
                        candidates.append((1, p))

                # 候选 2:4h OB(lookback 40)
                obs_4h = find_order_blocks(
                    candles_4h, 40,
                    displacement_body_mult=1.5, require_unmitigated=True, vp_result=vp_snap,
                )
                for p in obs_4h:
                    if p["direction"].startswith(want):
                        p["source"] = "4h_ob"
                        candidates.append((2, p))

                # 候选 3:4h Breaker(被击穿翻转的 OB)
                # 距离过滤:仅保留距当前价 ≤ 3% 的 Breaker,防止锁到陈年远 Breaker
                # 导致 Step3 永远等不来价格回踩
                BREAKER_MAX_DIST_PCT = 0.03
                breakers_4h = find_breaker_blocks(candles_4h, lookback=40, timeframe_tag="4h")
                for p in breakers_4h:
                    if p["direction"].startswith(want):
                        mid = (p["low"] + p["high"]) / 2
                        if price > 0 and abs(price - mid) / price <= BREAKER_MAX_DIST_PCT:
                            candidates.append((3, p))

                # 候选 4:1h FVG —— **已剔除**(187 天回测 -8.88R,净亏冠军)

                # 候选 5:1h OB(lookback 60)
                obs_1h = find_order_blocks(
                    candles_1h_win, 60,
                    displacement_body_mult=1.5, require_unmitigated=True, vp_result=vp_snap,
                )
                for p in obs_1h:
                    if p["direction"].startswith(want):
                        p["source"] = "1h_ob"
                        candidates.append((5, p))

                # 候选 6:1h Breaker(同距离过滤)
                breakers_1h = find_breaker_blocks(candles_1h_win, lookback=60, timeframe_tag="1h")
                for p in breakers_1h:
                    if p["direction"].startswith(want):
                        mid = (p["low"] + p["high"]) / 2
                        if price > 0 and abs(price - mid) / price <= BREAKER_MAX_DIST_PCT:
                            candidates.append((6, p))

                # 1h Equal Lows / Equal Highs 流动性池
                # 2026-04-20 新增:捕捉"水平支撑/阻力多次测试"场景(ICT 流动性概念)
                # 3+ 根 1h low/high 在 0.3% 内聚集 + 未被吃掉 = POI
                # **tier 1**(与 4h_fvg 同级):这样"远的 4h_fvg"不会压住"近的 equal_lows",
                # 同级内按距当前价升序排,保证最近的结构位优先
                eq_pois = find_equal_lows_highs_pois(
                    candles_1h_win,
                    direction=s.direction,
                    lookback=50, tolerance_pct=0.003, min_touches=3,
                    poi_width_pct=0.003, timeframe_tag="1h",
                )
                for p in eq_pois:
                    if p["direction"].startswith(want):
                        mid = (p["low"] + p["high"]) / 2
                        if price > 0 and abs(price - mid) / price <= BREAKER_MAX_DIST_PCT:
                            candidates.append((1, p))

                # 候选 7b:Failed-Sweep Reentry(中期方案,2026-04-21)
                # "第一次 sweep 失败进场挂单 → 第二次更深 sweep 被拒绝"的二次入场点。
                # 比 sweep_based 更严格(需要二次确认 + 拒绝收盘),优先级 **tier 1** 同
                # 4h_fvg / equal_lows。ICT 双底雏形,反转 RR 通常更高。
                fsr_pois = find_failed_sweep_reentry_pois(
                    candles_1h_win,
                    direction=s.direction,
                    lookback=48, min_depth_pct=0.003,
                    reclaim_window=4, max_age_bars=24, min_spacing_bars=3,
                    timeframe_tag="1h",
                )
                for p in fsr_pois:
                    if p["direction"].startswith(want):
                        mid = (p["low"] + p["high"]) / 2
                        if price > 0 and abs(price - mid) / price <= BREAKER_MAX_DIST_PCT:
                            candidates.append((1, p))

                # 候选 7d:Failed-FVG Reentry(2026-04-21,FVG 版 FSR)
                # 4h FVG 第一次 retest 被吃穿 → 深扫 → 收盘 reclaim = 强反转信号。
                # 针对 04-07 那种"FVG 被反复磨挂后深扫反弹"被遗漏的场景。
                ffvg_pois = find_failed_fvg_reentry_pois(
                    candles_4h,
                    direction=s.direction,
                    lookback=20, min_break_depth_pct=0.003,
                    reclaim_window=3, max_age_bars=6,
                    min_fvg_height_pct=0.003,
                    timeframe_tag="4h",
                )
                for p in ffvg_pois:
                    if p["direction"].startswith(want):
                        mid = (p["low"] + p["high"]) / 2
                        if price > 0 and abs(price - mid) / price <= BREAKER_MAX_DIST_PCT:
                            candidates.append((1, p))

                # 候选 7c:Double-Bottom / Double-Top(长期方案,2026-04-21)
                # 等高双底 + 颈线突破。结构最稳但发信少,tier 1 优先。
                db_pois = find_double_bottom_pois(
                    candles_1h_win,
                    direction=s.direction,
                    lookback=40, tol_pct=0.005,
                    min_spacing_bars=8, max_spacing_bars=30,
                    timeframe_tag="1h",
                )
                for p in db_pois:
                    if p["direction"].startswith(want):
                        mid = (p["low"] + p["high"]) / 2
                        if price > 0 and abs(price - mid) / price <= BREAKER_MAX_DIST_PCT:
                            candidates.append((1, p))

                # 候选 7:Sweep-Based POI(lookback 30,需用 level 池)
                levels = self.levels.snapshot() if hasattr(self.levels, "snapshot") else {}
                # 合入 1d / 4h / 1h 三级 swing 点(最近 3 高 + 3 低)
                # 三级覆盖:1d 捕长期结构,4h 捕中期,1h 捕短期反弹位
                # 2026-04-22:d1/h4 level 也改 ATR pivot(与 Step1 / h1 一致)
                # ATR pivot 按波动率自适应识别显著 swing,长期更稳定
                d1_win = self.candles.window(symbol, "1d", 30)
                if len(d1_win) >= 10:
                    atr_1d_lv = self._atr(symbol, "1d")
                    if atr_1d_lv > 0:
                        d1_highs, d1_lows = find_swing_points_atr(d1_win, atr_1d_lv, 1.0)
                    else:
                        d1_highs, d1_lows = find_swing_points(d1_win, self._STEP1_SWING_LOOKBACK)
                    for idx, sw in enumerate(d1_highs[-3:]):
                        levels[f"d1SwH{idx+1}"] = sw["price"]
                    for idx, sw in enumerate(d1_lows[-3:]):
                        levels[f"d1SwL{idx+1}"] = sw["price"]
                # 2026-04-23 回滚:4h / 1h level 从 ATR pivot 恢复到 fractal swing_lookback=5
                # 原因:ATR pivot 在 20 天回测里改变了 POI 锁定时序,让同时间点锁到不同 POI,
                # 破坏了原本 1h_equal_lows 赢家入场 → 回退到原 fractal 识别
                h4_win = self.candles.window(symbol, "4h", 80)
                if len(h4_win) >= 2 * self.swing_lookback + 1:
                    h4_highs, h4_lows = find_swing_points(h4_win, self.swing_lookback)
                    for idx, sw in enumerate(h4_highs[-3:]):
                        levels[f"h4SwH{idx+1}"] = sw["price"]
                    for idx, sw in enumerate(h4_lows[-3:]):
                        levels[f"h4SwL{idx+1}"] = sw["price"]
                if len(candles_1h_win) >= 2 * self.swing_lookback + 1:
                    h1_highs, h1_lows = find_swing_points(candles_1h_win, self.swing_lookback)
                    for idx, sw in enumerate(h1_highs[-3:]):
                        levels[f"h1SwH{idx+1}"] = sw["price"]
                    for idx, sw in enumerate(h1_lows[-3:]):
                        levels[f"h1SwL{idx+1}"] = sw["price"]
                sweep_pois = self._find_sweep_based_pois(s.direction, levels, candles_1h_win, lookback=30)
                for p in sweep_pois:
                    age_hours = (now_sec - int(p["time"] / 1000)) / 3600.0
                    if age_hours < 12:
                        candidates.append((3, p))   # 新鲜 sweep(与 breaker 同级)
                    elif age_hours < 48:
                        candidates.append((7, p))   # 较陈旧 sweep(最低优先)
                    # > 48h 不用

                if not candidates:
                    self._diag["step2_fail_no_ob"] += 1
                    continue

                # 排序:优先级 asc,然后距离当前价 asc
                def _poi_dist(poi):
                    center = (poi["low"] + poi["high"]) / 2
                    return abs(center - price)
                candidates.sort(key=lambda x: (x[0], _poi_dist(x[1])))

                # 遍历候选,挑**第一个注册表可锁**的 POI(2026-04-20 修复:原代码只看 top-1,
                # 若 top-1 已 INVALIDATED 就杀 signal,导致价格从反向位置反弹时最高优先级的
                # 老 POI 卡住所有新信号。现在跳过 INVALIDATED 的,取下一个 available。)
                poi = None
                for _, cand in candidates:
                    pkey_try = self._poi_key(symbol, s.direction, cand.get("time"))
                    if self._poi_available_for_lock(pkey_try):
                        poi = cand
                        break
                if poi is None:
                    self._diag["step2_fail_dedupe"] += 1
                    self._invalidate(sid, s, f"所有候选 POI 均已 INVALIDATED")
                    continue
                self._diag["step2_passed"] += 1
                s.poi_type = poi["direction"]
                s.poi_source = poi.get("source", "4h_ob")
                s.poi_high, s.poi_low = poi["high"], poi["low"]
                s.poi_origin_time = poi.get("time")
                s.poi_displacement_time = poi.get("displacement_time")
                s.poi_displacement_mult = poi.get("displacement_body_mult")
                # vp_poc:OB 已有,sweep-based POI 从当前 VP 兜底,让 C8 评分也能适用
                s.poi_vp_poc = poi.get("vp_poc")
                if s.poi_vp_poc is None and vp_snap is not None:
                    s.poi_vp_poc = getattr(vp_snap, "poc", None)
                s.step = SignalStep.STEP2_PASSED
                s.updated_at = self._now()
                logger.info(
                    f"[SIGNAL {sid[:8]}] STEP2 POI 锁定 {poi['direction']}(source={s.poi_source}) "
                    f"[{poi['low']:.2f}-{poi['high']:.2f}] 当前价 {price:.2f}"
                )

            # ---- Step3:扫荡进 POI(按 POI 类型分支 + ATR 缓冲放宽)---------
            if s.step == SignalStep.STEP2_PASSED:
                self._diag["step3_attempts"] += 1
                if s.poi_low is None or s.poi_high is None:
                    continue
                wick_strong = self._wick_strong(candle, s.direction)
                bar_d = self._bar_delta.get("1h", 0.0)
                delta_ok = (s.direction == "long" and bar_d > 0) or (
                    s.direction == "short" and bar_d < 0
                )
                # ATR 缓冲:POI 上下沿各外扩 0.15 × ATR_4h,补偿 wick 差一点点没到的遗憾
                atr_4h = self._atr(symbol, "4h")
                buffer = 0.15 * atr_4h if atr_4h > 0 else 0.0

                if s.poi_source == "sweep_based":
                    # Sweep POI 区间小,用 close 距中点 ≤ 1% + 缓冲
                    midpoint = (s.poi_low + s.poi_high) / 2
                    if midpoint <= 0:
                        self._diag["step3_fail_close_penetrated"] += 1
                        continue
                    # wick 触及外扩后的区间
                    wick_touches = (low <= s.poi_high + buffer) and (high >= s.poi_low - buffer)
                    if not wick_touches:
                        self._diag["step3_fail_wick_miss"] += 1
                        continue
                    if abs(price - midpoint) / midpoint > 0.01:
                        self._diag["step3_fail_close_penetrated"] += 1
                        continue
                else:
                    # OB / Breaker / FVG:wick 触达上/下沿(含 buffer),close 没穿透
                    if s.direction == "long":
                        wick_touches = low <= s.poi_high + buffer
                        close_not_broken = price >= s.poi_low - buffer      # close 未跌穿 POI 下沿
                        close_not_flown = price <= s.poi_high + buffer      # close 也不能飞过 POI 上沿太多
                    else:
                        wick_touches = high >= s.poi_low - buffer
                        close_not_broken = price <= s.poi_high + buffer
                        close_not_flown = price >= s.poi_low - buffer
                    if not wick_touches:
                        self._diag["step3_fail_wick_miss"] += 1
                        continue
                    if not close_not_broken:
                        self._diag["step3_fail_close_penetrated"] += 1
                        continue
                    # 2026-04-20 新增:防"wick 擦 POI 边但 close 已飞出"(continuation 中的假装 retest)
                    # 例:long 4h_fvg [2268-2352],wick 低 2342(触到),close 2363(飞过上沿 11)
                    # 原逻辑只检查"没跌穿下沿",现在要求 close 也在 POI 区间内(含 buffer)
                    if not close_not_flown:
                        self._diag["step3_fail_close_flown"] += 1
                        continue
                    # FVG / equal_lows/highs / failed_sweep_reentry / double_bottom(top) 是
                    # "zone 型"POI,回补时未必有显著 wick,不强求 wick/delta 门闸;
                    # OB / Breaker / Sweep 仍需 wick 强 OR delta 同向
                    src = s.poi_source or ""
                    is_zone = (
                        "fvg" in src or "equal_lows" in src or "equal_highs" in src
                        or "failed_sweep" in src or "failed_fvg" in src
                        or "double_bottom" in src or "double_top" in src
                    )
                    if not is_zone and not (wick_strong or delta_ok):
                        self._diag["step3_fail_gate"] += 1
                        continue
                # Step3 准入还需检查 POI 注册表是否 IDLE(核心:leave-then-reset)
                pkey_step3 = self._poi_key(symbol, s.direction, s.poi_origin_time)
                if not self._poi_available_for_advance(pkey_step3):
                    # POI 仍在 TRIGGERED / INVALIDATED 冷却,Step3 不推进(等重置)
                    self._diag["step3_fail_poi_cooldown"] += 1
                    continue
                self._diag["step3_passed"] += 1
                # 标记 POI 为 TRIGGERED(或达上限 → INVALIDATED)
                self._mark_poi_triggered(pkey_step3, s.poi_low, s.poi_high)
                s.liquidity_sweep_candle = {
                    "open": candle["open"] if isinstance(candle, dict) else candle.open,
                    "high": high,
                    "low": low,
                    "close": price,
                    "time": _t(candle),
                }
                s.sweep_bar_delta = bar_d
                # sweep-based POI 的区间本身就是"扫荡 K 线"的证据,视为 wick_strong=True
                # 让 C9(扫荡双过)在 delta 同向时自动命中,消除 sweep POI 的评分劣势
                s.sweep_wick_strong = True if s.poi_source == "sweep_based" else wick_strong
                # triggered_level 记为 POI 边界(靠近被 sweep 的那侧)
                s.triggered_level = "POI_low" if s.direction == "long" else "POI_high"
                s.triggered_level_value = s.poi_low if s.direction == "long" else s.poi_high
                s.step = SignalStep.STEP3_PASSED
                s.updated_at = self._now()
                both = wick_strong and delta_ok
                logger.info(
                    f"[SIGNAL {sid[:8]}] STEP3 扫荡进 POI "
                    f"[{s.poi_low:.2f}-{s.poi_high:.2f}]  "
                    f"wick={wick_strong} delta={bar_d:+.1f}  "
                    f"{'(双过)' if both else '(单过)'}  close={price:.2f}"
                )

    def _find_swept_level(
        self,
        candle: Any,
        direction: str,
        levels: Dict[str, Any],
    ) -> Optional[tuple]:
        """纯几何判定:找出被 wick 扫穿且 close 收回的 level(最深穿透优先)。

        **不再检查 wick/body 比**(解耦:wick 强弱由 caller 判定)。
        """
        c = _c(candle); h = _h(candle); l = _l(candle)
        candidates: List[tuple] = []
        for key, value in levels.items():
            if value is None:
                continue
            if direction == "long":
                if l < value < c:
                    candidates.append((value - l, key, value))
            else:
                if c < value < h:
                    candidates.append((h - value, key, value))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        _, key, value = candidates[0]
        return (key, value)

    def _find_sweep_based_pois(
        self,
        direction: str,
        levels: Dict[str, Any],
        candles_1h: Sequence[Any],
        lookback: int = 30,
    ) -> List[Dict[str, Any]]:
        """扫描最近 `lookback` 根 1h,找出 unmitigated sweep K 线作 POI 候选。

        单根 K_i 合格条件:
          ① wick(long:下影 / short:上影)刺破某个方向匹配的 level
          ② close 收回 level 同侧
          ③ 影线 ≥ 实体 × 1.2
          ④ 其后任何 K 线的 wick 都不得再次触及 K_i 的 wick 极值(unmitigated)
        POI 区间 = K_i 的 body (min(o,c), max(o,c))
        """
        n = len(candles_1h)
        if n < 2:
            return []
        start = max(0, n - lookback)
        out: List[Dict[str, Any]] = []
        for i in range(start, n - 1):
            c_i = candles_1h[i]
            o = c_i["open"] if isinstance(c_i, dict) else c_i.open
            h = _h(c_i); l = _l(c_i); close = _c(c_i)
            body = abs(close - o)
            if body == 0:
                continue
            if direction == "long":
                wick = min(o, close) - l
                if wick < body * 1.2:
                    continue
                level_key = None
                for k, v in levels.items():
                    if v is None:
                        continue
                    if l < v < close:
                        level_key = k
                        break
                if level_key is None:
                    continue
                # Unmitigated:后续任何 K 线 low 不得 ≤ l
                if any(_l(candles_1h[j]) <= l for j in range(i + 1, n)):
                    continue
                out.append({
                    "direction": "bullish_sweep",
                    "low": min(o, close),
                    "high": max(o, close),
                    "time": _t(c_i),
                    "level_key": level_key,
                    "source": "sweep_based",
                    "wick_extreme": l,
                })
            else:
                wick = h - max(o, close)
                if wick < body * 1.2:
                    continue
                level_key = None
                for k, v in levels.items():
                    if v is None:
                        continue
                    if close < v < h:
                        level_key = k
                        break
                if level_key is None:
                    continue
                if any(_h(candles_1h[j]) >= h for j in range(i + 1, n)):
                    continue
                out.append({
                    "direction": "bearish_sweep",
                    "low": min(o, close),
                    "high": max(o, close),
                    "time": _t(c_i),
                    "level_key": level_key,
                    "source": "sweep_based",
                    "wick_extreme": h,
                })
        return out

    # ---- POI 状态机注册表 --------------------------------------------------

    @staticmethod
    def _poi_key(symbol: str, direction: str, origin_time: Optional[int]) -> tuple:
        return (symbol, direction, int(origin_time) if origin_time is not None else 0)

    def _poi_state(self, key: tuple) -> str:
        entry = self._poi_registry.get(key)
        return entry["state"] if entry else "IDLE"

    def _poi_available_for_lock(self, key: tuple) -> bool:
        """Step2 锁定:非 INVALIDATED 都允许(TRIGGERED 可等重置)。"""
        return self._poi_state(key) != "INVALIDATED"

    def _poi_available_for_advance(self, key: tuple) -> bool:
        """Step3 推进:只有 IDLE 能进。"""
        return self._poi_state(key) == "IDLE"

    # POI 触发次数上限:同一 POI 被 Step3 触发达到此值 → 永久 INVALIDATED
    # N=3:FVG/OB 的机构订单量可能需要 3-5 次测试才消化完,给一次额外机会
    _POI_TRIGGER_LIMIT = 3

    def _mark_poi(self, key: tuple, state: str, poi_low: float, poi_high: float) -> None:
        """更新 POI 状态,保留 trigger_count(INVALIDATED 场景从外部直接设置)。"""
        existing = self._poi_registry.get(key, {})
        self._poi_registry[key] = {
            "state": state,
            "poi_low": float(poi_low),
            "poi_high": float(poi_high),
            "updated": self._now(),
            "trigger_count": existing.get("trigger_count", 0),
        }

    def _mark_poi_triggered(self, key: tuple, poi_low: float, poi_high: float) -> str:
        """Step3 成功时调用,trigger_count += 1。

        语义(LIMIT=2,N=2):
          - count=1:state → TRIGGERED,可通过 reset 机制回到 IDLE 再次被触发
          - count=2:**state → INVALIDATED**(永久),本次仍发信号但之后 POI 不再可用
          - count=3+:不可能进到这里(已经被 INVALIDATED 挡在 Step3 之前)
        """
        existing = self._poi_registry.get(key, {"trigger_count": 0})
        count = int(existing.get("trigger_count", 0)) + 1
        if count >= self._POI_TRIGGER_LIMIT:
            state = "INVALIDATED"
            self._diag["poi_exhausted"] += 1
        else:
            state = "TRIGGERED"
        self._poi_registry[key] = {
            "state": state,
            "poi_low": float(poi_low),
            "poi_high": float(poi_high),
            "updated": self._now(),
            "trigger_count": count,
        }
        return state

    def _reset_triggered_pois(self, symbol: str, cur_price: float, atr: float) -> None:
        """1h 收盘时扫一遍:TRIGGERED POI 若价格**同方向**远离中线 > 0.5×ATR → 回 IDLE。

        仅同方向(favorable)远离才重置,让 POI 能被未来新信号复用;
        反方向远离由 _invalidate_by_poi_break 处理(signal + POI 一起永久 INVALIDATED),
        避免两处机制在反向 leave 时冗余触发。
        """
        if atr <= 0 or not math.isfinite(cur_price):
            return
        reset_dist = 0.5 * atr
        for key, entry in self._poi_registry.items():
            if key[0] != symbol:
                continue
            if entry.get("state") != "TRIGGERED":
                continue
            _symbol, direction, _origin = key
            midpoint = (entry["poi_low"] + entry["poi_high"]) / 2
            if direction == "long":
                same_dir_leave = cur_price > midpoint + reset_dist
            else:
                same_dir_leave = cur_price < midpoint - reset_dist
            if same_dir_leave:
                entry["state"] = "IDLE"
                entry["updated"] = self._now()
                self._diag["poi_reset_to_idle"] += 1

    def _atr(self, symbol: str, timeframe: str, period: int = 14) -> float:
        """简化 ATR:取最近 period 根 K 线 (high - low) 的均值。
        若数据不足或全 0 返回 0(调用方应 fallback 用价格比例)。
        """
        cs = self.candles.window(symbol, timeframe, period + 5)
        if not cs:
            return 0.0
        window = cs[-period:] if len(cs) > period else cs
        total = sum(max(0.0, _h(c) - _l(c)) for c in window)
        n = len(window)
        return total / n if n else 0.0

    def _wick_strong(self, candle: Any, direction: str) -> bool:
        """判定 wick 是否达到 `sweep_wick_ratio` 倍实体长度。"""
        o = candle["open"] if isinstance(candle, dict) else candle.open
        c = _c(candle); h = _h(candle); l = _l(candle)
        body = abs(c - o)
        if direction == "long":
            wick = min(o, c) - l
        else:
            wick = h - max(o, c)
        if body == 0:
            return wick > 0
        return wick >= body * self.sweep_wick_ratio

    # ---- 15m micro CHoCH 检测(joshyattridge/smart-money-concepts)---------

    def _micro_choch_after(
        self,
        candles: Sequence[Any],
        after_time_ms: int,
        swing_length: int = 3,
    ) -> Optional[str]:
        """在 15m 窗口里,找 after_time_ms 之后最近的 CHoCH,返回 'long' / 'short' / None。
        CHoCH = Change of Character,比 BOS(continuation)更严格,要求**结构翻转**。
        用于 Step5 micro 确认:Step3 后等 15m 出现同向 CHoCH 才入场,过滤假突破。
        """
        if len(candles) < 2 * swing_length + 3:
            return None
        try:
            from smartmoneyconcepts import smc
            import pandas as pd
        except ImportError:
            return None

        opens, highs, lows, closes, vols, times = [], [], [], [], [], []
        for c in candles:
            if isinstance(c, dict):
                opens.append(float(c["open"]))
                vols.append(float(c.get("volume", 0)))
            else:
                opens.append(float(c.open))
                vols.append(float(getattr(c, "volume", 0)))
            highs.append(_h(c)); lows.append(_l(c)); closes.append(_c(c))
            times.append(_t(c))
        df = pd.DataFrame({"open": opens, "high": highs, "low": lows, "close": closes, "volume": vols})

        try:
            swing = smc.swing_highs_lows(df, swing_length=swing_length)
            choch = smc.bos_choch(df, swing, close_break=True)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"smc bos_choch 计算失败: {e}")
            return None

        # 从后往前扫 CHoCH 非空行,取 BrokenIndex 时间 > after_time_ms 的最新一条
        choch_col = choch["CHOCH"].values
        broken_col = choch["BrokenIndex"].values
        for i in range(len(df) - 1, -1, -1):
            ch = choch_col[i]
            if ch != ch or ch == 0:  # NaN 或 0
                continue
            bi = broken_col[i]
            if bi != bi:
                continue
            bi_int = int(bi)
            if 0 <= bi_int < len(times) and times[bi_int] > after_time_ms:
                return "long" if ch > 0 else "short"
        return None

    def _structure_matches_after(
        self,
        candles: Sequence[Any],
        direction: str,
        after_time_ms: int,
        swing_length: int = 3,
    ) -> bool:
        """Step5 备选路径:15m 当前 classify_structure 已匹配 signal 方向,且决定性
        swing 在 after_time_ms 之后形成(确保结构是 Step3 之后才成型,不是残留)。

        short → 要求 15m structure == "bearish"(LH + LL)
        long  → 要求 15m structure == "bullish"(HH + HL)

        与 CHoCH 的区别:不要求 prior 趋势必须是反向,只看当前结构是否已正向;
        适合"单边延续形成反向结构"的场景(SMC 库 CHoCH 不会触发,但结构客观已反)。
        """
        struct = classify_structure(candles, swing_length)
        expect = "bearish" if direction == "short" else "bullish"
        if struct != expect:
            return False
        highs, lows = find_swing_points(candles, swing_length)
        if len(highs) < 2 or len(lows) < 2:
            return False
        # 最新两个决定结构的 swing 中,至少一个必须在 Step3 之后形成(保证新鲜)
        latest_swing_time = max(highs[-1]["time"], lows[-1]["time"])
        return latest_swing_time > after_time_ms

    # ---- SL / TP 自动计算 --------------------------------------------------

    _SL_BUFFER_ATR_MULT = 0.15     # 选项 B:swing 外侧缓冲(复用系统 0.15×ATR_4h 约定)
    _SL_MIN_DIST_ATR_MULT = 0.5    # SL 距 entry 下限:0.5×ATR_1h(防 swing 太近致 R:R 扭曲)
    _SL_MIN_DIST_FALLBACK = 0.25   # ATR_1h 缺失时回退:0.25×ATR_4h
    _TP_FALLBACK_RR = 2.0          # 找不到结构 TP 时用 2R 兜底
    _TP_SWING_LOOKBACK_N = 5       # 取最近 5 个同向 swing 作为流动性候选

    def _calculate_sl(
        self,
        direction: str,
        candles_15m: Sequence[Any],
        entry: float,
        atr_4h: float,
        atr_1h: float,
    ) -> Optional[float]:
        """选项 B:SL 放在 15m 突破前最近 swing 外侧 + 0.15×ATR_4h 缓冲。

        - long:SL = 最近 15m swing_low - buffer
        - short:SL = 最近 15m swing_high + buffer
        - swing 找不到时回退到最近 2×swing_lookback 根 15m 的最低/最高
        - **SL 距 entry 下限**:0.5×ATR_1h(ATR_1h 缺失回退 0.25×ATR_4h)
        """
        if not candles_15m:
            return None
        buffer = self._SL_BUFFER_ATR_MULT * atr_4h if atr_4h > 0 else 0.0

        if atr_1h > 0:
            min_dist = self._SL_MIN_DIST_ATR_MULT * atr_1h
        elif atr_4h > 0:
            min_dist = self._SL_MIN_DIST_FALLBACK * atr_4h
        else:
            min_dist = 0.0

        if len(candles_15m) >= 2 * self.swing_lookback + 1:
            highs, lows = find_swing_points(candles_15m, self.swing_lookback)
        else:
            highs, lows = [], []

        if atr_1h > 0:
            min_dist = self._SL_MIN_DIST_ATR_MULT * atr_1h
        elif atr_4h > 0:
            min_dist = self._SL_MIN_DIST_FALLBACK * atr_4h
        else:
            min_dist = 0.0

        if direction == "long":
            if lows:
                raw = float(lows[-1]["price"]) - buffer
            else:
                window = candles_15m[-self.swing_lookback * 2:]
                if not window:
                    return None
                raw = min(_l(c) for c in window) - buffer
            floor_sl = entry - min_dist
            return min(raw, floor_sl)
        else:
            if highs:
                raw = float(highs[-1]["price"]) + buffer
            else:
                window = candles_15m[-self.swing_lookback * 2:]
                if not window:
                    return None
                raw = max(_h(c) for c in window) + buffer
            ceil_sl = entry + min_dist
            return max(raw, ceil_sl)

    def _calculate_tp(
        self,
        direction: str,
        entry: float,
        candles_1h: Sequence[Any],
        candles_4h: Sequence[Any],
        atr_1h: float,
        atr_4h: float,
        vp_snap: Any = None,
        min_reward: Optional[float] = None,
    ) -> Optional[tuple]:
        """按优先级挑最近的结构位做 TP,返回 (price, source) 或 None。

        P1 = 对手方流动性:4h / 1h 方向匹配的 swing(long → swing_high;short → swing_low)
        P2 = 反向未 mitigate POI:4h / 1h 的 bearish OB/FVG(long TP)或 bullish(short TP),
             取 POI 近端沿(long 取下沿,short 取上沿)——先 touch 到的那一侧
        优先级 asc → 距 entry asc
        """
        candidates: List[tuple] = []  # (priority, price, source)

        # P1:4h / 1h 对手方 swing
        for cs, tf in ((candles_4h, "4h"), (candles_1h, "1h")):
            if len(cs) < 2 * self.swing_lookback + 1:
                continue
            highs, lows = find_swing_points(cs, self.swing_lookback)
            if direction == "long":
                for sw in highs[-self._TP_SWING_LOOKBACK_N:]:
                    p = float(sw["price"])
                    if p > entry:
                        candidates.append((1, p, f"{tf}_swing_high"))
            else:
                for sw in lows[-self._TP_SWING_LOOKBACK_N:]:
                    p = float(sw["price"])
                    if p < entry:
                        candidates.append((1, p, f"{tf}_swing_low"))

        # P2:反向未 mitigate 的 OB / FVG
        want_reverse = "bearish" if direction == "long" else "bullish"
        poi_sources = (
            (candles_4h, 40, atr_4h, "4h"),
            (candles_1h, 60, atr_1h, "1h"),
        )
        for cs, lookback, atr, tf in poi_sources:
            if len(cs) < 5:
                continue
            try:
                obs = find_order_blocks(
                    cs, lookback,
                    displacement_body_mult=1.5,
                    require_unmitigated=True,
                    vp_result=vp_snap,
                )
            except Exception:  # noqa: BLE001
                obs = []
            try:
                fvgs = find_fvgs(
                    cs, lookback=lookback,
                    min_height=0.3 * atr if atr > 0 else 0.0,
                    timeframe_tag=tf,
                )
            except Exception:  # noqa: BLE001
                fvgs = []
            for p in list(obs) + list(fvgs):
                if not p.get("direction", "").startswith(want_reverse):
                    continue
                if direction == "long":
                    near_edge = float(p["low"])
                    if near_edge > entry:
                        candidates.append((2, near_edge, f"{tf}_rev_poi"))
                else:
                    near_edge = float(p["high"])
                    if near_edge < entry:
                        candidates.append((2, near_edge, f"{tf}_rev_poi"))

        if not candidates:
            return None
        # **2R 偏好**:若提供 min_reward,优先筛距 entry ≥ min_reward 的候选
        # 这样 TP 会跳过太近的 swing(可能是 range 边界),去找够远的结构位 = 更好 R:R
        if min_reward is not None and min_reward > 0:
            far_enough = [c for c in candidates if abs(c[1] - entry) >= min_reward]
            if far_enough:
                candidates = far_enough
        candidates.sort(key=lambda x: (x[0], abs(x[1] - entry)))
        _, price, src = candidates[0]
        return (price, src)

    def _dedupe_by_poi(
        self,
        symbol: str,
        direction: str,
        poi_origin_time: Optional[int],
        own_sid: str,
    ) -> bool:
        """True = 冲突(同 sym+dir+OB 已被其它信号锁定,锁 Step2 或更后)。"""
        if poi_origin_time is None:
            return False
        for sid, s in self.active_signals.items():
            if sid == own_sid:
                continue
            if s.symbol != symbol or s.direction != direction:
                continue
            if s.poi_origin_time != poi_origin_time:
                continue
            if s.step in (SignalStep.STEP2_PASSED, SignalStep.STEP3_PASSED,
                          SignalStep.STEP5_PASSED, SignalStep.STEP6_PASSED,
                          SignalStep.SCORED, SignalStep.NOTIFIED):
                return True
        return False

    def _advance_on_15m(self, symbol: str, candle: Any) -> None:
        """15m 收盘:Step5(CHoCH 确认) → Step6(入场 + SL) → 评分。

        入场价用 15m 收盘,提高入场精度;scoring 也用 15m 上下文(delta、candles 窗口)
        保持一致,避免架构师指出的"scoring 用 1h 过期 delta"问题。
        """
        candles_15m = self.candles.window(symbol, "15m", 80)
        if not candles_15m:
            return
        levels = self.levels.snapshot() if hasattr(self.levels, "snapshot") else {}
        price = _c(candle)

        for sid, s in list(self.active_signals.items()):
            if s.symbol != symbol:
                continue
            if s.step in (SignalStep.EXPIRED, SignalStep.NOTIFIED, SignalStep.INVALIDATED, SignalStep.SCORED):
                continue

            if s.step == SignalStep.STEP3_PASSED:
                # Step5 双通道(按 POI 来源选择):
                #  - sweep_based / 1h_breaker / 4h_breaker / 4h_ob / 1h_ob → micro CHoCH
                #    (187 天数据显示 CHoCH 过滤把这几类 per-signal EV 提升 1.5-2.1×)
                #  - 4h_fvg → breakout(CHoCH 过滤对 4h_fvg 反向杀赢家,EV -2.7×)
                self._diag["step5_attempts"] += 1
                if s.poi_low is None or s.poi_high is None:
                    self._diag["step5_fail_no_choch"] += 1
                elif s.poi_source == "4h_fvg":
                    # breakout 路径:close 穿 POI 沿
                    breakout = (s.direction == "long" and price > s.poi_high) or \
                               (s.direction == "short" and price < s.poi_low)
                    if breakout:
                        s.choch_break_price = price
                        s.choch_break_time = _t(candle)
                        s.step = SignalStep.STEP5_PASSED
                        s.updated_at = self._now()
                        self._diag["step5_passed"] += 1
                        edge = s.poi_high if s.direction == "long" else s.poi_low
                        op = ">" if s.direction == "long" else "<"
                        logger.info(
                            f"[SIGNAL {sid[:8]}] STEP5 breakout(4h_fvg) "
                            f"close={price:.2f} {op} {edge:.2f}"
                        )
                    else:
                        self._diag["step5_fail_no_choch"] += 1
                elif s.liquidity_sweep_candle is None:
                    self._diag["step5_fail_no_choch"] += 1
                else:
                    # micro CHoCH 路径:Step3 后 15m 必须出现同向 CHoCH
                    step3_close_ms = int(s.liquidity_sweep_candle["time"]) + 3600000
                    ch_dir = self._micro_choch_after(candles_15m, step3_close_ms)
                    # 2026-04-22 新增备选路径:直接 structure 匹配
                    # 不要求"先 bullish 再反转"(CHoCH 语义),只要求 15m 当前结构已经
                    # **完整形成同向**(short → LH+LL / long → HH+HL),且最新 swing
                    # 在 Step3 之后形成(保证是新鲜结构,不是历史残留)。
                    # 典型触发场景: 单边下跌中已形成 LH+LL 但 prior 不是清晰 bullish,
                    # SMC 库 CHoCH 不触发但结构确实已反向。
                    struct_flipped = self._structure_matches_after(
                        candles_15m, s.direction, step3_close_ms
                    )
                    if ch_dir == s.direction:
                        s.choch_break_price = price
                        s.choch_break_time = _t(candle)
                        s.step = SignalStep.STEP5_PASSED
                        s.updated_at = self._now()
                        self._diag["step5_passed"] += 1
                        logger.info(
                            f"[SIGNAL {sid[:8]}] STEP5 micro CHoCH({s.poi_source}) "
                            f"dir={s.direction} close={price:.2f}"
                        )
                    elif struct_flipped:
                        s.choch_break_price = price
                        s.choch_break_time = _t(candle)
                        s.step = SignalStep.STEP5_PASSED
                        s.updated_at = self._now()
                        self._diag["step5_passed"] += 1
                        self._diag["step5_struct_match"] += 1
                        logger.info(
                            f"[SIGNAL {sid[:8]}] STEP5 15m structure 匹配({s.poi_source}) "
                            f"dir={s.direction} close={price:.2f}"
                        )
                    else:
                        self._diag["step5_fail_no_choch"] += 1

            if s.step == SignalStep.STEP5_PASSED:
                # Step6:自动计算 SL(选项 B:15m swing 外侧)+ TP(反向 liquidity / POI)
                atr_4h = self._atr(symbol, "4h")
                atr_1h = self._atr(symbol, "1h")
                candles_4h = self.candles.window(symbol, "4h", 80)
                candles_1h = self.candles.window(symbol, "1h", 100)
                vp_snap = self._vp_provider() if self._vp_provider else None

                sl = self._calculate_sl(s.direction, candles_15m, price, atr_4h, atr_1h)
                if sl is None:
                    self._diag["step6_fail_no_sl"] += 1
                    self._invalidate(sid, s, "无法定位 15m swing 作为 SL")
                    continue
                # SL 方向合理性
                if (s.direction == "long" and sl >= price) or (s.direction == "short" and sl <= price):
                    self._diag["step6_fail_sl_wrong_side"] += 1
                    self._invalidate(sid, s, f"SL {sl:.2f} 与 entry {price:.2f} 方向冲突")
                    continue

                risk = abs(price - sl)
                tp_result = self._calculate_tp(
                    s.direction, price, candles_1h, candles_4h, atr_1h, atr_4h, vp_snap,
                    min_reward=2.0 * risk,   # 偏好 ≥2R 的候选,避开 range 内紧贴的 swing
                )
                if tp_result is None:
                    # 无结构 TP:用固定 2R 兜底(方便用户手动替换,也让低质量品种仍可出信号)
                    tp = price + self._TP_FALLBACK_RR * risk if s.direction == "long" \
                        else price - self._TP_FALLBACK_RR * risk
                    tp_src = "fallback_2R"
                else:
                    tp, tp_src = tp_result
                    if (s.direction == "long" and tp <= price) or (s.direction == "short" and tp >= price):
                        # 候选方向反了(理论上不会,但防御一下),回退 2R
                        tp = price + self._TP_FALLBACK_RR * risk if s.direction == "long" \
                            else price - self._TP_FALLBACK_RR * risk
                        tp_src = "fallback_2R"

                reward = abs(tp - price)
                rr = reward / risk if risk > 0 else 0.0

                # ⭐ 结构 TP 硬闸:如果结构给出的 TP 距离 < 1.5R,说明价格在范围顶/底,
                # 最近目标就在眼前,没有运行空间。此时不应入场(用 fallback 2R 也是
                # 蒙在范围外,不靠谱)。直接 INVALIDATE 信号。
                MIN_STRUCTURAL_RR = 1.5
                if tp_src != "fallback_2R" and rr < MIN_STRUCTURAL_RR:
                    self._diag["step6_fail_tp_too_close"] += 1
                    self._invalidate(
                        sid, s,
                        f"结构 TP 过近(R:R={rr:.2f} < {MIN_STRUCTURAL_RR}):价格可能在范围顶/底,"
                        f"entry={price:.2f} SL={sl:.2f} TP={tp:.2f}({tp_src})"
                    )
                    continue
                # R:R 只作参考展示,不做硬性 gate(用户明确:通知里给出 R:R 让交易员自己判断)
                if rr < self.rr_min:
                    self._diag["step6_rr_below_min_warn"] += 1

                s.entry_price = price
                s.stop_loss = float(sl)
                s.take_profit = float(tp)
                s.risk_reward = float(rr)
                s.step = SignalStep.STEP6_PASSED
                s.updated_at = self._now()
                self._diag["step6_passed"] += 1
                logger.info(
                    f"[SIGNAL {sid[:8]}] STEP6 entry={price:.4f} "
                    f"SL={sl:.4f} TP={tp:.4f}({tp_src}) R:R={rr:.2f}"
                )

            if s.step == SignalStep.STEP6_PASSED:
                h4 = self.candles.window(symbol, "4h", 100)
                h4_struct = classify_structure(h4, self.swing_lookback) if h4 else "neutral"
                vp = self._vp_provider() if self._vp_provider else None
                ctx = {
                    "candles": candles_15m,
                    "levels": levels,
                    "bar_delta": self._bar_delta.get("15m", 0.0),
                    "vp": vp,
                    "h4_structure": h4_struct,
                    "level_proximity_pct": self.prox_pct,
                }
                total, breakdown = self.scorer(s, ctx)
                s.scores = breakdown
                s.total_score = total
                s.step = SignalStep.SCORED
                s.updated_at = self._now()
                self._diag["scored"] += 1
                if total < self.score_threshold:
                    self._diag["scored_below_threshold"] += 1
                logger.info(f"[SIGNAL {sid[:8]}] SCORED total={total:.1f} breakdown={breakdown}")


__all__ = ["SignalStep", "SignalState", "SignalEngine"]
