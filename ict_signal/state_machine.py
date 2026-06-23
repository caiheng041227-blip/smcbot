"""ICT (Inner Circle Trader) 信号引擎 — 项目唯一信号 pipeline(SMC 管线 2026-06-10 删除)。

范围:
  - 4 个 ICT POI 检测器(engine/ict_pois.py):ob / ote / liquidity_raid / mss_retest
  - HTF gates(daily bias + premium/discount)在入场时硬过滤
  - 教科书确认 gate:反转类 setup 要求 displacement-FVG 证据
  - 信号注入 SignalTracker(advisory TP1/SL/trail 监视)

驱动方式:
  main.py 在每根 K 线 close 时调 self.on_candle_close()
  内部分发:
    - 4h close → 扫 OB / OTE + 注册 pending MSS
    - 1h close → 扫 Liquidity Raid Reversal + 检查 pending MSS retest
    - 其他 tf → 只更新时间锚 / 清理过期
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional

from engine.ict import detect_daily_bias, compute_dealing_range, price_zone, price_zone_pct
from engine.market_structure import classify_structure_atr
from engine.ict_pois import (
    find_ict_ob_setups,
    find_ict_ote_setups,
    find_ict_liquidity_raid_setups,
    find_ict_mss_retest_setups,
    _has_displacement_fvg,
)
from ict_signal.signal_state import SignalState, SignalStep
from utils.logger import logger


class ICTSignalEngine:
    """ICT 流派信号 engine。

    对外接口:
      - active_signals: Dict[str, SignalState]
      - on_candle_close(symbol, tf, candle, db) → None
      - get_notification_ready() → List[SignalState]
      - diagnostics() → Dict[str, int]
      - _atr(symbol, tf) → float (复用注入的 candle_manager + atr 计算)
    """

    def __init__(
        self,
        candle_manager,
        scorer=None,
        config: Dict[str, Any] = None,
    ):
        """
        candle_manager: 与 SMC engine 共用同一个 CandleManager(共享 K 线池,不双订阅)
        scorer:         保留接口,未使用(ICT 信号统一 score=3.0;SMC scorer 已删除)
        config:         整个 cfg dict,内部读 config.ict.*
        """
        self.candles = candle_manager
        self.scorer = scorer
        self.cfg = config or {}

        ict_cfg = self.cfg.get("ict", {}) if isinstance(self.cfg, dict) else {}
        # 主开关 — engine_enabled=False 时所有 on_candle_close 立即返回
        self.engine_enabled = bool(ict_cfg.get("engine_enabled", False))

        # HTF gates 配置
        self.daily_bias_enabled = bool(ict_cfg.get("daily_bias_enabled", True))
        self.daily_bias_lookback = int(ict_cfg.get("daily_bias_lookback", 30))
        self.daily_bias_min_move = float(ict_cfg.get("daily_bias_min_move_mult", 0.5))

        self.premium_discount_enabled = bool(ict_cfg.get("premium_discount_enabled", True))
        self.dealing_range_lookback = int(ict_cfg.get("dealing_range_lookback", 60))
        self.dealing_range_min_move = float(ict_cfg.get("dealing_range_min_move_mult", 2.0))
        self.premium_threshold = float(ict_cfg.get("premium_threshold", 0.55))
        self.discount_threshold = float(ict_cfg.get("discount_threshold", 0.45))

        # HTF gates ③ 4h MSS bias —— daily MSS 滞后时,4h 提前预警(catch 5-7 这种反转早期)
        self.h4_bias_enabled = bool(ict_cfg.get("h4_bias_enabled", True))
        self.h4_bias_min_move = float(ict_cfg.get("h4_bias_min_move_mult", 0.5))
        self.h4_bias_lookback = int(ict_cfg.get("h4_bias_lookback", 60))

        # RR 最低门槛(原 iFVG 配置;iFVG 路径已随 SMC 管线删除,mss_retest 仍复用)
        self.ifvg_rr_min = float(ict_cfg.get("ifvg_rr_min", 1.0))
        # 教科书 ICT 确认 gate(2026-06-09 补足):displacement / MSS 确认
        # require_displacement:反转类 setup(liquidity_raid + mss_retest)的位移确认
        self.require_displacement = bool(ict_cfg.get("require_displacement", True))
        # ote_require_displacement:OTE 额外 FVG-位移过滤。365d 数据显示净亏(OTE 的位移
        # 已由 swing_min_move_mult=1.5 腿幅保证),默认 OFF。
        self.ote_require_displacement = bool(ict_cfg.get("ote_require_displacement", False))
        self.displacement_min_height_atr = float(ict_cfg.get("displacement_min_height_atr", 0.25))
        self.raid_confirm_within_bars = int(ict_cfg.get("raid_confirm_within_bars", 5))
        # OTE 专项(2026-06-23,1095d OOS):#1 趋势门 + #2 定倍 TP
        self.ote_trend_gate_enabled = bool(ict_cfg.get("ote_trend_gate_enabled", True))
        self.ote_trend_ma_bars = int(ict_cfg.get("ote_trend_ma_bars", 720))
        self.ote_tp_fixed_rr = float(ict_cfg.get("ote_tp_fixed_rr", 0.0))
        # 4 个 POI 类型独立开关(Phase A 默认: OB+OTE+raid 开,mss_retest 关 — 后者需 2 步状态机)
        self.pois_enabled = ict_cfg.get("pois") or {}
        self.poi_ob_enabled = bool(self.pois_enabled.get("ob", True))
        self.poi_ote_enabled = bool(self.pois_enabled.get("ote", True))
        self.poi_liquidity_raid_enabled = bool(self.pois_enabled.get("liquidity_raid", True))
        self.poi_mss_retest_enabled = bool(self.pois_enabled.get("mss_retest", False))
        self.signal_ttl = int(
            self.cfg.get("signal", {}).get("signal_ttl_seconds", 129600)
        )

        self.active_signals: Dict[str, SignalState] = {}
        self._current_time_sec: Optional[int] = None
        self._diag: Dict[str, int] = defaultdict(int)
        # 历史 POI emit 记忆:(poi_type, direction, origin_time) → bool
        # 用于跨越 active_signals 清空的去重(防止同一 OB 在 SCORED 信号过期后又重新生成)
        # 回测窗口 187d 最多几千条,内存可控;live 中跑久了可以定期 prune(留 30 天)
        self._emitted_origins: set = set()
        # Phase B:MSS retest 2 步状态机
        # key=(symbol, mss_direction, break_time) → {break_price, break_time, expires_at}
        # 4h close 检测 MSS → 注册;1h close 检查 retest;到期前未 retest → 删除
        self._pending_mss: Dict[tuple, Dict[str, Any]] = {}
        # MSS retest 等待窗口(秒)
        ict_cfg2 = self.cfg.get("ict", {}) if isinstance(self.cfg, dict) else {}
        self.mss_retest_ttl_sec = int(ict_cfg2.get("mss_retest_ttl_hours", 24)) * 3600
        # retest 容差:close 必须在 break_price ± atr_4h × tol_mult 内
        self.mss_retest_tol_mult = float(ict_cfg2.get("mss_retest_tol_atr_mult", 0.3))

    # ---- 时间锚点 ----
    def _now(self) -> int:
        return self._current_time_sec if self._current_time_sec is not None else int(time.time())

    def _update_now(self, candle: Any) -> None:
        ct_ms = candle["close_time"] if isinstance(candle, dict) else getattr(candle, "close_time", None)
        if ct_ms is not None:
            self._current_time_sec = int(ct_ms / 1000)

    # ---- ATR 计算(复用 candles)----
    def _atr(self, symbol: str, tf: str, period: int = 14, closed_only: bool = False) -> float:
        candles = self.candles.window(symbol, tf, period + 5, closed_only=closed_only)
        if len(candles) < period + 1:
            return 0.0
        trs: List[float] = []
        for i in range(1, len(candles)):
            c = candles[i]; pc = candles[i - 1]
            h = c["high"] if isinstance(c, dict) else c.high
            l = c["low"] if isinstance(c, dict) else c.low
            pcc = pc["close"] if isinstance(pc, dict) else pc.close
            trs.append(max(h - l, abs(h - pcc), abs(l - pcc)))
        seed = sum(trs[:period]) / period
        prev = seed
        for tr in trs[period:]:
            prev = (prev * (period - 1) + tr) / period
        return prev

    # ---- 公共接口 ----
    def diagnostics(self) -> Dict[str, int]:
        return dict(self._diag)

    def get_notification_ready(self) -> List[SignalState]:
        """取所有 SCORED + 未推送 + 未过期的 ICT 信号,并标记为已推送。

        2026-06-10:补上标记副作用(notification_sent + step=NOTIFIED)。
        原先这个语义在 SMC engine 的同名方法里,删 SMC 管线时丢失 —— 没有它,
        notify_loop 每 5s 会把同一条信号重复推送 36h,且 tracker 被反复重置。
        """
        now = self._now()
        ready: List[SignalState] = []
        for s in self.active_signals.values():
            if s.step != SignalStep.SCORED:
                continue
            if s.notification_sent:
                continue
            if s.expires_at and now > s.expires_at:
                continue
            s.notification_sent = True
            s.step = SignalStep.NOTIFIED
            s.updated_at = now
            ready.append(s)
        return ready

    async def on_candle_close(self, symbol: str, timeframe: str, candle: Any, db: Any = None) -> None:
        """主入口:每根 K 线 close 时由 main.py 调用。

        分发(Phase A v2):
          - 4h close → 扫 OB / OTE / MSS retest
          - 1h close → 扫 Liquidity Raid Reversal
        """
        if not self.engine_enabled:
            return
        self._update_now(candle)
        # 清理过期 + terminal
        self._expire_stale()

        if timeframe == "4h":
            self._scan_4h_setups(symbol, candle)
        elif timeframe == "1h":
            self._scan_1h_setups(symbol, candle)

    def _expire_stale(self) -> None:
        now = self._now()
        for sid in list(self.active_signals.keys()):
            s = self.active_signals[sid]
            if s.expires_at and now > s.expires_at and s.step in (
                SignalStep.SCORED,
                SignalStep.NOTIFIED,
            ) and s.notification_sent:
                # 已推送过 + 已过 TTL → 从内存删除
                del self.active_signals[sid]
                self._diag["expired_pruned"] += 1

    # ---- HTF gates ----
    def _check_daily_bias(self, symbol: str) -> Optional[str]:
        """返回 'bullish' / 'bearish' / 'neutral' / None(数据不足)。

        2026-06-23:**只用已收盘日线**(closed_only)。原先 window 带着当日未收盘的
        forming bar,其 OHLC 随市价实时变动 → bias 盘中乱翻,且与回测(只喂收盘 K)不一致。
        closed_only 让 live 与回测对齐;回测无 forming bar,故回测结果不变。
        """
        if not self.daily_bias_enabled:
            return None
        d1 = self.candles.window(symbol, "1d", max(self.daily_bias_lookback + 5, 35),
                                 closed_only=True)
        if len(d1) < 10:
            return None
        atr_1d = self._atr(symbol, "1d", closed_only=True)
        if atr_1d <= 0:
            return None
        return detect_daily_bias(
            d1, atr_1d,
            lookback=self.daily_bias_lookback,
            min_move_mult=self.daily_bias_min_move,
        )

    def _check_h4_bias(self, symbol: str) -> Optional[str]:
        """4h ATR pivot 结构判定(daily MSS 滞后时的早期预警)。
        返回 'bullish' / 'bearish' / 'neutral' / None(数据不足)。
        用 mult=0.5 比较敏感,能 catch 5-7 那种 daily 还没翻但 4h 已经走 LH+LL 的反转早期。
        """
        if not self.h4_bias_enabled:
            return None
        h4 = self.candles.window(symbol, "4h", max(self.h4_bias_lookback + 5, 30))
        if len(h4) < 13:
            return None
        atr_4h = self._atr(symbol, "4h")
        if atr_4h <= 0:
            return None
        return classify_structure_atr(h4, atr_4h, min_move_mult=self.h4_bias_min_move)

    def _ote_trend(self, symbol: str) -> Optional[str]:
        """30d 动量趋势(1h MA + 7d 斜率),正交于结构 bias(daily/4h MSS)。

        返回 'up' / 'down' / 'range' / None。OTE 趋势门(#1)用:逆此趋势的 OTE 否决。
        数据不足 → None = 放行(fail-open;与回测早期窗口一致)。
        """
        if not self.ote_trend_gate_enabled:
            return None
        bars = self.ote_trend_ma_bars
        slope_bars = 168  # 7d 斜率窗口(诊断验证用值,固定)
        h1 = self.candles.window(symbol, "1h", bars + slope_bars + 10)
        if len(h1) < bars + slope_bars:
            return None
        closes = [float(c["close"]) if isinstance(c, dict) else float(c.close) for c in h1]
        c = closes[-1]
        ma_now = sum(closes[-bars:]) / bars
        ma_prev = sum(closes[-bars - slope_bars:-slope_bars]) / bars
        slope_up = ma_now > ma_prev
        if c > ma_now and slope_up:
            return "up"
        if c < ma_now and not slope_up:
            return "down"
        return "range"

    def _check_zone(self, symbol: str, price: float) -> Optional[str]:
        """返回 'premium' / 'discount' / 'equilibrium' / None。"""
        if not self.premium_discount_enabled:
            return None
        h4 = self.candles.window(symbol, "4h", max(self.dealing_range_lookback + 10, 70))
        if len(h4) < 20:
            return None
        atr_4h = self._atr(symbol, "4h")
        if atr_4h <= 0:
            return None
        dr = compute_dealing_range(
            h4, atr_4h,
            lookback=self.dealing_range_lookback,
            min_move_mult=self.dealing_range_min_move,
        )
        if dr is None:
            return None
        return price_zone(
            price, dr,
            premium_threshold=self.premium_threshold,
            discount_threshold=self.discount_threshold,
        )

    # ========================================================================
    # 新 POI 入口(4h close / 1h close 分别触发)
    # ========================================================================

    def _build_dealing_range(self, symbol: str) -> Optional[Dict[str, float]]:
        """工具:复用 compute_dealing_range,返回 dr 或 None。"""
        if not self.premium_discount_enabled:
            return None
        h4 = self.candles.window(symbol, "4h", max(self.dealing_range_lookback + 10, 70))
        if len(h4) < 20:
            return None
        atr_4h = self._atr(symbol, "4h")
        if atr_4h <= 0:
            return None
        return compute_dealing_range(
            h4, atr_4h,
            lookback=self.dealing_range_lookback,
            min_move_mult=self.dealing_range_min_move,
        )

    def _dedupe_by_origin(self, poi_type: str, direction: str,
                          poi_origin_time: Optional[int]) -> bool:
        """按 (poi_type, direction, origin_time) 去重 —— 同一个 POI 历史只允许一条信号。"""
        if poi_origin_time is None:
            return False
        key = (poi_type, direction, int(poi_origin_time))
        if key in self._emitted_origins:
            return True
        return False

    def _dedupe_cross_type(self, direction: str, entry: float, tp: float,
                            window_sec: int = 6 * 3600,
                            entry_tol_pct: float = 0.005) -> bool:
        """跨 POI 类型去重 —— 同方向 + entry 接近 + 时间窗口内 → 视为同一笔。

        不再检查 TP —— 不同 detector 用不同 TP 算法(Raid 用 dealing_range,
        OTE 用 leg_end,OB 用 dealing_range);TP 差异是检测器特性,不代表是不同交易。
        关键判定:**同方向 + entry 几乎一样(0.5% = 12 点在 2400 价位)+ 6h 内** = 同一笔。
        """
        now = self._now()
        for s in self.active_signals.values():
            if s.direction != direction:
                continue
            if not s.entry_price:
                continue
            age = now - (s.created_at or 0)
            if not (0 <= age <= window_sec):
                continue
            # entry 相近(0.5% 内 = 2400 价位 12 点)
            if abs(s.entry_price - entry) / max(entry, 1.0) > entry_tol_pct:
                continue
            return True
        return False

    def _emit_setup(self, symbol: str, setup: Dict[str, Any]) -> Optional[str]:
        """把一个 setup dict 转成 SCORED SignalState 并注册。返回 sid 或 None(去重)。

        entry_mode:
          - ict_ob:LIMIT(挂单等价格回到 OB 边界)
          - ict_ote / ict_liquidity_raid / ict_mss_retest:IMMEDIATE(信号触发时入场)
        """
        poi_type = setup["poi_type"]
        direction = setup["direction"]
        entry = float(setup["entry"])
        meta = setup.get("metadata") or {}
        # OB 和 MSS retest 用 limit(等价格回测到 POI 边界 / 突破价);
        # OTE 和 liquidity_raid 用 immediate(信号触发时价格已经在位)
        entry_mode_for_setup = "limit" if poi_type in ("ict_ob", "ict_mss_retest") else "immediate"
        # 取 POI origin time:OB 用 ob_time,OTE 用 leg_end 时间(无 leg time 用 leg_start),
        # liquidity raid 用 sweep_extreme 时刻 ≈ self._now(),
        # MSS retest 用 break_time。
        # 优先级:稳定 ID(同 setup 多次触发被去重) > sweep_bar_time(每根 K 新) > fallback
        poi_origin = (
            meta.get("ob_time") or
            meta.get("break_time") or
            meta.get("leg_end_time") or
            meta.get("pool_origin_id") or  # liquidity raid:同 pool 只 emit 一次
            meta.get("sweep_bar_time") or
            self._now() * 1000
        )
        try:
            poi_origin = int(poi_origin)
        except Exception:  # noqa: BLE001
            poi_origin = self._now() * 1000

        if self._dedupe_by_origin(poi_type, direction, poi_origin):
            self._diag[f"{poi_type}_dedup_skipped"] += 1
            return None
        # 跨 POI 类型去重:同方向 + entry 接近 + 6h 窗口内 → 同一笔交易想法
        if self._dedupe_cross_type(direction, entry, float(setup["tp"])):
            self._diag[f"{poi_type}_xtype_dedup_skipped"] += 1
            return None
        # HTF gate ③ 4h MSS bias —— daily 滞后时 4h 提前预警
        h4_bias = self._check_h4_bias(symbol)
        if h4_bias == "bearish" and direction == "long":
            self._diag[f"{poi_type}_h4_bias_veto_long"] += 1
            logger.info(f"[ICT-{poi_type}] {direction} 被 4h_bias=bearish 否决(entry={entry:.2f})")
            return None
        if h4_bias == "bullish" and direction == "short":
            self._diag[f"{poi_type}_h4_bias_veto_short"] += 1
            logger.info(f"[ICT-{poi_type}] {direction} 被 4h_bias=bullish 否决(entry={entry:.2f})")
            return None
        # ICT 方法论上下文(供 formatter 渲染 ICT 原生消息)
        _SL_BASIS = {
            "ict_ob": "OB 对侧极值 ±0.3×ATR",
            "ict_ote": "leg 起点(0% 回撤)±0.3×ATR",
            "ict_liquidity_raid": "扫荡极值 ±0.3×ATR",
            "ict_mss_retest": "MSS 突破价 ±1.0×ATR",
        }
        dr_now = self._build_dealing_range(symbol)
        ict_meta = {
            "poi_kind": poi_type,
            "daily_bias": self._check_daily_bias(symbol),
            "zone": price_zone(entry, dr_now, self.premium_threshold, self.discount_threshold) if dr_now else None,
            "zone_pct": price_zone_pct(entry, dr_now),
            "sl_basis": _SL_BASIS.get(poi_type, ""),
            **meta,
        }
        sid = str(uuid.uuid4())
        now = self._now()
        self.active_signals[sid] = SignalState(
            signal_id=sid,
            symbol=symbol,
            direction=direction,
            step=SignalStep.SCORED,
            poi_source=poi_type,
            poi_type=f"{'bearish' if direction == 'short' else 'bullish'}_{poi_type[4:]}",
            poi_low=float(setup["poi_low"]),
            poi_high=float(setup["poi_high"]),
            poi_origin_time=poi_origin,
            triggered_level="POI_high" if direction == "short" else "POI_low",
            triggered_level_value=float(setup["poi_high"] if direction == "short" else setup["poi_low"]),
            entry_price=entry,
            stop_loss=float(setup["sl"]),
            take_profit=float(setup["tp"]),
            risk_reward=float(setup["rr"]),
            entry_mode=entry_mode_for_setup,
            total_score=3.0,
            scores={"C_POI": 3.0, "C_Vol": 0.0, "C_Delta": 0.0,
                    "C_Override": 0.0, "C_Confluence": 0.0, "C_Fib": 0.0},
            created_at=now,
            updated_at=now,
            expires_at=now + self.signal_ttl,
            notification_sent=False,
            source_engine="ict",
            ict_meta=ict_meta,
        )
        self._emitted_origins.add((poi_type, direction, poi_origin))
        self._diag[f"{poi_type}_created"] += 1
        logger.info(
            f"[ICT-{poi_type}] 创建 {sid[:8]} {direction} entry={entry:.2f} "
            f"SL={setup['sl']:.2f} TP={setup['tp']:.2f} RR={setup['rr']:.2f}  "
            f"origin={poi_origin}  meta={meta}"
        )
        return sid

    def _scan_4h_setups(self, symbol: str, candle: Any) -> List[str]:
        """4h close 时扫:OB / OTE / MSS retest。"""
        new_sids: List[str] = []
        candles_4h = self.candles.window(symbol, "4h", 100)
        if len(candles_4h) < 20:
            return new_sids
        atr_4h = self._atr(symbol, "4h")
        if atr_4h <= 0:
            return new_sids
        current_price = float(candle["close"] if isinstance(candle, dict) else candle.close)
        daily_bias = self._check_daily_bias(symbol)
        dr = self._build_dealing_range(symbol)

        # OB at premium/discount
        if self.poi_ob_enabled:
            ob_setups = find_ict_ob_setups(
                candles_4h, atr_4h, current_price,
                daily_bias=daily_bias, dealing_range=dr,
                premium_th=self.premium_threshold, discount_th=self.discount_threshold,
            )
            for setup in ob_setups:
                sid = self._emit_setup(symbol, setup)
                if sid:
                    new_sids.append(sid)

        # OTE
        if self.poi_ote_enabled:
            ote_setups = find_ict_ote_setups(
                candles_4h, atr_4h, current_price,
                daily_bias=daily_bias, dealing_range=dr,
                require_displacement=self.ote_require_displacement,
                displacement_min_height_atr=self.displacement_min_height_atr,
                tp_fixed_rr=self.ote_tp_fixed_rr,   # #2 定倍 TP(0=回退 DR/3R)
            )
            ote_trend = self._ote_trend(symbol)     # #1 30d 动量趋势门
            for setup in ote_setups:
                if ote_trend in ("up", "down"):
                    d = setup["direction"]
                    if (d == "long" and ote_trend == "down") or (d == "short" and ote_trend == "up"):
                        self._diag["ote_trend_gate_veto"] += 1
                        logger.info(f"[ICT-ict_ote] {d} 被 OTE 趋势门否决"
                                    f"(30d trend={ote_trend}, entry={setup['entry']:.2f})")
                        continue
                sid = self._emit_setup(symbol, setup)
                if sid:
                    new_sids.append(sid)

        # MSS retest 2 步状态机(Phase B):4h close 检测 MSS → 注册 pending
        # (真正的 setup 在 1h close 检查 retest 时 emit)
        if self.poi_mss_retest_enabled:
            self._register_pending_mss(symbol, candles_4h, atr_4h, daily_bias)

        return new_sids

    def _scan_1h_setups(self, symbol: str, candle: Any) -> List[str]:
        """1h close 时扫:Liquidity Raid Reversal。"""
        new_sids: List[str] = []
        candles_1h = self.candles.window(symbol, "1h", 60)
        if len(candles_1h) < 10:
            return new_sids
        atr_1h = self._atr(symbol, "1h")
        if atr_1h <= 0:
            return new_sids
        current_price = float(candle["close"] if isinstance(candle, dict) else candle.close)
        daily_bias = self._check_daily_bias(symbol)
        dr = self._build_dealing_range(symbol)

        if self.poi_liquidity_raid_enabled:
            raid_setups = find_ict_liquidity_raid_setups(
                candles_1h, atr_1h, current_price,
                daily_bias=daily_bias, dealing_range=dr,
                premium_th=self.premium_threshold, discount_th=self.discount_threshold,
                sweep_window_bars=self.raid_confirm_within_bars,
                require_confirmation=self.require_displacement,
                confirm_min_height_atr=self.displacement_min_height_atr,
            )
            for setup in raid_setups:
                sid = self._emit_setup(symbol, setup)
                if sid:
                    new_sids.append(sid)

        # MSS retest 第 2 步:1h close 检查 pending_mss 是否被 retest
        if self.poi_mss_retest_enabled and self._pending_mss:
            mss_sids = self._check_pending_mss_retest(symbol, candle, current_price)
            new_sids.extend(mss_sids)

        return new_sids

    # ========================================================================
    # MSS retest 2 步状态机(Phase B)
    # ========================================================================

    def _register_pending_mss(self, symbol: str, candles_4h: List[Any],
                              atr_4h: float, daily_bias: Optional[str]) -> None:
        """4h close 检测 MSS,如果出现 close-break 反向 swing → 注册 pending,等 1h retest。"""
        from engine.market_structure import detect_choch
        if len(candles_4h) < 13:
            return
        mss = detect_choch(candles_4h, lookback=5)
        if mss is None:
            return
        mss_dir = mss["direction"]
        break_price = float(mss["break_price"])
        break_time = int(mss.get("break_time") or 0)
        # 与 daily_bias 一致才注册
        setup_dir = "long" if mss_dir == "bullish" else "short"
        if daily_bias == "bearish" and setup_dir == "long":
            return
        if daily_bias == "bullish" and setup_dir == "short":
            return
        # 教科书 ICT:真 MSS 的突破必须是位移(末尾几根留下方向一致 FVG),
        # 否则只是裸 close-break CHoCH,不注册 pending。
        if self.require_displacement and not _has_displacement_fvg(
            candles_4h, setup_dir, since_index=len(candles_4h) - 3,
            min_height=self.displacement_min_height_atr * atr_4h,
        ):
            return
        key = (symbol, mss_dir, break_time)
        if key in self._pending_mss:
            return  # 已注册
        # 已经 emit 过同 origin 的 mss_retest 信号 → 不再 pending
        if (("ict_mss_retest", setup_dir, break_time) in self._emitted_origins):
            return
        now = self._now()
        self._pending_mss[key] = {
            "symbol": symbol,
            "mss_direction": mss_dir,
            "setup_direction": setup_dir,
            "break_price": break_price,
            "break_time": break_time,
            "registered_at": now,
            "expires_at": now + self.mss_retest_ttl_sec,
            "atr_4h": atr_4h,
        }
        self._diag["mss_pending_registered"] += 1
        logger.info(
            f"[ICT-MSS] 注册 pending mss_retest {setup_dir} break={break_price:.2f} "
            f"break_time={break_time} TTL={self.mss_retest_ttl_sec}s"
        )

    def _check_pending_mss_retest(self, symbol: str, candle: Any, current_price: float) -> List[str]:
        """1h close 检查所有 pending mss 是否被 retest → emit setup。"""
        new_sids: List[str] = []
        now = self._now()
        for key in list(self._pending_mss.keys()):
            event = self._pending_mss[key]
            if event["symbol"] != symbol:
                continue
            # 过期 → 删
            if now > event["expires_at"]:
                del self._pending_mss[key]
                self._diag["mss_pending_expired"] += 1
                continue
            atr_4h = float(event["atr_4h"]) or self._atr(symbol, "4h")
            if atr_4h <= 0:
                continue
            break_price = event["break_price"]
            setup_dir = event["setup_direction"]
            # retest 判定:这根 1h 的 wick 触及 break_price ± atr_4h × tol
            hi = float(candle["high"] if isinstance(candle, dict) else candle.high)
            lo = float(candle["low"] if isinstance(candle, dict) else candle.low)
            cl = float(candle["close"] if isinstance(candle, dict) else candle.close)
            tol = atr_4h * self.mss_retest_tol_mult
            retested = False
            if setup_dir == "long":
                # bullish MSS:price 从上方回测 break_price(low 触及 break_price + tol 以内,close 仍在 break_price 之上)
                if lo <= break_price + tol and cl > break_price - tol:
                    retested = True
            else:
                # bearish MSS:price 从下方回测 break_price
                if hi >= break_price - tol and cl < break_price + tol:
                    retested = True
            if not retested:
                continue
            # 构造 setup
            dr = self._build_dealing_range(symbol)
            entry = cl
            if setup_dir == "long":
                sl = break_price - 1.0 * atr_4h  # SL 在 break_price 下方 1×ATR
                # TP 必须在 entry 上方;dr.high 可能已经被 MSS 打破,要校验
                if dr and float(dr["high"]) > entry:
                    tp = float(dr["high"])
                else:
                    tp = entry + 3.0 * abs(entry - sl)
            else:
                sl = break_price + 1.0 * atr_4h
                if dr and float(dr["low"]) < entry:
                    tp = float(dr["low"])
                else:
                    tp = entry - 3.0 * abs(entry - sl)
            # 方向校验:long → tp > entry > sl;short → tp < entry < sl
            if setup_dir == "long":
                if not (tp > entry > sl):
                    del self._pending_mss[key]
                    continue
                risk = entry - sl
                reward = tp - entry
            else:
                if not (tp < entry < sl):
                    del self._pending_mss[key]
                    continue
                risk = sl - entry
                reward = entry - tp
            if risk <= 0 or reward <= 0:
                del self._pending_mss[key]
                continue
            rr = reward / risk
            if rr < self.ifvg_rr_min:
                del self._pending_mss[key]
                continue
            setup = {
                "poi_type": "ict_mss_retest",
                "direction": setup_dir,
                "poi_low": break_price - 0.3 * atr_4h,
                "poi_high": break_price + 0.3 * atr_4h,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "rr": rr,
                "metadata": {
                    "mss_direction": event["mss_direction"],
                    "break_price": break_price,
                    "break_time": event["break_time"],
                    "retest_at": now,
                },
            }
            sid = self._emit_setup(symbol, setup)
            if sid:
                new_sids.append(sid)
            # 不论是否 emit(可能 dedupe),都从 pending 删除
            del self._pending_mss[key]
        return new_sids
