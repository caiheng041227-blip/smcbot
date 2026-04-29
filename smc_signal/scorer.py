"""信号质量打分。

2026-04-20 重构:
  旧版 C1-C9 经 187 天 + 20 天实盘数据验证:
    - C2 (vol) / C3 (delta) 弱正(+6pp 胜率差),保留
    - C5 (div) / C6 (wick) 反向(命中胜率更低),删除
    - C4 / C7 / C8 / C9 噪声(胜率差 <3pp),删除
    - POI 类型是最强判别信号(1h_equal_lows 89% vs 1h_equal_highs 0%),但没纳入,加入
  新版 5 个维度 + POI 加权,threshold 3。
"""
from __future__ import annotations

from typing import Any, Dict, Sequence, Tuple


def _h(c: Any) -> float:
    return c["high"] if isinstance(c, dict) else c.high


def _l(c: Any) -> float:
    return c["low"] if isinstance(c, dict) else c.low


def _o(c: Any) -> float:
    return c["open"] if isinstance(c, dict) else c.open


def _c(c: Any) -> float:
    return c["close"] if isinstance(c, dict) else c.close


def _v(c: Any) -> float:
    return c["volume"] if isinstance(c, dict) else c.volume


# POI 类型加权表(基于 187 天 + 20 天实盘数据)
#   tier A +2:验证过正 EV 或反转早期高胜率
#   tier B +1:平均或"次优但遮盖更烂"(保留避免暴露下级)
#   tier C  0:确认负 EV 或小样本 0% 胜率,不加分
# 新出现的 POI 源(未在表里)默认 0 分,保守
_POI_TIER_WEIGHTS: Dict[str, float] = {
    # Tier A(+3)—— 数据验证过的高质 POI,单独够格过阈值 3.0,
    # 其它证据作为 bonus(数据显示 equal_lows 单点 89% WR,无需其它确认)
    "1h_equal_lows": 3.0,     # 20 天 89% WR / +19R
    "1h_breaker": 3.0,        # 187 天 41.8% WR / +27R(最佳)
    "failed_sweep_reentry": 3.0,   # 2026-04-23 升 1.0→3.0:187d 全量 49 笔 49% WR +51.49R,
                                   # 未通知赢家 +43.71R 占漏网总 R 的 88%,事实最强源
    # Tier B(+1)—— 平均或次优,需要至少 2 个其它证据才过阈值
    "4h_fvg": 1.0,            # 187 天 -10R 但删了暴露更烂,保留中位
    "4h_ob": 1.0,             # 小样本 80% WR(不可信但方向正)
    "1h_equal_highs": 1.0,    # 2026-04-21 升级:去重后 187d 37 笔 27% WR +2.99R 事实净正
                              # 原 0.0 基于带重复样本的失真统计,去重后纠偏
    # Tier C(0)—— 确认负 EV,只能靠 3 个其它证据救
    "4h_breaker": 0.0,        # 187 天 -8R
    "1h_ob": 0.0,             # 小样本全败
    "sweep_based": 1.0,       # 2026-04-23:Tier B(历经 3.0→0.0→1.0 调整)
                              # 数据证实负 EV(187d 10% -16R,20d 0/7),
                              # 1.0 下需其它 2 项证据才过阈值,不轻易推送
    # 2026-04-21 新增 POI 源(Tier B,保守):需至少 2 个其它证据才过阈值。
    # 在历史数据验证前不给 A 级,避免过度乐观;跑一轮 187d 后再升降级。
    "failed_fvg_reentry": 1.0,     # 4h FVG 破位后 reclaim,针对"FVG 被磨损后深扫反弹"
    "double_bottom": 1.0,          # 长期方案:等高双底 + 颈线突破
    "double_top": 1.0,             # 长期方案:等高双顶 + 颈线跌破
}


def score(signal_state, context: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    """返回 (总分, {维度: 分数})。最大 5 分,threshold 3 = 通知门闸。

    必选 context:
      - candles:最近 21+ 根 K 线(用于 C_Vol)
      - bar_delta:当前 K delta
    可选:
      - levels / vp / h4_structure / level_proximity_pct(新版不使用,为保向后兼容)
    """
    breakdown: Dict[str, float] = {
        k: 0.0 for k in ("C_POI", "C_Vol", "C_Delta", "C_Override", "C_Confluence", "C_Fib")
    }
    candles: Sequence[Any] = context.get("candles") or []
    if not candles:
        return 0.0, breakdown

    last = candles[-1]
    price = _c(last)
    direction = getattr(signal_state, "direction", None)

    # --- C_POI:POI 类型加权(0-2)---
    source = getattr(signal_state, "poi_source", None) or ""
    breakdown["C_POI"] = _POI_TIER_WEIGHTS.get(source, 0.0)

    # --- C_Vol:量能放大(0-1)---
    if len(candles) >= 21:
        prev20 = candles[-21:-1]
        mean_v = sum(_v(x) for x in prev20) / 20.0
        if mean_v > 0 and _v(last) > mean_v * 2:
            breakdown["C_Vol"] = 1.0

    # --- C_Delta:1h Step3 sweep K 的订单流方向(0-1)---
    # 2026-04-26 改:从"15m 当前 K delta"切换到"1h Step3 sweep K delta"
    # 原 15m delta 在 Step6 时刻取,和"Step5 破位 K"高度相关,信息量重复
    # 1h sweep K delta 反映的是"价格进入 POI 时多/空头的真实净订单流"
    # 同向逻辑(long → 买强 = 买盘吸收卖压 / short → 卖强 = 卖盘吸收买压)= POI 真拒绝
    # 反向逻辑(long → 卖强 / short → 买强)= 趋势顺势,有冲突不加分
    # 缺失 sweep_bar_delta 或 volume 时(早期信号或非 zone POI 路径)→ 0 分
    sweep_delta = getattr(signal_state, "sweep_bar_delta", None)
    sweep_vol = getattr(signal_state, "sweep_bar_volume", None)
    if sweep_delta is not None and sweep_vol and sweep_vol > 0:
        ratio = float(sweep_delta) / float(sweep_vol)
        if direction == "long" and ratio > 0.3:
            breakdown["C_Delta"] = 1.0
        elif direction == "short" and ratio < -0.3:
            breakdown["C_Delta"] = 1.0

    # --- C_Override:Step1 通过 deep sweep override 放行(0-1)---
    # 反转早期的 stop-hunt 完成信号,和 POI 一起构成反转证据链
    if getattr(signal_state, "step1_override_by_sweep", False):
        breakdown["C_Override"] = 1.0

    # --- C_Confluence:价格在关键价位 ±0.3% 内(0-1,从旧 C1 保留)---
    levels: Dict[str, Any] = context.get("levels") or {}
    prox = float(context.get("level_proximity_pct", 0.003))
    level_values = [v for v in levels.values() if v is not None]
    if level_values and price > 0:
        nearest = min(abs(price - lv) / price for lv in level_values)
        if nearest <= prox:
            breakdown["C_Confluence"] = 1.0

    # --- C_Fib:入场价落在最近 4h swing 的 OTE 区间(0.62~0.79 回撤)---
    # 2026-04-29 实验:Fib OTE confluence 维度
    # 用 ATR pivot 取最近一对 4h swing(high+low),算 0.62~0.79 区间
    # long → 入场价在 (low + 0.21*range, low + 0.38*range) → C_Fib = 1
    # short → 入场价在 (high - 0.38*range, high - 0.21*range) → C_Fib = 1
    # range = high - low,Fib 0.62 = 起点 + 0.62*range,0.79 = 起点 + 0.79*range
    candles_4h = context.get("candles_4h") or []
    atr_4h = float(context.get("atr_4h") or 0.0)
    if len(candles_4h) >= 13 and atr_4h > 0:
        try:
            from engine.market_structure import find_swing_points_atr
            highs, lows = find_swing_points_atr(candles_4h, atr_4h, min_move_mult=1.0)
            if highs and lows:
                last_high = highs[-1]["price"]
                last_low = lows[-1]["price"]
                # 仅当 swing pair 形成"摆动段"(low<high)才算
                if last_high > last_low:
                    rng = last_high - last_low
                    if direction == "long":
                        # 多头从 low 开始算回撤;OTE = 0.62~0.79 回撤(对 long 是从 low 反弹接近 high 的中段)
                        # 实际入场常在回撤后的 0.62~0.79 区间(从 high 算下来 38%~21%)
                        ote_low = last_high - 0.79 * rng
                        ote_high = last_high - 0.62 * rng
                        if ote_low <= price <= ote_high:
                            breakdown["C_Fib"] = 1.0
                    elif direction == "short":
                        # 空头对称:从 high 算回撤,OTE 是 0.62~0.79
                        ote_low = last_low + 0.62 * rng
                        ote_high = last_low + 0.79 * rng
                        if ote_low <= price <= ote_high:
                            breakdown["C_Fib"] = 1.0
        except Exception:  # noqa: BLE001
            pass

    total = sum(breakdown.values())
    return total, breakdown


__all__ = ["score"]
