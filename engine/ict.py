"""ICT (Inner Circle Trader) 方法学工具集。

本模块提供 3 个核心 ICT 概念的纯函数实现:
  - detect_daily_bias: Daily MSS bias gate(close-break opposing swing)
  - compute_dealing_range: 识别最近的 ZigZag 区间(swing H 到 swing L)
  - price_zone: 判定价格在 dealing range 的 premium/discount 位置

设计原则:
  - 纯函数,无状态,易于回测和单元验证
  - 复用 engine.market_structure 的 ATR pivot 工具
  - 不写防御性 try/except —— 输入异常时返回 'neutral' / None 即可

接入点见 smc_signal/state_machine._check_step1 + scripts/backtest.py。
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Sequence

from engine.market_structure import find_swing_points_atr

Zone = Literal["premium", "discount", "equilibrium"]
Bias = Literal["bullish", "bearish", "neutral"]


def _close(c: Any) -> float:
    return c["close"] if isinstance(c, dict) else c.close


def detect_daily_bias(
    d1_candles: Sequence[Any],
    atr_1d: float,
    lookback: int = 30,
    min_move_mult: float = 1.0,
) -> Bias:
    """根据最近 N 根 Daily 的 close-break MSS 历史,返回当前 daily bias。

    ICT 定义:
      - bullish bias 状态下,close < 最近一个 swing low → 翻 bearish(bearish MSS)
      - bearish bias 状态下,close > 最近一个 swing high → 翻 bullish(bullish MSS)
      - 初始 neutral,第一次 close-break 任意方向 swing 即锁定 bias
      - bias 是 sticky 的,不会无故翻转,只在反向 MSS 出现时翻

    算法:
      1. 用 ATR pivot 找最近 lookback 根 Daily 的所有 swing H / swing L
      2. 按时间顺序遍历每根 K 的 close,维护 bias 状态机
      3. 返回最后状态

    复杂度 O(n × s) where s = swing count,通常 n ≤ 30 / s ≤ 6 → O(180),完全可接受。

    注意:swings 用"最终极值索引",这里假设极值在 K 收盘时已可见(对 Daily 适用)。
    """
    if not d1_candles or atr_1d <= 0:
        return "neutral"

    candles = list(d1_candles[-lookback:]) if len(d1_candles) > lookback else list(d1_candles)
    n = len(candles)
    if n < 5:
        return "neutral"

    highs, lows = find_swing_points_atr(candles, atr_1d, min_move_mult)
    if not highs or not lows:
        return "neutral"

    high_pairs: List[tuple] = [(h["index"], h["price"]) for h in highs]
    low_pairs: List[tuple] = [(l["index"], l["price"]) for l in lows]

    bias: Bias = "neutral"

    for i in range(n):
        c = _close(candles[i])
        recent_high: Optional[float] = None
        for hi, hp in high_pairs:
            if hi <= i:
                recent_high = hp
            else:
                break
        recent_low: Optional[float] = None
        for li, lp in low_pairs:
            if li <= i:
                recent_low = lp
            else:
                break

        if bias == "bullish":
            if recent_low is not None and c < recent_low:
                bias = "bearish"
        elif bias == "bearish":
            if recent_high is not None and c > recent_high:
                bias = "bullish"
        else:
            if recent_high is not None and c > recent_high:
                bias = "bullish"
            elif recent_low is not None and c < recent_low:
                bias = "bearish"

    return bias


def compute_dealing_range(
    candles: Sequence[Any],
    atr: float,
    lookback: int = 30,
    min_move_mult: float = 1.0,
) -> Optional[Dict[str, float]]:
    """识别最近的 ICT dealing range,以最近一对 ATR pivot 的 swing H / swing L 为边界。

    返回 {'high', 'low', 'equilibrium', 'high_idx', 'low_idx'},
    无法确定时返回 None(swing 不足 / 区间无效)。

    用途:premium/discount 判定的基础数据。
    """
    if not candles or atr <= 0:
        return None
    sub = list(candles[-lookback:]) if len(candles) > lookback else list(candles)
    highs, lows = find_swing_points_atr(sub, atr, min_move_mult)
    if not highs or not lows:
        return None
    last_h = highs[-1]
    last_l = lows[-1]
    high = float(last_h["price"])
    low = float(last_l["price"])
    if high <= low:
        return None
    return {
        "high": high,
        "low": low,
        "equilibrium": (high + low) / 2,
        "high_idx": int(last_h["index"]),
        "low_idx": int(last_l["index"]),
    }


def price_zone(
    price: float,
    dealing_range: Optional[Dict[str, float]],
    premium_threshold: float = 0.55,
    discount_threshold: float = 0.45,
) -> Zone:
    """判定价格在 dealing range 的位置:premium / discount / equilibrium。

    标准 ICT:>= 50% = premium(只 short),<= 50% = discount(只 long)。
    本实现默认 ±5% 边带:55% 以上 premium,45% 以下 discount,中间 equilibrium。
    """
    if not dealing_range:
        return "equilibrium"
    high = dealing_range["high"]
    low = dealing_range["low"]
    if high <= low:
        return "equilibrium"
    pct = (price - low) / (high - low)
    if pct >= premium_threshold:
        return "premium"
    if pct <= discount_threshold:
        return "discount"
    return "equilibrium"


def price_zone_pct(price: float, dealing_range: Optional[Dict[str, float]]) -> Optional[float]:
    """返回价格在 dealing range 的百分位(0.0 ~ 1.0),用于日志和评分。"""
    if not dealing_range:
        return None
    high = dealing_range["high"]
    low = dealing_range["low"]
    if high <= low:
        return None
    return (price - low) / (high - low)
