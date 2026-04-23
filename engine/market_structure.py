"""市场结构识别：摆动点 + 趋势分类 + CHoCH/BOS 检测。

为什么把这些放一起：三者共享同一套 swing pivots，分散到多个文件会重复扫描。
BOS/CHoCH 严格用收盘价确认 —— 影线假突破在 SMC 手册里被明确排除。
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

# 统一的 candle 访问器：既兼容 Candle dataclass，也兼容 dict
def _h(c: Any) -> float:
    return c["high"] if isinstance(c, dict) else c.high


def _l(c: Any) -> float:
    return c["low"] if isinstance(c, dict) else c.low


def _close(c: Any) -> float:
    return c["close"] if isinstance(c, dict) else c.close


def _t(c: Any) -> int:
    # 优先 open_time，缺失时回退到 time
    if isinstance(c, dict):
        return int(c.get("open_time", c.get("time", 0)))
    return int(getattr(c, "open_time", getattr(c, "time", 0)))


Structure = Literal["bullish", "bearish", "neutral"]


def find_swing_points(
    candles: Sequence[Any],
    lookback: int = 5,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """分形摆动点：第 i 根 K 线左右各 lookback 根都不高/低于它。

    返回 (swing_highs, swing_lows)，每个元素为 {"index", "price", "time"}，按时间正序。
    边界 lookback 根无法判定（左右不足）会被自动跳过。
    """
    n = len(candles)
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []
    if n < 2 * lookback + 1:
        return highs, lows

    for i in range(lookback, n - lookback):
        ch = _h(candles[i])
        cl = _l(candles[i])
        is_high = True
        is_low = True
        for j in range(1, lookback + 1):
            # 用 >= / <= 防止等高平台被误判为 swing（更严格）
            if _h(candles[i - j]) >= ch or _h(candles[i + j]) >= ch:
                is_high = False
            if _l(candles[i - j]) <= cl or _l(candles[i + j]) <= cl:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high:
            highs.append({"index": i, "price": ch, "time": _t(candles[i])})
        if is_low:
            lows.append({"index": i, "price": cl, "time": _t(candles[i])})
    return highs, lows


def classify_structure(
    candles: Sequence[Any],
    lookback: int = 5,
) -> Structure:
    """基于最近两个 swing highs / lows 判断 HH+HL / LH+LL。

    需要各至少 2 个 swing；不够就返回 neutral。
    """
    highs, lows = find_swing_points(candles, lookback)
    if len(highs) < 2 or len(lows) < 2:
        return "neutral"
    hh = highs[-1]["price"] > highs[-2]["price"]
    hl = lows[-1]["price"] > lows[-2]["price"]
    lh = highs[-1]["price"] < highs[-2]["price"]
    ll = lows[-1]["price"] < lows[-2]["price"]
    if hh and hl:
        return "bullish"
    if lh and ll:
        return "bearish"
    return "neutral"


def detect_choch(
    candles: Sequence[Any],
    lookback: int = 5,
) -> Optional[Dict[str, Any]]:
    """用最后一根已收盘 K 线的收盘价判定是否发生 CHoCH。

    - 当前结构为 bullish：最新收盘价跌破最近 swing low  => CHoCH 转空
    - 当前结构为 bearish：最新收盘价突破最近 swing high => CHoCH 转多
    返回 {"direction", "break_price", "break_time"} 或 None。

    这里只识别"转折"（CHoCH），不返回 BOS —— BOS 逻辑留给 state machine 用趋势
    方向 + 突破组合判定，避免这里与状态机重复维护 trend 状态。
    """
    if not candles:
        return None
    highs, lows = find_swing_points(candles, lookback)
    trend = classify_structure(candles, lookback)
    last = candles[-1]
    c = _close(last)

    if trend == "bullish" and lows:
        recent_low = lows[-1]["price"]
        if c < recent_low:
            return {"direction": "bearish", "break_price": c, "break_time": _t(last)}
    elif trend == "bearish" and highs:
        recent_high = highs[-1]["price"]
        if c > recent_high:
            return {"direction": "bullish", "break_price": c, "break_time": _t(last)}
    return None


def find_swing_points_atr(
    candles: Sequence[Any],
    atr: float,
    min_move_mult: float = 1.0,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """ATR-weighted ZigZag pivot detection(2026-04-22 新增,可选替代 fractal)。

    核心思想:只有相对 local extremum 反向移动 ≥ min_move_mult × atr 的点才算 pivot。
    优点:天然过滤噪声,"视觉明显"的峰谷必然被识别(fractal 的严格 >= 判死问题消除)。
    返回结构与 find_swing_points 完全兼容: (highs, lows),每项 {"index","price","time"}。

    典型 min_move_mult:
      - 0.5: 宽松,更多 pivot
      - 1.0: 平衡(本项目默认)
      - 1.5-2.0: 只识别趋势级转折
    """
    n = len(candles)
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []
    if n < 2 or atr <= 0:
        return highs, lows

    threshold = min_move_mult * atr
    # 初始候选:用 bar 0 作为 max/min 候选,方向未知
    current_max_idx = 0
    current_max_price = _h(candles[0])
    current_min_idx = 0
    current_min_price = _l(candles[0])
    direction: Optional[str] = None   # "up" / "down" / None

    for i in range(1, n):
        ch = _h(candles[i])
        cl = _l(candles[i])

        if direction is None:
            # 初始阶段:同时追踪 max 和 min,等首个 threshold 级移动确定方向
            if ch > current_max_price:
                current_max_idx = i
                current_max_price = ch
            if cl < current_min_price:
                current_min_idx = i
                current_min_price = cl
            if current_max_price - current_min_price >= threshold:
                if current_max_idx > current_min_idx:
                    # 最新极值是 max,已形成向上段 → 先前 min 作为首个 swing low
                    direction = "up"
                    lows.append({
                        "index": current_min_idx,
                        "price": current_min_price,
                        "time": _t(candles[current_min_idx]),
                    })
                else:
                    direction = "down"
                    highs.append({
                        "index": current_max_idx,
                        "price": current_max_price,
                        "time": _t(candles[current_max_idx]),
                    })
        elif direction == "up":
            # 向上段:刷新 local max;反向 >= threshold → 确认 swing high
            if ch > current_max_price:
                current_max_idx = i
                current_max_price = ch
            elif cl < current_max_price - threshold:
                highs.append({
                    "index": current_max_idx,
                    "price": current_max_price,
                    "time": _t(candles[current_max_idx]),
                })
                direction = "down"
                current_min_idx = i
                current_min_price = cl
        else:  # direction == "down"
            if cl < current_min_price:
                current_min_idx = i
                current_min_price = cl
            elif ch > current_min_price + threshold:
                lows.append({
                    "index": current_min_idx,
                    "price": current_min_price,
                    "time": _t(candles[current_min_idx]),
                })
                direction = "up"
                current_max_idx = i
                current_max_price = ch

    return highs, lows


def classify_structure_atr(
    candles: Sequence[Any],
    atr: float,
    min_move_mult: float = 1.0,
) -> Structure:
    """ATR pivot 版 classify_structure,语义与 fractal 版一致:HH+HL / LH+LL / neutral。"""
    highs, lows = find_swing_points_atr(candles, atr, min_move_mult)
    if len(highs) < 2 or len(lows) < 2:
        return "neutral"
    hh = highs[-1]["price"] > highs[-2]["price"]
    hl = lows[-1]["price"] > lows[-2]["price"]
    lh = highs[-1]["price"] < highs[-2]["price"]
    ll = lows[-1]["price"] < lows[-2]["price"]
    if hh and hl:
        return "bullish"
    if lh and ll:
        return "bearish"
    return "neutral"


__all__ = [
    "find_swing_points",
    "find_swing_points_atr",
    "classify_structure",
    "classify_structure_atr",
    "detect_choch",
]
