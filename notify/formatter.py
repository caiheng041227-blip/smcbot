"""信号消息格式化器。

把鸭子类型的 signal 对象 -> Markdown 字符串（Telegram parse_mode='Markdown'）。
字段通过 getattr 读取，不耦合具体类定义。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

import pytz

# 评分项中文标签
SCORE_LABELS: Dict[str, str] = {
    # 新版(2026-04-20 重构):5 维度,POI 类型 + 订单流信号
    "C_POI": "POI类型",
    "C_Vol": "Volume放量",
    "C_Delta": "Delta强度",
    "C_Override": "反转SweepOverride",
    "C_Confluence": "价位共振",
    # 旧版维度保留显示名(若回测/历史信号带旧 scores,仍能正常展示)
    "C1": "价位共振",
    "C2": "Volume放量",
    "C3": "Delta强度",
    "C4": "4H结构",
    "C5": "价量背离",
    "C6": "长影线",
    "C7": "价-POC共振",
    "C8": "OB内含POC",
    "C9": "扫荡双过",
    "C10": "反转SweepOverride",
}

# POI 类型中文
POI_TYPE_LABELS: Dict[str, str] = {
    "order_block": "订单块",
    "fvg": "FVG (公允价值缺口)",
}

_NY_TZ = pytz.timezone("America/New_York")


def _get(signal: Any, name: str, default: Any = None) -> Any:
    """dict / SimpleNamespace / dataclass 通吃的字段访问。"""
    if isinstance(signal, dict):
        return signal.get(name, default)
    return getattr(signal, name, default)


def _fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return f"{x:.2f}"


def _fmt_ts_et(ts: Optional[int]) -> str:
    """把 created_at（Unix 秒或毫秒）转为 'YYYY-MM-DD HH:MM ET'。"""
    if ts is None:
        return "-"
    # 自动判断秒/毫秒：> 1e12 认为是毫秒
    if ts > 10_000_000_000:
        ts = ts / 1000.0
    try:
        dt = datetime.fromtimestamp(float(ts), tz=pytz.utc).astimezone(_NY_TZ)
        return dt.strftime("%Y-%m-%d %H:%M ET")
    except (ValueError, OSError, OverflowError):
        return "-"


def _pct(target: Optional[float], entry: Optional[float]) -> str:
    """计算 (target - entry) / entry 的百分比，带正负号。"""
    if target is None or entry is None or entry == 0:
        return ""
    pct = (target - entry) / entry * 100.0
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def _format_sweep(sweep: Optional[Dict[str, Any]]) -> str:
    """格式化流动性猎取信息。"""
    if not sweep:
        return "未检测"
    ratio = sweep.get("wick_body_ratio") if isinstance(sweep, dict) else None
    direction = sweep.get("direction") if isinstance(sweep, dict) else None
    label = "长影线"
    if direction == "upper":
        label = "长上影线"
    elif direction == "lower":
        label = "长下影线"
    if ratio is not None:
        return f"✅ {label} (影线/实体 = {float(ratio):.1f}x)"
    return f"✅ {label}"


def _format_poi(poi_type: Optional[str], poi_low: Optional[float], poi_high: Optional[float]) -> str:
    if not poi_type:
        return "-"
    label = POI_TYPE_LABELS.get(poi_type, poi_type)
    if poi_low is not None and poi_high is not None:
        return f"{label} [{_fmt_price(poi_low)} - {_fmt_price(poi_high)}]"
    return label


def _format_triggered_level(level: Optional[str], level_value: Optional[float]) -> str:
    """展示触发价位名 + 实际价(优先用 level_value,无则留空)。"""
    if not level:
        return "-"
    if level_value is not None:
        return f"{level} @ {_fmt_price(level_value)}"
    return level


def _fmt_ms_et(ms: Optional[int]) -> str:
    """Binance open_time 是毫秒,格式化为 'MM-DD HH:MM ET'。"""
    if ms is None:
        return "-"
    try:
        dt = datetime.fromtimestamp(float(ms) / 1000.0, tz=pytz.utc).astimezone(_NY_TZ)
        return dt.strftime("%m-%d %H:%M ET")
    except (ValueError, OSError, OverflowError, TypeError):
        return "-"


def _format_evidence(signal: Any) -> str:
    """溯源证据区块:展示原始 K 线时间/价位,便于回 TradingView 对图复盘。"""
    lines: list[str] = []

    sweep = _get(signal, "liquidity_sweep_candle")
    if isinstance(sweep, dict):
        t = sweep.get("time")
        hi, lo = sweep.get("high"), sweep.get("low")
        delta = _get(signal, "sweep_bar_delta")
        delta_str = f"  Δ={delta:+.1f}" if isinstance(delta, (int, float)) else ""
        lines.append(
            f"• 猎杀 K 线: {_fmt_ms_et(t)}  H={_fmt_price(hi)} L={_fmt_price(lo)}{delta_str}"
        )

    poi_origin = _get(signal, "poi_origin_time")
    disp_t = _get(signal, "poi_displacement_time")
    disp_mult = _get(signal, "poi_displacement_mult")
    poi_type = _get(signal, "poi_type") or ""
    if poi_origin is not None:
        if "ob" in poi_type and disp_t is not None:
            mult_str = f"  位移 body×{disp_mult:.2f}" if isinstance(disp_mult, (int, float)) else ""
            lines.append(
                f"• OB 原 K: {_fmt_ms_et(poi_origin)}  →  位移 K: {_fmt_ms_et(disp_t)}{mult_str}"
            )
        else:
            lines.append(f"• POI K 线: {_fmt_ms_et(poi_origin)}")

    vp_poc = _get(signal, "poi_vp_poc")
    if isinstance(vp_poc, (int, float)):
        lines.append(f"• VP POC 支撑: {_fmt_price(vp_poc)}")

    ch_price = _get(signal, "choch_break_price")
    ch_time = _get(signal, "choch_break_time")
    if ch_price is not None or ch_time is not None:
        lines.append(
            f"• CHoCH 破位: {_fmt_price(ch_price)} @ {_fmt_ms_et(ch_time)}"
        )

    return "\n".join(lines) if lines else "(无证据记录)"


def _format_scores(scores: Optional[Dict[str, float]]) -> str:
    """只展示触发了的（非 0）条件。"""
    if not scores:
        return "(无评分明细)"
    lines = []
    # 固定顺序 C1..C7，保证输出稳定
    for key in sorted(scores.keys(), key=lambda k: (k[0], k[1:])):
        val = scores.get(key, 0)
        try:
            val_f = float(val)
        except (TypeError, ValueError):
            continue
        if val_f == 0:
            continue
        label = SCORE_LABELS.get(key, key)
        sign = "+" if val_f >= 0 else ""
        # 尽量用整数展示
        val_str = f"{int(val_f)}" if val_f.is_integer() else f"{val_f:g}"
        lines.append(f"{key} {label} {sign}{val_str}")
    return "\n".join(lines) if lines else "(无评分明细)"


def format_signal(signal: Any) -> str:
    """把 signal 对象格式化成 Markdown 消息。"""
    direction = (_get(signal, "direction") or "").lower()
    is_long = direction == "long"
    emoji = "🟢" if is_long else "🔴"
    direction_cn = "做多信号" if is_long else "做空信号"

    symbol = _get(signal, "symbol", "?")
    total_score = _get(signal, "total_score", 0)
    try:
        score_str = f"{float(total_score):g}"
    except (TypeError, ValueError):
        score_str = str(total_score)

    triggered_level = _get(signal, "triggered_level")
    triggered_level_value = _get(signal, "triggered_level_value")
    entry_price = _get(signal, "entry_price")
    stop_loss = _get(signal, "stop_loss")
    take_profit = _get(signal, "take_profit")
    risk_reward = _get(signal, "risk_reward")
    poi_type = _get(signal, "poi_type")
    poi_low = _get(signal, "poi_low")
    poi_high = _get(signal, "poi_high")
    created_at = _get(signal, "created_at")
    sweep = _get(signal, "liquidity_sweep_candle")
    scores = _get(signal, "scores") or {}

    trade_lines = [
        "💰 交易参数:",
        f"• 入场价: {_fmt_price(entry_price)}",
    ]
    # SL / TP / RR 都由用户手动决定,仅当信号里真的带了才展示(当前正常路径下都为 None)
    if stop_loss is not None:
        sl_pct = _pct(stop_loss, entry_price)
        trade_lines.append(f"• 止损价: {_fmt_price(stop_loss)}" + (f"  ({sl_pct})" if sl_pct else ""))
    if take_profit is not None:
        tp_pct = _pct(take_profit, entry_price)
        trade_lines.append(f"• 目标价: {_fmt_price(take_profit)}" + (f"  ({tp_pct})" if tp_pct else ""))
    if risk_reward is not None:
        try:
            trade_lines.append(f"• 盈亏比: 1:{float(risk_reward):.1f}")
        except (TypeError, ValueError):
            pass
    # 2026-04-23:推送 TP1 + trail 指引(bot 追踪器会在 TP1/TP2/SL 命中时继续发提醒)
    if take_profit is not None and entry_price is not None and stop_loss is not None:
        trade_lines.append(f"• TP1(建议平 50%): {_fmt_price(take_profit)}")
        trade_lines.append("• TP2(剩余 50%): 达 TP1 后启用 peak − 1.5×ATR_4h 移动止盈")
        trade_lines.append("• bot 会在 TP1 / TP2 / SL 命中时发 TG 提醒")
    else:
        trade_lines.append("• 止损 / 目标由你自行判断")

    lines = [
        f"{emoji} {direction_cn} | {symbol} | 评分: {score_str}/10",
        "",
        f"📍 触发价位: {_format_triggered_level(triggered_level, triggered_level_value)}",
        f"🕐 时间: {_fmt_ts_et(created_at)}",
        "",
        "📊 信号详情:",
        f"• 流动性猎取: {_format_sweep(sweep)}",
        f"• POI 类型: {_format_poi(poi_type, poi_low, poi_high)}",
        "• 止损依据: POI 对侧边界",
        "",
        *trade_lines,
        "",
        "🔍 证据溯源 (回图可核对):",
        _format_evidence(signal),
        "",
        "📈 评分明细:",
        _format_scores(scores),
        "",
        "⚠️ 仅供参考，人工确认后再入场",
    ]
    return "\n".join(lines)


__all__ = ["format_signal"]
