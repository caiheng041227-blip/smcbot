"""ICT 信号消息格式化器(2026-06-10 重写,SMC 评分制随管线删除)。

把鸭子类型的 signal 对象 -> Markdown 字符串(Telegram parse_mode='Markdown')。
字段通过 getattr 读取,不耦合具体类定义。

ICT 信号方法论是 **gate 制**(过 displacement/bias/zone 就发,不打分),所以这里
不展示评分,改为展示 ICT 真正的上下文:HTF daily bias、premium/discount 分位、
POI 类型与溯源(leg / pool / 位移确认)。这些来自 SignalState.ict_meta。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

import pytz

_NY_TZ = pytz.timezone("America/New_York")

# ICT POI 类型中文(key 用 poi_source,如 "ict_ote")
POI_KIND_LABELS: Dict[str, str] = {
    "ict_ote": "OTE 最优入场区(62-79% 回撤)",
    "ict_ob": "订单块 OB(premium/discount)",
    "ict_liquidity_raid": "流动性扫荡反转(turtle soup)",
    "ict_mss_retest": "MSS 突破回测",
}

_BIAS_DISPLAY = {
    "bullish": "🟢 bullish(只放行 long)",
    "bearish": "🔴 bearish(只放行 short)",
    "neutral": "⚪ neutral(两向均可)",
}


def _get(signal: Any, name: str, default: Any = None) -> Any:
    """dict / SimpleNamespace / dataclass 通吃的字段访问。"""
    if isinstance(signal, dict):
        return signal.get(name, default)
    return getattr(signal, name, default)


def _fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return f"{float(x):.2f}"


def _fmt_ts_et(ts: Optional[int]) -> str:
    """把 created_at(Unix 秒或毫秒)转为 'YYYY-MM-DD HH:MM ET'。"""
    if ts is None:
        return "-"
    if ts > 10_000_000_000:  # > 1e12 视为毫秒
        ts = ts / 1000.0
    try:
        dt = datetime.fromtimestamp(float(ts), tz=pytz.utc).astimezone(_NY_TZ)
        return dt.strftime("%Y-%m-%d %H:%M ET")
    except (ValueError, OSError, OverflowError):
        return "-"


def _fmt_ms_et(ms: Optional[int]) -> str:
    """毫秒时间戳 -> 'MM-DD HH:MM ET'。"""
    if ms is None:
        return "-"
    try:
        dt = datetime.fromtimestamp(float(ms) / 1000.0, tz=pytz.utc).astimezone(_NY_TZ)
        return dt.strftime("%m-%d %H:%M ET")
    except (ValueError, OSError, OverflowError, TypeError):
        return "-"


def _pct(target: Optional[float], entry: Optional[float]) -> str:
    """计算 (target - entry) / entry 的百分比,带正负号。"""
    if target is None or entry is None or entry == 0:
        return ""
    pct = (target - entry) / entry * 100.0
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def _poi_kind(signal: Any, meta: Dict[str, Any]) -> str:
    """取干净的 POI 类型 key(ict_ote / ict_ob / ...)。"""
    return meta.get("poi_kind") or _get(signal, "poi_source") or ""


def _format_htf_context(meta: Dict[str, Any]) -> list:
    """ICT HTF 背景:daily bias + premium/discount 分位 + 位移确认。"""
    lines: list = []
    bias = meta.get("daily_bias")
    if bias:
        lines.append(f"• Daily bias: {_BIAS_DISPLAY.get(bias, bias)}")
    zone_pct = meta.get("zone_pct")
    zone = meta.get("zone")
    if isinstance(zone_pct, (int, float)):
        zone_cn = {"premium": "premium 区", "discount": "discount 区",
                   "equilibrium": "均衡区"}.get(zone, zone or "")
        lines.append(f"• Dealing range 分位: {zone_pct*100:.0f}%({zone_cn})")
    # 位移确认(OB / raid / mss 才有意义)
    disp = meta.get("displacement_mult") or meta.get("displacement_body_mult")
    if isinstance(disp, (int, float)):
        lines.append(f"• 位移确认: ✅ body×{disp:.2f}")
    return lines


def _format_source_evidence(kind: str, meta: Dict[str, Any]) -> list:
    """按 POI 类型展示溯源(回图核对用)。"""
    lines: list = []
    if kind == "ict_ote":
        ls, le = meta.get("leg_start"), meta.get("leg_end")
        rp = meta.get("retracement_pct")
        if ls is not None and le is not None:
            rp_str = f"(回撤 {rp*100:.0f}%)" if isinstance(rp, (int, float)) else ""
            lines.append(f"• OTE leg: {_fmt_price(ls)} → {_fmt_price(le)} {rp_str}")
        if meta.get("leg_end_time"):
            lines.append(f"• leg 终点 K: {_fmt_ms_et(meta.get('leg_end_time'))}")
    elif kind == "ict_ob":
        if meta.get("ob_time"):
            lines.append(f"• OB 原 K: {_fmt_ms_et(meta.get('ob_time'))}")
        if meta.get("ob_zone"):
            lines.append(f"• OB 落区: {meta.get('ob_zone')}")
    elif kind == "ict_liquidity_raid":
        pool = meta.get("pool_type")
        if pool:
            pool_cn = {"SSL": "等低点(SSL)", "BSL": "等高点(BSL)"}.get(pool, pool)
            lines.append(f"• 扫荡池: {pool_cn}  触点×{meta.get('touches', '?')}")
        if meta.get("sweep_extreme") is not None:
            lines.append(f"• 扫荡极值: {_fmt_price(meta.get('sweep_extreme'))}")
        if meta.get("sweep_bar_time"):
            lines.append(f"• 扫荡 K: {_fmt_ms_et(meta.get('sweep_bar_time'))}")
    elif kind == "ict_mss_retest":
        if meta.get("break_price") is not None:
            lines.append(f"• MSS 突破价: {_fmt_price(meta.get('break_price'))}")
        if meta.get("break_time"):
            lines.append(f"• MSS 突破 K: {_fmt_ms_et(meta.get('break_time'))}")
    return lines


def format_signal(signal: Any) -> str:
    """把 ICT signal 对象格式化成 Markdown 消息。"""
    direction = (_get(signal, "direction") or "").lower()
    is_long = direction == "long"
    emoji = "🟢" if is_long else "🔴"
    direction_cn = "做多信号" if is_long else "做空信号"
    dir_cn_short = "做多" if is_long else "做空"

    symbol = _get(signal, "symbol", "?")
    meta = _get(signal, "ict_meta") or {}
    kind = _poi_kind(signal, meta)
    kind_label = POI_KIND_LABELS.get(kind, kind or "ICT setup")

    entry_price = _get(signal, "entry_price")
    stop_loss = _get(signal, "stop_loss")
    take_profit = _get(signal, "take_profit")
    risk_reward = _get(signal, "risk_reward")
    poi_low = _get(signal, "poi_low")
    poi_high = _get(signal, "poi_high")
    created_at = _get(signal, "created_at")

    entry_mode = (_get(signal, "entry_mode") or "limit").lower()
    is_limit = (entry_mode == "limit")

    # ---- 入场区块 ----
    trade_lines = ["📊 入场:"]
    if is_limit:
        trade_lines.append(f"• 📋 挂单{dir_cn_short} @ {_fmt_price(entry_price)}(等价格回测此位成交)")
    else:
        trade_lines.append(f"• 市价{dir_cn_short} @ {_fmt_price(entry_price)}(价格已在位)")
    if poi_low is not None and poi_high is not None:
        trade_lines.append(f"• POI 区间: {_fmt_price(poi_low)} - {_fmt_price(poi_high)}")
    if stop_loss is not None:
        sl_pct = _pct(stop_loss, entry_price)
        basis = meta.get("sl_basis")
        basis_str = f" — {basis}" if basis else ""
        trade_lines.append(f"• 止损价: {_fmt_price(stop_loss)}" + (f"  ({sl_pct})" if sl_pct else "") + basis_str)
    if take_profit is not None:
        tp_pct = _pct(take_profit, entry_price)
        trade_lines.append(f"• 目标价: {_fmt_price(take_profit)}" + (f"  ({tp_pct})" if tp_pct else "") + " — dealing range 对侧")
    if risk_reward is not None:
        try:
            trade_lines.append(f"• 盈亏比: 1:{float(risk_reward):.1f}")
        except (TypeError, ValueError):
            pass

    # ---- 仓位管理 ----
    pos_lines = ["🎯 仓位管理:"]
    if take_profit is not None and entry_price is not None and stop_loss is not None:
        pos_lines.append(f"• TP1(建议平 50%): {_fmt_price(take_profit)}")
        pos_lines.append("• TP2(剩余 50%): 达 TP1 后 peak ∓ 1.5×ATR_4h 移动止盈")
        if is_limit:
            pos_lines.append("• bot 会在 limit 成交 / TP1 / TP2 / SL 命中时发 TG 提醒")
            pos_lines.append("• ⏱️ Limit 24h 未成交 → 自动作废")
        else:
            pos_lines.append("• bot 会在 TP1 / TP2 / SL 命中时发 TG 提醒")
    else:
        pos_lines.append("• 止损 / 目标由你自行判断")

    htf = _format_htf_context(meta)
    evidence = _format_source_evidence(kind, meta)

    lines = [
        f"{emoji} {direction_cn} | {symbol}",
        f"ICT · {kind_label}",
        f"🕐 {_fmt_ts_et(created_at)}",
        "",
    ]
    if htf:
        lines += ["🧭 HTF 背景:", *htf, ""]
    lines += [*trade_lines, "", *pos_lines]
    if evidence:
        lines += ["", "🔍 溯源(回图核对):", *evidence]
    lines += ["", "⚠️ 仅供参考,人工确认后再入场"]
    return "\n".join(lines)


__all__ = ["format_signal"]
