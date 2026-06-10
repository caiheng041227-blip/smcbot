"""ICT POI 检测器集 — 4 个 ICT 经典入场类型。

每个检测器返回**统一格式**的 setup dict:
    {
        "poi_type": "ict_ob" | "ict_ote" | "ict_liquidity_raid" | "ict_mss_retest",
        "direction": "long" | "short",
        "poi_low": float, "poi_high": float,
        "entry": float, "sl": float, "tp": float, "rr": float,
        "metadata": {...},   # 调试信息(原 POI 时间、leg 端点等)
    }

所有检测器**内置 HTF gates**(daily bias + premium/discount),不通过的 setup 直接不返回。
TP 优先用 dealing_range 对侧极值(HTF target);备选 3R 投射。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from engine.market_structure import (
    classify_structure_atr,
    detect_choch,
    find_swing_points_atr,
    _h, _l, _close, _t,
)
from engine.poi import find_order_blocks, find_equal_lows_highs_pois, find_fvgs


# ----------------------------------------------------------------------------
# 共用工具
# ----------------------------------------------------------------------------

def _has_displacement_fvg(
    candles: Sequence[Any],
    direction: str,
    since_index: int = 0,
    min_height: float = 0.0,
) -> bool:
    """ICT displacement 证据:在 [since_index, 末尾] 区间内存在方向一致、未填补的 FVG。

    教科书 ICT 把"位移"(displacement)定义为一段强势的失衡推进 —— 它会在 3-candle
    结构里留下一个 FVG(fair value gap)。所以:
      - bullish 位移 → 留 bullish_fvg(支持 long setup)
      - bearish 位移 → 留 bearish_fvg(支持 short setup)
    没有这个 FVG = 那段腿只是普通摆动,不是机构位移,ICT setup 不成立。

    since_index:只认这一根(含)之后形成的 FVG —— 把位移绑定到当前 leg / 突破,
    避免拿很久以前的老 FVG 来"凑"位移。
    """
    want = "bullish_fvg" if direction == "long" else "bearish_fvg"
    n = len(candles)
    since = max(0, since_index)
    lookback = n - since + 3  # 覆盖 [since, n) 全段(+3 容 3-candle 结构边界)
    fvgs = find_fvgs(list(candles), lookback=lookback, min_height=min_height)
    return any(f["direction"] == want and f["index"] >= since for f in fvgs)


def _leg_displaced(
    candles: Sequence[Any],
    direction: str,
    start_index: int,
    end_index: int,
    min_height: float = 0.0,
) -> bool:
    """leg [start, end] 内是否发生过位移 —— **fill-agnostic** 版的 FVG 检测。

    与 _has_displacement_fvg 的区别:这里只问"当初有没有跳空(3-candle 缺口)",
    不管缺口之后是否被填。OTE 专用 —— OTE 的入场本身就是价格回撤填这个缺口,
    若要求缺口仍未填补,等于自我否决,会把绝大多数合法 OTE 误杀。

    bullish leg(long):向上跳空 k1.high < k3.low;bearish 对称。
    """
    lo = max(0, start_index)
    hi = min(end_index, len(candles) - 1)
    for i in range(lo, hi - 1):
        k1, k3 = candles[i], candles[i + 2]
        if direction == "long":
            if _h(k1) < _l(k3) and (_l(k3) - _h(k1)) >= min_height:
                return True
        else:
            if _l(k1) > _h(k3) and (_l(k1) - _h(k3)) >= min_height:
                return True
    return False


def _check_gates(direction: str, daily_bias: Optional[str],
                 zone: Optional[str]) -> bool:
    """HTF gates 检查。返回 True 表示通过(允许 setup)。"""
    if daily_bias == "bearish" and direction == "long":
        return False
    if daily_bias == "bullish" and direction == "short":
        return False
    if zone == "premium" and direction == "long":
        return False
    if zone == "discount" and direction == "short":
        return False
    return True


def _calc_tp_from_dealing_range(direction: str, entry: float, sl: float,
                                dealing_range: Optional[Dict[str, float]],
                                fallback_rr: float = 3.0) -> float:
    """TP 优先用 dealing_range 对侧极值;否则(或方向不对)用 fallback_rr × risk。

    方向校验:
      - long:dr["high"] 必须 > entry,否则 fallback(dr 已被打破或入场已超出 range)
      - short:dr["low"] 必须 < entry
    """
    risk = abs(entry - sl)
    if dealing_range:
        if direction == "long":
            dr_high = float(dealing_range["high"])
            if dr_high > entry:
                return dr_high
            # dr.high 已被价格突破,不能用 → fallback
        else:
            dr_low = float(dealing_range["low"])
            if dr_low < entry:
                return dr_low
    if direction == "long":
        return entry + fallback_rr * risk
    else:
        return entry - fallback_rr * risk


def _compute_zone(price: float, dr: Optional[Dict[str, float]],
                  premium_th: float, discount_th: float) -> Optional[str]:
    if not dr or dr["high"] <= dr["low"]:
        return None
    pct = (price - dr["low"]) / (dr["high"] - dr["low"])
    if pct >= premium_th:
        return "premium"
    if pct <= discount_th:
        return "discount"
    return "equilibrium"


# ----------------------------------------------------------------------------
# ① OB at Premium/Discount
# ----------------------------------------------------------------------------

def find_ict_ob_setups(
    candles_4h: Sequence[Any],
    atr_4h: float,
    current_price: float,
    daily_bias: Optional[str] = None,
    dealing_range: Optional[Dict[str, float]] = None,
    lookback: int = 30,
    sl_atr_mult: float = 0.3,
    min_rr: float = 1.0,
    premium_th: float = 0.55,
    discount_th: float = 0.45,
) -> List[Dict[str, Any]]:
    """ICT OB:复用 find_order_blocks,加 HTF gates,生成入场 setup。

    入场逻辑:bullish OB 在 discount → long entry @ OB high;
              bearish OB 在 premium → short entry @ OB low。
    SL = OB 反向极值 ± sl_atr_mult × ATR_4h。
    TP = dealing_range 对侧极值(若有),否则 3R 投射。
    """
    if not candles_4h or atr_4h <= 0:
        return []
    obs = find_order_blocks(list(candles_4h), lookback=lookback)
    setups: List[Dict[str, Any]] = []
    for ob in obs:
        ob_dir = "long" if ob["direction"] == "bullish_ob" else "short"
        # HTF bias gate
        if not _check_gates(ob_dir, daily_bias, None):
            continue
        # OB 在 zone 的位置(对 long 必须 discount,对 short 必须 premium)
        ob_mid = (ob["high"] + ob["low"]) / 2
        ob_zone = _compute_zone(ob_mid, dealing_range, premium_th, discount_th)
        if ob_dir == "long" and ob_zone == "premium":
            continue
        if ob_dir == "short" and ob_zone == "discount":
            continue
        # 入场:挂单等价格回到 OB 边界
        if ob_dir == "long":
            entry = float(ob["high"])
            sl = float(ob["low"]) - sl_atr_mult * atr_4h
            if current_price <= entry:
                # 价格已经在 OB 内或下方,放弃(等待新 setup)
                continue
        else:
            entry = float(ob["low"])
            sl = float(ob["high"]) + sl_atr_mult * atr_4h
            if current_price >= entry:
                continue
        tp = _calc_tp_from_dealing_range(ob_dir, entry, sl, dealing_range)
        risk = abs(entry - sl)
        reward = abs(entry - tp)
        if risk <= 0:
            continue
        rr = reward / risk
        if rr < min_rr:
            continue
        # Direction check on TP side
        if ob_dir == "long" and tp <= entry:
            continue
        if ob_dir == "short" and tp >= entry:
            continue
        setups.append({
            "poi_type": "ict_ob",
            "direction": ob_dir,
            "poi_low": float(ob["low"]),
            "poi_high": float(ob["high"]),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "metadata": {
                "ob_time": ob.get("time"),
                "ob_zone": ob_zone,
                "displacement_mult": ob.get("displacement_body_mult"),
            },
        })
    return setups


# ----------------------------------------------------------------------------
# ② OTE (Optimal Trade Entry) — 62-79% retracement
# ----------------------------------------------------------------------------

def find_ict_ote_setups(
    candles_4h: Sequence[Any],
    atr_4h: float,
    current_price: float,
    daily_bias: Optional[str] = None,
    dealing_range: Optional[Dict[str, float]] = None,
    swing_min_move_mult: float = 1.5,
    ote_low_pct: float = 0.62,
    ote_high_pct: float = 0.79,
    sl_atr_mult: float = 0.3,
    min_rr: float = 1.0,
    require_displacement: bool = True,
    displacement_min_height_atr: float = 0.25,
) -> List[Dict[str, Any]]:
    """OTE 检测:最近 4h 显著 leg 的 62-79% 回撤区间 + HTF aligned。

    Leg 方向判定:用 ATR pivot 找最近一对 swing H / swing L,index 大者为 leg 终点。
      - leg 是 bullish(low → high):OTE = retracement from high,做 long(若 daily bias 允许)
      - leg 是 bearish(high → low):OTE = retracement from low,做 short
    """
    if not candles_4h or atr_4h <= 0:
        return []
    highs, lows = find_swing_points_atr(list(candles_4h), atr_4h,
                                        min_move_mult=swing_min_move_mult)
    if not highs or not lows:
        return []
    last_h = highs[-1]
    last_l = lows[-1]
    # 哪一个 swing 更新 → leg 端点
    if last_h["index"] > last_l["index"]:
        # Bullish leg: low → high。回撤 = 从 high 往下
        leg_start = float(last_l["price"])
        leg_end = float(last_h["price"])
        leg_dir = "long"
        rng = leg_end - leg_start
        if rng <= 0:
            return []
        ote_zone_high = leg_end - ote_low_pct * rng
        ote_zone_low = leg_end - ote_high_pct * rng
    else:
        leg_start = float(last_h["price"])
        leg_end = float(last_l["price"])
        leg_dir = "short"
        rng = leg_start - leg_end
        if rng <= 0:
            return []
        ote_zone_low = leg_end + ote_low_pct * rng
        ote_zone_high = leg_end + ote_high_pct * rng

    # 当前价格必须在 OTE 区间内
    if not (ote_zone_low <= current_price <= ote_zone_high):
        return []
    # HTF gate
    if not _check_gates(leg_dir, daily_bias, None):
        return []
    # 教科书 ICT:OTE 只在"位移腿"上有效 —— leg 内必须发生过方向一致的跳空(位移)。
    # 用 fill-agnostic 检测:OTE 入场本身会填掉 leg 的 FVG,所以只看位移是否发生过。
    if require_displacement:
        leg_start_idx = min(int(last_h["index"]), int(last_l["index"]))
        leg_end_idx = max(int(last_h["index"]), int(last_l["index"]))
        if not _leg_displaced(
            candles_4h, leg_dir, leg_start_idx, leg_end_idx,
            min_height=displacement_min_height_atr * atr_4h,
        ):
            return []

    entry = current_price
    if leg_dir == "long":
        sl = leg_start - sl_atr_mult * atr_4h
        tp = _calc_tp_from_dealing_range(leg_dir, entry, sl, dealing_range)
        # 若 dealing_range 不提供,默认 TP = leg_end + 30% leg(extension)
        if dealing_range is None:
            tp = leg_end + 0.3 * rng
    else:
        sl = leg_start + sl_atr_mult * atr_4h
        tp = _calc_tp_from_dealing_range(leg_dir, entry, sl, dealing_range)
        if dealing_range is None:
            tp = leg_end - 0.3 * rng

    risk = abs(entry - sl)
    reward = abs(entry - tp)
    if risk <= 0:
        return []
    rr = reward / risk
    if rr < min_rr:
        return []
    if leg_dir == "long" and tp <= entry:
        return []
    if leg_dir == "short" and tp >= entry:
        return []

    leg_end_time = int(
        (last_h["time"] if last_h["index"] > last_l["index"] else last_l["time"]) or 0
    )
    return [{
        "poi_type": "ict_ote",
        "direction": leg_dir,
        "poi_low": ote_zone_low,
        "poi_high": ote_zone_high,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "metadata": {
            "leg_start": leg_start,
            "leg_end": leg_end,
            "leg_end_time": leg_end_time,
            "retracement_pct": ((leg_end - current_price) / rng) if leg_dir == "long"
                              else ((current_price - leg_end) / rng),
        },
    }]


# ----------------------------------------------------------------------------
# ③ Liquidity Raid + Reversal
# ----------------------------------------------------------------------------

def find_ict_liquidity_raid_setups(
    candles_1h: Sequence[Any],
    atr_1h: float,
    current_price: float,
    daily_bias: Optional[str] = None,
    dealing_range: Optional[Dict[str, float]] = None,
    equal_lookback: int = 50,
    equal_min_touches: int = 2,    # Phase B 放宽:2 触点(double bottom 也算)
    equal_tolerance_pct: float = 0.003,
    sweep_window_bars: int = 5,    # Phase B:最近 5 根 K 内任一根 sweep 都算
    sl_atr_mult: float = 0.3,
    min_rr: float = 1.0,
    premium_th: float = 0.55,
    discount_th: float = 0.45,
    require_confirmation: bool = True,
    confirm_min_height_atr: float = 0.25,
) -> List[Dict[str, Any]]:
    """Liquidity Raid Reversal:equal H/L 被扫破但 close 回到原区间(stop hunt 失败)。

    检测逻辑:
      1. 找 equal_lows 池(SSL) / equal_highs 池(BSL)
      2. 看最近一根 1h K 是否:
         - SSL: low < poi_low 但 close >= poi_low(下扫但收回)→ 做 long
         - BSL: high > poi_high 但 close <= poi_high(上扫但收回)→ 做 short
      3. HTF gates 过滤
      4. 教科书确认(require_confirmation):扫荡后必须有反向位移(MSS)证据 ——
         最近 sweep_window_bars 根内出现方向一致的 FVG。缺确认 = 纯猜反转,跳过。
      5. SL = 扫荡极值 ± sl_atr_mult × ATR_1h
      6. TP = dealing_range 对侧
    """
    if not candles_1h or atr_1h <= 0 or len(candles_1h) < 5:
        return []
    last = candles_1h[-1]
    last_low = float(_l(last))
    last_high = float(_h(last))
    last_close = float(_close(last))
    # Phase B:看最近 sweep_window_bars 根 K 的极值,放宽 sweep+reversal 窗口
    window = list(candles_1h[-sweep_window_bars:])
    window_min_low = min(float(_l(b)) for b in window)
    window_max_high = max(float(_h(b)) for b in window)

    setups: List[Dict[str, Any]] = []
    # Find SSL (equal lows)
    ssl_pools = find_equal_lows_highs_pois(
        list(candles_1h), direction="long",
        lookback=equal_lookback, tolerance_pct=equal_tolerance_pct,
        min_touches=equal_min_touches,
    )
    for pool in ssl_pools:
        poi_low = float(pool["low"])
        poi_high = float(pool["high"])
        # Phase B:最近 N 根任一根 sweep 过 + 当前 close 已回到 inside
        if window_min_low < poi_low and last_close >= poi_low:
            entry_dir = "long"
            if not _check_gates(entry_dir, daily_bias, None):
                continue
            zone = _compute_zone(last_close, dealing_range, premium_th, discount_th)
            if zone == "premium":
                continue
            # 教科书确认:扫荡 SSL 后必须有 bullish 位移(反转推进留下的 FVG)
            if require_confirmation and not _has_displacement_fvg(
                candles_1h, "long", since_index=len(candles_1h) - sweep_window_bars,
                min_height=confirm_min_height_atr * atr_1h,
            ):
                continue
            entry = last_close
            sl = window_min_low - sl_atr_mult * atr_1h
            tp = _calc_tp_from_dealing_range(entry_dir, entry, sl, dealing_range)
            risk = entry - sl
            reward = tp - entry
            if risk <= 0 or reward <= 0:
                continue
            rr = reward / risk
            if rr < min_rr:
                continue
            setups.append({
                "poi_type": "ict_liquidity_raid",
                "direction": entry_dir,
                "poi_low": poi_low,
                "poi_high": poi_high,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "rr": rr,
                "metadata": {
                    "pool_type": "SSL",
                    "sweep_extreme": window_min_low,
                    "sweep_bar_time": int(_t(last)),
                    "touches": pool.get("touches"),
                    # pool_origin_id:用 zone 中心价 × 1000 作 stable id,同 pool 只 emit 一次
                    "pool_origin_id": int(round(((poi_low + poi_high) / 2) * 1000)),
                },
            })

    # Find BSL (equal highs)
    bsl_pools = find_equal_lows_highs_pois(
        list(candles_1h), direction="short",
        lookback=equal_lookback, tolerance_pct=equal_tolerance_pct,
        min_touches=equal_min_touches,
    )
    for pool in bsl_pools:
        poi_low = float(pool["low"])
        poi_high = float(pool["high"])
        if window_max_high > poi_high and last_close <= poi_high:
            entry_dir = "short"
            if not _check_gates(entry_dir, daily_bias, None):
                continue
            zone = _compute_zone(last_close, dealing_range, premium_th, discount_th)
            if zone == "discount":
                continue
            # 教科书确认:扫荡 BSL 后必须有 bearish 位移(反转推进留下的 FVG)
            if require_confirmation and not _has_displacement_fvg(
                candles_1h, "short", since_index=len(candles_1h) - sweep_window_bars,
                min_height=confirm_min_height_atr * atr_1h,
            ):
                continue
            entry = last_close
            sl = window_max_high + sl_atr_mult * atr_1h
            tp = _calc_tp_from_dealing_range(entry_dir, entry, sl, dealing_range)
            risk = sl - entry
            reward = entry - tp
            if risk <= 0 or reward <= 0:
                continue
            rr = reward / risk
            if rr < min_rr:
                continue
            setups.append({
                "poi_type": "ict_liquidity_raid",
                "direction": entry_dir,
                "poi_low": poi_low,
                "poi_high": poi_high,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "rr": rr,
                "metadata": {
                    "pool_type": "BSL",
                    "sweep_extreme": window_max_high,
                    "sweep_bar_time": int(_t(last)),
                    "touches": pool.get("touches"),
                    "pool_origin_id": int(round(((poi_low + poi_high) / 2) * 1000)),
                },
            })
    return setups


# ----------------------------------------------------------------------------
# ④ MSS + Retest of Breakpoint
# ----------------------------------------------------------------------------

def find_ict_mss_retest_setups(
    candles_4h: Sequence[Any],
    atr_4h: float,
    current_price: float,
    daily_bias: Optional[str] = None,
    dealing_range: Optional[Dict[str, float]] = None,
    swing_lookback: int = 5,
    proximity_atr_mult: float = 0.5,
    sl_atr_mult: float = 1.0,
    min_rr: float = 1.0,
    require_displacement: bool = True,
    displacement_min_height_atr: float = 0.25,
) -> List[Dict[str, Any]]:
    """HTF MSS + LTF retest setup。

    步骤:
      1. detect_choch 在 4h 检测 CHoCH(close break)
      2. 取 break_price = 突破时的 close 价
      3. 当前价距 break_price 在 proximity_atr_mult × ATR_4h 内 → 视为 retest
      4. 入场方向 = MSS 方向(bullish MSS → long / bearish → short)
      5. HTF gate(daily bias)同方向
      6. SL = break_price 反向 sl_atr_mult × ATR_4h
      7. TP = dealing_range 对侧
    """
    if not candles_4h or atr_4h <= 0:
        return []
    mss = detect_choch(list(candles_4h), lookback=swing_lookback)
    if mss is None:
        return []
    mss_dir = mss["direction"]  # "bullish" / "bearish"
    break_price = float(mss["break_price"])
    setup_dir = "long" if mss_dir == "bullish" else "short"

    # HTF bias 必须一致
    if not _check_gates(setup_dir, daily_bias, None):
        return []
    # 教科书 ICT:区分 MSS 与普通 CHoCH —— 真 MSS 的突破必须是位移(留下 FVG)。
    # detect_choch 以最后一根 close 判定突破,所以位移 FVG 应在末尾几根内形成。
    if require_displacement and not _has_displacement_fvg(
        candles_4h, setup_dir, since_index=len(candles_4h) - 3,
        min_height=displacement_min_height_atr * atr_4h,
    ):
        return []
    # 检查 retest:当前价距 break_price 在 0.5×ATR 内
    if abs(current_price - break_price) > proximity_atr_mult * atr_4h:
        return []
    # 对 long:break_price 应该在 entry 下方(supply 翻 demand);对 short 反之
    if setup_dir == "long" and current_price < break_price - proximity_atr_mult * atr_4h:
        return []
    if setup_dir == "short" and current_price > break_price + proximity_atr_mult * atr_4h:
        return []

    # entry 应该是 break_price(MSS 突破点),limit 挂单等 price 回测到这里
    # 如果用 current_price(=突破当下 close),则信号会立即入场,根本没等"retest"
    entry = break_price
    if setup_dir == "long":
        sl = break_price - sl_atr_mult * atr_4h
    else:
        sl = break_price + sl_atr_mult * atr_4h
    tp = _calc_tp_from_dealing_range(setup_dir, entry, sl, dealing_range)

    risk = abs(entry - sl)
    reward = abs(entry - tp)
    if risk <= 0 or reward <= 0:
        return []
    rr = reward / risk
    if rr < min_rr:
        return []
    if setup_dir == "long" and tp <= entry:
        return []
    if setup_dir == "short" and tp >= entry:
        return []

    return [{
        "poi_type": "ict_mss_retest",
        "direction": setup_dir,
        "poi_low": break_price - 0.3 * atr_4h,
        "poi_high": break_price + 0.3 * atr_4h,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "metadata": {
            "mss_direction": mss_dir,
            "break_price": break_price,
            "break_time": mss.get("break_time"),
            "proximity_atr": abs(current_price - break_price) / atr_4h,
        },
    }]
