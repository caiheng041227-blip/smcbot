"""POI（Point of Interest）识别：Order Block 与 Fair Value Gap。

OB 采用严格 SMC 定义：候选反向 K 线 + 位移 (displacement) + BOS + 未被回测 +（可选）VP 支撑。
FVG 采用宽松填补判定：后续 K 线收盘价进入缺口即视为消耗。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence

from engine.market_structure import _close, _h, _l, _t


def _open(c: Any) -> float:
    return c["open"] if isinstance(c, dict) else c.open


def _is_bull(c: Any) -> bool:
    return _close(c) > _open(c)


def _is_bear(c: Any) -> bool:
    return _close(c) < _open(c)


def _body(c: Any) -> float:
    return abs(_close(c) - _open(c))


def find_order_blocks(
    candles: Sequence[Any],
    lookback: int = 10,
    displacement_body_mult: float = 1.5,
    require_unmitigated: bool = True,
    vp_result: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """严格 SMC Order Block 识别。

    Bullish OB 必须同时满足：
      1. 候选：最近 lookback 根内一根阴线
      2. Displacement：紧随其后必须是阳线，body ≥ OB body × displacement_body_mult，
         且收盘价突破 OB 高点
      3. BOS：OB 之后有 K 线收盘价 > lookback 窗口内、OB 之前的最高高点
      4. Unmitigated（require_unmitigated=True 时）：除位移 K 线外，之后任何 K 线都不能触及 OB 区间
      5. VP 支撑（提供 vp_result 时）：POC 必须落在 OB 区间内

    Bearish OB 对称。

    返回最新一条 bullish_ob 和最新一条 bearish_ob（若都存在）。
    """
    n = len(candles)
    if n < 4:
        return []
    start = max(0, n - lookback)
    out: List[Dict[str, Any]] = []

    for direction in ("bullish", "bearish"):
        ob = _find_one_ob(
            candles, start, n, direction,
            displacement_body_mult, require_unmitigated, vp_result,
        )
        if ob is not None:
            out.append(ob)
    return out


def _find_one_ob(
    candles: Sequence[Any],
    start: int,
    n: int,
    direction: str,
    disp_mult: float,
    unmitigated: bool,
    vp: Optional[Any],
) -> Optional[Dict[str, Any]]:
    is_candidate = _is_bear if direction == "bullish" else _is_bull

    # 倒序扫描候选（最近优先）；位移 K 线要 i+1 存在，所以最多到 n-2
    for i in range(n - 2, start - 1, -1):
        ob_c = candles[i]
        if not is_candidate(ob_c):
            continue
        ob_high = _h(ob_c)
        ob_low = _l(ob_c)
        ob_body = _body(ob_c)

        # 1) Displacement: 紧跟下一根必须强势反向突破
        nxt = candles[i + 1]
        if direction == "bullish":
            if not _is_bull(nxt):
                continue
            if _close(nxt) <= ob_high:
                continue
        else:
            if not _is_bear(nxt):
                continue
            if _close(nxt) >= ob_low:
                continue
        if ob_body > 0 and _body(nxt) < ob_body * disp_mult:
            continue

        # 2) BOS: OB 之前窗口内的极值点，被 OB 之后 K 线的收盘价突破
        if direction == "bullish":
            prev_max = max((_h(candles[j]) for j in range(start, i)), default=None)
            if prev_max is not None:
                broken = any(_close(candles[j]) > prev_max for j in range(i + 1, n))
                if not broken:
                    continue
        else:
            prev_min = min((_l(candles[j]) for j in range(start, i)), default=None)
            if prev_min is not None:
                broken = any(_close(candles[j]) < prev_min for j in range(i + 1, n))
                if not broken:
                    continue

        # 3) Unmitigated: 除位移 K 线（i+1）外，之后的 K 线都不能进入 OB 区间
        if unmitigated:
            touched = False
            for j in range(i + 2, n):
                cj = candles[j]
                if _l(cj) <= ob_high and _h(cj) >= ob_low:
                    touched = True
                    break
            if touched:
                continue

        # 4) Volume Profile POC 共振:不再作为硬门闸(降为评分 C8),这里只记录事实
        poc = None
        if vp is not None:
            poc = getattr(vp, "poc", None)
            if poc is None and isinstance(vp, dict):
                poc = vp.get("poc")
        poc_in_ob = bool(poc is not None and ob_low <= poc <= ob_high)

        # 溯源证据:记录位移 K 线时间 + body 倍数,便于复盘核对
        disp_body = _body(nxt)
        disp_mult_actual = (disp_body / ob_body) if ob_body > 0 else None
        return {
            "direction": f"{direction}_ob",
            "high": ob_high,
            "low": ob_low,
            "time": _t(ob_c),
            "index": i,
            "displacement_time": _t(nxt),
            "displacement_body_mult": disp_mult_actual,
            "vp_poc": poc,
            "poc_in_ob": poc_in_ob,
        }
    return None


def find_fvgs(
    candles: Sequence[Any],
    lookback: int = 10,
    min_height: float = 0.0,
    timeframe_tag: str = "",
) -> List[Dict[str, Any]]:
    """扫描最近 lookback 根内的 3-candle FVG,返回未被后续 K 线填补的。

    Bullish FVG:K1.high < K3.low(向上跳空),区间 = (K1.high, K3.low)
    Bearish FVG:K1.low  > K3.high(向下跳空),区间 = (K3.high, K1.low)
    填补判定:之后任一 K 线收盘价落入缺口区间。
    噪声过滤:区间高度 < min_height 的直接丢弃(调用方传 0.3×ATR)。
    """
    n = len(candles)
    if n < 3:
        return []
    start = max(0, n - lookback)
    out: List[Dict[str, Any]] = []

    for i in range(start, n - 2):
        k1, k2, k3 = candles[i], candles[i + 1], candles[i + 2]
        if _h(k1) < _l(k3):
            bottom, top = _h(k1), _l(k3)
            if (top - bottom) < min_height:
                continue
            filled = any(
                bottom < _close(candles[j]) < top
                for j in range(i + 3, n)
            )
            if not filled:
                out.append({
                    "direction": "bullish_fvg",
                    "top": top,
                    "bottom": bottom,
                    "high": top,
                    "low": bottom,
                    "time": _t(k2),
                    "index": i + 1,
                    "source": f"{timeframe_tag}_fvg" if timeframe_tag else "fvg",
                    "bar_index_from_end": n - (i + 1),
                })
        elif _l(k1) > _h(k3):
            bottom, top = _h(k3), _l(k1)
            if (top - bottom) < min_height:
                continue
            filled = any(
                bottom < _close(candles[j]) < top
                for j in range(i + 3, n)
            )
            if not filled:
                out.append({
                    "direction": "bearish_fvg",
                    "top": top,
                    "bottom": bottom,
                    "high": top,
                    "low": bottom,
                    "time": _t(k2),
                    "index": i + 1,
                    "source": f"{timeframe_tag}_fvg" if timeframe_tag else "fvg",
                    "bar_index_from_end": n - (i + 1),
                })
    return out


def find_breaker_blocks(
    candles: Sequence[Any],
    lookback: int = 40,
    timeframe_tag: str = "",
) -> List[Dict[str, Any]]:
    """Breaker Block:被 close 击穿的 OB 翻转成反方向 POI。

    Bullish OB(阴线 + 位移阳线 + close > OB.high)之后若某根 K 线 close < OB.low,
    则该 OB 翻转为 **bearish_breaker** —— 价格回到该区间成为做空 POI。
    Bearish OB 对称,翻转为 bullish_breaker。

    额外要求:
    - 破位之后到当前为止,OB 区间最多被触及一次(否则已经被"消化"过)
    - 只返回最近 lookback 根内的候选,防止扫历史老 OB
    """
    n = len(candles)
    if n < 4:
        return []
    start = max(0, n - lookback)
    out: List[Dict[str, Any]] = []

    for i in range(start, n - 2):
        c_i = candles[i]
        o_i = c_i["open"] if isinstance(c_i, dict) else c_i.open
        h_i = _h(c_i); l_i = _l(c_i); close_i = _close(c_i)
        body_i = abs(close_i - o_i)
        if body_i == 0:
            continue

        # 候选 bullish OB(阴线)→ 可能成为 bearish_breaker
        if _is_bear(c_i):
            nxt = candles[i + 1]
            if not _is_bull(nxt) or _close(nxt) <= h_i:
                continue
            # 找到之后某根 close 击穿 OB.low 的 K 线
            break_idx = None
            for j in range(i + 2, n):
                if _close(candles[j]) < l_i:
                    break_idx = j
                    break
            if break_idx is None:
                continue
            # 破位之后允许最多 2 次 wick 触及(首次弱触 + 二次入场是常见形态),第 3 次作废
            touches = 0
            for j in range(break_idx + 1, n):
                cj = candles[j]
                if _l(cj) <= h_i and _h(cj) >= l_i:
                    touches += 1
                    if touches > 2:
                        break
            if touches > 2:
                continue
            out.append({
                "direction": "bearish_breaker",
                "high": h_i,
                "low": l_i,
                "time": _t(c_i),
                "break_time": _t(candles[break_idx]),
                "source": f"{timeframe_tag}_breaker" if timeframe_tag else "breaker",
            })
        elif _is_bull(c_i):
            # 候选 bearish OB(阳线)→ 可能成为 bullish_breaker
            nxt = candles[i + 1]
            if not _is_bear(nxt) or _close(nxt) >= l_i:
                continue
            break_idx = None
            for j in range(i + 2, n):
                if _close(candles[j]) > h_i:
                    break_idx = j
                    break
            if break_idx is None:
                continue
            touches = 0
            for j in range(break_idx + 1, n):
                cj = candles[j]
                if _l(cj) <= h_i and _h(cj) >= l_i:
                    touches += 1
                    if touches > 2:
                        break
            if touches > 2:
                continue
            out.append({
                "direction": "bullish_breaker",
                "high": h_i,
                "low": l_i,
                "time": _t(c_i),
                "break_time": _t(candles[break_idx]),
                "source": f"{timeframe_tag}_breaker" if timeframe_tag else "breaker",
            })
    return out


def find_equal_lows_highs_pois(
    candles: Sequence[Any],
    direction: str,                    # "long"(找 equal lows)/ "short"(找 equal highs)
    lookback: int = 50,
    tolerance_pct: float = 0.003,      # 低/高点容差:0.3%
    min_touches: int = 3,              # 至少 3 根 K 线低/高点聚在一起
    poi_width_pct: float = 0.003,      # POI 区间宽度(围绕 cluster 极值各展 ±0.3%)
    timeframe_tag: str = "",
) -> List[Dict[str, Any]]:
    """Equal Lows / Equal Highs 流动性池作为 POI 候选。

    逻辑(long / bullish equal lows):
      1. 最近 `lookback` 根 K 线里,找 ≥ `min_touches` 根 **low** 互相在 `tolerance_pct` 内的 cluster
      2. cluster 最低价 X = 流动性聚集点(下方散户 SL 堆积)
      3. POI 区间 = [X - poi_width_pct × X, X + poi_width_pct × X]
      4. **unmitigated**:cluster 之后没有任何 K 线 low 跌破 poi_low(如果破了就说明流动性已被扫)
      5. 触发期望:价格后续再次进入 POI 区间即 Step3 扫荡,CHoCH 确认入场

    short / bearish equal highs 对称处理。

    返回 POI dict 结构与 find_order_blocks / find_fvgs 同构:
      {direction, low, high, time, source, touches, timeframe_tag}
    """
    n = len(candles)
    if n < min_touches:
        return []
    start = max(0, n - lookback)

    # 先按价格排序(long 升序找最低,short 降序找最高),从极值向外贪心扩簇
    # (原按时间顺序取 anchor 的做法会漏掉 anchor 附近外的点,e.g. anchor=2189 漏掉 2183)
    pool = []
    for i in range(start, n):
        val = _l(candles[i]) if direction == "long" else _h(candles[i])
        if val > 0:
            pool.append((val, i))
    pool.sort(key=lambda x: x[0], reverse=(direction == "short"))

    # "cluster 极值必须靠近 lookback 绝对最低/最高" —— 真正的支撑/阻力就在波段极值附近
    # bullish:cluster_min 必须 ≤ lookback 最低值 × 1.01(1% 容差)
    # bearish:cluster_max 必须 ≥ lookback 最高值 × 0.99
    # 比"bottom 33% 分位"更严格,彻底过滤"趋势中段 consolidation 误判为支撑"
    all_vals = [v for v, _ in pool]
    if not all_vals:
        return []
    lookback_min = min(all_vals)
    lookback_max = max(all_vals)
    EXTREME_TOLERANCE = 0.01  # 1%

    MIN_TIME_SPAN_BARS = 10  # cluster 前后必须跨 ≥ 10 根 1h(~10h),否则是 consolidation 不是 re-test

    out: List[Dict[str, Any]] = []
    used: set = set()

    for base_val, base_idx in pool:
        if base_idx in used:
            continue
        # 阈值:从 base 出发 2×tolerance_pct 范围内都算一簇
        if direction == "long":
            threshold = base_val * (1 + tolerance_pct * 2)
            matches = [(v, idx) for v, idx in pool if idx not in used and v <= threshold]
        else:
            threshold = base_val * (1 - tolerance_pct * 2)
            matches = [(v, idx) for v, idx in pool if idx not in used and v >= threshold]
        if len(matches) < min_touches:
            continue

        cluster_idxs = [idx for _, idx in matches]
        cluster_vals = [v for v, _ in matches]
        cluster_extreme = min(cluster_vals) if direction == "long" else max(cluster_vals)

        # 质量过滤 1:cluster 极值必须靠近 lookback 绝对最低/最高(1% 容差)
        # 真正的支撑/阻力应该在波段极值;"中段 consolidation"虽然 lows 接近但不在波段底
        if direction == "long" and cluster_extreme > lookback_min * (1 + EXTREME_TOLERANCE):
            used.update(cluster_idxs)
            continue
        if direction == "short" and cluster_extreme < lookback_max * (1 - EXTREME_TOLERANCE):
            used.update(cluster_idxs)
            continue

        # 质量过滤 2:cluster touch 必须跨足够时间,否则是"急跌后挤在一起"而非"多次 re-test 支撑"
        time_span = max(cluster_idxs) - min(cluster_idxs)
        if time_span < MIN_TIME_SPAN_BARS:
            used.update(cluster_idxs)
            continue

        half_w = cluster_extreme * poi_width_pct
        poi_low = cluster_extreme - half_w
        poi_high = cluster_extreme + half_w

        # unmitigated:cluster 最后一个 touch 之后,不应有 K 线跌破(long)/ 突破(short)cluster 极值
        # 跌破 poi_low 意味着流动性已被吃掉,POI 失效
        last_idx = max(cluster_idxs)
        mitigated = False
        for k in range(last_idx + 1, n):
            if direction == "long" and _l(candles[k]) < poi_low:
                mitigated = True
                break
            if direction == "short" and _h(candles[k]) > poi_high:
                mitigated = True
                break
        if mitigated:
            used.update(cluster_idxs)
            continue

        poi_time = max(_t(candles[k]) for k in cluster_idxs)
        tag = "equal_lows" if direction == "long" else "equal_highs"
        source = f"{timeframe_tag}_{tag}" if timeframe_tag else tag
        out.append({
            "direction": f"bullish_{tag}" if direction == "long" else f"bearish_{tag}",
            "low": poi_low,
            "high": poi_high,
            "time": poi_time,
            "source": source,
            "touches": len(cluster_idxs),
            "cluster_extreme": cluster_extreme,
            "timeframe_tag": timeframe_tag,
        })
        used.update(cluster_idxs)

    return out


def find_failed_fvg_reentry_pois(
    candles: Sequence[Any],
    direction: str,
    lookback: int = 20,
    min_break_depth_pct: float = 0.003,
    reclaim_window: int = 3,
    max_age_bars: int = 6,
    min_fvg_height_pct: float = 0.003,
    poi_buffer_pct: float = 0.002,
    timeframe_tag: str = "4h",
) -> List[Dict[str, Any]]:
    """Failed-FVG Reentry POI:FVG 第一次 retest 被吃穿 → 深扫 → 收盘 reclaim 的二次入场点。

    long 逻辑(bullish FVG failed-then-reclaimed):
      1. 最近 `lookback` 根中找所有 bullish FVG(K1.high < K3.low)
      2. FVG 高度 ≥ `min_fvg_height_pct`(过滤噪声)
      3. FVG 形成后,在 `max_age_bars + reclaim_window` 根内,存在一根 **close < FVG 下沿** 的
         "破位 K"(mitigated),且破位深度 ≥ `min_break_depth_pct`(确保是真破而非擦边)
      4. 破位后 `reclaim_window` 根内,有一根 **close > FVG 下沿**(reclaim)
      5. reclaim 发生在最近 `max_age_bars` 根内(信号够新鲜)
      6. reclaim 之后**没有**再次 close 低于 FVG 下沿(拒绝有效)
      7. POI 区间 = [破位深处的最低 wick, FVG 下沿 × (1 + buffer)]
      8. origin_time = reclaim K 线的 time(POI registry key 稳定)

    short 对称:找 bearish FVG → close 破 FVG 上沿 → reclaim 回到 FVG 上沿下方。
    """
    n = len(candles)
    if n < 4:
        return []
    start = max(0, n - lookback)
    out: List[Dict[str, Any]] = []
    latest_idx = n - 1

    for i in range(start, n - 2):
        k1, k3 = candles[i], candles[i + 2]
        if direction == "long":
            if _h(k1) >= _l(k3):
                continue
            fvg_low, fvg_high = _h(k1), _l(k3)
        else:
            if _l(k1) <= _h(k3):
                continue
            fvg_low, fvg_high = _h(k3), _l(k1)
        if (fvg_high - fvg_low) < fvg_low * min_fvg_height_pct:
            continue

        # 扫描 FVG 之后的 K,找"破位 + reclaim"
        break_idx = None
        break_depth = 0.0
        deepest_wick = None
        for j in range(i + 3, n):
            cj = candles[j]
            cl = _close(cj)
            if direction == "long":
                if cl < fvg_low:
                    depth = (fvg_low - cl) / fvg_low
                    if depth >= min_break_depth_pct:
                        if break_idx is None:
                            break_idx = j
                            deepest_wick = _l(cj)
                            break_depth = depth
                        else:
                            # 更新最深 wick
                            if _l(cj) < (deepest_wick if deepest_wick is not None else fvg_low):
                                deepest_wick = _l(cj)
            else:
                if cl > fvg_high:
                    depth = (cl - fvg_high) / fvg_high
                    if depth >= min_break_depth_pct:
                        if break_idx is None:
                            break_idx = j
                            deepest_wick = _h(cj)
                            break_depth = depth
                        else:
                            if _h(cj) > (deepest_wick if deepest_wick is not None else fvg_high):
                                deepest_wick = _h(cj)
        if break_idx is None:
            continue

        # 从 break_idx 起最多 reclaim_window 根内找 reclaim close
        reclaim_idx = None
        for j in range(break_idx + 1, min(n, break_idx + 1 + reclaim_window)):
            cj = candles[j]
            cl = _close(cj)
            if direction == "long" and cl > fvg_low:
                reclaim_idx = j
                break
            if direction == "short" and cl < fvg_high:
                reclaim_idx = j
                break
        if reclaim_idx is None:
            continue

        # reclaim 必须足够新鲜
        if (latest_idx - reclaim_idx) > max_age_bars:
            continue

        # reclaim 后不能再次 close 破位(确保 reclaim 仍有效)
        re_broken = False
        for j in range(reclaim_idx + 1, n):
            cj = candles[j]
            cl = _close(cj)
            if direction == "long" and cl < fvg_low:
                re_broken = True
                break
            if direction == "short" and cl > fvg_high:
                re_broken = True
                break
        if re_broken:
            continue

        # POI 区间
        if direction == "long":
            poi_low = deepest_wick if deepest_wick is not None else fvg_low
            poi_high = fvg_low * (1 + poi_buffer_pct)
            tag = "bullish_failed_fvg"
        else:
            poi_low = fvg_high * (1 - poi_buffer_pct)
            poi_high = deepest_wick if deepest_wick is not None else fvg_high
            tag = "bearish_failed_fvg"

        out.append({
            "direction": tag,
            "low": poi_low,
            "high": poi_high,
            "time": _t(candles[reclaim_idx]),
            "source": "failed_fvg_reentry",
            "fvg_boundary": fvg_low if direction == "long" else fvg_high,
            "break_depth_pct": break_depth,
            "deepest_wick": deepest_wick,
            "timeframe_tag": timeframe_tag,
        })

    return out


def find_failed_sweep_reentry_pois(
    candles: Sequence[Any],
    direction: str,
    lookback: int = 48,
    min_depth_pct: float = 0.003,
    reclaim_window: int = 4,
    max_age_bars: int = 24,
    min_spacing_bars: int = 3,
    poi_buffer_pct: float = 0.002,
    timeframe_tag: str = "1h",
) -> List[Dict[str, Any]]:
    """Failed-sweep re-entry POI(中期方案):捕捉"第一次 sweep 失败、第二次更深 sweep 被拒绝"的经典反转。

    long 逻辑:
      1. 最近 `lookback` 根中找两个 swing low(左右各 3 根确认):
         - prior(较早,较浅)
         - deeper(较晚,price < prior * (1 - min_depth_pct),即真破了 prior)
         - 间隔 ≥ `min_spacing_bars` 根 (避免同一根 K 线两次识别)
      2. deeper 出现在最近 `max_age_bars` 根内(避免太陈旧)
      3. deeper 之后最近 `reclaim_window` 根里,至少一根 **close** 收回 prior 上方
         (即价格拒绝了第二次破位)
      4. 此后没有第三次更低的 low 出现(deeper 仍是有效谷底)
      5. POI 区间 = [deeper.price, prior.price + buffer] —— 二次探底后的拒绝区
      6. origin_time = deeper.time(POI registry key 稳定)

    short 对称处理。
    """
    n = len(candles)
    if n < 2 * 3 + 1:
        return []

    # 用 lookback=3 识别短期 swing(比主结构 swing_lookback=5 更敏感,才能看到二次回调)
    from engine.market_structure import find_swing_points
    win_start = max(0, n - lookback)
    window = candles[win_start:]
    highs, lows = find_swing_points(window, lookback=3)
    if direction == "long":
        pts = lows
    else:
        pts = highs
    if len(pts) < 2:
        return []

    out: List[Dict[str, Any]] = []
    # 所有点索引是**相对 window**的,换算回全局 idx
    base_idx = win_start
    for i in range(len(pts) - 1):
        prior = pts[i]
        for j in range(i + 1, len(pts)):
            deeper = pts[j]
            if deeper["index"] - prior["index"] < min_spacing_bars:
                continue
            # 方向性:long 要 deeper < prior;short 要 deeper > prior
            if direction == "long":
                if deeper["price"] >= prior["price"] * (1 - min_depth_pct):
                    continue
            else:
                if deeper["price"] <= prior["price"] * (1 + min_depth_pct):
                    continue
            global_deeper_idx = base_idx + deeper["index"]
            # deeper 必须在最近 max_age_bars 根内
            if (n - 1) - global_deeper_idx > max_age_bars:
                continue
            # 拒绝窗:deeper 之后到现在,至少一根 close 越回 prior 方向
            reclaim_start = global_deeper_idx + 1
            reclaim_end = min(n, reclaim_start + reclaim_window)
            reclaimed = False
            for k in range(reclaim_start, reclaim_end):
                cl = _close(candles[k])
                if direction == "long" and cl > prior["price"]:
                    reclaimed = True
                    break
                if direction == "short" and cl < prior["price"]:
                    reclaimed = True
                    break
            if not reclaimed:
                continue
            # 其后不得有更极端的 low / high(deeper 必须仍有效)
            invalid = False
            for k in range(global_deeper_idx + 1, n):
                if direction == "long" and _l(candles[k]) < deeper["price"]:
                    invalid = True
                    break
                if direction == "short" and _h(candles[k]) > deeper["price"]:
                    invalid = True
                    break
            if invalid:
                continue

            buf = prior["price"] * poi_buffer_pct
            if direction == "long":
                poi_low = deeper["price"]
                poi_high = prior["price"] + buf
            else:
                poi_low = prior["price"] - buf
                poi_high = deeper["price"]
            tag = "bullish_failed_sweep" if direction == "long" else "bearish_failed_sweep"
            out.append({
                "direction": tag,
                "low": poi_low,
                "high": poi_high,
                "time": deeper["time"],
                "source": "failed_sweep_reentry",
                "prior_level": prior["price"],
                "deeper_extreme": deeper["price"],
                "reclaim_bars": reclaim_end - reclaim_start,
                "timeframe_tag": timeframe_tag,
            })
            break  # 每个 prior 只配最近的一个 deeper 即可,避免组合爆炸

    return out


def find_double_bottom_pois(
    candles: Sequence[Any],
    direction: str,
    lookback: int = 40,
    tol_pct: float = 0.005,
    min_spacing_bars: int = 8,
    max_spacing_bars: int = 30,
    neckline_buffer_pct: float = 0.001,
    poi_buffer_pct: float = 0.002,
    timeframe_tag: str = "1h",
) -> List[Dict[str, Any]]:
    """Double-Bottom / Double-Top POI(长期方案):等高双底 + 颈线突破。

    long (double bottom) 逻辑:
      1. 最近 `lookback` 根中找 swing lows(lookback=3)
      2. 两两配对,早的叫 low1,晚的叫 low2,要求:
         - |low1.price - low2.price| / avg(prices) < `tol_pct`(几乎等高)
         - `min_spacing_bars` ≤ (low2.idx - low1.idx) ≤ `max_spacing_bars`
      3. 两低之间必须有至少一个 swing high(颈线)
      4. **颈线已突破**:当前 close > 颈线 × (1 + neckline_buffer_pct)
      5. 两底都在最近 `lookback` 根内,low2 在最近 12 根内(足够新鲜)
      6. POI 区间 = [min(low1,low2), max(low1,low2) × (1 + poi_buffer_pct)]
      7. origin_time = low2.time

    short (double top) 对称处理。
    """
    n = len(candles)
    if n < 2 * 3 + 1:
        return []

    from engine.market_structure import find_swing_points
    win_start = max(0, n - lookback)
    window = candles[win_start:]
    highs, lows = find_swing_points(window, lookback=3)
    if direction == "long":
        pts = lows
        neck_pts = highs
    else:
        pts = highs
        neck_pts = lows
    if len(pts) < 2 or len(neck_pts) < 1:
        return []

    base_idx = win_start
    latest_close = _close(candles[-1])
    out: List[Dict[str, Any]] = []
    seen_pairs: set = set()
    # 从最近的 pts 向前配,一旦找到合格 pair 就 break(每方向至多 1 个 pattern)
    for j in range(len(pts) - 1, -1, -1):
        low2 = pts[j]
        g2 = base_idx + low2["index"]
        if (n - 1) - g2 > 12:
            continue  # low2 太陈旧,不再找
        for i in range(j - 1, -1, -1):
            low1 = pts[i]
            spacing = low2["index"] - low1["index"]
            if spacing < min_spacing_bars:
                continue
            if spacing > max_spacing_bars:
                break
            avg = (low1["price"] + low2["price"]) / 2
            if avg <= 0:
                continue
            if abs(low1["price"] - low2["price"]) / avg > tol_pct:
                continue
            # 颈线:两低之间的最高 swing high(long)/最低 swing low(short)
            between = [p for p in neck_pts if low1["index"] < p["index"] < low2["index"]]
            if not between:
                continue
            if direction == "long":
                neckline = max(p["price"] for p in between)
                if latest_close <= neckline * (1 + neckline_buffer_pct):
                    continue
            else:
                neckline = min(p["price"] for p in between)
                if latest_close >= neckline * (1 - neckline_buffer_pct):
                    continue
            key = (low1["index"], low2["index"])
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            if direction == "long":
                poi_low = min(low1["price"], low2["price"])
                poi_high = max(low1["price"], low2["price"]) * (1 + poi_buffer_pct)
                tag = "bullish_double_bottom"
            else:
                poi_low = min(low1["price"], low2["price"]) * (1 - poi_buffer_pct)
                poi_high = max(low1["price"], low2["price"])
                tag = "bearish_double_top"
            out.append({
                "direction": tag,
                "low": poi_low,
                "high": poi_high,
                "time": low2["time"],
                "source": "double_bottom" if direction == "long" else "double_top",
                "neckline": neckline,
                "spacing_bars": spacing,
                "timeframe_tag": timeframe_tag,
            })
            # 每方向最多 1 个 pattern,找到即返回
            return out
    return out


__all__ = [
    "find_order_blocks", "find_fvgs", "find_breaker_blocks",
    "find_equal_lows_highs_pois",
    "find_failed_sweep_reentry_pois",
    "find_failed_fvg_reentry_pois",
    "find_double_bottom_pois",
]
