"""POI 识别单测：Order Block, FVG, failed-sweep reentry, double-bottom。"""
from __future__ import annotations

from engine.poi import (
    find_fvgs, find_order_blocks,
    find_failed_sweep_reentry_pois, find_double_bottom_pois,
)


def _mk(o: float, h: float, l: float, c: float, t: int = 0) -> dict:
    return {"open": o, "high": h, "low": l, "close": c, "volume": 1.0, "open_time": t}


def test_find_order_blocks_bullish_strict():
    # 严格 SMC：候选阴线 + displacement + BOS + unmitigated
    candles = [
        _mk(110, 111, 109, 109, 0),   # idx 0 阴, high=111
        _mk(109, 110, 108, 108, 1),   # idx 1 阴
        _mk(108, 109, 107, 107, 2),   # idx 2 阴, high=109
        _mk(107, 108, 104, 104, 3),   # idx 3 阴 —— OB 候选, body=3, high=108, low=104
        _mk(104, 115, 104, 114, 4),   # idx 4 位移阳: body=10 > 3*1.5, close 114 > 108 ✓
        _mk(114, 118, 113, 117, 5),   # idx 5 继续上行，不回到 OB
        _mk(117, 120, 116, 119, 6),   # idx 6 继续上行
    ]
    # BOS: prev_max(0..2)=max(111,110,109)=111, close 114/117/119 > 111 ✓
    # Unmitigated: idx 5 low=113, idx 6 low=116，均未触及 [104,108] ✓
    obs = find_order_blocks(candles, lookback=10)
    bull = [o for o in obs if o["direction"] == "bullish_ob"]
    assert bull, f"should find a bullish OB, got {obs}"
    ob = bull[0]
    assert ob["index"] == 3
    assert ob["high"] == 108 and ob["low"] == 104


def test_find_order_blocks_bearish_strict():
    candles = [
        _mk(100, 102, 99, 101, 0),
        _mk(101, 103, 100, 102, 1),
        _mk(102, 104, 101, 103, 2),
        _mk(103, 106, 103, 105, 3),   # idx 3 阳 —— OB 候选, body=2, high=106, low=103
        _mk(105, 105, 95, 96, 4),     # idx 4 位移阴: body=9, close 96 < 103 ✓
        _mk(96, 97, 92, 93, 5),
        _mk(93, 94, 89, 90, 6),
    ]
    # BOS: prev_min(0..2)=min(99,100,101)=99; 96/93/90 < 99 ✓
    # Unmitigated: idx 5 high=97 < 103, idx 6 high=94 < 103 ✓
    obs = find_order_blocks(candles, lookback=10)
    bear = [o for o in obs if o["direction"] == "bearish_ob"]
    assert bear, f"should find a bearish OB, got {obs}"
    ob = bear[0]
    assert ob["index"] == 3
    assert ob["high"] == 106 and ob["low"] == 103


def test_find_order_blocks_no_displacement():
    # 候选阴线之后的阳线 body 太小，位移验证失败
    candles = [
        _mk(110, 111, 109, 109, 0),
        _mk(109, 110, 108, 108, 1),
        _mk(108, 109, 107, 107, 2),
        _mk(107, 108, 104, 104, 3),   # OB 候选, body=3
        _mk(104, 109, 104, 108.5, 4), # 阳但 body=4.5 < 3*1.5=4.5 刚好不满足（严格小于）
        _mk(108, 112, 107, 111, 5),
        _mk(111, 114, 110, 113, 6),
    ]
    obs = find_order_blocks(candles, lookback=10, displacement_body_mult=1.5)
    assert not [o for o in obs if o["direction"] == "bullish_ob"], \
        f"no-displacement OB must be rejected, got {obs}"


def test_find_order_blocks_mitigated_rejected():
    # OB 之后价格回到 OB 区间 -> 应被过滤
    candles = [
        _mk(110, 111, 109, 109, 0),
        _mk(109, 110, 108, 108, 1),
        _mk(108, 109, 107, 107, 2),
        _mk(107, 108, 104, 104, 3),   # OB 候选
        _mk(104, 115, 104, 114, 4),   # 位移
        _mk(114, 115, 106, 107, 5),   # ❌ low=106 回到 OB 区间 [104,108]
        _mk(107, 110, 105, 109, 6),
    ]
    obs = find_order_blocks(candles, lookback=10, require_unmitigated=True)
    assert not [o for o in obs if o["direction"] == "bullish_ob"], \
        f"mitigated OB must be rejected, got {obs}"


def test_find_order_blocks_poc_reported_not_filtered():
    """POC 共振已从硬门闸降级为评分(C8),OB 照常返回,但 poc_in_ob 标志必须准确。"""
    candles = [
        _mk(110, 111, 109, 109, 0),
        _mk(109, 110, 108, 108, 1),
        _mk(108, 109, 107, 107, 2),
        _mk(107, 108, 104, 104, 3),   # OB 区间 [104, 108]
        _mk(104, 115, 104, 114, 4),
        _mk(114, 118, 113, 117, 5),
        _mk(117, 120, 116, 119, 6),
    ]
    # POC 在 OB 外:仍然返回 OB,但 poc_in_ob=False
    obs_no = find_order_blocks(candles, lookback=10, vp_result={"poc": 150.0})
    bull_no = [o for o in obs_no if o["direction"] == "bullish_ob"]
    assert bull_no, "POC 不再是硬门闸,应仍返回 OB"
    assert bull_no[0]["poc_in_ob"] is False
    # POC 在 OB 内:poc_in_ob=True
    obs_yes = find_order_blocks(candles, lookback=10, vp_result={"poc": 106.0})
    bull_yes = [o for o in obs_yes if o["direction"] == "bullish_ob"]
    assert bull_yes
    assert bull_yes[0]["poc_in_ob"] is True


def test_find_fvgs_bullish_gap():
    # K1.high=100, K2 跳高，K3.low=103 => bullish FVG (100, 103)
    candles = [
        _mk(98, 100, 97, 99, 0),      # K1
        _mk(101, 104, 101, 103, 1),   # K2
        _mk(104, 106, 103, 105, 2),   # K3 — low=103 > K1.high=100
        _mk(105, 107, 104, 106, 3),
    ]
    fvgs = find_fvgs(candles, lookback=10)
    bull = [f for f in fvgs if f["direction"] == "bullish_fvg"]
    assert bull, f"should find bullish FVG, got {fvgs}"
    f = bull[0]
    assert f["bottom"] == 100 and f["top"] == 103


def test_find_fvgs_bearish_gap_filled_excluded():
    # K1.low=102, K3.high=99 => bearish FVG (99, 102)
    # 然后 K4 收盘 100.5 进入缺口 => 应被过滤
    candles = [
        _mk(104, 105, 102, 103, 0),   # K1
        _mk(102, 103, 99, 100, 1),    # K2
        _mk(99, 99, 96, 97, 2),       # K3 — high=99 < K1.low=102
        _mk(97, 101, 97, 100.5, 3),   # 填补（收盘在区间内）
    ]
    fvgs = find_fvgs(candles, lookback=10)
    bear = [f for f in fvgs if f["direction"] == "bearish_fvg"]
    assert not bear, f"filled FVG should be excluded, got {fvgs}"


def test_find_fvgs_no_gap():
    candles = [
        _mk(100, 101, 99, 100.5, 0),
        _mk(100, 102, 99, 101, 1),
        _mk(101, 103, 100, 102, 2),
    ]
    fvgs = find_fvgs(candles, lookback=10)
    assert fvgs == []


def test_failed_sweep_reentry_long():
    # prior low @ bar 7 = 2000;deeper low @ bar 14 = 1980;reclaim above 2000 in bars 15-20
    candles = []
    for i in range(7):
        candles.append(_mk(2055, 2060, 2045, 2050, i))
    candles.append(_mk(2040, 2045, 2000, 2010, 7))
    for i in range(8, 14):
        candles.append(_mk(2015, 2030, 2005, 2025, i))
    candles.append(_mk(2020, 2025, 1980, 1995, 14))
    for i in range(15, 21):
        candles.append(_mk(2005, 2020, 1990, 2015, i))
    res = find_failed_sweep_reentry_pois(candles, direction="long", lookback=30)
    assert len(res) == 1
    p = res[0]
    assert p["source"] == "failed_sweep_reentry"
    assert p["direction"] == "bullish_failed_sweep"
    assert p["prior_level"] == 2000
    assert p["deeper_extreme"] == 1980


def test_failed_sweep_reentry_no_reclaim_no_signal():
    # deeper sweep 之后 close 没收回 prior level 上方 → 不应触发
    candles = []
    for i in range(7):
        candles.append(_mk(2055, 2060, 2045, 2050, i))
    candles.append(_mk(2040, 2045, 2000, 2010, 7))
    for i in range(8, 14):
        candles.append(_mk(2015, 2030, 2005, 2025, i))
    candles.append(_mk(2020, 2025, 1980, 1995, 14))
    # 之后 close 一直在 2000 以下
    for i in range(15, 21):
        candles.append(_mk(1995, 1998, 1985, 1990, i))
    res = find_failed_sweep_reentry_pois(candles, direction="long", lookback=30)
    assert res == []


def test_double_bottom_long():
    # low1 @ bar 6 = 2000;neckline pivot high @ bar 10 = 2070;low2 @ bar 14 = 2005
    # 当前 close 突破颈线
    candles = []
    for i in range(6):
        candles.append(_mk(2080, 2100, 2070, 2085, i))
    candles.append(_mk(2070, 2075, 2000, 2020, 6))
    for i in range(7, 10):
        candles.append(_mk(2030, 2055, 2020, 2045, i))
    candles.append(_mk(2045, 2070, 2030, 2055, 10))
    for i in range(11, 14):
        candles.append(_mk(2030, 2055, 2020, 2045, i))
    candles.append(_mk(2040, 2055, 2005, 2020, 14))
    for i in range(15, 21):
        candles.append(_mk(2030, 2080, 2020, 2075, i))
    res = find_double_bottom_pois(candles, direction="long", lookback=40)
    assert len(res) == 1
    p = res[0]
    assert p["source"] == "double_bottom"
    assert p["direction"] == "bullish_double_bottom"
    assert p["spacing_bars"] == 8
    assert 2065 < p["neckline"] < 2075


def test_double_bottom_neckline_not_broken_no_signal():
    # 与上一个相同,但当前 close 没突破颈线 → 不应触发
    candles = []
    for i in range(6):
        candles.append(_mk(2080, 2100, 2070, 2085, i))
    candles.append(_mk(2070, 2075, 2000, 2020, 6))
    for i in range(7, 10):
        candles.append(_mk(2030, 2055, 2020, 2045, i))
    candles.append(_mk(2045, 2070, 2030, 2055, 10))
    for i in range(11, 14):
        candles.append(_mk(2030, 2055, 2020, 2045, i))
    candles.append(_mk(2040, 2055, 2005, 2020, 14))
    # 当前 close 在 2055,颈线 2070 未破
    for i in range(15, 21):
        candles.append(_mk(2020, 2060, 2015, 2055, i))
    res = find_double_bottom_pois(candles, direction="long", lookback=40)
    assert res == []


def test_failed_fvg_reentry_long():
    # 构造: bar 0-2 形成 bullish FVG(K1.high=100, K3.low=110 → FVG [100,110])
    # bar 3-5 在 FVG 上方盘整
    # bar 6 close=95 破位(破位深度 5% > 0.3%)
    # bar 7 close=102 reclaim
    # bar 8-9 站稳在 FVG 下沿之上
    from engine.poi import find_failed_fvg_reentry_pois
    candles = [
        _mk(95, 100, 94, 100, 0),    # K1
        _mk(100, 105, 100, 105, 1),  # K2
        _mk(108, 115, 110, 113, 2),  # K3 (low=110 > K1.high=100 → bullish FVG [100,110])
        _mk(113, 116, 111, 115, 3),
        _mk(115, 118, 112, 114, 4),
        _mk(114, 115, 111, 112, 5),
        _mk(112, 112, 90, 95, 6),    # 破位 close=95 < FVG low=100
        _mk(95, 103, 94, 102, 7),    # reclaim close=102 > 100
        _mk(102, 108, 101, 107, 8),
        _mk(107, 112, 106, 110, 9),
    ]
    res = find_failed_fvg_reentry_pois(candles, direction="long", lookback=20)
    assert len(res) >= 1
    assert all(r["source"] == "failed_fvg_reentry" for r in res)
    assert all(r["direction"] == "bullish_failed_fvg" for r in res)
    with_100 = [r for r in res if r["fvg_boundary"] == 100]
    assert with_100, "应识别 K1.high=100 边界的 FVG"
    assert with_100[0]["deepest_wick"] == 90


def test_failed_fvg_reentry_no_break_no_signal():
    # bullish FVG 但从未被破位 → 不应触发(应该是正常 unmitigated FVG)
    from engine.poi import find_failed_fvg_reentry_pois
    candles = [
        _mk(95, 100, 94, 100, 0),
        _mk(100, 105, 100, 105, 1),
        _mk(108, 115, 110, 113, 2),
        _mk(113, 116, 111, 115, 3),
        _mk(115, 118, 112, 114, 4),
        _mk(114, 115, 111, 112, 5),
    ]
    res = find_failed_fvg_reentry_pois(candles, direction="long", lookback=20)
    assert res == []


def test_failed_fvg_reentry_no_reclaim_no_signal():
    # 破位后 close 一直在 FVG 下沿之下(没 reclaim)→ 不触发
    from engine.poi import find_failed_fvg_reentry_pois
    candles = [
        _mk(95, 100, 94, 100, 0),
        _mk(100, 105, 100, 105, 1),
        _mk(108, 115, 110, 113, 2),
        _mk(113, 116, 111, 115, 3),
        _mk(112, 112, 90, 95, 4),    # 破位
        _mk(95, 98, 85, 92, 5),      # 仍在 FVG 下方
        _mk(92, 96, 88, 94, 6),
    ]
    res = find_failed_fvg_reentry_pois(candles, direction="long", lookback=20)
    assert res == []
