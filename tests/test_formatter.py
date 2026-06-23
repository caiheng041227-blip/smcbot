"""notify/formatter.py 单测(ICT 原生格式,2026-06-10 随 SMC 删除重写)。

只测纯函数输出,不触发任何网络调用。
"""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytz

from notify.formatter import format_signal


def _make_ote_long() -> SimpleNamespace:
    # 2026-04-19 17:32 ET -> UTC 毫秒
    ny = pytz.timezone("America/New_York")
    created_ms = int(ny.localize(datetime(2026, 4, 19, 17, 32, 0)).timestamp() * 1000)
    return SimpleNamespace(
        signal_id="sig_001",
        symbol="ETHUSDT",
        direction="long",
        poi_source="ict_ote",
        poi_type="bullish_ote",
        poi_high=2263.5,
        poi_low=2260.0,
        entry_price=2261.00,
        stop_loss=2255.00,
        take_profit=2300.00,
        risk_reward=6.5,
        entry_mode="immediate",
        created_at=created_ms,
        ict_meta={
            "poi_kind": "ict_ote",
            "daily_bias": "bullish",
            "zone": "discount",
            "zone_pct": 0.32,
            "sl_basis": "leg 起点(0% 回撤)±0.3×ATR",
            "leg_start": 2204.0,
            "leg_end": 2300.0,
            "retracement_pct": 0.41,
            "leg_end_time": created_ms - 6 * 3600 * 1000,
        },
    )


def _make_raid_short() -> SimpleNamespace:
    return SimpleNamespace(
        signal_id="sig_002",
        symbol="ETHUSDT",
        direction="short",
        poi_source="ict_liquidity_raid",
        poi_type="bearish_liquidity_raid",
        poi_high=2310.0,
        poi_low=2300.0,
        entry_price=2305.00,
        stop_loss=2325.00,
        take_profit=2233.00,
        risk_reward=3.6,
        entry_mode="immediate",
        created_at=int(datetime(2026, 5, 14, 20, 0, tzinfo=pytz.utc).timestamp()),
        ict_meta={
            "poi_kind": "ict_liquidity_raid",
            "daily_bias": "bearish",
            "zone": "premium",
            "zone_pct": 0.58,
            "sl_basis": "扫荡极值 ±0.3×ATR",
            "pool_type": "BSL",
            "touches": 12,
            "sweep_extreme": 2315.88,
        },
    )


def test_ote_long_ict_native():
    msg = format_signal(_make_ote_long())
    # 方向 + 标题
    assert "🟢" in msg
    assert "做多信号" in msg
    assert "ETHUSDT" in msg
    # ICT 类型标签,不再有评分
    assert "OTE 最优入场区" in msg
    assert "评分" not in msg
    assert "/10" not in msg
    assert "C_POI" not in msg
    assert "C1" not in msg
    # HTF 背景
    assert "Daily bias" in msg
    assert "bullish" in msg
    assert "32%" in msg
    assert "discount" in msg
    # 入场 + 真实 SL 依据(不是 0.15×ATR)
    assert "2261.00" in msg
    assert "leg 起点" in msg
    assert "0.15" not in msg
    assert "1:6.5" in msg
    # 溯源 leg
    assert "OTE leg: 2204.00 → 2300.00" in msg
    assert "回撤 41%" in msg
    # 时间 + 免责
    assert "2026-04-19 17:32 ET" in msg
    assert "仅供参考" in msg


def test_raid_short_ict_native():
    msg = format_signal(_make_raid_short())
    assert "🔴" in msg
    assert "做空信号" in msg
    assert "流动性扫荡反转" in msg
    assert "扫荡极值 ±0.3×ATR" in msg
    # 扫荡池溯源
    assert "等高点(BSL)" in msg
    assert "触点×12" in msg
    assert "1:3.6" in msg


def test_limit_mode_shows_pending_order():
    s = _make_ote_long()
    s.entry_mode = "limit"
    msg = format_signal(s)
    assert "挂单做多" in msg
    assert "Limit 24h 未成交" in msg


def test_works_with_dict_and_missing_meta():
    """鸭子类型 dict + 空 ict_meta(老信号 / 降级)应不崩。"""
    d = {
        "symbol": "ETHUSDT",
        "direction": "short",
        "poi_source": "ict_ob",
        "entry_price": 2000.0,
        "stop_loss": 2010.0,
        "take_profit": 1980.0,
        "risk_reward": 2.0,
        "entry_mode": "limit",
    }
    msg = format_signal(d)
    assert "🔴" in msg
    assert "订单块 OB" in msg
    assert "2000.00" in msg
    # 无 ict_meta → 无 HTF 区块,但不报错
    assert "仅供参考" in msg
