"""notify/formatter.py 单测。

只测纯函数输出，不触发任何网络调用。
"""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytz

from notify.formatter import format_signal


def _make_long_signal() -> SimpleNamespace:
    # 2026-04-19 17:32 ET  -> 对应 UTC 毫秒
    ny = pytz.timezone("America/New_York")
    dt_et = ny.localize(datetime(2026, 4, 19, 17, 32, 0))
    created_ms = int(dt_et.timestamp() * 1000)
    return SimpleNamespace(
        signal_id="sig_001",
        symbol="ETHUSDT",
        direction="long",
        triggered_level="pdPOC",
        poi_type="order_block",
        poi_high=2263.5,
        poi_low=2260.0,
        entry_price=2264.80,
        stop_loss=2258.00,
        take_profit=2275.00,
        risk_reward=2.0,
        total_score=6,
        scores={"C1": 2, "C3": 1, "C4": 1, "C6": 2, "C5": 0},
        created_at=created_ms,
        liquidity_sweep_candle={
            "wick_body_ratio": 3.2,
            "direction": "lower",
        },
    )


def _make_short_signal() -> SimpleNamespace:
    return SimpleNamespace(
        signal_id="sig_002",
        symbol="BTCUSDT",
        direction="short",
        triggered_level="pdVAH",
        poi_type="fvg",
        poi_high=65300.0,
        poi_low=65100.0,
        entry_price=65200.0,
        stop_loss=65400.0,
        take_profit=64600.0,
        risk_reward=3.0,
        total_score=7.5,
        scores={"C1": 2, "C4": 2, "C7": 1},
        created_at=int(datetime(2026, 4, 19, 20, 0, tzinfo=pytz.utc).timestamp()),
        liquidity_sweep_candle=None,
    )


def test_format_signal_long_contains_expected_fields():
    msg = format_signal(_make_long_signal())
    # 方向 emoji 和标题
    assert "🟢" in msg
    assert "做多信号" in msg
    assert "ETHUSDT" in msg
    assert "评分: 6/10" in msg
    # 触发价位
    assert "pdPOC" in msg
    assert "2264.80" in msg
    # ET 时间
    assert "2026-04-19 17:32 ET" in msg
    # POI
    assert "订单块" in msg
    assert "2260.00" in msg
    assert "2263.50" in msg
    # 交易参数 + 百分比
    assert "入场价: 2264.80" in msg
    assert "止损价: 2258.00" in msg
    assert "目标价: 2275.00" in msg
    # 盈亏比
    assert "1:2.0" in msg
    # 百分比带正负号（止损应为负，目标应为正）
    assert "-0.30%" in msg
    assert "+0.45%" in msg
    # 评分明细仅显示触发项，C5=0 不应出现
    assert "C1 价位共振 +2" in msg
    assert "C3 Delta强度 +1" in msg
    assert "C4 4H结构 +1" in msg
    assert "C6 长影线 +2" in msg
    assert "C5" not in msg
    # 流动性猎取
    assert "影线/实体 = 3.2x" in msg
    # 免责声明
    assert "仅供参考" in msg


def test_format_signal_short_uses_red_emoji_and_no_sweep():
    msg = format_signal(_make_short_signal())
    assert "🔴" in msg
    assert "做空信号" in msg
    assert "BTCUSDT" in msg
    # 没有长影线
    assert "未检测" in msg
    # FVG 中文
    assert "FVG" in msg
    # 盈亏比
    assert "1:3.0" in msg


def test_format_signal_works_with_dict():
    """鸭子类型：dict 形式也应可正常格式化。"""
    d = {
        "symbol": "ETHUSDT",
        "direction": "long",
        "triggered_level": "pdPOC",
        "entry_price": 2000.0,
        "stop_loss": 1990.0,
        "take_profit": 2020.0,
        "risk_reward": 2.0,
        "total_score": 5,
        "scores": {"C1": 2, "C3": 1, "C6": 2},
        "poi_type": "order_block",
        "poi_low": 1995.0,
        "poi_high": 1998.0,
        "created_at": 1_700_000_000,  # 秒
        "liquidity_sweep_candle": None,
    }
    msg = format_signal(d)
    assert "ETHUSDT" in msg
    assert "入场价: 2000.00" in msg
    assert "C1 价位共振 +2" in msg


def test_format_signal_handles_missing_optional_fields():
    d = {
        "symbol": "ETHUSDT",
        "direction": "short",
        "entry_price": 2000.0,
        "stop_loss": 2010.0,
        "take_profit": 1980.0,
        "risk_reward": 2.0,
        "total_score": 4,
        "scores": {},
    }
    msg = format_signal(d)
    assert "🔴" in msg
    # 没有 triggered_level / poi / created_at，应以 '-' 或占位符展示
    assert "触发价位: -" in msg
    assert "POI 类型: -" in msg
    assert "时间: -" in msg
    assert "(无评分明细)" in msg
