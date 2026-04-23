"""VP 核心算法单测。

构造一组固定成交，手工算好 POC/VAH/VAL，验证算法正确性。
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest
import pytz

from engine.volume_profile import (
    VolumeProfileSession,
    compute_profile,
)


def test_compute_profile_basic():
    # bucket_size = 1.0；成交集中在 100 附近
    # 价格=100 量=100 最大 => POC bucket_center = 100.5
    # 向两侧累加直到 >= 70%
    buckets = {
        98:  5.0,
        99:  10.0,
        100: 100.0,
        101: 20.0,
        102: 8.0,
        103: 2.0,
    }
    total = sum(buckets.values())  # 145
    target = total * 0.70          # 101.5

    vp = compute_profile(buckets, bucket_size=1.0, value_area_pct=0.70)
    assert vp is not None
    # POC bucket = 100，center = 100.5
    assert vp.poc == pytest.approx(100.5)
    # 从 100 向两侧扩展：up_pair(101+102)=28 vs dn_pair(99+98)=15 => 向上
    # acc = 100 + 28 = 128 >= 101.5，hi=102, lo=100
    # VAH = center(102)+0.5 = 103.0; VAL = center(100)-0.5 = 100.0
    assert vp.vah == pytest.approx(103.0)
    assert vp.val == pytest.approx(100.0)
    assert vp.total_volume == pytest.approx(total)


def test_compute_profile_empty():
    assert compute_profile({}, bucket_size=1.0) is None
    assert compute_profile({1: 0.0}, bucket_size=1.0) is None


def test_session_accumulates_and_resets_at_17_00_ny():
    sess = VolumeProfileSession(
        timezone="America/New_York",
        daily_reset_hour=17,
        bucket_count=200,
        value_area_pct=0.70,
    )
    ny = pytz.timezone("America/New_York")

    # day1: NY 15:00（session 起点 = 前一天 17:00），喂几笔
    t1 = ny.localize(datetime(2025, 1, 10, 15, 0, 0)).astimezone(timezone.utc)
    assert sess.on_trade(2000.0, 1.0, t1) is None
    assert sess.on_trade(2001.0, 2.0, t1 + timedelta(seconds=5)) is None
    assert sess.current() is not None

    # day1: NY 18:00（已跨过 17:00）=> 应触发锁定昨日
    t2 = ny.localize(datetime(2025, 1, 10, 18, 0, 0)).astimezone(timezone.utc)
    locked = sess.on_trade(2005.0, 1.0, t2)
    assert locked is not None, "应在跨过 17:00 时返回昨日 VP 快照"
    assert sess.prev_vp is locked
    # 新 session 已开始，当前 VP 应只含最新一笔
    cur = sess.current()
    assert cur is not None
    assert cur.total_volume == pytest.approx(1.0)


def test_session_poc_stable_with_tie():
    # 两个 bucket 量相等，算法应取较低价 bucket（tie-break: -idx）
    sess = VolumeProfileSession(bucket_count=10)
    base_ts = datetime(2025, 1, 10, 20, 0, 0, tzinfo=timezone.utc)
    # 先塞一笔初始化 bucket_size
    sess.on_trade(1000.0, 0.0001, base_ts)  # 极小量，避免影响
    bs = sess.bucket_size
    assert bs is not None
    # 两组同量
    sess.on_trade(1000.0, 5.0, base_ts)
    sess.on_trade(1000.0 + bs * 3, 5.0, base_ts)
    cur = sess.current()
    assert cur is not None
    # POC 应对应较低价的 bucket（即 1000 所在 bucket 中心）
    low_bucket_center = (int(1000.0 // bs) + 0.5) * bs
    assert cur.poc == pytest.approx(low_bucket_center)
