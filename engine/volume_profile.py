"""Volume Profile: 按价格分档累加成交量，计算 POC/VAH/VAL。

为什么不引 numpy/pandas：这里只涉及一维 dict 累加 + 两侧扩展，
纯 Python 运行足够快（每日成交数十万条也在毫秒级），保持依赖轻量。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import pytz


@dataclass
class VPResult:
    poc: float          # 成交量最大价位的中心价
    vah: float          # Value Area High
    val: float          # Value Area Low
    total_volume: float
    buckets: Dict[int, float]  # bucket_idx -> volume


def _bucket_idx(price: float, bucket_size: float) -> int:
    # 用整数 bucket index 避免浮点 key 冲突
    return int(price // bucket_size)


def _bucket_center(idx: int, bucket_size: float) -> float:
    return (idx + 0.5) * bucket_size


def compute_profile(
    buckets: Dict[int, float],
    bucket_size: float,
    value_area_pct: float = 0.70,
) -> Optional[VPResult]:
    """从累计 buckets 计算 POC/VAH/VAL。buckets 为空返回 None。"""
    if not buckets:
        return None

    total = sum(buckets.values())
    if total <= 0:
        return None

    # POC：最大成交量 bucket（若并列取最低价 bucket，保持稳定）
    poc_idx = max(buckets.keys(), key=lambda k: (buckets[k], -k))
    target = total * value_area_pct

    # 从 POC 向两侧扩展：每步比较左右邻居 +1 / +2 格的总量，选量大的一侧
    lo = hi = poc_idx
    acc = buckets[poc_idx]
    sorted_keys = set(buckets.keys())

    while acc < target:
        up1 = buckets.get(hi + 1, 0.0)
        up2 = buckets.get(hi + 2, 0.0)
        dn1 = buckets.get(lo - 1, 0.0)
        dn2 = buckets.get(lo - 2, 0.0)
        up_pair = up1 + up2
        dn_pair = dn1 + dn2

        # 没有可扩展的邻居则跳出（成交稀疏时的兜底）
        if up_pair == 0 and dn_pair == 0:
            # 若 buckets 还有剩余未覆盖，跳到最近的 bucket
            remaining = sorted_keys - set(range(lo, hi + 1))
            if not remaining:
                break
            nearest = min(remaining, key=lambda k: min(abs(k - lo), abs(k - hi)))
            if nearest > hi:
                hi = nearest
            else:
                lo = nearest
            acc += buckets[nearest]
            continue

        if up_pair >= dn_pair:
            hi += 2
            acc += up_pair
        else:
            lo -= 2
            acc += dn_pair

    return VPResult(
        poc=_bucket_center(poc_idx, bucket_size),
        vah=_bucket_center(hi, bucket_size) + bucket_size / 2,
        val=_bucket_center(lo, bucket_size) - bucket_size / 2,
        total_volume=total,
        buckets=dict(buckets),
    )


class VolumeProfileSession:
    """滚动维护当日 VP：接收 trades，按 NY 时间 17:00 切换新日。"""

    def __init__(
        self,
        timezone: str = "America/New_York",
        daily_reset_hour: int = 17,
        bucket_count: int = 200,
        value_area_pct: float = 0.70,
    ):
        self.tz = pytz.timezone(timezone)
        self.reset_hour = daily_reset_hour
        self.bucket_count = bucket_count
        self.value_area_pct = value_area_pct

        # bucket_size 需要根据首条成交价动态估算，避免硬编码品种价格
        self._bucket_size: Optional[float] = None
        self._buckets: Dict[int, float] = {}
        self._session_start: Optional[datetime] = None
        self._session_hi: float = 0.0
        self._session_lo: float = float("inf")

        # 昨日（锁定后）快照
        self.prev_vp: Optional[VPResult] = None

    # -- helpers ----------------------------------------------------------

    def _session_boundary(self, now_utc: datetime) -> datetime:
        """返回 now 所属 session 的起点（UTC）：上一个 17:00 ET。"""
        now_ny = now_utc.astimezone(self.tz)
        anchor = now_ny.replace(hour=self.reset_hour, minute=0, second=0, microsecond=0)
        if now_ny < anchor:
            anchor -= timedelta(days=1)
        return anchor.astimezone(pytz.UTC)

    def _init_bucket_size(self, price: float) -> None:
        # 用首价 * 20% 的价格区间作为 VP 范围估算，bucket_size = 区间 / count
        # 这是启动兜底；运行过程中区间外的价格也会被正确计入（dict 无上限）
        span = price * 0.2
        self._bucket_size = span / self.bucket_count

    # -- public API -------------------------------------------------------

    def on_trade(self, price: float, qty: float, ts_utc: datetime) -> Optional[VPResult]:
        """喂入一笔 aggTrade。返回本次触发的 prev_vp（若跨日），否则 None。"""
        locked: Optional[VPResult] = None

        boundary = self._session_boundary(ts_utc)
        if self._session_start is None:
            self._session_start = boundary
        elif boundary > self._session_start:
            # 跨过 17:00 ET：锁定昨日快照
            if self._bucket_size is not None:
                self.prev_vp = compute_profile(
                    self._buckets, self._bucket_size, self.value_area_pct
                )
                locked = self.prev_vp
            # 重置
            self._buckets = {}
            self._session_start = boundary
            self._session_hi = 0.0
            self._session_lo = float("inf")
            # bucket_size 保持不变（避免快照一致性问题），每日 bucket_size 不会飘太多

        if self._bucket_size is None:
            self._init_bucket_size(price)

        idx = _bucket_idx(price, self._bucket_size)
        self._buckets[idx] = self._buckets.get(idx, 0.0) + qty
        self._session_hi = max(self._session_hi, price)
        self._session_lo = min(self._session_lo, price)
        return locked

    def current(self) -> Optional[VPResult]:
        if self._bucket_size is None:
            return None
        return compute_profile(self._buckets, self._bucket_size, self.value_area_pct)

    @property
    def bucket_size(self) -> Optional[float]:
        return self._bucket_size

    @property
    def session_start(self) -> Optional[datetime]:
        return self._session_start


class WeeklyVolumeProfileSession:
    """周 VP:按"周日 17:00 ET 为周起点"累加,跨周时锁定上周快照。

    与日 VP 的区别:
    - bucket_size 预估更大(初始价的 40% 区间),容纳周级波动
    - 周边界判定用 ISO-style "上一次周日 17:00" 作为 key
    """

    def __init__(
        self,
        timezone: str = "America/New_York",
        bucket_count: int = 200,
        value_area_pct: float = 0.70,
    ):
        self.tz = pytz.timezone(timezone)
        self.bucket_count = bucket_count
        self.value_area_pct = value_area_pct

        self._bucket_size: Optional[float] = None
        self._buckets: Dict[int, float] = {}
        self._week_key: Optional[str] = None
        self.prev_vp: Optional[VPResult] = None

    def _week_key_of(self, ts_utc: datetime) -> str:
        """周起点 = 最近一次"周日 17:00 本地时间"。用该日期作为 week key。"""
        local = ts_utc.astimezone(self.tz)
        shifted = local - timedelta(hours=17)
        days_since_sunday = (shifted.weekday() + 1) % 7  # Sun=6 -> 0
        week_start = (shifted - timedelta(days=days_since_sunday)).date()
        return week_start.isoformat()

    def _init_bucket_size(self, price: float) -> None:
        # 周波动通常 5-20%,取 40% 作区间兜底
        span = price * 0.4
        self._bucket_size = span / self.bucket_count

    def on_trade(self, price: float, qty: float, ts_utc: datetime) -> Optional[VPResult]:
        locked: Optional[VPResult] = None
        key = self._week_key_of(ts_utc)
        if self._week_key is None:
            self._week_key = key
        elif key != self._week_key:
            if self._bucket_size is not None:
                self.prev_vp = compute_profile(
                    self._buckets, self._bucket_size, self.value_area_pct
                )
                locked = self.prev_vp
            self._buckets = {}
            self._week_key = key

        if self._bucket_size is None:
            self._init_bucket_size(price)
        idx = _bucket_idx(price, self._bucket_size)
        self._buckets[idx] = self._buckets.get(idx, 0.0) + qty
        return locked

    def current(self) -> Optional[VPResult]:
        if self._bucket_size is None:
            return None
        return compute_profile(self._buckets, self._bucket_size, self.value_area_pct)
