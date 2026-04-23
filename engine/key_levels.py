"""关键价位管理:上周 VP(wVAH/wPOC/wVAL)、本周极值(wHigh/wLow)、上月极值(mHigh/mLow)。

Step2 的"贴近价位"闸口从这里读快照。日级 pd* VP 已废弃(一周交易 3-4 次不需要
日级粒度),但 daily_vp 表仍保留作为历史数据。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import pytz

from engine.volume_profile import VPResult


@dataclass
class KeyLevels:
    # 上周 VP(跨周锁定,来自 WeeklyVolumeProfileSession)
    wVAH: Optional[float] = None
    wPOC: Optional[float] = None
    wVAL: Optional[float] = None
    # 本周极值(实时刷新)
    wHigh: Optional[float] = None
    wLow: Optional[float] = None
    # 上月极值(跨月锁定)
    mHigh: Optional[float] = None
    mLow: Optional[float] = None

    _week_key: Optional[str] = None
    _month_key: Optional[str] = None
    _cur_month_hi: Optional[float] = None
    _cur_month_lo: Optional[float] = None

    def update_prev_week(self, vp: VPResult) -> None:
        self.wVAH = vp.vah
        self.wPOC = vp.poc
        self.wVAL = vp.val

    def update_weekly(self, price: float, ts_utc: datetime, tz_name: str = "America/New_York") -> None:
        """维护本周极值:周起点 = 周日 17:00 本地。"""
        tz = pytz.timezone(tz_name)
        local = ts_utc.astimezone(tz)
        shifted = local - timedelta(hours=17)
        days_since_sunday = (shifted.weekday() + 1) % 7
        week_start = (shifted - timedelta(days=days_since_sunday)).date()
        week_key = week_start.isoformat()
        if week_key != self._week_key:
            self._week_key = week_key
            self.wHigh = price
            self.wLow = price
            return
        if self.wHigh is None or price > self.wHigh:
            self.wHigh = price
        if self.wLow is None or price < self.wLow:
            self.wLow = price

    def update_monthly(self, price: float, ts_utc: datetime, tz_name: str = "America/New_York") -> None:
        """维护上月极值:跨月时把"本月 hi/lo"锁成 mHigh/mLow。"""
        tz = pytz.timezone(tz_name)
        local = ts_utc.astimezone(tz)
        month_key = f"{local.year:04d}-{local.month:02d}"
        if month_key != self._month_key:
            # 跨月:把刚结束的那个月的极值锁定为 mHigh/mLow
            if self._month_key is not None:
                self.mHigh = self._cur_month_hi
                self.mLow = self._cur_month_lo
            self._month_key = month_key
            self._cur_month_hi = price
            self._cur_month_lo = price
            return
        if self._cur_month_hi is None or price > self._cur_month_hi:
            self._cur_month_hi = price
        if self._cur_month_lo is None or price < self._cur_month_lo:
            self._cur_month_lo = price

    def snapshot(self) -> dict:
        return {
            "wVAH": self.wVAH,
            "wPOC": self.wPOC,
            "wVAL": self.wVAL,
            "wHigh": self.wHigh,
            "wLow": self.wLow,
            "mHigh": self.mHigh,
            "mLow": self.mLow,
        }
