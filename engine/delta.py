"""Delta / CVD：按 aggTrade 的 is_buyer_maker 判断主动方向。

规则（Binance 语义）：
- is_buyer_maker=False  => taker 是买方 => 主动买 => +qty
- is_buyer_maker=True   => taker 是卖方 => 主动卖 => -qty

CVD = 启动以来累计 Delta；per-bar Delta 由 candle_manager 按 bar close 分桶。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class DeltaTracker:
    cvd: float = 0.0                                # 全局累计
    per_bar: Dict[str, float] = field(default_factory=dict)   # timeframe -> 当前 bar delta

    def on_trade(self, qty: float, is_buyer_maker: bool) -> float:
        d = -qty if is_buyer_maker else qty
        self.cvd += d
        for tf in self.per_bar:
            self.per_bar[tf] += d
        return d

    def ensure_tf(self, tf: str) -> None:
        if tf not in self.per_bar:
            self.per_bar[tf] = 0.0

    def reset_bar(self, tf: str) -> float:
        """K 线 close 时调用，返回该 bar 的 delta 并重置。"""
        val = self.per_bar.get(tf, 0.0)
        self.per_bar[tf] = 0.0
        return val
