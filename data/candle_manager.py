"""K 线缓存：每个 (symbol, timeframe) 一条 deque，带容量上限。

Binance kline 流会推送同一根未闭合 bar 多次，这里用 (open_time) 去重，
只有 bar.closed=True 时才算正式落档并回调订阅者。
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class Candle:
    open_time: int   # ms
    close_time: int  # ms
    open: float
    high: float
    low: float
    close: float
    volume: float
    closed: bool


class CandleManager:
    def __init__(self, maxlen: int = 2000):
        # key = (symbol, timeframe)
        self._store: Dict[Tuple[str, str], Deque[Candle]] = {}
        self._maxlen = maxlen
        self._on_close: List[Callable[[str, str, Candle], None]] = []

    def subscribe_close(self, cb: Callable[[str, str, Candle], None]) -> None:
        self._on_close.append(cb)

    def on_kline(self, symbol: str, timeframe: str, payload: dict) -> Optional[Candle]:
        """payload 为 Binance kline stream 的 'k' 字段。"""
        candle = Candle(
            open_time=int(payload["t"]),
            close_time=int(payload["T"]),
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=float(payload["v"]),
            closed=bool(payload["x"]),
        )
        key = (symbol, timeframe)
        dq = self._store.setdefault(key, deque(maxlen=self._maxlen))

        if dq and dq[-1].open_time == candle.open_time:
            dq[-1] = candle  # 更新未闭合 bar
        else:
            dq.append(candle)

        if candle.closed:
            for cb in self._on_close:
                try:
                    cb(symbol, timeframe, candle)
                except Exception:
                    # 单个订阅者异常不影响其他
                    from utils.logger import logger
                    logger.exception("candle close callback failed")
        return candle

    def last(self, symbol: str, timeframe: str) -> Optional[Candle]:
        dq = self._store.get((symbol, timeframe))
        return dq[-1] if dq else None

    def window(self, symbol: str, timeframe: str, n: int) -> List[Candle]:
        dq = self._store.get((symbol, timeframe))
        if not dq:
            return []
        return list(dq)[-n:]
