"""Binance futures combined stream 客户端。

设计：
- 单 WS 连接订阅 aggTrade + 多周期 kline，降低连接数
- 断线指数退避重连（最大 60s）
- 支持 SOCKS5 代理（python-socks + websockets）
- 收到消息 dispatch 到注册的 handler（by stream name）
"""
from __future__ import annotations

import asyncio
import json
import random
from typing import Callable, Dict, List, Optional
from urllib.parse import urlparse

import websockets
from websockets.exceptions import ConnectionClosed

from utils.logger import logger


StreamHandler = Callable[[str, dict], "asyncio.Future | None"]


class BinanceWS:
    def __init__(
        self,
        symbol: str,
        timeframes: List[str],
        ws_base: str = "wss://fstream.binance.com",
        proxy: Optional[str] = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 60.0,
        extra_streams: Optional[List[str]] = None,
    ):
        self.symbol = symbol.lower()
        self.timeframes = timeframes
        self.ws_base = ws_base.rstrip("/")
        self.proxy = proxy or None
        self.reconnect_min = reconnect_min
        self.reconnect_max = reconnect_max
        # 额外订阅的 stream 名(例 'btcusdt@kline_4h'),复用同一 WS 连接
        self.extra_streams: List[str] = [s.lower() for s in (extra_streams or [])]

        self._handlers: Dict[str, StreamHandler] = {}
        self._stop = asyncio.Event()

    def on(self, stream: str, handler: StreamHandler) -> None:
        """stream 如 'ethusdt@aggTrade' / 'ethusdt@kline_1m'."""
        self._handlers[stream] = handler

    # ----- url -----------------------------------------------------------

    def build_url(self) -> str:
        streams = [f"{self.symbol}@aggTrade"]
        for tf in self.timeframes:
            streams.append(f"{self.symbol}@kline_{tf}")
        streams.extend(self.extra_streams)
        return f"{self.ws_base}/stream?streams={'/'.join(streams)}"

    # ----- connect ------------------------------------------------------

    async def _open_ws(self, url: str):
        """根据是否配置 proxy 选择连接方式。"""
        if self.proxy:
            # 通过 python-socks 建立 SOCKS5 隧道，再给 websockets 手工 TLS
            from python_socks.async_.asyncio import Proxy

            parsed = urlparse(url)
            host = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "wss" else 80)
            proxy = Proxy.from_url(self.proxy)
            sock = await proxy.connect(dest_host=host, dest_port=port)
            return await websockets.connect(
                url,
                sock=sock,
                server_hostname=host,
                ping_interval=20,
                ping_timeout=20,
                max_size=2 ** 22,
            )
        return await websockets.connect(
            url, ping_interval=20, ping_timeout=20, max_size=2 ** 22
        )

    async def run(self) -> None:
        url = self.build_url()
        backoff = self.reconnect_min
        logger.info(f"WS 目标: {url}  代理: {self.proxy or '无'}")

        while not self._stop.is_set():
            try:
                async with await self._open_ws(url) as ws:
                    logger.info("WebSocket 已连接")
                    backoff = self.reconnect_min
                    await self._read_loop(ws)
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as e:
                logger.warning(f"连接关闭: {e.code} {e.reason}")
            except Exception as e:
                logger.error(f"WS 异常: {type(e).__name__}: {e}")

            if self._stop.is_set():
                break
            # 指数退避 + 抖动
            delay = min(self.reconnect_max, backoff) + random.uniform(0, 0.5)
            logger.info(f"{delay:.1f}s 后重连")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(self.reconnect_max, backoff * 2)

    async def _read_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            stream = msg.get("stream")
            data = msg.get("data")
            if not stream or data is None:
                continue

            handler = self._handlers.get(stream)
            if handler is None:
                continue

            res = handler(stream, data)
            if asyncio.iscoroutine(res):
                await res

    def stop(self) -> None:
        self._stop.set()
