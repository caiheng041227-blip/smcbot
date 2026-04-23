"""OKX 永续 public WebSocket 客户端。

与 `BinanceWS` 暴露相同的对外接口(symbol / timeframes / on / run / stop),内部
把 OKX 消息**翻译成 Binance 格式**后再 dispatch 给注册的 handler,主业务层(main.py
的 handle_agg / handle_kline)零改动。

翻译映射:
  Symbol   ETHUSDT         → ETH-USDT-SWAP
  Stream   ethusdt@aggTrade→ 内部沿用(向 handler dispatch 时仍用该名)
           ethusdt@kline_1h→ 内部沿用
  OKX trades   {px,sz,side,ts}  → {p,q,m,T}  (m = side==sell)
  OKX candle   [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
               → {k:{t,i,o,h,l,c,v,x,T}}
  Interval 1m→1m / 1h→1H / 4h→4H / 1d→1D(OKX 大小写敏感)
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

_OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
_OKX_WS_BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"   # candle K 线在这个端点

# Binance 符号 → OKX instId(永续合约)
_OKX_SYMBOL_MAP: Dict[str, str] = {
    "ETHUSDT": "ETH-USDT-SWAP",
    "BTCUSDT": "BTC-USDT-SWAP",
}

# Binance tf → OKX channel 后缀(大小写敏感)
_OKX_TF_MAP: Dict[str, str] = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H",
    "1d": "1D", "1w": "1W",
}

# Interval → ms(用于反推 Binance kline 的 close_time T)
_TF_MS: Dict[str, int] = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "12h": 43_200_000, "1d": 86_400_000, "1w": 604_800_000,
}


def _okx_symbol(sym: str) -> str:
    s = sym.upper()
    return _OKX_SYMBOL_MAP.get(s, s)


def _okx_bar(tf: str) -> str:
    return _OKX_TF_MAP.get(tf, tf)


def _binance_stream_name(inst_id: str, channel: str) -> Optional[str]:
    """OKX (instId, channel) → Binance stream 名,方便 handler 复用。
    channel 可能是 'trades' / 'candle1H' / 'candle15m' / ...
    """
    # instId 形如 ETH-USDT-SWAP → binance lower "ethusdt"
    parts = inst_id.split("-")
    if len(parts) >= 2:
        bsym = (parts[0] + parts[1]).lower()
    else:
        bsym = inst_id.lower()
    if channel == "trades":
        return f"{bsym}@aggTrade"
    if channel.startswith("candle"):
        # OKX candle1H → binance kline_1h(反向映射大小写)
        bar = channel[len("candle"):]
        # 反向找回小写 tf
        rev = {v: k for k, v in _OKX_TF_MAP.items()}
        tf = rev.get(bar, bar.lower())
        return f"{bsym}@kline_{tf}"
    return None


class OkxWS:
    """OKX 永续 public WebSocket。接口对标 BinanceWS,平滑替换。"""

    def __init__(
        self,
        symbol: str,
        timeframes: List[str],
        proxy: Optional[str] = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 60.0,
        extra_streams: Optional[List[str]] = None,
    ):
        self.symbol = symbol.upper()
        self.timeframes = timeframes
        self.proxy = proxy or None
        self.reconnect_min = reconnect_min
        self.reconnect_max = reconnect_max
        self.extra_streams: List[str] = list(extra_streams or [])

        self._handlers: Dict[str, StreamHandler] = {}
        self._stop = asyncio.Event()

    def on(self, stream: str, handler: StreamHandler) -> None:
        """stream 名和 Binance 保持一致(如 'ethusdt@aggTrade')。"""
        self._handlers[stream] = handler

    # ----- subscribe ----------------------------------------------------

    def _build_args(self) -> tuple:
        """按 endpoint 分组 subscribe.args:
            public   = trades  → public 端点
            business = candles → business 端点(v5 专用)
        """
        public_args: List[dict] = []
        business_args: List[dict] = []
        inst_id = _okx_symbol(self.symbol)
        public_args.append({"channel": "trades", "instId": inst_id})
        for tf in self.timeframes:
            business_args.append({"channel": f"candle{_okx_bar(tf)}", "instId": inst_id})
        # extra_streams: Binance 命名(ethusdt@kline_4h / btcusdt@kline_4h)
        for s in self.extra_streams:
            lower = s.lower()
            if "@kline_" in lower:
                bsym, _, tf = lower.partition("@kline_")
                xinst = _okx_symbol(bsym.upper())
                business_args.append({"channel": f"candle{_okx_bar(tf)}", "instId": xinst})
            elif "@aggtrade" in lower:
                bsym = lower.split("@", 1)[0]
                xinst = _okx_symbol(bsym.upper())
                public_args.append({"channel": "trades", "instId": xinst})
        return public_args, business_args

    # ----- connect ------------------------------------------------------

    async def _open_ws(self, url: str):
        if self.proxy:
            from python_socks.async_.asyncio import Proxy
            parsed = urlparse(url)
            host = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "wss" else 80)
            proxy = Proxy.from_url(self.proxy)
            sock = await proxy.connect(dest_host=host, dest_port=port)
            return await websockets.connect(
                url, sock=sock, server_hostname=host,
                ping_interval=None,  # OKX 自带 ping/pong,不用 websockets 的
                max_size=2 ** 22,
            )
        return await websockets.connect(
            url, ping_interval=None, max_size=2 ** 22
        )

    async def run(self) -> None:
        public_args, business_args = self._build_args()
        logger.info(
            f"OKX WS: public 订阅 {len(public_args)} / business 订阅 {len(business_args)}  "
            f"代理: {self.proxy or '无'}"
        )
        # 两个端点并发运行(public=trades / business=candles),任一断了只影响对应流
        tasks = [
            asyncio.create_task(self._run_endpoint(_OKX_WS_PUBLIC, public_args, tag="public")),
            asyncio.create_task(self._run_endpoint(_OKX_WS_BUSINESS, business_args, tag="business")),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            raise

    async def _run_endpoint(self, url: str, args: List[dict], tag: str) -> None:
        if not args:
            return
        backoff = self.reconnect_min
        while not self._stop.is_set():
            try:
                async with await self._open_ws(url) as ws:
                    logger.info(f"OKX {tag} 已连接")
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    backoff = self.reconnect_min
                    ping_task = asyncio.create_task(self._keepalive(ws))
                    try:
                        await self._read_loop(ws)
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as e:
                logger.warning(f"OKX {tag} 关闭: {e.code} {e.reason}")
            except Exception as e:  # noqa: BLE001
                logger.error(f"OKX {tag} 异常: {type(e).__name__}: {e}")
            if self._stop.is_set():
                break
            delay = min(self.reconnect_max, backoff) + random.uniform(0, 0.5)
            logger.info(f"{delay:.1f}s 后重连 OKX {tag}")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(self.reconnect_max, backoff * 2)

    async def _keepalive(self, ws) -> None:
        """OKX 无数据 30s 会断,需要周期发 'ping' 字符串保活。"""
        try:
            while not self._stop.is_set():
                await asyncio.sleep(25)
                try:
                    await ws.send("ping")
                except Exception:  # noqa: BLE001
                    break
        except asyncio.CancelledError:
            pass

    async def _read_loop(self, ws) -> None:
        async for raw in ws:
            # pong 回复 / 订阅确认消息,忽略
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")
            if raw == "pong":
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # 事件消息(订阅确认 / 错误)
            if "event" in msg:
                event = msg.get("event")
                if event == "error":
                    logger.error(f"OKX WS 错误: {msg}")
                elif event == "subscribe":
                    logger.debug(f"OKX 订阅确认: {msg.get('arg')}")
                continue

            arg = msg.get("arg") or {}
            channel = arg.get("channel", "")
            inst_id = arg.get("instId", "")
            data = msg.get("data")
            if not data or not channel:
                continue

            stream_name = _binance_stream_name(inst_id, channel)
            if stream_name is None:
                continue
            handler = self._handlers.get(stream_name)
            if handler is None:
                continue

            if channel == "trades":
                # data: list of trades
                for t in data:
                    b = _translate_trade(t)
                    if b is not None:
                        res = handler(stream_name, b)
                        if asyncio.iscoroutine(res):
                            await res
            elif channel.startswith("candle"):
                # data: list of candle arrays
                tf_bar = channel[len("candle"):]
                rev = {v: k for k, v in _OKX_TF_MAP.items()}
                tf_binance = rev.get(tf_bar, tf_bar.lower())
                for c in data:
                    b = _translate_candle(c, tf_binance)
                    if b is not None:
                        res = handler(stream_name, b)
                        if asyncio.iscoroutine(res):
                            await res

    def stop(self) -> None:
        self._stop.set()


# ----- 翻译工具 -----------------------------------------------------------


def _translate_trade(okx_trade: dict) -> Optional[dict]:
    """OKX trades 单条 → Binance aggTrade 同构 dict。"""
    try:
        px = okx_trade["px"]
        sz = okx_trade["sz"]
        side = okx_trade.get("side", "buy")   # taker side
        ts = int(okx_trade["ts"])
    except (KeyError, TypeError, ValueError):
        return None
    # Binance aggTrade 的 m = True 表示 buyer is market maker(即 taker 是 sell)
    # OKX side 是 taker side → side == 'sell' 时,买方是 maker,m=True
    m = (side == "sell")
    return {
        "p": px,
        "q": sz,
        "m": m,
        "T": ts,
    }


def _translate_candle(okx_candle: list, tf_binance: str) -> Optional[dict]:
    """OKX candle 数组 → Binance kline event(带 'k' 子 dict)。
    OKX 数组格式:[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
      - ts: 开盘时间(毫秒)
      - vol:合约张数
      - volCcy:基础币数量(ETH)← 使用这个匹配 Binance 的 v
      - confirm:'1'=已收线,'0'=进行中
    """
    try:
        t = int(okx_candle[0])
        o = okx_candle[1]
        h = okx_candle[2]
        low = okx_candle[3]
        c = okx_candle[4]
        # volCcy(基础币计量)更贴近 Binance aggTrade 的 "v"(ETH 数量)
        v = okx_candle[6] if len(okx_candle) > 6 else okx_candle[5]
        confirmed = (str(okx_candle[8]) == "1") if len(okx_candle) > 8 else False
    except (IndexError, TypeError, ValueError):
        return None

    int_ms = _TF_MS.get(tf_binance, 0)
    close_time = t + int_ms - 1 if int_ms else t

    # 外层 envelope 模拟 Binance combined stream:data.k 结构
    return {
        "k": {
            "t": t,
            "T": close_time,
            "i": tf_binance,
            "o": o,
            "h": h,
            "l": low,
            "c": c,
            "v": v,
            "x": confirmed,
        }
    }


__all__ = ["OkxWS"]
