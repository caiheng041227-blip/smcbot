"""Binance Futures REST 历史 K 线回填。

在 WS 订阅前先拉一批历史 K 线喂进 CandleManager,避免启动初期 state_machine 因为
window 不足直接跳过推进。也支持代理(socks5)。
"""
from __future__ import annotations

import asyncio
from typing import List, Optional

import aiohttp

from data.candle_manager import CandleManager
from utils.logger import logger


_REST_BASE = "https://fapi.binance.com"

# OKX 回落路径:Binance 返回 451(地区限制)时自动切这里,数据本质等价
_OKX_BASE = "https://www.okx.com"
_OKX_INTERVAL = {
    "1m": ("1m", 60_000),
    "15m": ("15m", 900_000),
    "1h": ("1H", 3_600_000),
    "2h": ("2H", 7_200_000),
    "4h": ("4H", 14_400_000),
    "1d": ("1D", 86_400_000),
}
_OKX_SYMBOL = {
    "ETHUSDT": "ETH-USDT-SWAP",
    "BTCUSDT": "BTC-USDT-SWAP",
}


async def _fetch_klines_okx(
    symbol: str,
    timeframe: str,
    limit: int,
    proxy: Optional[str],
    timeout_s: float,
) -> List[dict]:
    """OKX 两段拉取:
    - Phase 1: `/market/candles`(limit=300,硬盖最近 ~1440 根)
    - Phase 2: `/market/history-candles`(limit=100,可翻到 ~1 年前,限频更严)
    当 Phase 1 用尽且 limit 未满时自动续 Phase 2。
    """
    okx_sym = _OKX_SYMBOL.get(symbol.upper(), symbol.upper())
    if timeframe not in _OKX_INTERVAL:
        raise ValueError(f"OKX 不支持的 timeframe: {timeframe}")
    okx_int, int_ms = _OKX_INTERVAL[timeframe]

    timeout = aiohttp.ClientTimeout(total=timeout_s)
    connector = None
    http_proxy: Optional[str] = None
    if proxy:
        if proxy.startswith("socks"):
            try:
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(proxy)
            except ImportError:
                return []
        else:
            http_proxy = proxy

    async def _fetch_page(sess, endpoint: str, page_size: int, after: Optional[str]) -> list:
        url = f"{_OKX_BASE}/api/v5/market/{endpoint}"
        params = {"instId": okx_sym, "bar": okx_int, "limit": page_size}
        if after:
            params["after"] = after
        async with sess.get(url, params=params, proxy=http_proxy) as resp:
            resp.raise_for_status()
            js = await resp.json()
        if js.get("code") not in ("0", 0):
            raise RuntimeError(f"OKX API error ({endpoint}): {js}")
        return js.get("data") or []

    all_rows: List[list] = []
    cursor: Optional[str] = None  # 最早一条 ts,作为下一页 after(OKX 语义:翻更早)
    need = int(limit)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as sess:
        # Phase 1:/candles(300/页,总量硬盖 ~1440)
        while need > 0:
            page = min(need, 300)
            data = await _fetch_page(sess, "candles", page, cursor)
            if not data:
                break
            all_rows.extend(data)
            cursor = data[-1][0]
            need -= len(data)
            if len(data) < page:
                break  # /candles 耗尽,进入 Phase 2

        # Phase 2:/history-candles(100/页,可拉 ~1 年)
        # OKX 限频 20 请求/2 秒,保守每请求间隔 100ms
        while need > 0:
            page = min(need, 100)
            try:
                data = await _fetch_page(sess, "history-candles", page, cursor)
            except aiohttp.ClientResponseError as e:
                # 429 限频时稍等重试一次;其它错误直接抛
                if e.status == 429:
                    await asyncio.sleep(1.0)
                    data = await _fetch_page(sess, "history-candles", page, cursor)
                else:
                    raise
            if not data:
                break
            all_rows.extend(data)
            cursor = data[-1][0]
            need -= len(data)
            if len(data) < page:
                break
            await asyncio.sleep(0.1)

    # 边界去重(Phase 1/2 拼接处可能重叠一根)
    seen: set = set()
    dedup: List[list] = []
    for r in all_rows:
        ts = r[0]
        if ts in seen:
            continue
        seen.add(ts)
        dedup.append(r)
    all_rows = dedup

    # 过滤未确认 + 按时间升序
    confirmed = [r for r in all_rows if r[8] == "1"]
    confirmed.sort(key=lambda r: int(r[0]))

    out: List[dict] = []
    for r in confirmed:
        open_t = int(r[0])
        out.append({
            "t": open_t,
            "T": open_t + int_ms - 1,
            "o": r[1], "h": r[2], "l": r[3], "c": r[4],
            "v": r[5],   # base-ccy volume (ETH 数量)
            "x": True,
            "i": timeframe,
        })
    return out


async def fetch_klines(
    symbol: str,
    timeframe: str,
    limit: int = 200,
    proxy: Optional[str] = None,
    timeout_s: float = 15.0,
) -> List[dict]:
    """返回 Binance kline 数组,每条为 WS 'k' 同构字典(t/T/o/h/l/c/v/x)。"""
    url = f"{_REST_BASE}/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": timeframe, "limit": int(limit)}
    timeout = aiohttp.ClientTimeout(total=timeout_s)

    connector = None
    http_proxy: Optional[str] = None
    if proxy:
        if proxy.startswith("socks"):
            # SOCKS5 走 aiohttp_socks 的 ProxyConnector
            try:
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(proxy)
            except ImportError:
                logger.warning(f"检测到 SOCKS 代理但 aiohttp_socks 未安装,跳过 REST 回填")
                return []
        else:
            http_proxy = proxy

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as sess:
            async with sess.get(url, params=params, proxy=http_proxy) as resp:
                resp.raise_for_status()
                rows = await resp.json()
    except aiohttp.ClientResponseError as e:
        if e.status == 451:
            logger.warning(f"Binance {symbol} {timeframe} 返回 451,回落 OKX")
            return await _fetch_klines_okx(symbol, timeframe, limit, proxy, timeout_s)
        raise

    # rows 格式:[openTime, open, high, low, close, volume, closeTime, ...]
    out: List[dict] = []
    for r in rows:
        out.append({
            "t": int(r[0]),
            "T": int(r[6]),
            "o": r[1],
            "h": r[2],
            "l": r[3],
            "c": r[4],
            "v": r[5],
            "x": True,  # 历史 K 均视为闭合
            "i": timeframe,
        })
    return out


async def backfill_to_manager(
    candle_manager: CandleManager,
    symbol: str,
    timeframes: List[str],
    counts: dict,
    proxy: Optional[str] = None,
) -> None:
    """并发拉取多个 TF 的历史 K 线,灌入 candle_manager。

    counts: {tf: desired_count}。不在 counts 里的 tf 用默认 200。
    """
    async def one(tf: str) -> None:
        limit = int(counts.get(tf, 200))
        try:
            rows = await fetch_klines(symbol, tf, limit=limit, proxy=proxy)
        except Exception as e:  # noqa: BLE001
            logger.error(f"REST 回填 {symbol} {tf} 失败: {e}")
            return
        # 喂进 CandleManager:on_kline 会按 open_time 去重,历史条目自动落盘
        for payload in rows:
            candle_manager.on_kline(symbol, tf, payload)
        logger.info(f"REST 回填 {symbol} {tf}: {len(rows)} 根")

    await asyncio.gather(*(one(tf) for tf in timeframes))
