"""一次性回填 OKX ETHUSDT 1h 历史 K 线到 storage.candles 表。

用途:为 PatternMatcher 提供 2-3 年历史数据做相似走势检索。
幂等:可反复跑,INSERT OR IGNORE 防重复。

使用:
  python scripts/backfill_history_okx.py                  # 默认 3 年(可能拉到 2 年)
  python scripts/backfill_history_okx.py --days 30        # 短测试:30 天 = 720 根
  python scripts/backfill_history_okx.py --symbol BTCUSDT
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.rest_backfill import _fetch_klines_okx
from utils.logger import logger, setup_logger


_CANDLES_SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    symbol      TEXT    NOT NULL,
    timeframe   TEXT    NOT NULL,
    open_time   INTEGER NOT NULL,
    close_time  INTEGER NOT NULL,
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      REAL    NOT NULL,
    PRIMARY KEY (symbol, timeframe, open_time)
)
"""


async def backfill(symbol: str, timeframe: str, limit: int, db_path: str, proxy):
    logger.info(f"OKX 拉取 {symbol} {timeframe} × limit={limit} ...")
    rows = await _fetch_klines_okx(symbol, timeframe, limit, proxy, timeout_s=60)
    if not rows:
        logger.error("OKX 返回空,可能 API/代理出问题")
        return
    logger.info(f"OKX 实际返回 {len(rows)} 根(<= limit 是正常,OKX 历史有限)")

    batch = [
        (symbol, timeframe, int(r["t"]), int(r["T"]),
         float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"]), float(r["v"]))
        for r in rows
    ]
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(_CANDLES_SCHEMA)
        await conn.executemany(
            "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?,?)",
            batch,
        )
        await conn.commit()
        async with conn.execute(
            "SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM candles "
            "WHERE symbol=? AND timeframe=?",
            (symbol, timeframe),
        ) as cur:
            row = await cur.fetchone()
            count, mn, mx = row

    if count and count > 0:
        mn_iso = datetime.fromtimestamp(mn / 1000.0, tz=timezone.utc).isoformat()
        mx_iso = datetime.fromtimestamp(mx / 1000.0, tz=timezone.utc).isoformat()
        days_span = (mx - mn) / 86_400_000
        logger.info(
            f"DB {symbol} {timeframe}: total={count} 根, "
            f"range=[{mn_iso[:16]} → {mx_iso[:16]}], 跨度 {days_span:.1f} 天"
        )
    else:
        logger.warning("DB 写入后 count=0,异常")


def main():
    setup_logger("INFO")
    ROOT = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default=None, help="覆盖 config.symbol(默认 ETHUSDT)")
    ap.add_argument("--timeframe", default="1h", help="K 线周期(默认 1h)")
    ap.add_argument(
        "--days", type=int, default=1095,
        help="目标拉取多少天(默认 3 年 = 1095 天)。"
             "OKX 实际能拉到的会少一些(history-candles 限频)。"
    )
    args = ap.parse_args()

    with open(ROOT / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    symbol = args.symbol or cfg["symbol"]
    db_path = str(ROOT / cfg["storage"]["db_path"])
    proxy = cfg.get("binance", {}).get("proxy") or os.getenv("HTTP_PROXY") or None

    bars_per_day = {"1m": 1440, "15m": 96, "1h": 24, "4h": 6, "1d": 1}[args.timeframe]
    limit = args.days * bars_per_day
    logger.info(
        f"目标 {symbol} {args.timeframe} × {args.days} 天 "
        f"= ~{limit} 根 → {db_path}  proxy={proxy or '无'}"
    )

    asyncio.run(backfill(symbol, args.timeframe, limit, db_path, proxy))


if __name__ == "__main__":
    main()
