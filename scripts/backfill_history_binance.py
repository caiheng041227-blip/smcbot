"""从 Binance 公开数据(data.binance.vision)拉 ETHUSDT 1h 历史月度 ZIP,
写入 storage.candles 表。

用途:补足 2017-08 到 2024-05 的历史(OKX 只能拉 ~2 年),
共 ~70,000 根 1h K 线,加上现有 OKX 数据后 PatternMatcher 有完整 8+ 年历史。

URL 例:
  https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1h/ETHUSDT-1h-2017-08.zip

幂等:INSERT OR IGNORE 防重复,可反复跑。

使用:
  python scripts/backfill_history_binance.py
  python scripts/backfill_history_binance.py --start 2017-08 --end 2024-04
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import logger, setup_logger


BASE_URL = "https://data.binance.vision/data/spot/monthly/klines/{symbol}/{tf}/{symbol}-{tf}-{ym}.zip"

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


def _normalize_ts_ms(ts: int) -> int:
    """Binance 历史 timestamp 兼容:2024 中后改成微秒(16 位),早期毫秒(13 位)。
    统一归一化到毫秒。"""
    s = str(int(ts))
    if len(s) >= 16:
        return int(ts) // 1000
    return int(ts)


def _list_months(start_ym: str, end_ym: str):
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    cur = (sy, sm)
    out = []
    while cur <= (ey, em):
        out.append(f"{cur[0]:04d}-{cur[1]:02d}")
        m = cur[1] + 1
        y = cur[0]
        if m > 12:
            m = 1
            y += 1
        cur = (y, m)
    return out


async def _fetch_one(sess: aiohttp.ClientSession, sem: asyncio.Semaphore,
                     ym: str, symbol: str, tf: str):
    url = BASE_URL.format(symbol=symbol, tf=tf, ym=ym)
    async with sem:
        try:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 404:
                    return ym, None, "404"
                resp.raise_for_status()
                data = await resp.read()
        except Exception as e:  # noqa: BLE001
            return ym, None, f"err:{e}"

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                text = f.read().decode("utf-8")
    except Exception as e:  # noqa: BLE001
        return ym, None, f"zip-err:{e}"

    rows = []
    reader = csv.reader(io.StringIO(text))
    for r in reader:
        if not r or len(r) < 7:
            continue
        if r[0] == "open_time":  # 2025+ 新格式带 header
            continue
        try:
            ot = _normalize_ts_ms(int(float(r[0])))
            ct = _normalize_ts_ms(int(float(r[6])))
            o, h, l, c, v = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
            rows.append((symbol, tf, ot, ct, o, h, l, c, v))
        except (ValueError, IndexError):
            continue
    return ym, rows, "ok"


async def main_async(symbol: str, tf: str, start_ym: str, end_ym: str, db_path: str):
    months = _list_months(start_ym, end_ym)
    logger.info(f"目标:{symbol} {tf} × {len(months)} 个月({start_ym} → {end_ym})")

    sem = asyncio.Semaphore(5)  # 限 5 并发,礼貌限速
    async with aiohttp.ClientSession() as sess:
        tasks = [_fetch_one(sess, sem, ym, symbol, tf) for ym in months]
        results = await asyncio.gather(*tasks)

    n_ok = n_404 = n_err = 0
    all_rows = []
    for ym, rows, status in results:
        if status == "404":
            n_404 += 1
            continue
        if rows is None:
            n_err += 1
            logger.warning(f"  {ym}: {status}")
            continue
        n_ok += 1
        all_rows.extend(rows)

    logger.info(f"汇总:{len(all_rows)} 根 K 线 | OK {n_ok} 月 / 404 {n_404} 月 / 失败 {n_err} 月")

    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(_CANDLES_SCHEMA)
        # 用 executemany + INSERT OR IGNORE,已存在的 (symbol+tf+open_time) 不会覆盖
        await conn.executemany(
            "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?,?)",
            all_rows,
        )
        await conn.commit()
        async with conn.execute(
            "SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM candles "
            "WHERE symbol=? AND timeframe=?",
            (symbol, tf),
        ) as cur:
            cnt, mn, mx = await cur.fetchone()

    if cnt and cnt > 0:
        mn_iso = datetime.fromtimestamp(mn / 1000, tz=timezone.utc).isoformat()[:16]
        mx_iso = datetime.fromtimestamp(mx / 1000, tz=timezone.utc).isoformat()[:16]
        days_span = (mx - mn) / 86_400_000
        logger.info(
            f"DB {symbol} {tf}: total={cnt} 根, "
            f"range=[{mn_iso} → {mx_iso}], 跨度 {days_span:.0f} 天"
        )


def main():
    setup_logger("INFO")
    ROOT = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="ETHUSDT")
    ap.add_argument("--timeframe", default="1h")
    ap.add_argument("--start", default="2017-08", help="开始月份 YYYY-MM(默认 2017-08 = ETHUSDT 上线)")
    ap.add_argument("--end", default=None, help="结束月份 YYYY-MM(默认本月)")
    args = ap.parse_args()

    with open(ROOT / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    db_path = str(ROOT / cfg["storage"]["db_path"])

    end_ym = args.end or datetime.now(timezone.utc).strftime("%Y-%m")
    logger.info(f"DB → {db_path}")
    asyncio.run(main_async(args.symbol, args.timeframe, args.start, end_ym, db_path))


if __name__ == "__main__":
    main()
