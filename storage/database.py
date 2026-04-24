"""SQLite（aiosqlite）：K 线 / VP 快照 / 信号 / 持仓 四张表。

- candles / daily_vp：原始行情快照
- signals：通知出去的每条信号（持久化,重启后可恢复)
- positions：用户通过 Telegram 确认"已开仓"后记录；同 symbol 有 open 态时阻止新信号
"""
from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional

import aiosqlite


SCHEMA = [
    """
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
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_vp (
        symbol      TEXT    NOT NULL,
        session_date TEXT   NOT NULL,   -- 'YYYY-MM-DD'（NY 时间）
        poc         REAL,
        vah         REAL,
        val         REAL,
        total_vol   REAL,
        PRIMARY KEY (symbol, session_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals (
        signal_id        TEXT PRIMARY KEY,
        symbol           TEXT NOT NULL,
        direction        TEXT NOT NULL,
        entry_price      REAL,
        stop_loss        REAL,
        take_profit      REAL,
        risk_reward      REAL,
        total_score      REAL,
        triggered_level  TEXT,
        poi_type         TEXT,
        poi_low          REAL,
        poi_high         REAL,
        created_at       INTEGER,
        notified_at      INTEGER,
        user_action      TEXT,          -- 'opened' / 'ignored' / NULL
        user_action_at   INTEGER,
        outcome          TEXT,          -- 'tp' / 'sl' / 'manual_close' / NULL
        outcome_at       INTEGER,
        pnl_r            REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id   TEXT PRIMARY KEY,  -- 复用 signal_id
        symbol        TEXT NOT NULL,
        direction     TEXT NOT NULL,
        entry_price   REAL,
        stop_loss     REAL,
        take_profit   REAL,
        opened_at     INTEGER,
        status        TEXT NOT NULL,     -- 'open' / 'closed'
        closed_at     INTEGER,
        close_reason  TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_positions_symbol_status ON positions(symbol, status)",
]


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        for stmt in SCHEMA:
            await self._conn.execute(stmt)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def upsert_candle(
        self,
        symbol: str,
        timeframe: str,
        open_time: int,
        close_time: int,
        o: float,
        h: float,
        l: float,
        c: float,
        v: float,
    ) -> None:
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, timeframe, open_time) DO UPDATE SET
                close_time=excluded.close_time,
                open=excluded.open, high=excluded.high,
                low=excluded.low,   close=excluded.close,
                volume=excluded.volume
            """,
            (symbol, timeframe, open_time, close_time, o, h, l, c, v),
        )
        await self._conn.commit()

    async def save_daily_vp(
        self,
        symbol: str,
        session_date: str,
        poc: float,
        vah: float,
        val: float,
        total_vol: float,
    ) -> None:
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO daily_vp VALUES (?,?,?,?,?,?)
            ON CONFLICT(symbol, session_date) DO UPDATE SET
                poc=excluded.poc, vah=excluded.vah,
                val=excluded.val, total_vol=excluded.total_vol
            """,
            (symbol, session_date, poc, vah, val, total_vol),
        )
        await self._conn.commit()

    async def recent_candles(self, symbol: str, timeframe: str, n: int) -> Iterable:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT open_time, open, high, low, close, volume FROM candles "
            "WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT ?",
            (symbol, timeframe, n),
        ) as cur:
            return await cur.fetchall()

    # ---- signals / positions ----------------------------------------------

    async def insert_signal(self, s: Any) -> None:
        """把 SignalState 快照写入 signals 表；重复 signal_id 幂等更新。"""
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO signals (
                signal_id, symbol, direction,
                entry_price, stop_loss, take_profit, risk_reward,
                total_score, triggered_level, poi_type, poi_low, poi_high,
                created_at, notified_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(signal_id) DO UPDATE SET
                entry_price=excluded.entry_price,
                stop_loss=excluded.stop_loss,
                take_profit=excluded.take_profit,
                risk_reward=excluded.risk_reward,
                total_score=excluded.total_score,
                notified_at=excluded.notified_at
            """,
            (
                getattr(s, "signal_id"),
                getattr(s, "symbol"),
                getattr(s, "direction"),
                getattr(s, "entry_price", None),
                getattr(s, "stop_loss", None),
                getattr(s, "take_profit", None),
                getattr(s, "risk_reward", None),
                getattr(s, "total_score", None),
                getattr(s, "triggered_level", None),
                getattr(s, "poi_type", None),
                getattr(s, "poi_low", None),
                getattr(s, "poi_high", None),
                getattr(s, "created_at", int(time.time())),
                int(time.time()),
            ),
        )
        await self._conn.commit()

    async def recent_signals(
        self, hours: int, symbol: Optional[str] = None, limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """返回近 `hours` 小时 notified 信号(按 notified_at 倒序)。"""
        assert self._conn is not None
        cutoff = int(time.time()) - hours * 3600
        sql = (
            "SELECT signal_id, symbol, direction, entry_price, stop_loss, take_profit, "
            "risk_reward, total_score, triggered_level, poi_type, "
            "created_at, notified_at, outcome, pnl_r "
            "FROM signals WHERE notified_at >= ? "
        )
        params: tuple = (cutoff,)
        if symbol:
            sql += "AND symbol = ? "
            params = (cutoff, symbol)
        sql += "ORDER BY notified_at DESC LIMIT ?"
        params = (*params, limit)
        async with self._conn.execute(sql, params) as cur:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in await cur.fetchall()]

    async def set_signal_action(self, signal_id: str, action: str) -> Optional[Dict[str, Any]]:
        """记录用户在 Telegram 的选择：'opened' / 'ignored'。返回信号行（或 None）。"""
        assert self._conn is not None
        now = int(time.time())
        await self._conn.execute(
            "UPDATE signals SET user_action=?, user_action_at=? WHERE signal_id=?",
            (action, now, signal_id),
        )
        await self._conn.commit()
        async with self._conn.execute(
            "SELECT * FROM signals WHERE signal_id=?", (signal_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def open_position(self, signal_row: Dict[str, Any]) -> None:
        assert self._conn is not None
        now = int(time.time())
        await self._conn.execute(
            """
            INSERT INTO positions (
                position_id, symbol, direction,
                entry_price, stop_loss, take_profit,
                opened_at, status
            ) VALUES (?,?,?,?,?,?,?, 'open')
            ON CONFLICT(position_id) DO UPDATE SET
                status='open', opened_at=excluded.opened_at,
                closed_at=NULL, close_reason=NULL
            """,
            (
                signal_row["signal_id"],
                signal_row["symbol"],
                signal_row["direction"],
                signal_row.get("entry_price"),
                signal_row.get("stop_loss"),
                signal_row.get("take_profit"),
                now,
            ),
        )
        await self._conn.commit()

    async def close_position(self, position_id: str, reason: str) -> Optional[Dict[str, Any]]:
        assert self._conn is not None
        now = int(time.time())
        await self._conn.execute(
            "UPDATE positions SET status='closed', closed_at=?, close_reason=? "
            "WHERE position_id=? AND status='open'",
            (now, reason, position_id),
        )
        await self._conn.commit()
        async with self._conn.execute(
            "SELECT * FROM positions WHERE position_id=?", (position_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def has_open_position(self, symbol: str) -> bool:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT 1 FROM positions WHERE symbol=? AND status='open' LIMIT 1",
            (symbol,),
        ) as cur:
            return (await cur.fetchone()) is not None

    async def list_open_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        assert self._conn is not None
        if symbol:
            q = "SELECT * FROM positions WHERE status='open' AND symbol=?"
            args: tuple = (symbol,)
        else:
            q = "SELECT * FROM positions WHERE status='open'"
            args = ()
        async with self._conn.execute(q, args) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]
