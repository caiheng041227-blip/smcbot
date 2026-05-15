"""回填历史验尸报告(一次性脚本,不暴露 TG 命令)。

逻辑:
  1. 连接 data/market.db
  2. 找所有 outcome ∈ {sl, tp1_then_sl} 且未在 postmortems 表的 signals
  3. 串行调 `claude -p <prompt>`,落盘到 postmortems
  4. 进度打印 stdout

用法(在 Lightsail bot venv 下,bot 在跑的同时也能跑 — SQLite WAL 模式):
  ~/smcbot/venv/bin/python ~/smcbot/scripts/backfill_postmortems.py
  ~/smcbot/venv/bin/python ~/smcbot/scripts/backfill_postmortems.py --limit 3

默认全部缺失;--limit N 限量(测试用)。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# 复用 SignalTracker.build_postmortem_prompt(静态方法,不需要实例化 tracker)
from notify.signal_tracker import SignalTracker

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "market.db"


def _query_todo(conn: sqlite3.Connection, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """查 outcome ∈ {sl, tp1_then_sl} 且未在 postmortems 表的信号。"""
    cur = conn.cursor()
    cur.execute("""
        SELECT s.signal_id, s.direction, s.entry_price, s.stop_loss, s.take_profit,
               s.outcome, s.notified_at, s.created_at
        FROM signals s
        LEFT JOIN postmortems p ON p.signal_id = s.signal_id
        WHERE s.outcome IN ('sl', 'tp1_then_sl')
          AND s.notified_at IS NOT NULL
          AND p.id IS NULL
        ORDER BY s.notified_at ASC
    """)
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    if limit:
        rows = rows[:limit]
    return rows


async def _run_claude(prompt: str, timeout_s: int = 600) -> Dict[str, Any]:
    """spawn claude -p,返回 {'status', 'output', 'error_msg', 'duration_ms'}。"""
    started = int(time.time() * 1000)
    full_output: Optional[str] = None
    status = "error"
    error_msg: Optional[str] = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "claude", "-p", prompt,
            "--dangerously-skip-permissions",
            cwd=str(Path(__file__).resolve().parent.parent),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=8 * 1024 * 1024,
            env={**os.environ},
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
        full_output = stdout.decode("utf-8", errors="ignore").strip()
        if not full_output:
            status = "empty"
            error_msg = stderr.decode("utf-8", errors="ignore")[:500] or None
        else:
            status = "success"
    except asyncio.TimeoutError:
        status = "timeout"
        error_msg = f"claude -p {timeout_s}s 超时"
    except Exception as e:  # noqa: BLE001
        status = "error"
        error_msg = repr(e)[:500]
    return {
        "status": status,
        "output": full_output,
        "error_msg": error_msg,
        "duration_ms": int(time.time() * 1000) - started,
    }


def _save(conn: sqlite3.Connection, signal_id: str, reason: str, prompt: str,
          result: Dict[str, Any]) -> None:
    """落盘到 postmortems 表(同 Database.save_postmortem)。"""
    now = int(time.time())
    conn.execute("""
        INSERT INTO postmortems
          (signal_id, reason, prompt, output, status, error_msg, duration_ms, created_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        signal_id, reason, prompt, result["output"],
        result["status"], result["error_msg"], result["duration_ms"], now,
    ))
    conn.commit()


async def main_async(limit: Optional[int]) -> int:
    if not DB_PATH.exists():
        print(f"❌ DB 不存在:{DB_PATH}", file=sys.stderr)
        return 2
    conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
    try:
        todo = _query_todo(conn, limit=limit)
    finally:
        conn.close()
    if not todo:
        print("✅ 无需回填:所有 SL 信号都已有验尸")
        return 0
    print(f"🔬 开始回填 {len(todo)} 份验尸(串行,预计 {len(todo)*3}-{len(todo)*8} 分钟)")
    print(f"   DB: {DB_PATH}\n")

    ok, err = 0, 0
    for i, r in enumerate(todo, 1):
        sid = r["signal_id"]
        reason = r.get("outcome") or "sl"
        prompt = SignalTracker.build_postmortem_prompt(
            signal_id=sid,
            direction=r.get("direction"),
            entry=r.get("entry_price"),
            sl=r.get("stop_loss"),
            tp1=r.get("take_profit"),
            atr_4h=None,  # signals 表没存 ATR
            reason=reason,
        )
        t_start = time.time()
        print(f"[{i}/{len(todo)}] {sid[:8]} {reason} ... ", end="", flush=True)
        result = await _run_claude(prompt)
        elapsed = time.time() - t_start
        # 写 DB(每次新开 connection,避免长持有锁)
        conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
        try:
            _save(conn, sid, reason, prompt, result)
        finally:
            conn.close()
        if result["status"] == "success":
            ok += 1
            tag = "✓"
            preview = (result["output"] or "")[:80].replace("\n", " ")
            print(f"{tag} ({elapsed:.0f}s)  {preview}...")
        else:
            err += 1
            print(f"✗ {result['status']} ({elapsed:.0f}s)  err={result['error_msg']}")
    print(f"\n🏁 回填完成:成功 {ok} / 失败 {err} / 共 {len(todo)}")
    return 0 if err == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="只跑前 N 条(测试用),默认跑全部缺失")
    args = ap.parse_args()
    return asyncio.run(main_async(args.limit))


if __name__ == "__main__":
    sys.exit(main())
