"""CSV -> JSONL importer for SMC backtest output.

Reads data/analysis/signals_187d_v2.csv and emits one JSONL file under
gbrain_integration/jsonl/ where every row carries `source: backtest` plus
a `backtest_run_id` so multiple backtest runs can coexist in one lake.

All live-only fields are explicitly set to None (not omitted).

Usage
-----
    python gbrain_integration/import_backtest_csv.py \
        --csv data/analysis/signals_187d_v2.csv \
        --run-id smc_v2_187d_20260420
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Make this runnable both as a module AND as a script.
_HERE = Path(__file__).resolve().parent
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(_HERE.parent))
    from gbrain_integration.schema import TradeRecord  # type: ignore
else:
    from .schema import TradeRecord


DEFAULT_CSV = Path("data/analysis/signals_187d_v2.csv")
DEFAULT_RUN_ID = "smc_v2_187d_20260420"
DEFAULT_OUT_DIR = _HERE / "jsonl"


# ---------------------------------------------------------------------------
# Coercion
# ---------------------------------------------------------------------------

def _f(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _b(v: Any) -> Optional[bool]:
    if v is None or v == "":
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _time_iso_from_row(row: dict[str, str]) -> Optional[str]:
    """Prefer explicit time_ms, fall back to time_iso string."""
    tms = _i(row.get("time_ms"))
    if tms is not None:
        return datetime.fromtimestamp(tms / 1000, tz=timezone.utc).isoformat()
    t = row.get("time_iso")
    return t.strip() if t else None


def _signal_id(row: dict[str, str], run_id: str, row_idx: int) -> str:
    """Deterministic id so re-imports overwrite the same md file.

    We include the CSV row index as a final tie-breaker — the upstream CSV can
    legitimately have two signals at the same (time_ms, direction, poi_source)
    (e.g. two overlapping POIs on the same bar). Without the index a second
    one would collide and be overwritten.
    """
    tms = row.get("time_ms") or "0"
    direction = (row.get("direction") or "x")[:1]
    ps = (row.get("poi_source") or "x").replace("_", "")[:6]
    return f"{run_id}_{tms}_{direction}_{ps}_{row_idx:04d}"


# ---------------------------------------------------------------------------
# Row -> TradeRecord
# ---------------------------------------------------------------------------

def row_to_record(row: dict[str, str], run_id: str, row_idx: int = 0) -> TradeRecord:
    time_ms = _i(row.get("time_ms"))
    return TradeRecord(
        signal_id=_signal_id(row, run_id, row_idx),
        source="backtest",
        backtest_run_id=run_id,
        symbol="ETHUSDT",
        time_iso=_time_iso_from_row(row),
        time_ms=time_ms,
        hour_et=_i(row.get("hour_et")),
        weekday=_i(row.get("weekday")),
        direction=(row.get("direction") or None),
        poi_source=(row.get("poi_source") or None),
        poi_type=(row.get("poi_type") or None),
        h4_structure=(row.get("h4_structure") or None),
        entry=_f(row.get("entry")),
        sl=_f(row.get("sl")),
        tp=_f(row.get("tp")),
        rr=_f(row.get("rr")),
        atr_4h=_f(row.get("atr_4h")),
        atr_1h=_f(row.get("atr_1h")),
        atr_15m=_f(row.get("atr_15m")),
        atr_ratio_4h_1h=_f(row.get("atr_ratio_4h_1h")),
        poi_height_pct=_f(row.get("poi_height_pct")),
        poi_distance_pct=_f(row.get("poi_distance_pct")),
        poi_age_hours=_f(row.get("poi_age_hours")),
        poi_displacement_mult=_f(row.get("poi_displacement_mult")),
        # POI hi/lo + origin_time + vp_poc are not in this CSV -> null
        poi_high=None,
        poi_low=None,
        poi_origin_time=None,
        poi_vp_poc=None,
        sweep_bar_delta=_f(row.get("sweep_bar_delta")),
        sweep_wick_strong=_b(row.get("sweep_wick_strong")),
        # CHoCH fields not in CSV -> null
        choch_break_price=None,
        choch_break_time=None,
        step1_trigger_price=None,
        step1_override_by_sweep=None,
        triggered_level=None,
        triggered_level_value=None,
        score_total=_f(row.get("score_total")),
        C1=_f(row.get("C1")),
        C2=_f(row.get("C2")),
        C3=_f(row.get("C3")),
        C4=_f(row.get("C4")),
        C5=_f(row.get("C5")),
        C6=_f(row.get("C6")),
        C7=_f(row.get("C7")),
        C8=_f(row.get("C8")),
        C9=_f(row.get("C9")),
        outcome=(row.get("outcome") or None),
        pnl_r=_f(row.get("pnl_r")),
        win=_i(row.get("win")),
        notified=_i(row.get("notified")),
        # --- LIVE-ONLY fields: explicit None ---
        step=None,
        created_at=None,
        updated_at=None,
        trader_notes=None,
        emotional_state=None,
        real_slippage_pct=None,
        execution_latency_ms=None,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Import SMC backtest CSV into GBrain JSONL.")
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Input CSV path")
    p.add_argument("--run-id", default=DEFAULT_RUN_ID, help="backtest_run_id tag")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="Output dir")
    args = p.parse_args(argv)

    csv_path: Path = args.csv
    if not csv_path.exists():
        print(f"[error] CSV not found: {csv_path}", file=sys.stderr)
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f"backtest_{args.run_id}.jsonl"

    total = 0
    wins = 0
    poi_source_counts: Counter[str] = Counter()
    outcome_counts: Counter[str] = Counter()

    with csv_path.open("r", encoding="utf-8", newline="") as fin, \
         out_path.open("w", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        for row_idx, row in enumerate(reader):
            rec = row_to_record(row, run_id=args.run_id, row_idx=row_idx)
            fout.write(json.dumps(rec.to_dict(), ensure_ascii=False, default=str))
            fout.write("\n")
            total += 1
            if rec.win == 1:
                wins += 1
            poi_source_counts[rec.poi_source or "null"] += 1
            outcome_counts[rec.outcome or "null"] += 1

    # summary
    print(f"[ok] wrote {total} records -> {out_path}")
    wr = (wins / total * 100.0) if total else 0.0
    print(f"     wins: {wins} ({wr:.1f}%)")
    print("     poi_source distribution:")
    for src, n in poi_source_counts.most_common():
        print(f"       - {src}: {n}")
    print("     outcome distribution:")
    for o, n in outcome_counts.most_common():
        print(f"       - {o}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
