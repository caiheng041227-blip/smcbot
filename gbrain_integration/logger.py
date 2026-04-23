"""Zero-dependency live/paper signal logger -> monthly JSONL.

Design contract
---------------
- Never raises. Never blocks. If anything goes wrong we swallow + warn.
  The bot must keep running even if the GBrain append layer is broken.
- Pure sync, stdlib only. JSON append is <1ms — no need for async.
- JSONL is the durable source of truth; markdown is a later derivation.
- Duck-types SignalState (uses getattr with defaults) so we don't import
  bot internals and don't break on schema drift.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .schema import TradeRecord, _dt_to_iso, _ms_to_iso

log = logging.getLogger(__name__)

# Default location: <repo>/gbrain_integration/jsonl/
_DEFAULT_JSONL_DIR = Path(__file__).resolve().parent / "jsonl"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def log_signal(
    signal_state: Any,
    source: str = "live",
    jsonl_dir: Optional[Path | str] = None,
) -> Optional[Path]:
    """Append one signal snapshot to `{jsonl_dir}/{source}_{YYYY-MM}.jsonl`.

    Returns the path written, or None on any error (errors are logged, not raised).
    """
    try:
        rec = _signal_state_to_record(signal_state, source=source)
        path = _target_path(source, jsonl_dir, rec.time_ms)
        _append_jsonl(path, rec.to_dict())
        return path
    except Exception as e:  # deliberately broad — never crash the bot
        log.warning("gbrain_integration.log_signal failed: %s", e)
        return None


def update_outcome(
    signal_id: str,
    outcome: str,
    pnl_r: Optional[float] = None,
    win: Optional[int] = None,
    source: str = "live",
    jsonl_dir: Optional[Path | str] = None,
    extras: Optional[dict[str, Any]] = None,
) -> Optional[Path]:
    """Append an outcome-update row for a previously-logged signal.

    We append rather than mutate — a later reducer step merges outcome rows
    back onto their originating signal row by `signal_id`. This keeps the
    JSONL append-only (Bronze-layer discipline).
    """
    try:
        now = datetime.now(timezone.utc)
        event = {
            "_event_type": "outcome_update",
            "signal_id": signal_id,
            "source": source,
            "outcome": outcome,
            "pnl_r": pnl_r,
            "win": win,
            "updated_at": now.isoformat(),
        }
        if extras:
            event["extras"] = extras
        ms = int(now.timestamp() * 1000)
        path = _target_path(source, jsonl_dir, ms)
        _append_jsonl(path, event)
        return path
    except Exception as e:
        log.warning("gbrain_integration.update_outcome failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _target_path(source: str, jsonl_dir: Optional[Path | str], time_ms: Optional[int]) -> Path:
    base = Path(jsonl_dir) if jsonl_dir else _DEFAULT_JSONL_DIR
    base.mkdir(parents=True, exist_ok=True)
    if time_ms:
        dt = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)
    return base / f"{source}_{dt.strftime('%Y-%m')}.jsonl"


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    line = json.dumps(payload, ensure_ascii=False, default=str)
    # newline-terminated, utf-8; open/close per-call keeps us crash-safe
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        f.write("\n")


def _g(obj: Any, name: str, default: Any = None) -> Any:
    """Safe getattr that also tolerates dict-like objects."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _signal_state_to_record(s: Any, source: str) -> TradeRecord:
    """Project a SignalState (or dict) into a TradeRecord."""

    # scores dict -> C1..C9
    scores = _g(s, "scores") or {}
    if not isinstance(scores, dict):
        scores = {}

    created_at = _g(s, "created_at")
    time_ms = _g(s, "time_ms")
    time_iso = _g(s, "time_iso")
    if time_ms is None and isinstance(created_at, datetime):
        time_ms = int(created_at.timestamp() * 1000)
    if time_iso is None:
        time_iso = _ms_to_iso(time_ms) or _dt_to_iso(created_at)

    hour_et = _g(s, "hour_et")
    weekday = _g(s, "weekday")

    rec = TradeRecord(
        signal_id=str(_g(s, "signal_id") or f"auto_{time_ms or 'x'}"),
        source=source,
        symbol=str(_g(s, "symbol", "ETHUSDT")),
        time_iso=time_iso,
        time_ms=int(time_ms) if time_ms is not None else None,
        hour_et=int(hour_et) if hour_et is not None else None,
        weekday=int(weekday) if weekday is not None else None,
        direction=_g(s, "direction"),
        poi_source=_g(s, "poi_source"),
        poi_type=_g(s, "poi_type"),
        h4_structure=_g(s, "h4_structure"),
        entry=_f(_g(s, "entry_price") or _g(s, "entry")),
        sl=_f(_g(s, "stop_loss") or _g(s, "sl")),
        tp=_f(_g(s, "take_profit") or _g(s, "tp")),
        rr=_f(_g(s, "risk_reward") or _g(s, "rr")),
        atr_4h=_f(_g(s, "atr_4h")),
        atr_1h=_f(_g(s, "atr_1h")),
        atr_15m=_f(_g(s, "atr_15m")),
        atr_ratio_4h_1h=_f(_g(s, "atr_ratio_4h_1h")),
        poi_height_pct=_f(_g(s, "poi_height_pct")),
        poi_distance_pct=_f(_g(s, "poi_distance_pct")),
        poi_age_hours=_f(_g(s, "poi_age_hours")),
        poi_displacement_mult=_f(_g(s, "poi_displacement_mult")),
        poi_high=_f(_g(s, "poi_high")),
        poi_low=_f(_g(s, "poi_low")),
        poi_origin_time=_dt_to_iso(_g(s, "poi_origin_time")),
        poi_vp_poc=_f(_g(s, "poi_vp_poc")),
        sweep_bar_delta=_f(_g(s, "sweep_bar_delta")),
        sweep_wick_strong=_b(_g(s, "sweep_wick_strong")),
        choch_break_price=_f(_g(s, "choch_break_price")),
        choch_break_time=_dt_to_iso(_g(s, "choch_break_time")),
        step1_trigger_price=_f(_g(s, "step1_trigger_price")),
        step1_override_by_sweep=_b(_g(s, "step1_override_by_sweep")),
        triggered_level=_g(s, "triggered_level"),
        triggered_level_value=_f(_g(s, "triggered_level_value")),
        score_total=_f(_g(s, "total_score") or _g(s, "score_total")),
        C1=_f(scores.get("C1")),
        C2=_f(scores.get("C2")),
        C3=_f(scores.get("C3")),
        C4=_f(scores.get("C4")),
        C5=_f(scores.get("C5")),
        C6=_f(scores.get("C6")),
        C7=_f(scores.get("C7")),
        C8=_f(scores.get("C8")),
        C9=_f(scores.get("C9")),
        outcome=_g(s, "outcome"),
        pnl_r=_f(_g(s, "pnl_r")),
        win=_i(_g(s, "win")),
        notified=_i(_g(s, "notified")),
        step=_g(s, "step"),
        created_at=_dt_to_iso(created_at),
        updated_at=_dt_to_iso(_g(s, "updated_at")),
        # live-only manual fields (optional)
        trader_notes=_g(s, "trader_notes"),
        emotional_state=_g(s, "emotional_state"),
        real_slippage_pct=_f(_g(s, "real_slippage_pct")),
        execution_latency_ms=_i(_g(s, "execution_latency_ms")),
    )
    return rec


# ---- small coercion helpers ------------------------------------------------

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
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "t"):
        return True
    if s in ("false", "0", "no", "n", "f"):
        return False
    return None
