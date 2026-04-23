"""TradeRecord schema for GBrain knowledge-graph memory layer.

Single canonical record shape shared by:
  * logger.py        (live / paper signals off the running bot)
  * import_backtest_csv.py (historical backtest CSV rows)
  * jsonl_to_markdown.py   (rendering records to md for `gbrain import`)

Design rules
------------
- Every record carries a `source` tag: 'backtest' | 'paper' | 'live'.
  This is the primary slicing key; never omit it.
- Live-only fields are kept on the schema but set to None for backtest rows
  (explicit null, not missing).
- `to_dict()` is JSONL-friendly (datetimes -> ISO string).
- `to_markdown()` emits YAML frontmatter (for GBrain field filtering) + a
  natural-language body (for embedding/vector search) + a raw numeric table.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FRONTMATTER_WHITELIST = (
    # identity / filtering
    "signal_id", "source", "backtest_run_id", "symbol", "direction",
    "poi_source", "poi_type", "h4_structure",
    "time_iso", "time_ms", "hour_et", "weekday",
    # outcome
    "outcome", "win", "pnl_r", "notified",
    # coarse score (handy for filters like score_total >= 10)
    "score_total",
)


def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _dt_to_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat() if dt.tzinfo else dt.isoformat()
    # Unix seconds/ms (int/float): bot uses time.time() -> seconds; state_machine
    # poi_origin_time / choch_break_time are K-line open_time in ms.
    if isinstance(dt, (int, float)):
        try:
            ts = float(dt)
            if ts > 1e12:  # heuristic: > year 5138 in sec => must be ms
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            return None
    return str(dt)


def _yaml_scalar(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    s = str(v).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


# ---------------------------------------------------------------------------
# TradeRecord
# ---------------------------------------------------------------------------

@dataclass
class TradeRecord:
    """One trading signal record (backtest row or live signal snapshot).

    Field groups
    ------------
    identity / provenance:  signal_id, source, backtest_run_id, symbol
    time:                   time_iso, time_ms, hour_et, weekday
    setup:                  direction, poi_source, poi_type, h4_structure
    prices:                 entry, sl, tp, rr
    atr / volatility:       atr_4h, atr_1h, atr_15m, atr_ratio_4h_1h
    poi geometry:           poi_height_pct, poi_distance_pct, poi_age_hours,
                            poi_displacement_mult, poi_high, poi_low,
                            poi_origin_time, poi_vp_poc
    sweep / choch:          sweep_bar_delta, sweep_wick_strong,
                            choch_break_price, choch_break_time,
                            step1_trigger_price, step1_override_by_sweep,
                            triggered_level, triggered_level_value
    scoring:                score_total, C1..C9
    outcome:                outcome, pnl_r, win, notified
    live-only (None in backtest): trader_notes, emotional_state,
                                  real_slippage_pct, execution_latency_ms,
                                  step, created_at, updated_at
    """

    # --- identity / provenance ------------------------------------------------
    signal_id: str
    source: str                              # 'backtest' | 'paper' | 'live'
    symbol: str = "ETHUSDT"
    backtest_run_id: Optional[str] = None

    # --- time -----------------------------------------------------------------
    time_iso: Optional[str] = None
    time_ms: Optional[int] = None
    hour_et: Optional[int] = None
    weekday: Optional[int] = None

    # --- setup ----------------------------------------------------------------
    direction: Optional[str] = None          # 'long' / 'short'
    poi_source: Optional[str] = None         # e.g. '1h_breaker', '4h_ob'
    poi_type: Optional[str] = None           # e.g. 'bullish_breaker'
    h4_structure: Optional[str] = None       # 'bullish' / 'bearish' / 'neutral'

    # --- prices ---------------------------------------------------------------
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    rr: Optional[float] = None

    # --- ATR / volatility -----------------------------------------------------
    atr_4h: Optional[float] = None
    atr_1h: Optional[float] = None
    atr_15m: Optional[float] = None
    atr_ratio_4h_1h: Optional[float] = None

    # --- POI geometry ---------------------------------------------------------
    poi_height_pct: Optional[float] = None
    poi_distance_pct: Optional[float] = None
    poi_age_hours: Optional[float] = None
    poi_displacement_mult: Optional[float] = None
    poi_high: Optional[float] = None
    poi_low: Optional[float] = None
    poi_origin_time: Optional[str] = None
    poi_vp_poc: Optional[float] = None

    # --- sweep / choch --------------------------------------------------------
    sweep_bar_delta: Optional[float] = None
    sweep_wick_strong: Optional[bool] = None
    choch_break_price: Optional[float] = None
    choch_break_time: Optional[str] = None
    step1_trigger_price: Optional[float] = None
    step1_override_by_sweep: Optional[bool] = None
    triggered_level: Optional[str] = None
    triggered_level_value: Optional[float] = None

    # --- scoring --------------------------------------------------------------
    score_total: Optional[float] = None
    C1: Optional[float] = None
    C2: Optional[float] = None
    C3: Optional[float] = None
    C4: Optional[float] = None
    C5: Optional[float] = None
    C6: Optional[float] = None
    C7: Optional[float] = None
    C8: Optional[float] = None
    C9: Optional[float] = None

    # --- outcome --------------------------------------------------------------
    outcome: Optional[str] = None            # 'tp' | 'sl' | 'be' | 'open' | ...
    pnl_r: Optional[float] = None
    win: Optional[int] = None                # 0 / 1
    notified: Optional[int] = None

    # --- LIVE-ONLY (None in backtest) ----------------------------------------
    step: Optional[str] = None                      # live state-machine step
    created_at: Optional[str] = None                # live timestamp
    updated_at: Optional[str] = None                # live timestamp
    trader_notes: Optional[str] = None              # manual journal
    emotional_state: Optional[str] = None           # self-report
    real_slippage_pct: Optional[float] = None       # actual fill gap
    execution_latency_ms: Optional[int] = None      # signal -> fill latency

    # free-form bag for anything we haven't promoted to a proper field
    extras: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict (datetimes already strings)."""
        d = asdict(self)
        # asdict already handles nested dict `extras`; no datetime instances expected
        return d

    # ------------------------------------------------------------------
    def filename(self) -> str:
        """Idempotent markdown filename.

        Layout: {source}_{symbol}_{time_ms}_{short_id}.md

        `short_id` must be discriminative even when multiple signals share the
        same time_ms (different direction / poi_source). We therefore take the
        TAIL of signal_id — backtest ids look like
        ``{run_id}_{time_ms}_{dir}_{poi}`` where the distinguishing bits live
        at the end.
        """
        sid = (self.signal_id or "na").replace("/", "_").replace(" ", "_")
        short = sid[-16:]
        tms = self.time_ms if self.time_ms is not None else 0
        return f"{self.source}_{self.symbol}_{tms}_{short}.md"

    # ------------------------------------------------------------------
    def to_markdown(self) -> str:
        """YAML frontmatter + natural-language body + raw numeric table."""
        d = self.to_dict()

        # --- frontmatter (small whitelist so md stays readable) -------
        fm_lines = ["---"]
        for k in _FRONTMATTER_WHITELIST:
            fm_lines.append(f"{k}: {_yaml_scalar(d.get(k))}")
        fm_lines.append("---")
        frontmatter = "\n".join(fm_lines)

        # --- natural-language body ------------------------------------
        body = self._render_body()

        # --- raw numeric table ----------------------------------------
        table_rows = ["| field | value |", "| --- | --- |"]
        for k in sorted(d.keys()):
            if k == "extras":
                continue
            v = d[k]
            if isinstance(v, float):
                vs = f"{v:.6g}"
            elif v is None:
                vs = ""
            else:
                vs = str(v).replace("|", "\\|")
            table_rows.append(f"| {k} | {vs} |")
        table = "\n".join(table_rows)

        return f"{frontmatter}\n\n{body}\n\n## Raw fields\n\n{table}\n"

    # ------------------------------------------------------------------
    def _render_body(self) -> str:
        """Natural-language summary — the text that embeddings will index."""
        d = self.to_dict()

        direction = (d.get("direction") or "?").capitalize()
        poi_type = d.get("poi_type") or "?"
        symbol = d.get("symbol") or "?"
        entry = d.get("entry")
        entry_s = f"{entry:.2f}" if isinstance(entry, (int, float)) else "?"
        h4 = d.get("h4_structure") or "?"
        hour_et = d.get("hour_et")
        session = _classify_session(hour_et)
        poi_source = d.get("poi_source") or "?"
        outcome = d.get("outcome") or "open"
        pnl_r = d.get("pnl_r")
        pnl_s = f"{pnl_r:+.2f}R" if isinstance(pnl_r, (int, float)) else "n/a"
        sweep_wick = d.get("sweep_wick_strong")
        sweep_delta = d.get("sweep_bar_delta")
        sweep_delta_dir = _classify_sweep_delta(sweep_delta, d.get("direction"))
        score_total = d.get("score_total")
        score_s = f"{score_total:.2f}" if isinstance(score_total, (int, float)) else "n/a"
        rr = d.get("rr")
        rr_s = f"{rr:.2f}" if isinstance(rr, (int, float)) else "n/a"
        src = d.get("source")
        run_id = d.get("backtest_run_id")
        run_tag = f" [run={run_id}]" if run_id else ""

        lines = [
            f"# {direction} {poi_type} on {symbol}{run_tag}",
            "",
            (
                f"{direction} {poi_type} setup on {symbol} at {entry_s}. "
                f"4h structure {h4}, {session} session (hour_et={hour_et}). "
                f"POI source: {poi_source}. "
                f"Total score: {score_s}, planned RR: {rr_s}. "
                f"Sweep wick strong: {sweep_wick}, delta {sweep_delta_dir}. "
                f"Outcome: {outcome}, pnl {pnl_s}. "
                f"Data source: {src}."
            ),
        ]

        # live-only commentary (only if present)
        if self.trader_notes:
            lines.extend(["", f"**Trader notes**: {self.trader_notes}"])
        if self.emotional_state:
            lines.append(f"**Emotional state**: {self.emotional_state}")
        if self.real_slippage_pct is not None:
            lines.append(f"**Real slippage**: {self.real_slippage_pct:.4f}%")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Small classifiers used by the body renderer
# ---------------------------------------------------------------------------

def _classify_session(hour_et: Any) -> str:
    try:
        h = int(hour_et)
    except (TypeError, ValueError):
        return "unknown"
    if 2 <= h < 8:
        return "Asia"
    if 8 <= h < 12:
        return "London"
    if 12 <= h < 17:
        return "NY"
    return "off-hours"


def _classify_sweep_delta(delta: Any, direction: Any) -> str:
    try:
        d = float(delta)
    except (TypeError, ValueError):
        return "unknown"
    if direction == "long":
        return "same-side" if d >= 0 else "opposite-side"
    if direction == "short":
        return "same-side" if d <= 0 else "opposite-side"
    return "neutral"
