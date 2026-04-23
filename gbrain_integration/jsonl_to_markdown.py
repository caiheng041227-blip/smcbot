"""JSONL -> Markdown renderer.

Scans `gbrain_integration/jsonl/*.jsonl` and writes one md file per record to
`gbrain_integration/out_markdown/`. Filename is deterministic
({source}_{symbol}_{time_ms}_{short_id}.md) so reruns overwrite the same
file — fully idempotent.

Outcome-update events (produced by logger.update_outcome) are skipped here;
they are merged back into their signal rows in a separate reducer step
(TODO: see README). For now we just render the original signal rows.

Usage
-----
    python gbrain_integration/jsonl_to_markdown.py
    python gbrain_integration/jsonl_to_markdown.py --jsonl-dir custom/path --out-dir out/
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).resolve().parent
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(_HERE.parent))
    from gbrain_integration.schema import TradeRecord  # type: ignore
else:
    from .schema import TradeRecord


DEFAULT_JSONL_DIR = _HERE / "jsonl"
DEFAULT_OUT_DIR = _HERE / "out_markdown"


def _dict_to_record(d: dict) -> TradeRecord:
    """Rebuild a TradeRecord from a plain dict (filters unknown keys)."""
    valid = {f for f in TradeRecord.__dataclass_fields__.keys()}
    filtered = {k: v for k, v in d.items() if k in valid}
    # required field fallback
    filtered.setdefault("signal_id", "unknown")
    filtered.setdefault("source", "unknown")
    return TradeRecord(**filtered)


def _is_signal_row(d: dict) -> bool:
    """Filter: skip outcome-update events produced by logger.update_outcome."""
    return d.get("_event_type") not in ("outcome_update",)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Render JSONL records to markdown for GBrain import.")
    p.add_argument("--jsonl-dir", type=Path, default=DEFAULT_JSONL_DIR)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--glob", default="*.jsonl", help="File glob inside jsonl-dir")
    args = p.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(args.jsonl_dir.glob(args.glob))
    if not files:
        print(f"[warn] no JSONL files found in {args.jsonl_dir}", file=sys.stderr)
        return 1

    written = 0
    skipped = 0
    errors = 0
    for jf in files:
        print(f"[read] {jf.name}")
        with jf.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    if not _is_signal_row(d):
                        skipped += 1
                        continue
                    rec = _dict_to_record(d)
                    md = rec.to_markdown()
                    target = args.out_dir / rec.filename()
                    target.write_text(md, encoding="utf-8")
                    written += 1
                except Exception as e:
                    errors += 1
                    print(f"  [error] {jf.name}:{lineno} -> {e}", file=sys.stderr)

    print(f"[ok] wrote {written} markdown files -> {args.out_dir}")
    if skipped:
        print(f"     skipped {skipped} non-signal events (e.g. outcome_update)")
    if errors:
        print(f"     {errors} row(s) failed to render")
    return 0 if errors == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
