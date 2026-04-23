"""GBrain knowledge-graph memory integration for the SMC signal bot.

Sub-modules
-----------
- schema           : TradeRecord dataclass (single canonical record shape)
- logger           : append live/paper signals to JSONL (never raises)
- import_backtest_csv : CSV -> JSONL importer (CLI)
- jsonl_to_markdown   : JSONL -> markdown renderer (CLI, idempotent)
"""

from __future__ import annotations

from .schema import TradeRecord
from .logger import log_signal, update_outcome

__all__ = ["TradeRecord", "log_signal", "update_outcome"]
