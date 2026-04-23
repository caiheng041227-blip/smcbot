"""信号层入口：暴露状态机 + 评分函数给 main.py。"""
from __future__ import annotations

from .scorer import score
from .state_machine import SignalEngine, SignalState, SignalStep

__all__ = ["SignalEngine", "SignalState", "SignalStep", "score"]
