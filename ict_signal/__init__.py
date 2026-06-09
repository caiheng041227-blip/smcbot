"""ICT (Inner Circle Trader) 信号引擎。

与 smc_signal 完全独立的 pipeline:
  - 自己的状态机(ICTSignalEngine)
  - 自己的入场逻辑(iFVG reversed,后续扩 OTE / OB)
  - 自己的 HTF gates(daily bias + premium/discount)
  - 自己的诊断计数

由 main.py 与 SMC engine **并行**驱动 ——
每根 K 线 close 时,两个 engine 各自跑一遍,各自发信号到 SignalTracker。

Tracker / TG / DB 用 source_engine 字段区分 SMC vs ICT 信号。
"""
from ict_signal.state_machine import ICTSignalEngine

__all__ = ["ICTSignalEngine"]
