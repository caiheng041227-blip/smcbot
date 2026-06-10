"""ICT (Inner Circle Trader) 信号引擎 —— 项目唯一信号 pipeline。

(SMC 管线已于 2026-06-10 删除:365d ΣR -16.9 / 730d -146.8,全 POI source 两年为负。)

  - 状态机:ICTSignalEngine(4h/1h close 驱动,4 POI 检测器 + HTF gates)
  - 数据结构:SignalState / SignalStep(原 smc_signal 迁入,DB/TG/tracker 共用)
  - 信号经 SignalTracker 跟踪 TP1/SL/trail,经 TelegramNotifier 推送
"""
from ict_signal.signal_state import SignalState, SignalStep
from ict_signal.state_machine import ICTSignalEngine

__all__ = ["ICTSignalEngine", "SignalState", "SignalStep"]
