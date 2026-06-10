"""信号状态数据结构 —— SignalStep / SignalState。

2026-06-10 从 smc_signal.state_machine 迁出(SMC 管线删除时保留共享数据结构)。
DB 持久化 / TG formatter / SignalTracker / gbrain logger 都 duck-type 这两个类,
字段保持向后兼容(含 SMC 时代字段,如 scores / sweep_* / choch_*),
旧 DB 行和历史信号仍能正常 hydrate / 展示。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class SignalStep(str, Enum):
    IDLE = "IDLE"
    STEP1_PASSED = "STEP1_PASSED"   # 4H 方向共识(D1 不冲突)
    STEP2_PASSED = "STEP2_PASSED"   # POI 已锁定(找到方向匹配的未回测 OB)
    STEP3_PASSED = "STEP3_PASSED"   # 扫荡发生在 POI 区间内 + wick/delta 至少一个确认
    STEP4_PASSED = "STEP4_PASSED"   # [已弃用] 旧流程的"找 POI"步骤,新版合并到 Step2
    STEP5_PASSED = "STEP5_PASSED"   # POI 内 15m CHoCH 确认
    STEP6_PASSED = "STEP6_PASSED"   # 入场(SL+TP 用户手动)
    SCORED = "SCORED"
    NOTIFIED = "NOTIFIED"
    EXPIRED = "EXPIRED"              # 时间 TTL 超时
    INVALIDATED = "INVALIDATED"      # 场景前提失效(结构翻转 / POI 破坏 / 重复 POI)


@dataclass
class SignalState:
    signal_id: str
    symbol: str
    direction: str                          # "long" / "short"
    step: SignalStep
    triggered_level: Optional[str] = None
    triggered_level_value: Optional[float] = None    # 触发价位的实际价
    step1_trigger_price: Optional[float] = None      # Step1 创建时的 4h 收盘价,用于漂移失效检查
    step1_override_by_sweep: bool = False            # Step1 是否通过 1h 冲突 + deep sweep override 放行(用于 C10 评分)
    liquidity_sweep_candle: Optional[dict] = None
    sweep_bar_delta: Optional[float] = None          # Step3 1h sweep K delta(证据 + C_Delta 评分用)
    sweep_bar_volume: Optional[float] = None         # Step3 1h sweep K volume(算 delta/vol ratio 用)
    sweep_wick_strong: Optional[bool] = None         # Step3 长影 >= 实体 × sweep_wick_ratio
    poi_type: Optional[str] = None
    poi_source: Optional[str] = None                 # "ict_ob" / "ict_ote" / "ict_liquidity_raid" / ...
    poi_high: Optional[float] = None
    poi_low: Optional[float] = None
    poi_origin_time: Optional[int] = None            # POI 候选 K 线 open_time
    sweep_poi_break_count: int = 0                   # sweep POI 连续 1h close 穿破计数
    poi_displacement_time: Optional[int] = None      # OB 位移 K 线 open_time
    poi_displacement_mult: Optional[float] = None    # OB 位移 K 线 body / OB body
    poi_vp_poc: Optional[float] = None               # OB 检测时的 VP POC
    choch_break_price: Optional[float] = None        # Step5 突破确认收盘价
    choch_break_time: Optional[int] = None           # Step5 突破 K 线 open_time
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward: Optional[float] = None
    # 入场模式:
    #   "limit"     - entry_price 是挂单价,tracker 等价格回测才成交
    #   "immediate" - 信号触发时价格已在位,tracker 立即按 entry_price 接管
    entry_mode: str = "limit"
    scores: dict = field(default_factory=dict)
    total_score: float = 0.0
    created_at: int = field(default_factory=lambda: int(time.time()))
    updated_at: int = field(default_factory=lambda: int(time.time()))
    expires_at: int = 0
    notification_sent: bool = False
    scored_persisted: bool = False  # 已写入 DB(SCORED 时落盘,无论是否过 threshold)
    # source_engine 标识信号来源 engine(TG 前缀 / tracker trail 模式 / 统计分组)。
    # SMC 管线已删除(2026-06-10),现役信号均为 "ict";历史 DB 行可能仍是 "smc"。
    source_engine: str = "ict"
