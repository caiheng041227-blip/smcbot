"""历史相似走势检索(文字推送版,无画图)。

灵感来自 AAAI26 ArchetypeTrader Phase I:不复刻 RL/VQ-VAE,只移植"把当前走势编码、
从历史里检索最相似的几个"这个核心思路。

设计:
  - 7 channel(OHLC + log_return + atr_normalized + volume_normalized)
  - 3 scale 加权融合(24/48/72 根 → 权重 0.5/0.3/0.2)
  - z-score normalize per channel per scale
  - cosine 相似度
  - Leakage 防护:历史滑窗末尾必须 < anchor - 48h

每次 NOTIFIED 信号触发时:
  1. find_similar(anchor_time) → top-3 历史相似窗口
  2. _format_text_message → 列出时间范围 + 后续走向数据
  3. send_text 到 Telegram (用户自己去 OKX 查对应时间的 K 线)

成本:每次信号 ~200-300ms,启动时一次性预算 z-score 滑窗矩阵约 2-5s。
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiosqlite
import numpy as np

from utils.logger import logger


# --- 常量 ----------------------------------------------------------------

SCALES: List[tuple] = [(72, 0.5), (120, 0.3), (168, 0.2)]  # 3天 / 5天 / 1周
# 旧版用 24/48/72(1/2/3天)发现 24h 太短巧合相似多,改用 72/120/168 抓真正的中期形态
N_CHANNELS = 7  # OHLC + log_return + atr_normalized + volume_normalized
MIN_SIMILARITY = 0.40  # 综合 cosine < 此值 → 不推送(宏观 gate 过滤后池子小,
                       # 真实最高约 0.55,0.40 是合理底线)
DISPLAY_NOW_BARS = 72       # NOW 子图显示的 K 线数(3 天上下文)
DISPLAY_FOLLOW_BARS = 48    # Similar 子图后续 AFTER 段长度(2 天)
# 最少历史数据要求(用户硬要求:不足 2 年的历史样本无参考价值,直接禁用功能)
MIN_HISTORY_BARS = 17000    # ≈ 2 年 1h(2y × 365 × 24 = 17520,留点余量)
# Leakage buffer:历史相似窗口的 end_time 必须早于 anchor - 此值
# 用户要求:不能匹配半年内的相似走势(避免"5 天前的同段下行"被算成 Top 1)
# 同时也覆盖了"防 follow 段 48h 渗入 anchor"的原始 leakage 需求
LEAKAGE_BUFFER_BARS = 4320  # 180 天 × 24h
ATR_PERIOD = 14
VOL_MA_PERIOD = 14
# 宏观环境过滤(z-score 抹掉了大趋势,需要单独 gate)
MA30_BARS = 720     # 30 天 × 24h
MA90_BARS = 2160    # 90 天
YEAR_BARS = 8760    # 365 天
FIB_BARS = 4320     # 180 天 — 用于算 Fib retracement 端点
# 历史窗口的年内分位差 ≥ 此值 → 排除(0.25 = 25%,例如 NOW 是 0.2 底部区,
# 候选最多到 0.45 中下区,排除任何 0.5+ 中部/顶部区)
YEAR_POS_TOLERANCE = 0.25
# Fib 档位差容忍(0.20 允许跨 1 个相邻档,例如 NOW 在底部 [0.236-0.382],
# 候选可以在深底 [< 0.236] 或中下 [0.382-0.5])
FIB_POS_TOLERANCE = 0.20
# 是否要求 close 与 MA30/MA90 的相对位置同号(默认是)
REQUIRE_SAME_MA_REGIME = True

# Fib 标准档位边界(用于 _format_text_message 显示)
_FIB_ZONES = [
    (0.236, "深底 (< 0.236)"),
    (0.382, "底部 (0.236-0.382)"),
    (0.500, "中下 (0.382-0.5)"),
    (0.618, "中上 (0.5-0.618)"),
    (0.786, "高位 (0.618-0.786)"),
    (1.000, "顶部 (> 0.786)"),
]


def _fib_zone_label(pos: float) -> str:
    for upper, label in _FIB_ZONES:
        if pos < upper:
            return label
    return _FIB_ZONES[-1][1]


# --- 特征工程 ------------------------------------------------------------

def _compute_features(
    open_: np.ndarray, high: np.ndarray, low: np.ndarray,
    close: np.ndarray, volume: np.ndarray,
) -> np.ndarray:
    """从原始 OHLCV 算 7 channel features,返回 shape (N, 7)。
    前 ATR_PERIOD 根的 atr_normalized / volume_normalized / log_return 用兜底值
    (1.0 或 0.0)避免 NaN 干扰。"""
    n = len(close)
    feats = np.zeros((n, N_CHANNELS), dtype=np.float32)
    feats[:, 0] = open_
    feats[:, 1] = high
    feats[:, 2] = low
    feats[:, 3] = close

    # log_return: log(close[t] / close[t-1])
    log_ret = np.zeros(n, dtype=np.float32)
    log_ret[1:] = np.log(close[1:] / np.maximum(close[:-1], 1e-9))
    feats[:, 4] = log_ret

    # ATR(period=14):TR = max(high-low, |high-prev_close|, |low-prev_close|)
    tr = np.zeros(n, dtype=np.float32)
    tr[0] = high[0] - low[0]
    h_minus_pc = np.abs(high[1:] - close[:-1])
    l_minus_pc = np.abs(low[1:] - close[:-1])
    tr[1:] = np.maximum.reduce([high[1:] - low[1:], h_minus_pc, l_minus_pc])
    # 简单 SMA-ATR(够用,不用 Wilder)
    atr = np.full(n, np.nan, dtype=np.float32)
    if n >= ATR_PERIOD:
        cumsum = np.cumsum(tr)
        atr[ATR_PERIOD - 1:] = (cumsum[ATR_PERIOD - 1:] -
                                 np.concatenate([[0], cumsum[:-ATR_PERIOD]])) / ATR_PERIOD
    # atr_normalized = TR / ATR_14;早期未就绪时取 1.0
    atr_safe = np.where(np.isnan(atr) | (atr <= 0), 1.0, atr)
    feats[:, 5] = tr / np.maximum(atr_safe, 1e-9)

    # vol_normalized = vol / SMA(vol, 14);早期取 1.0
    vol_ma = np.full(n, np.nan, dtype=np.float32)
    if n >= VOL_MA_PERIOD:
        cumsum_v = np.cumsum(volume)
        vol_ma[VOL_MA_PERIOD - 1:] = (cumsum_v[VOL_MA_PERIOD - 1:] -
                                       np.concatenate([[0], cumsum_v[:-VOL_MA_PERIOD]])) / VOL_MA_PERIOD
    vol_ma_safe = np.where(np.isnan(vol_ma) | (vol_ma <= 0), 1.0, vol_ma)
    feats[:, 6] = volume / np.maximum(vol_ma_safe, 1e-9)

    return feats


def _zscore_window(window: np.ndarray) -> np.ndarray:
    """对单个窗口做 per-channel z-score,shape (W, 7) → (W, 7)。
    若某 channel std=0 则该 channel 置 0(避免除零 NaN)。"""
    mean = window.mean(axis=0, keepdims=True)
    std = window.std(axis=0, keepdims=True)
    std_safe = np.where(std > 1e-9, std, 1.0)
    normalized = (window - mean) / std_safe
    # std=0 的 channel 全置 0(信号本身不变化,无信息量)
    normalized = np.where(std > 1e-9, normalized, 0.0)
    # 防御:NaN / inf 兜底(罕见但保险)
    return np.nan_to_num(normalized, nan=0.0, posinf=0.0, neginf=0.0)


def _zscore_sliding_windows(feats: np.ndarray, w: int) -> np.ndarray:
    """对全量 features 做 w 大小的滑窗 z-score 归一化,返回 shape (N-w+1, w*7)。
    用 stride trick 避免拷贝。"""
    n = feats.shape[0]
    if n < w:
        return np.zeros((0, w * N_CHANNELS), dtype=np.float32)
    n_windows = n - w + 1
    # 用 np.lib.stride_tricks.sliding_window_view (numpy>=1.20)
    windows = np.lib.stride_tricks.sliding_window_view(feats, (w, N_CHANNELS))
    # windows.shape = (n_windows, 1, w, N_CHANNELS) → squeeze 那个 1
    windows = windows.reshape(n_windows, w, N_CHANNELS)
    # 矩阵化 z-score:每个窗口 per-channel
    mean = windows.mean(axis=1, keepdims=True)  # (n_windows, 1, 7)
    std = windows.std(axis=1, keepdims=True)
    std_safe = np.where(std > 1e-9, std, 1.0)
    normalized = (windows - mean) / std_safe
    normalized = np.where(std > 1e-9, normalized, 0.0)
    # flatten 成 (n_windows, w*7) 便于 cosine
    flat = normalized.reshape(n_windows, w * N_CHANNELS).astype(np.float32)
    flat = np.nan_to_num(flat, nan=0.0, posinf=0.0, neginf=0.0)
    # 预算 L2 norm,后续 cosine 用归一化向量点积
    norms = np.linalg.norm(flat, axis=1, keepdims=True)
    flat_unit = flat / np.maximum(norms, 1e-9)
    return np.nan_to_num(flat_unit, nan=0.0, posinf=0.0, neginf=0.0)


# --- 主类 ----------------------------------------------------------------

class PatternMatcher:
    """启动时一次性加载历史,每次 NOTIFIED 信号触发 process_signal。"""

    def __init__(
        self,
        db_path: str,
        telegram=None,  # 可选,无则只生成 PNG 不发
        symbol: str = "ETHUSDT",
        timeframe: str = "1h",
        top_k: int = 3,
    ):
        self.db_path = db_path
        self.telegram = telegram
        self.symbol = symbol
        self.timeframe = timeframe
        self.top_k = top_k

        self._hist_features: Optional[np.ndarray] = None  # (N, 7)
        self._hist_times: Optional[np.ndarray] = None     # (N,) open_time_ms
        self._hist_ohlc: Optional[np.ndarray] = None      # (N, 4) 原始 OHLC,render 用
        self._hist_volume: Optional[np.ndarray] = None    # (N,) 原始 volume,append 重算 features 用
        # 宏观特征(每个 idx 一个 3 维向量,未 z-score 归一化):
        # [close/MA30 - 1, close/MA90 - 1, year_position(0=底,1=顶)]
        self._macro_features: Optional[np.ndarray] = None  # (N, 3)
        # 每个 scale 预算好的 unit-norm z-score 滑窗:{w: (N-w+1, w*7)}
        self._normalized_windows: Dict[int, np.ndarray] = {}

    def n_history(self) -> int:
        return 0 if self._hist_features is None else len(self._hist_features)

    def _compute_macro_features(self) -> np.ndarray:
        """对每个 idx 算 5 个未归一化的宏观特征:
        - [0] close/MA30 - 1:在 30 天均线上/下多少
        - [1] close/MA90 - 1:在 90 天均线上/下多少
        - [2] year_position:近 365 天内位置(0=底,1=顶)
        - [3] ma90_slope_30d:MA90 vs 30 天前 MA90 的变化率(牛市回调 vs 熊市反弹)
        - [4] fib_pos_180d:近 180 天 Fib retracement 位置(0=底,1=顶,
              对应标准 Fib 档位 0.236/0.382/0.5/0.618/0.786)

        用 pandas rolling 矩阵化,17k 行 ~200ms,76k 行 ~1s。
        """
        import pandas as pd
        closes = self._hist_ohlc[:, 3].astype(np.float64)
        s = pd.Series(closes)
        ma30 = s.rolling(window=MA30_BARS, min_periods=1).mean().values
        ma90 = s.rolling(window=MA90_BARS, min_periods=1).mean().values
        y_low = s.rolling(window=YEAR_BARS, min_periods=1).min().values
        y_high = s.rolling(window=YEAR_BARS, min_periods=1).max().values
        # Fib retracement:180 天的 high/low
        fib_low = s.rolling(window=FIB_BARS, min_periods=1).min().values
        fib_high = s.rolling(window=FIB_BARS, min_periods=1).max().values

        # MA90 30 天斜率
        ma90_30d_ago = np.empty_like(ma90)
        ma90_30d_ago[:MA30_BARS] = ma90[:MA30_BARS]
        ma90_30d_ago[MA30_BARS:] = ma90[:-MA30_BARS]
        ma90_slope = (ma90 - ma90_30d_ago) / np.maximum(ma90_30d_ago, 1e-9)

        macro = np.column_stack([
            closes / np.maximum(ma30, 1e-9) - 1,
            closes / np.maximum(ma90, 1e-9) - 1,
            (closes - y_low) / np.maximum(y_high - y_low, 1e-9),
            ma90_slope,
            (closes - fib_low) / np.maximum(fib_high - fib_low, 1e-9),
        ]).astype(np.float32)
        return np.nan_to_num(macro, nan=0.0, posinf=0.0, neginf=0.0)

    async def load_history(self) -> int:
        """从 SQLite 拉所有历史 K,算 features + 预算 z-score 滑窗。
        返回加载的根数。若 < MIN_HISTORY_BARS,禁用 matcher 并返回 0。"""
        t0 = time.time()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT open_time, open, high, low, close, volume "
                "FROM candles WHERE symbol=? AND timeframe=? ORDER BY open_time ASC",
                (self.symbol, self.timeframe),
            ) as cur:
                rows = await cur.fetchall()

        if not rows:
            logger.warning(f"PatternMatcher: DB 内 {self.symbol} {self.timeframe} 无历史")
            return 0

        # 硬要求:< 2 年数据无参考价值,直接禁用
        if len(rows) < MIN_HISTORY_BARS:
            logger.warning(
                f"PatternMatcher: DB 内 {self.symbol} {self.timeframe} 仅 {len(rows)} 根历史 "
                f"(< {MIN_HISTORY_BARS} = 约 2 年硬下限),禁用 matcher。"
                f"先跑 `python scripts/backfill_history_okx.py --days 730` 补足历史。"
            )
            return 0

        ts = np.array([r[0] for r in rows], dtype=np.int64)
        o = np.array([r[1] for r in rows], dtype=np.float32)
        h = np.array([r[2] for r in rows], dtype=np.float32)
        l = np.array([r[3] for r in rows], dtype=np.float32)
        c = np.array([r[4] for r in rows], dtype=np.float32)
        v = np.array([r[5] for r in rows], dtype=np.float32)

        self._hist_times = ts
        self._hist_ohlc = np.stack([o, h, l, c], axis=1)
        self._hist_volume = v
        self._hist_features = _compute_features(o, h, l, c, v)

        # 预算 3 个 scale 的滑窗 z-score 矩阵
        for w, _weight in SCALES:
            self._normalized_windows[w] = _zscore_sliding_windows(self._hist_features, w)

        # 算宏观特征(用 boolean gate 过滤掉大趋势位置不一致的历史)
        self._macro_features = self._compute_macro_features()

        elapsed = time.time() - t0
        scale_shape_str = ", ".join(
            f"{w}h={self._normalized_windows[w].shape}" for w, _ in SCALES
        )
        logger.info(
            f"PatternMatcher 加载 {len(rows)} 根 {self.symbol} {self.timeframe},"
            f"预算 {len(SCALES)} 个 scale 滑窗({scale_shape_str})"
            f"+ macro {self._macro_features.shape},耗时 {elapsed:.2f}s"
        )
        return len(rows)

    def find_similar(self, anchor_time_ms: int) -> List[Dict]:
        """以 anchor 为终点取最近 W 根 features,对每个 scale 算 cosine,
        3 scale 加权融合 → top-k 综合最相似窗口。

        Leakage 防护:历史滑窗的 end_time_ms 必须 < anchor - LEAKAGE_BUFFER_BARS × 1h。
        且必须留出 +24h "后续" 给 render(即 end_time + 24h ≤ anchor - 24h 也成立)。

        返回 [{
            similar_start_time, similar_end_time, weighted_sim,
            sim_24, sim_48, sim_72,
            similar_ohlc:   (24, 4) 该相似窗口的 OHLC,
            follow_ohlc:    (24, 4) 紧接着的 24h OHLC,可能不足 24 根 → 截短,
        }, ...] (按 weighted_sim 降序,过滤 sim < MIN_SIMILARITY)
        """
        if self._hist_features is None or len(self._hist_features) == 0:
            return []

        # 找 anchor 在 hist_times 里的位置(open_time 严格小于 anchor 的最大 idx)
        anchor_idx = int(np.searchsorted(self._hist_times, anchor_time_ms, side="right")) - 1
        if anchor_idx < 0:
            logger.warning(f"PatternMatcher: anchor_time_ms={anchor_time_ms} 早于所有历史 K")
            return []

        # 检查主 scale (24) 数据够不够
        main_w = SCALES[0][0]
        max_w = max(w for w, _ in SCALES)
        if anchor_idx < max_w - 1:
            logger.warning(
                f"PatternMatcher: anchor 位置 {anchor_idx} 不足以容纳最大 scale {max_w}"
            )
            return []

        # 各 scale 算相似度
        scale_sims: Dict[int, np.ndarray] = {}
        for w, _weight in SCALES:
            # anchor 窗口:从 anchor_idx 往回 w 根(含 anchor_idx 自己)
            anchor_window = self._hist_features[anchor_idx - w + 1: anchor_idx + 1]
            anchor_normalized = _zscore_window(anchor_window).reshape(-1)
            anchor_norm = np.linalg.norm(anchor_normalized)
            if anchor_norm < 1e-9:
                # anchor 自身无变化(罕见,如插值平段)→ 该 scale 全部相似度 = 0
                scale_sims[w] = np.zeros(self._normalized_windows[w].shape[0], dtype=np.float32)
                continue
            anchor_unit = anchor_normalized / anchor_norm
            # 所有历史滑窗已经是 unit-norm → cosine = unit_hist · anchor_unit
            # errstate:nan_to_num 已防御,中间过程的 warning 可静默
            with np.errstate(all="ignore"):
                sims_raw = self._normalized_windows[w] @ anchor_unit
            scale_sims[w] = np.nan_to_num(sims_raw, nan=0.0, posinf=0.0, neginf=0.0)

        # 综合加权 — 但 3 个 scale 的滑窗数量不同(N-w+1),要对齐到同一索引空间
        # 约定:每个 scale 的 "window i" 表示 hist_features[i : i+w] 这个窗口
        # → 它的 "end open_time" = hist_times[i + w - 1]
        # 为了对齐,我们以"窗口的 end_idx" 为统一坐标(即 hist 上的最后一根)
        # 对于 scale w,window_i 的 end_idx = i + w - 1
        # 我们要找的是 "end_idx ≤ anchor_idx - LEAKAGE_BUFFER_BARS - 24"(即留 24h 给 follow)
        # 用一个 dict: end_idx → {sim_24, sim_48, sim_72}
        leakage_cutoff_idx = anchor_idx - LEAKAGE_BUFFER_BARS  # 历史滑窗 end_idx 必须 ≤ 这个值
        # 给 render 留 +24h 后续:end_idx + 24 ≤ N - 1 也要满足(下面 follow 提取时再检查)

        # 找出最大的可用 end_idx(对每个 scale 都要存在该窗口 + 不越界)
        # 取 end_idx 范围:[max_w - 1, leakage_cutoff_idx]
        if leakage_cutoff_idx < max_w - 1:
            logger.warning("PatternMatcher: leakage 缓冲后无可用历史滑窗")
            return []
        end_indices = np.arange(max_w - 1, leakage_cutoff_idx + 1, dtype=np.int64)

        # 各 scale 对应的 window_idx = end_idx - (w - 1)
        weighted = np.zeros(len(end_indices), dtype=np.float32)
        per_scale: Dict[int, np.ndarray] = {}
        for w, weight in SCALES:
            window_indices = end_indices - (w - 1)
            sims = scale_sims[w][window_indices]
            per_scale[w] = sims
            weighted = weighted + weight * sims

        # === 宏观 gate(关键修复:z-score 抹掉了大趋势,在 cosine 之前加硬过滤)===
        macro_mask = np.ones(len(end_indices), dtype=bool)
        if self._macro_features is not None:
            anchor_macro = self._macro_features[anchor_idx]  # shape (3,)
            hist_macro = self._macro_features[end_indices]   # shape (n_candidates, 3)
            if REQUIRE_SAME_MA_REGIME:
                # close 与 MA30 同号(都在均线上 / 都在均线下)
                ma30_same = np.sign(hist_macro[:, 0]) == np.sign(anchor_macro[0])
                # close 与 MA90 同号
                ma90_same = np.sign(hist_macro[:, 1]) == np.sign(anchor_macro[1])
                macro_mask &= (ma30_same & ma90_same)
            # 年内分位差 < YEAR_POS_TOLERANCE(防止"底部" vs "顶部"配对)
            year_diff = np.abs(hist_macro[:, 2] - anchor_macro[2])
            macro_mask &= (year_diff < YEAR_POS_TOLERANCE)
            # MA90 30 天斜率同号(区分"牛市回调"vs"熊市反弹")
            ma90_slope_same = np.sign(hist_macro[:, 3]) == np.sign(anchor_macro[3])
            macro_mask &= ma90_slope_same
            # Fib retracement 档位差 < FIB_POS_TOLERANCE(允许跨 1 相邻 Fib 档)
            fib_diff = np.abs(hist_macro[:, 4] - anchor_macro[4])
            macro_mask &= (fib_diff < FIB_POS_TOLERANCE)
            n_gated = int(macro_mask.sum())
            logger.debug(
                f"PatternMatcher macro gate:从 {len(end_indices)} 候选筛到 {n_gated} "
                f"(anchor: close/MA30={anchor_macro[0]:+.2%}, close/MA90={anchor_macro[1]:+.2%}, "
                f"year_pos={anchor_macro[2]:.2f}, ma90_slope={anchor_macro[3]:+.2%}, "
                f"fib_180d={anchor_macro[4]:.2f})"
            )

        # 过滤太低的 + 宏观环境不符的
        mask = (weighted >= MIN_SIMILARITY) & macro_mask
        if not mask.any():
            return []

        # top-k:取 weighted[mask] 最大的 top_k 个,但同时要去重相邻
        valid_end_indices = end_indices[mask]
        valid_weighted = weighted[mask]
        # 按相似度降序
        order = np.argsort(-valid_weighted)
        # 去重:避免 top-3 都集中在同一段(end_idx 相差 < 主 scale 的就视为同一段)
        selected: List[int] = []
        min_gap = main_w
        for idx in order:
            ei = int(valid_end_indices[idx])
            if all(abs(ei - s) >= min_gap for s in selected):
                selected.append(ei)
                if len(selected) >= self.top_k:
                    break

        # 组装返回
        out: List[Dict] = []
        for end_idx in selected:
            start_idx = end_idx - main_w + 1
            similar_ohlc = self._hist_ohlc[start_idx: end_idx + 1].copy()
            # follow:相似窗口之后的 DISPLAY_FOLLOW_BARS 根(默认 48 = 2 天)
            follow_end = min(end_idx + 1 + DISPLAY_FOLLOW_BARS, len(self._hist_ohlc))
            follow_ohlc = self._hist_ohlc[end_idx + 1: follow_end].copy()
            # 该 end_idx 对应到 valid 数组的位置
            valid_pos = int(np.where(valid_end_indices == end_idx)[0][0])
            global_pos = int(np.where(end_indices == end_idx)[0][0])
            out.append({
                "similar_start_time": int(self._hist_times[start_idx]),
                "similar_end_time": int(self._hist_times[end_idx]),
                "weighted_sim": float(valid_weighted[valid_pos]),
                "per_scale_sim": {w: float(per_scale[w][global_pos]) for w, _ in SCALES},
                "similar_ohlc": similar_ohlc,
                "follow_ohlc": follow_ohlc,
            })
        return out

    def append_candle(self, open_time_ms: int, o: float, h: float, l: float,
                       c: float, v: float) -> None:
        """新 1h close 时增量追加一根 K + 更新 3 个 scale 的最末一个滑窗。
        若该 open_time 已存在(去重),跳过。"""
        if self._hist_times is None or len(self._hist_times) == 0:
            logger.warning("PatternMatcher.append_candle: 未 load_history,跳过")
            return
        if open_time_ms <= int(self._hist_times[-1]):
            return  # 已存在或更早,无需追加

        # 拼到 OHLC / times / volume 数组末尾
        self._hist_times = np.append(self._hist_times, np.int64(open_time_ms))
        self._hist_ohlc = np.vstack([self._hist_ohlc, [[o, h, l, c]]])
        self._hist_volume = np.append(self._hist_volume, np.float32(v))

        # 重算全段 features(atr/vol_ma/log_ret 依赖前几根,增量逻辑易出错;
        # N=25k 时 ~10ms,每 1h 一次完全可接受)
        ohlc = self._hist_ohlc
        self._hist_features = _compute_features(
            ohlc[:, 0].astype(np.float32),
            ohlc[:, 1].astype(np.float32),
            ohlc[:, 2].astype(np.float32),
            ohlc[:, 3].astype(np.float32),
            self._hist_volume.astype(np.float32),
        )

        # 重算 3 个 scale 滑窗 + 宏观特征(append 频率 = 每 1h 1 次,可接受)
        for w, _weight in SCALES:
            self._normalized_windows[w] = _zscore_sliding_windows(self._hist_features, w)
        self._macro_features = self._compute_macro_features()

    def get_anchor_window_ohlc(self, anchor_time_ms: int, w: int = 24) -> Optional[np.ndarray]:
        """提取 anchor 处 W 根的 OHLC(render 当前走势用),shape (W, 4) 或 None。"""
        if self._hist_features is None:
            return None
        anchor_idx = int(np.searchsorted(self._hist_times, anchor_time_ms, side="right")) - 1
        if anchor_idx < w - 1:
            return None
        return self._hist_ohlc[anchor_idx - w + 1: anchor_idx + 1].copy()

    def _format_text_message(
        self,
        anchor_time_ms: int,
        anchor_ohlc: np.ndarray,
        similar_list: List[Dict],
        signal_info: Optional[Dict] = None,
    ) -> str:
        """格式化文字消息:列出 top-k 历史相似窗口的时间范围 + sim + 后续走向。
        用户自己去 OKX/TradingView 看具体 K 线。"""
        anchor_iso = datetime.fromtimestamp(
            anchor_time_ms / 1000.0, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M UTC")
        # NOW 涨跌
        now_open = float(anchor_ohlc[0, 3])
        now_close = float(anchor_ohlc[-1, 3])
        now_pct = (now_close - now_open) / now_open * 100

        # 当前宏观特征(让用户看到环境对得上)
        anchor_idx = int(np.searchsorted(self._hist_times, anchor_time_ms, side="right")) - 1
        macro_str = ""
        if self._macro_features is not None and 0 <= anchor_idx < len(self._macro_features):
            mac = self._macro_features[anchor_idx]
            yr = mac[2]
            if yr < 0.3: yr_label = "底部区"
            elif yr < 0.7: yr_label = "中部区"
            else: yr_label = "顶部区"
            ma30_state = "上" if mac[0] > 0 else "下"
            ma90_state = "上" if mac[1] > 0 else "下"
            ma90_trend = "上行" if mac[3] > 0 else "下行"
            fib_label = _fib_zone_label(float(mac[4]))
            macro_str = (f"宏观: MA30 {ma30_state}方({mac[0]*100:+.1f}%) / "
                         f"MA90 {ma90_state}方({mac[1]*100:+.1f}%) / "
                         f"MA90 {ma90_trend}({mac[3]*100:+.1f}%) / 年内 {yr_label}({yr:.2f}) / "
                         f"180d Fib {fib_label}={mac[4]:.2f}")

        # 动态显示历史跨度(从 DB 实际数据算)
        if self._hist_times is not None and len(self._hist_times) > 1:
            years_span = (int(self._hist_times[-1]) - int(self._hist_times[0])) / (365 * 86400 * 1000)
            span_label = f"{years_span:.1f} 年历史"
        else:
            span_label = "历史"

        lines = [
            f"🔍 历史相似走势 ({self.symbol} {self.timeframe}, {span_label})",
            f"NOW: {anchor_iso}, 过去 {len(anchor_ohlc)}h 净 {now_pct:+.2f}%",
        ]
        if macro_str:
            lines.append(macro_str)
        lines.append("")
        for i, s in enumerate(similar_list):
            st = datetime.fromtimestamp(
                s["similar_start_time"] / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M")
            et = datetime.fromtimestamp(
                s["similar_end_time"] / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M")
            base = float(s["similar_ohlc"][0, 3])
            sim_end = float(s["similar_ohlc"][-1, 3])
            sim_pct = (sim_end - base) / base * 100
            if len(s["follow_ohlc"]) > 0:
                follow_end = float(s["follow_ohlc"][-1, 3])
                follow_pct = (follow_end - sim_end) / sim_end * 100
                max_up = (float(s["follow_ohlc"][:, 1].max()) - sim_end) / sim_end * 100
                max_down = (float(s["follow_ohlc"][:, 2].min()) - sim_end) / sim_end * 100
                follow_str = (f"净 {follow_pct:+.2f}% "
                              f"(高 {max_up:+.2f}% / 低 {max_down:+.2f}%)")
            else:
                follow_str = "(无后续数据)"
            # 多 scale 分量(动态根据 SCALES)
            per_scale = s.get("per_scale_sim") or {}
            scale_parts = "/".join(f"{w}h={per_scale.get(w, 0):.2f}" for w, _ in SCALES)
            # 该相似窗口当时的 Fib 档位
            sim_fib_str = ""
            if self._macro_features is not None:
                et_idx = int(np.searchsorted(self._hist_times, s["similar_end_time"]))
                if 0 <= et_idx < len(self._macro_features):
                    sim_fib = float(self._macro_features[et_idx, 4])
                    sim_fib_str = f"  当时 Fib={sim_fib:.2f} ({_fib_zone_label(sim_fib)})"
            lines.append(
                f"Top {i+1}: {st} → {et} UTC  sim={s['weighted_sim']:.2f} "
                f"({scale_parts})"
            )
            main_w = SCALES[0][0]
            lines.append(f"  那 {main_w}h: {sim_pct:+.2f}% | 后续 {DISPLAY_FOLLOW_BARS}h: {follow_str}{sim_fib_str}")
        return "\n".join(lines)

    async def process_signal(self, signal: Dict) -> Optional[str]:
        """端到端:NOTIFIED 信号 → find_similar → 文字消息 → send_text。

        signal 至少包含:
          - notified_at (ms)
          - direction / entry_price (可选,文字消息里不显示但保留接口)
          - signal_id (可选)

        异常完全 catch,失败返回 None 不影响主流程。
        返回文字消息字符串(成功)或 None(失败 / 无相似窗口)。
        """
        try:
            anchor_t = int(signal.get("notified_at") or signal.get("close_time") or 0)
            if anchor_t <= 0:
                logger.warning("PatternMatcher.process_signal: 无 anchor_t,跳过")
                return None

            similar = self.find_similar(anchor_t)
            if not similar:
                logger.info(f"PatternMatcher: 信号 {signal.get('signal_id','?')[:8]} "
                            f"无 ≥{MIN_SIMILARITY} 相似窗口,跳过推送")
                return None

            anchor_ohlc = self.get_anchor_window_ohlc(anchor_t, w=DISPLAY_NOW_BARS)
            if anchor_ohlc is None:
                return None

            signal_info = {
                "direction": signal.get("direction"),
                "entry": signal.get("entry_price"),
            }
            message = self._format_text_message(
                anchor_t, anchor_ohlc, similar, signal_info=signal_info
            )

            if self.telegram is not None:
                try:
                    await self.telegram.send_text(message, parse_mode=None)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"PatternMatcher: send_text 失败 {e}")
            return message
        except Exception as e:  # noqa: BLE001
            logger.error(f"PatternMatcher.process_signal 异常: {e}", exc_info=True)
            return None
