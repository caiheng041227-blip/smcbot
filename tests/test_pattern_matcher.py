"""PatternMatcher 单元测试 + 30 天数据 smoke 测试。"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.pattern_matcher import (
    PatternMatcher, SCALES, MIN_SIMILARITY, LEAKAGE_BUFFER_BARS,
    _compute_features, _zscore_window, _zscore_sliding_windows, N_CHANNELS,
)


# --- 单元测试:数学正确性 ------------------------------------------------

def test_compute_features_shape():
    o = np.array([100, 101, 102, 103, 104], dtype=np.float32)
    h = o + 0.5
    l = o - 0.5
    c = o + 0.2
    v = np.array([1.0] * 5, dtype=np.float32)
    feats = _compute_features(o, h, l, c, v)
    assert feats.shape == (5, N_CHANNELS)
    # log_return[0] 应为 0(无 prev)
    assert feats[0, 4] == 0.0
    # log_return[1] = log(101.2/100.2) ≈ 0.00994
    assert abs(feats[1, 4] - np.log(101.2 / 100.2)) < 1e-5


def test_zscore_window_zero_std_safe():
    # 完全平坦的 channel
    w = np.array([[100, 100, 100, 100, 0, 1, 1]] * 24, dtype=np.float32)
    out = _zscore_window(w)
    # 所有元素应为 0(std=0 时安全 fallback)
    assert np.allclose(out, 0.0)


def test_zscore_sliding_windows_basic():
    # 100 根 K,7 channel → 24 滑窗共 77 个
    feats = np.random.randn(100, 7).astype(np.float32)
    out = _zscore_sliding_windows(feats, 24)
    assert out.shape == (77, 24 * 7)
    # 每行应该是 unit-norm
    norms = np.linalg.norm(out, axis=1)
    # 允许少数行因 std=0 被置零(应该是 unit 或 0)
    for n in norms:
        assert abs(n - 1.0) < 1e-4 or n < 1e-4


def test_zscore_window_inverted_anti_correlated():
    """正向走势 vs 反向走势,z-score normalized 后的 close channel 应反相关。"""
    n = 24
    o = np.linspace(100, 110, n).astype(np.float32)
    h = o + 0.5
    l = o - 0.5
    c_up = np.linspace(100, 110, n).astype(np.float32)
    c_down = np.linspace(110, 100, n).astype(np.float32)
    v = np.ones(n, dtype=np.float32)

    feats_up = _compute_features(o, h, l, c_up, v)
    feats_down = _compute_features(o, h, l, c_down, v)

    norm_up = _zscore_window(feats_up).reshape(-1)
    norm_down = _zscore_window(feats_down).reshape(-1)

    # 完全反向的 close 应该让整体 cosine 接近 -1?但 OHLC 4 个 channel 中 O/H/L 都是涨的,
    # 只有 close 反了,所以整体 cosine 不会是 -1,但 close-only 部分应反向
    # 取 close channel(idx=3)位置
    close_indices = np.arange(3, n * N_CHANNELS, N_CHANNELS)
    cos_close = np.dot(norm_up[close_indices], norm_down[close_indices]) / (
        np.linalg.norm(norm_up[close_indices]) * np.linalg.norm(norm_down[close_indices])
    )
    assert cos_close < -0.9, f"close 反向走势 cosine 应 ≈ -1,实际 {cos_close}"


# --- 30 天数据 smoke 测试 ------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "market.db"


@pytest.mark.skipif(not DB_PATH.exists(), reason="market.db 不存在 (先跑 backfill_history_okx.py)")
def test_load_history_smoke():
    """加载 DB 历史,验证 features + 滑窗形状。"""
    matcher = PatternMatcher(db_path=str(DB_PATH), symbol="ETHUSDT", timeframe="1h")
    n = asyncio.run(matcher.load_history())
    assert n > 100, f"DB 历史 K 太少 ({n}),先跑 backfill 30 天"
    assert matcher._hist_features.shape[1] == N_CHANNELS
    for w, _ in SCALES:
        assert w in matcher._normalized_windows
        n_windows_w = matcher._normalized_windows[w].shape[0]
        assert n_windows_w == n - w + 1


@pytest.mark.skipif(not DB_PATH.exists(), reason="market.db 不存在")
def test_find_similar_self_anchor():
    """anchor 设在中间某个时刻,验证返回 top-k 都是合法的、weighted_sim 降序。"""
    matcher = PatternMatcher(db_path=str(DB_PATH), symbol="ETHUSDT", timeframe="1h", top_k=3)
    n = asyncio.run(matcher.load_history())
    if n < 200:
        pytest.skip(f"30 天数据太少做 find_similar 测试 ({n} 根)")

    # anchor = 最新时间
    anchor_t = int(matcher._hist_times[-1])
    similar = matcher.find_similar(anchor_t)
    print(f"\n[smoke] anchor_t={anchor_t}, 返回 {len(similar)} 个相似(MIN_SIM={MIN_SIMILARITY})")
    for s in similar:
        from datetime import datetime, timezone
        st = datetime.fromtimestamp(s["similar_start_time"] / 1000.0, tz=timezone.utc).isoformat()[:16]
        et = datetime.fromtimestamp(s["similar_end_time"] / 1000.0, tz=timezone.utc).isoformat()[:16]
        print(
            f"  [{st} → {et}] sim={s['weighted_sim']:.3f} "
            f"({'/'.join(f'{w}h={ps:.2f}' for w, ps in s.get('per_scale_sim', {}).items())}) "
            f"follow_n={len(s['follow_ohlc'])}"
        )

    # 验证排序
    if len(similar) > 1:
        sims = [s["weighted_sim"] for s in similar]
        assert sims == sorted(sims, reverse=True), "结果未按 weighted_sim 降序"

    # 验证 leakage:所有相似窗口的 end_time < anchor - LEAKAGE_BUFFER_BARS × 3600000
    cutoff = anchor_t - LEAKAGE_BUFFER_BARS * 3600 * 1000
    for s in similar:
        assert s["similar_end_time"] <= cutoff, \
            f"leakage 违规:相似窗口 end={s['similar_end_time']} > cutoff={cutoff}"


if __name__ == "__main__":
    # 直接跑文件做 smoke check
    test_compute_features_shape()
    test_zscore_window_zero_std_safe()
    test_zscore_sliding_windows_basic()
    test_zscore_window_inverted_anti_correlated()
    print("[unit tests] 全部通过")

    if DB_PATH.exists():
        test_load_history_smoke()
        test_find_similar_self_anchor()
        print("[smoke tests] 全部通过")
    else:
        print("[skip] DB 不存在,跳过 smoke 测试")
