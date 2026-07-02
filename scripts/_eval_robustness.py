"""#3 评估护栏:Probabilistic / Deflated Sharpe Ratio + 回测过拟合概率(PBO/CSCV)。

为什么要这个:本项目头号敌人是过拟合(187d→730d 反复翻盘)。单看 ΣR / 胜率会自欺 ——
跑了几十组参数后,"最好的那组"天然被选择偏差灌水。本脚本给出:
  - PSR(Probabilistic Sharpe Ratio):考虑偏度/峰度/样本量后,真实 Sharpe>0 的概率
  - DSR(Deflated Sharpe Ratio):再扣掉"试了 N 组"的选择偏差(Bailey & López de Prado 2014)
  - PBO(Probability of Backtest Overfitting,CSCV):在你试过的变体里,
    "样本内最优"在样本外掉到中位数以下的概率(López de Prado)。PBO>0.5 = 大概率过拟合。

纯事后分析,不改 bot。用法:python3 scripts/_eval_robustness.py --days 1095 --trials 30
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from statistics import NormalDist

import pytz
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.backtest import simulate_outcome
from scripts._regime_diag import collect_signals
from utils.logger import setup_logger

NY = pytz.timezone("America/New_York")
_N = NormalDist()
_EMC = 0.5772156649015329  # Euler–Mascheroni


# --- 统计原语 ----------------------------------------------------------------

def _moments(xs):
    n = len(xs)
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1) if n > 1 else 0.0
    sd = math.sqrt(var)
    if sd == 0 or n < 3:
        return m, sd, 0.0, 3.0
    skew = (sum((x - m) ** 3 for x in xs) / n) / (sd ** 3)
    kurt = (sum((x - m) ** 4 for x in xs) / n) / (sd ** 4)  # 非超额(正态=3)
    return m, sd, skew, kurt


def sharpe(xs):
    m, sd, *_ = _moments(xs)
    return m / sd if sd > 0 else 0.0


def psr(xs, sr_benchmark=0.0):
    """真实(每单)Sharpe 超过 sr_benchmark 的概率,修正偏度/峰度/样本量。"""
    n = len(xs)
    if n < 3:
        return float("nan")
    m, sd, skew, kurt = _moments(xs)
    if sd == 0:
        return float("nan")
    sr = m / sd
    denom = math.sqrt(max(1e-12, 1 - skew * sr + ((kurt - 1) / 4.0) * sr ** 2))
    return _N.cdf((sr - sr_benchmark) * math.sqrt(n - 1) / denom)


def deflated_sr(xs, n_trials, sigma_sr):
    """DSR:把 benchmark 设成"试 N 组时纯靠运气能拿到的期望最高 Sharpe"(SR0)。
    sigma_sr = 各 trial 的 Sharpe 标准差(变体间离散度)。返回 (DSR, SR0)。
    """
    if n_trials < 2 or sigma_sr <= 0:
        return psr(xs, 0.0), 0.0
    z1 = _N.inv_cdf(1 - 1.0 / n_trials)
    z2 = _N.inv_cdf(1 - 1.0 / (n_trials * math.e))
    sr0 = sigma_sr * ((1 - _EMC) * z1 + _EMC * z2)
    return psr(xs, sr0), sr0


def pbo_cscv(matrix, S=8):
    """CSCV 估 PBO。matrix: rows=期(月), cols=策略变体,值=该期该变体收益。
    返回 (PBO, 中位 logit)。PBO = 样本内最优在样本外 rank 落到下半区的频率。
    """
    T = len(matrix)
    ncol = len(matrix[0])
    if T < S or ncol < 2:
        return float("nan"), float("nan")
    # 连续切 S 块
    bnds = [round(i * T / S) for i in range(S + 1)]
    blocks = [list(range(bnds[i], bnds[i + 1])) for i in range(S)]
    logits = []
    n_below = 0
    for is_sel in combinations(range(S), S // 2):
        is_rows = [r for b in is_sel for r in blocks[b]]
        os_rows = [r for b in range(S) if b not in is_sel for r in blocks[b]]
        if not is_rows or not os_rows:
            continue
        is_sr = [sharpe([matrix[r][c] for r in is_rows]) for c in range(ncol)]
        os_sr = [sharpe([matrix[r][c] for r in os_rows]) for c in range(ncol)]
        n_star = max(range(ncol), key=lambda c: is_sr[c])  # 样本内最优
        # 样本外 rank(1=最差 … ncol=最好)
        rank = 1 + sum(1 for c in range(ncol) if os_sr[c] < os_sr[n_star])
        omega = rank / (ncol + 1)
        omega = min(max(omega, 1e-6), 1 - 1e-6)
        logits.append(math.log(omega / (1 - omega)))
        if omega <= 0.5:
            n_below += 1
    if not logits:
        return float("nan"), float("nan")
    logits.sort()
    return n_below / len(logits), logits[len(logits) // 2]


# --- ② Sharpe 显著性 + Minimum Track Record Length --------------------------

def sharpe_stats(xs):
    """每单 Sharpe + Lo(2002) 标准误 + t/p/95%CI。xs=每单 R 序列(非求和)。
    SE = sqrt((1 + 0.5·SR²)/n)(IID 近似);p 为 H0:SR=0 的双侧。"""
    n = len(xs)
    if n < 3:
        return None
    m, sd, skew, kurt = _moments(xs)
    if sd == 0:
        return None
    sr = m / sd
    se = math.sqrt((1 + 0.5 * sr ** 2) / n)
    t = sr / se if se > 0 else 0.0
    p = 2 * (1 - _N.cdf(abs(t)))
    return {"n": n, "sharpe": sr, "se": se, "t": t, "p": p,
            "ci": (sr - 1.96 * se, sr + 1.96 * se), "skew": skew, "kurt": kurt}


def min_trl(xs, c=0.0, alpha=0.05):
    """确认 真实 Sharpe>c 在 (1-alpha) 置信下所需的最少单数(Bailey & López de Prado)。
    MinTRL = (1 − skew·SR + ((kurt−1)/4)·SR²)·(z_{1−α}/(SR−c))²。SR<=c 时返回 inf。"""
    st = sharpe_stats(xs)
    if st is None:
        return None
    sr, skew, kurt = st["sharpe"], st["skew"], st["kurt"]
    if sr <= c:
        return float("inf")
    z = _N.inv_cdf(1 - alpha)
    return (1 - skew * sr + ((kurt - 1) / 4.0) * sr ** 2) * (z / (sr - c)) ** 2


def avg_uniqueness(spans, bar_ms=3_600_000):
    """③ 平均唯一性(López de Prado AFML Ch.4):spans=[(entry_ms, exit_ms)]。
    每单生命周期内逐 bar 1/并发 的均值,再对所有单求均值。无重叠→1.0,重叠多→趋 0。"""
    if not spans:
        return 1.0
    t0 = min(e for e, _ in spans)
    conc = defaultdict(int)
    idx = []
    for e, x in spans:
        b0 = (e - t0) // bar_ms
        b1 = max(b0 + 1, (x - t0) // bar_ms)
        idx.append((b0, b1))
        for b in range(b0, b1):
            conc[b] += 1
    us = []
    for b0, b1 in idx:
        us.append(sum(1.0 / conc[b] for b in range(b0, b1)) / max(1, b1 - b0))
    return sum(us) / len(us)


def _closed_trades(sigs):
    """取线上配置(信号自带 tp)的全部已闭合交易:[(entry_ms, exit_ms, R, source)]。"""
    out = []
    for s in sigs:
        if s.get("outcome") not in ("tp", "sl"):
            continue
        fb = s.get("fill_bars", 0) or 0
        nb = s.get("bars", 1) or 1
        e = int(s["time"]) + fb * 3_600_000
        out.append((e, e + max(1, nb) * 3_600_000, float(s["pnl_r"]), s.get("source", "?")))
    return out


# --- 取数:单仓口径的逐单 R + TP 变体矩阵 ------------------------------------

def _year_month(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(NY).strftime("%Y-%m")


def _single_position_trades(sigs, tp_fn, bars_1h):
    """对给定 TP 规则重算 outcome,单仓口径(在仓跳过),返回按时间序的 (entry_ms, R)。"""
    trades = []
    for s in sigs:
        if not s.get("entry") or not s.get("sl") or s["entry"] == s["sl"]:
            continue
        tp = tp_fn(s)
        if tp is None:
            continue
        if s["direction"] == "long" and tp <= s["entry"]:
            continue
        if s["direction"] == "short" and tp >= s["entry"]:
            continue
        o = simulate_outcome(s["time"], s["direction"], s["entry"], s["sl"], tp, bars_1h,
                             max_hours=168, entry_mode=s.get("entry_mode", "limit"), limit_ttl_hours=24)
        if o["outcome"] not in ("tp", "sl"):
            continue
        fb = o.get("fill_bars", 0) or 0
        nb = o.get("bars", 1) or 1
        entry_ms = s["time"] + fb * 3600_000
        trades.append((entry_ms, entry_ms + max(1, nb) * 3600_000, o["pnl_r"]))
    trades.sort()
    busy = 0
    out = []
    for e, x, r in trades:
        if e < busy:
            continue
        out.append((e, r))
        busy = x
    return out


def main_async():
    pass


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1095)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--trials", type=int, default=30, help="项目里试过的策略变体总数(估)→ DSR 用")
    args = ap.parse_args()
    setup_logger("WARNING")

    ROOT = Path(__file__).resolve().parent.parent
    with open(ROOT / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    symbol = args.symbol or cfg["symbol"]

    sigs, bars_1h = await collect_signals(symbol, args.days, cfg)

    def fixed(k):
        return lambda s: (s["entry"] + k * abs(s["entry"] - s["sl"])) if s["direction"] == "long" \
            else (s["entry"] - k * abs(s["entry"] - s["sl"]))
    # 当前线上 OTE 已是 2.5R;这里 current = 信号自带 tp(即线上配置产物)
    variants = {
        "线上(2.5R)": lambda s: s["tp"],
        "fixed 2R": fixed(2.0), "fixed 3R": fixed(3.0), "fixed 4R": fixed(4.0),
        "fixed 1.5R": fixed(1.5), "fixed 2.5R": fixed(2.5),
    }

    # 1) 每个变体 → 单仓逐月收益序列
    var_monthly = {}
    var_sharpe = {}
    chosen_trades = None
    for name, fn in variants.items():
        tr = _single_position_trades(sigs, fn, bars_1h)
        by_m = defaultdict(float)
        for e, r in tr:
            by_m[_year_month(e)] += r
        var_monthly[name] = by_m
        per_trade = [r for _, r in tr]
        var_sharpe[name] = sharpe(per_trade) if per_trade else 0.0
        if name == "线上(2.5R)":
            chosen_trades = per_trade

    # 2) 对齐月份成矩阵
    months = sorted(set(m for d in var_monthly.values() for m in d))
    names = list(variants.keys())
    matrix = [[var_monthly[nm].get(mo, 0.0) for nm in names] for mo in months]

    # 3) 输出
    print("=" * 78)
    print(f"评估护栏:{symbol} {args.days}d  单仓口径  (DSR 假设试过 {args.trials} 组变体)")
    print("-" * 78)
    sr = sharpe(chosen_trades)
    print(f"线上配置(OTE 2.5R + 趋势门)逐单 R:n={len(chosen_trades)}  "
          f"每单 Sharpe={sr:.3f}")
    p0 = psr(chosen_trades, 0.0)
    print(f"  PSR(真实 Sharpe>0 的概率)= {p0:.1%}  "
          + ("✅ 稳" if p0 >= 0.95 else "⚠️ 不够硬" if p0 >= 0.80 else "❌ 弱"))
    sigma_sr = (_moments(list(var_sharpe.values()))[1])  # 变体间 Sharpe 离散度
    dsr, sr0 = deflated_sr(chosen_trades, args.trials, sigma_sr)
    print(f"  DSR(扣掉试 {args.trials} 组的选择偏差)= {dsr:.1%}  "
          f"(SR0 门槛={sr0:.3f},变体 Sharpe 离散 σ={sigma_sr:.3f})  "
          + ("✅" if dsr >= 0.95 else "⚠️" if dsr >= 0.80 else "❌"))

    print("-" * 78)
    print(f"PBO(回测过拟合概率,CSCV over {len(names)} 个 TP 变体,{len(months)} 个月):")
    for S in (6, 8, 10):
        if len(months) >= S:
            pbo, med = pbo_cscv(matrix, S=S)
            print(f"  S={S:2d} 块: PBO={pbo:.1%}  中位 logit={med:+.2f}  "
                  + ("✅ 不像过拟合" if pbo < 0.5 else "❌ 大概率过拟合"))
    print("  解读:PBO<0.5 = 你选的最优变体在样本外大概率仍居上半区(可信);>0.5 = 选了过拟合的。")

    print("-" * 78)
    print("各变体每单 Sharpe(看 2.5R 是否一枝独秀=过拟合迹象,平台=稳):")
    for nm in sorted(names, key=lambda n: -var_sharpe[n]):
        print(f"  {nm:<14} Sharpe={var_sharpe[nm]:+.3f}")

    # ===== ② 显著性 + Minimum Track Record Length =====
    print("=" * 78)
    print("[② 还要多少单才能信(MinTRL + 显著性)]")
    st = sharpe_stats(chosen_trades)
    mtrl = min_trl(chosen_trades, c=0.0)
    if st:
        ci = st["ci"]
        print(f"  线上整体(单仓 {st['n']} 单):Sharpe {st['sharpe']:+.3f} ± {st['se']:.3f}  "
              f"t={st['t']:.2f}  p={st['p']:.3f}  95%CI[{ci[0]:+.3f},{ci[1]:+.3f}]  "
              f"skew={st['skew']:+.2f} kurt={st['kurt']:.1f}")
        sig = "✅ 已显著" if st["p"] < 0.05 else ("⚠️ 边缘" if st["p"] < 0.20 else "❌ 远不显著(样本太薄)")
        mtrl_s = "已达标" if (mtrl is not None and mtrl <= st["n"]) else (f"需 ~{mtrl:.0f} 单(还差 {mtrl - st['n']:.0f})" if mtrl not in (None, float('inf')) else "SR≤0,无意义")
        print(f"  H0:Sharpe=0 → {sig};MinTRL(95%确认>0)= {mtrl_s}")
    # 逐检测器(各自的 edge 是否接近可确认)
    by_src = defaultdict(list)
    for _, _, r, src in _closed_trades(sigs):
        by_src[src].append(r)
    print("  逐检测器(全闭合单,判各自 edge 离统计可信多远):")
    for src in sorted(by_src, key=lambda s: -sum(by_src[s])):
        rs = by_src[src]
        s2 = sharpe_stats(rs)
        if not s2:
            print(f"    {src:<22} n={len(rs)}  样本不足")
            continue
        mt = min_trl(rs, c=0.0)
        mt_s = "已达标" if (mt is not None and mt <= s2["n"]) else (f"需~{mt:.0f}单" if mt not in (None, float('inf')) else "SR≤0")
        print(f"    {src:<22} n={s2['n']:>3}  ΣR{sum(rs):+6.1f}  Sharpe{s2['sharpe']:+.3f}  "
              f"p={s2['p']:.2f}  MinTRL: {mt_s}")

    # ===== ③ DSR 的 N 校准 + 重叠唯一性 =====
    print("=" * 78)
    print("[③ DSR 随'试过几组'的敏感度 + 单仓唯一性]")
    # 更细的 TP 网格估变体 Sharpe 离散度(比 6 组更稳)
    grid = [round(1.5 + 0.25 * i, 2) for i in range(11)]  # 1.5..4.0
    grid_sr = []
    for k in grid:
        tr = _single_position_trades(sigs, (lambda kk: (lambda s: (s["entry"] + kk * abs(s["entry"] - s["sl"])) if s["direction"] == "long" else (s["entry"] - kk * abs(s["entry"] - s["sl"]))))(k), bars_1h)
        pr = [r for _, r in tr]
        if pr:
            grid_sr.append(sharpe(pr))
    sigma_fine = _moments(grid_sr)[1] if len(grid_sr) > 2 else sigma_sr
    print(f"  变体 Sharpe 离散 σ(11 档 TP 网格)= {sigma_fine:.3f}")
    print(f"  {'假设试过 N 组':<16}{'SR0 门槛':>9}{'DSR':>9}")
    for N in (1, 10, 30, 50, 100):
        dN, s0 = deflated_sr(chosen_trades, max(N, 2), sigma_fine)
        tag = "(=PSR)" if N == 1 else ""
        print(f"  {str(N)+' '+tag:<16}{s0:>9.3f}{dN:>8.1%}")
    print("  解读:你跑过的参数组合越多(N↑),门槛越高、DSR 越低 —— 这是诚实的'选择偏差'代价。")
    # 唯一性:单仓 vs 无限叠仓
    all_spans = [(e, x) for e, x, _, _ in _closed_trades(sigs)]
    u_all = avg_uniqueness(all_spans)
    print(f"  平均唯一性:无限叠仓口径={u_all:.2f}(<1=有重叠,显著性被注水)  /  单仓口径=1.00(同时只1单,零重叠)")
    print(f"  → 单仓口径下 {len(chosen_trades)} 单 ≈ {len(chosen_trades)} 个独立证据,没被叠相关单灌水;这是单仓的统计优势。")


if __name__ == "__main__":
    asyncio.run(main())
