"""Walk-forward + Wilson CI 分析。

读 backtest 导出的 CSV(--export-csv),做以下事(纯 stdlib,无依赖):

1) 描述统计:总样本 / NOTIFIED / 各 POI tier 样本量
2) Wilson CI:每个 POI tier / 每个 scorer 维度的真实 WR 95% 区间
3) 简化 walk-forward:按时间切 30d 验证段(滚动,无 overlap),
   用 *当前权重* 在每段跑 NOTIFIED ΣR / WR / N。看分段是否稳定。
4) Markdown 报告输出到 stdout(也可以 --out 写文件)。

用法:
  python3 scripts/walkforward_wilson.py /tmp/bt_365.csv [--out /tmp/report.md]
                                                       [--seg-days 30]
                                                       [--baseline]   # 用 baseline 而不是 hybrid
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# ------------------------------ Wilson CI ------------------------------

def wilson_ci(wins: int, n: int, z: float = 1.96) -> Tuple[float, float, float]:
    """返回 (point_wr, lower_95, upper_95)。n=0 时返回 (0, 0, 1)。"""
    if n <= 0:
        return 0.0, 0.0, 1.0
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return p, max(0.0, center - half), min(1.0, center + half)


# ------------------------------ load ------------------------------

def load_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # 转换数值字段
    for r in rows:
        for k in ("entry", "sl", "tp", "rr", "score_total",
                  "atr_4h", "atr_1h", "atr_15m", "atr_ratio_4h_1h",
                  "poi_height_pct", "poi_distance_pct", "poi_age_hours",
                  "poi_displacement_mult", "sweep_bar_delta",
                  "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9",
                  "pnl_r", "hybrid_pnl_r", "time_ms"):
            v = r.get(k)
            if v in (None, "", "None"):
                r[k] = None
            else:
                try:
                    r[k] = float(v)
                except (TypeError, ValueError):
                    r[k] = None
        for k in ("notified", "win", "hybrid_tp1_hit", "sweep_wick_strong"):
            v = r.get(k)
            if v in (None, "", "None"):
                r[k] = None
            else:
                try:
                    r[k] = int(float(v))
                except (TypeError, ValueError):
                    r[k] = None
    return rows


# ------------------------------ filters ------------------------------

def filter_notified_closed(rows: List[Dict[str, Any]], use_hybrid: bool) -> List[Dict[str, Any]]:
    """只看 NOTIFIED 且已闭合(有 outcome)的信号。"""
    out = []
    for r in rows:
        if not r.get("notified"):
            continue
        oc = (r.get("hybrid_outcome") if use_hybrid else r.get("outcome")) or ""
        if oc not in ("tp", "sl"):
            continue
        out.append(r)
    return out


def get_pnl(r: Dict[str, Any], use_hybrid: bool) -> Optional[float]:
    v = r.get("hybrid_pnl_r" if use_hybrid else "pnl_r")
    return None if v is None else float(v)


def get_outcome(r: Dict[str, Any], use_hybrid: bool) -> str:
    return (r.get("hybrid_outcome") if use_hybrid else r.get("outcome")) or ""


# ------------------------------ stats ------------------------------

def group_stats(rows: List[Dict[str, Any]], key_fn, use_hybrid: bool) -> Dict[Any, Dict[str, Any]]:
    """按 key_fn(r) 分组,算 N / wins / WR Wilson CI / ΣR / avg R。"""
    groups: Dict[Any, Dict[str, Any]] = defaultdict(
        lambda: {"n": 0, "wins": 0, "sum_r": 0.0}
    )
    for r in rows:
        k = key_fn(r)
        if k is None:
            continue
        g = groups[k]
        g["n"] += 1
        if get_outcome(r, use_hybrid) == "tp":
            g["wins"] += 1
        pnl = get_pnl(r, use_hybrid)
        if pnl is not None:
            g["sum_r"] += pnl
    out: Dict[Any, Dict[str, Any]] = {}
    for k, g in groups.items():
        wr, lo, hi = wilson_ci(g["wins"], g["n"])
        out[k] = {
            "n": g["n"], "wins": g["wins"],
            "wr": wr, "wr_lo": lo, "wr_hi": hi,
            "sum_r": g["sum_r"], "avg_r": g["sum_r"] / max(1, g["n"]),
        }
    return out


def wf_segments(
    rows: List[Dict[str, Any]],
    use_hybrid: bool,
    seg_days: int = 30,
) -> List[Dict[str, Any]]:
    """简化 walk-forward:按 time_ms 切等长非 overlap 段,每段算 N / WR / ΣR。"""
    timed = [r for r in rows if r.get("time_ms")]
    if not timed:
        return []
    timed.sort(key=lambda r: r["time_ms"])
    t_min = timed[0]["time_ms"]
    t_max = timed[-1]["time_ms"]
    seg_ms = seg_days * 86400 * 1000
    segs = []
    t0 = t_min
    while t0 < t_max:
        t1 = t0 + seg_ms
        in_seg = [r for r in timed if t0 <= r["time_ms"] < t1]
        if in_seg:
            wins = sum(1 for r in in_seg if get_outcome(r, use_hybrid) == "tp")
            sum_r = sum((get_pnl(r, use_hybrid) or 0.0) for r in in_seg)
            wr, lo, hi = wilson_ci(wins, len(in_seg))
            segs.append({
                "start_iso": datetime.fromtimestamp(t0/1000, tz=timezone.utc).strftime("%Y-%m-%d"),
                "end_iso": datetime.fromtimestamp(t1/1000, tz=timezone.utc).strftime("%Y-%m-%d"),
                "n": len(in_seg), "wins": wins,
                "wr": wr, "wr_lo": lo, "wr_hi": hi,
                "sum_r": sum_r, "avg_r": sum_r / max(1, len(in_seg)),
            })
        t0 = t1
    return segs


# ------------------------------ scorer 维度边际 ------------------------------

def dimension_lift(
    rows: List[Dict[str, Any]],
    dim_keys: List[str],
    use_hybrid: bool,
) -> Dict[str, Dict[str, Any]]:
    """每个维度 Cn:对比 (Cn=0) vs (Cn>0) 的 WR / ΣR / avg R。
    给出 lift = group(>0) - group(=0) 在 WR 和 avg_r 上的差。"""
    out = {}
    for dk in dim_keys:
        on_rows = [r for r in rows if r.get(dk) is not None and r.get(dk) > 0]
        off_rows = [r for r in rows if r.get(dk) is not None and r.get(dk) == 0]
        def _stats(lst):
            if not lst:
                return {"n": 0, "wins": 0, "wr": 0, "wr_lo": 0, "wr_hi": 1, "sum_r": 0, "avg_r": 0}
            wins = sum(1 for r in lst if get_outcome(r, use_hybrid) == "tp")
            sum_r = sum((get_pnl(r, use_hybrid) or 0.0) for r in lst)
            wr, lo, hi = wilson_ci(wins, len(lst))
            return {"n": len(lst), "wins": wins, "wr": wr, "wr_lo": lo, "wr_hi": hi,
                    "sum_r": sum_r, "avg_r": sum_r / max(1, len(lst))}
        on_s = _stats(on_rows)
        off_s = _stats(off_rows)
        out[dk] = {
            "on": on_s, "off": off_s,
            "wr_lift": on_s["wr"] - off_s["wr"],
            "avg_r_lift": on_s["avg_r"] - off_s["avg_r"],
        }
    return out


# ------------------------------ render ------------------------------

def fmt_pct(p: float) -> str:
    return f"{p*100:.1f}%"


def render_markdown(rows_all: List[Dict[str, Any]], rows: List[Dict[str, Any]],
                    use_hybrid: bool, seg_days: int) -> str:
    label = "hybrid" if use_hybrid else "baseline"
    lines: List[str] = []
    lines.append(f"# Walk-forward + Wilson CI 分析 ({label})\n")
    lines.append(f"- 数据源:CSV 共 {len(rows_all)} 条信号,其中 NOTIFIED 已闭合 = **{len(rows)}**\n")

    if not rows:
        lines.append("⚠️ 无 NOTIFIED 已闭合信号,无法分析\n")
        return "\n".join(lines)

    sum_r = sum((get_pnl(r, use_hybrid) or 0.0) for r in rows)
    wins = sum(1 for r in rows if get_outcome(r, use_hybrid) == "tp")
    wr, lo, hi = wilson_ci(wins, len(rows))
    lines.append(f"- 全样本 ΣR = **{sum_r:+.2f}R**, WR = {fmt_pct(wr)} (Wilson 95% CI [{fmt_pct(lo)}, {fmt_pct(hi)}])\n")

    # ---- Section 1: POI tier Wilson CI ----
    lines.append("\n## 1. 各 POI tier 真实 WR(Wilson 95% CI)\n")
    lines.append("> 重点看 **WR 下界** —— 5 笔 80% 的下界可能只有 36%,不能按点估计给权重。\n")
    tiers = group_stats(rows, lambda r: r.get("poi_source"), use_hybrid)
    lines.append("| POI source | N | wins | WR | Wilson 95% CI | ΣR | avg R |")
    lines.append("|---|---|---|---|---|---|---|")
    for k, s in sorted(tiers.items(), key=lambda kv: -kv[1]["sum_r"]):
        lines.append(
            f"| `{k}` | {s['n']} | {s['wins']} | {fmt_pct(s['wr'])} | "
            f"[{fmt_pct(s['wr_lo'])}, {fmt_pct(s['wr_hi'])}] | "
            f"{s['sum_r']:+.2f} | {s['avg_r']:+.3f} |"
        )

    # ---- Section 2: 维度边际 ----
    lines.append("\n## 2. 各 scorer 维度的边际(C_X=0 vs C_X>0)\n")
    lines.append("> lift > 0 = 该维度 ON 时表现更好;lift ≤ 0 = 该维度可能是装饰\n")
    dims = ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9"]
    lift = dimension_lift(rows, dims, use_hybrid)
    lines.append("| 维度 | ON N | ON WR | ON avg R | OFF N | OFF WR | OFF avg R | WR lift | R lift |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for dk in dims:
        d = lift[dk]
        if d["on"]["n"] == 0 and d["off"]["n"] == 0:
            continue
        lines.append(
            f"| {dk} | {d['on']['n']} | {fmt_pct(d['on']['wr'])} | {d['on']['avg_r']:+.3f} | "
            f"{d['off']['n']} | {fmt_pct(d['off']['wr'])} | {d['off']['avg_r']:+.3f} | "
            f"{d['wr_lift']*100:+.1f}pp | {d['avg_r_lift']:+.3f} |"
        )

    # ---- Section 3: 方向 / 时段 ----
    lines.append("\n## 3. 方向 / 时段 / 结构维度\n")
    by_dir = group_stats(rows, lambda r: r.get("direction"), use_hybrid)
    lines.append("\n### 方向\n")
    lines.append("| direction | N | WR | Wilson CI | ΣR |")
    lines.append("|---|---|---|---|---|")
    for k, s in by_dir.items():
        lines.append(f"| {k} | {s['n']} | {fmt_pct(s['wr'])} | [{fmt_pct(s['wr_lo'])}, {fmt_pct(s['wr_hi'])}] | {s['sum_r']:+.2f} |")

    by_h4 = group_stats(rows, lambda r: r.get("h4_structure"), use_hybrid)
    lines.append("\n### 4h 结构\n")
    lines.append("| h4_structure | N | WR | Wilson CI | ΣR |")
    lines.append("|---|---|---|---|---|")
    for k, s in by_h4.items():
        lines.append(f"| {k} | {s['n']} | {fmt_pct(s['wr'])} | [{fmt_pct(s['wr_lo'])}, {fmt_pct(s['wr_hi'])}] | {s['sum_r']:+.2f} |")

    # ---- Section 4: walk-forward 分段 ----
    lines.append(f"\n## 4. 简化 walk-forward({seg_days}d 滚动验证段,**当前权重不变**)\n")
    lines.append("> 看每段是不是都净正。若大部分段轻微正、少数段重亏拉总分,警惕过拟合。\n")
    segs = wf_segments(rows, use_hybrid, seg_days=seg_days)
    lines.append("| 段 | start | end | N | WR | Wilson CI | ΣR |")
    lines.append("|---|---|---|---|---|---|---|")
    n_pos = sum(1 for s in segs if s["sum_r"] > 0)
    n_neg = sum(1 for s in segs if s["sum_r"] < 0)
    n_zero = sum(1 for s in segs if s["sum_r"] == 0)
    for i, s in enumerate(segs, 1):
        emoji = "🟢" if s["sum_r"] > 0 else ("🔴" if s["sum_r"] < 0 else "⚪")
        lines.append(
            f"| {emoji} {i} | {s['start_iso']} | {s['end_iso']} | {s['n']} | "
            f"{fmt_pct(s['wr'])} | [{fmt_pct(s['wr_lo'])}, {fmt_pct(s['wr_hi'])}] | "
            f"{s['sum_r']:+.2f} |"
        )
    lines.append(f"\n**段汇总:正 {n_pos} / 负 {n_neg} / 平 {n_zero}**(共 {len(segs)} 段)")

    # ---- Section 5: 解读 ----
    lines.append("\n## 5. 解读速读\n")
    risky_tiers = [(k, s) for k, s in tiers.items() if s["wr_lo"] < 0.40 and s["sum_r"] > 0]
    if risky_tiers:
        lines.append("\n**⚠️ 可疑过拟合的 tier(全样本 ΣR 正,但 WR 下界 < 40%):**")
        for k, s in risky_tiers:
            lines.append(f"- `{k}`: N={s['n']}, WR={fmt_pct(s['wr'])} (下界 {fmt_pct(s['wr_lo'])}), ΣR={s['sum_r']:+.2f}")
    else:
        lines.append("- 各 tier 的 Wilson 下界都 ≥ 40%,无明显小样本风险")

    if n_neg >= len(segs) * 0.4:
        lines.append(f"\n**⚠️ 分段稳定性差**:{n_neg}/{len(segs)} 段净负,全期 ΣR 可能由少数赚钱段拉起 → 过拟合风险高")
    elif n_pos >= len(segs) * 0.7:
        lines.append(f"\n**✓ 分段稳定**:{n_pos}/{len(segs)} 段净正,跨期表现稳定")

    return "\n".join(lines)


# ------------------------------ main ------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", help="backtest --export-csv 输出的 CSV")
    ap.add_argument("--out", default=None, help="markdown 报告写入文件(默认 stdout)")
    ap.add_argument("--seg-days", type=int, default=60, help="walk-forward 分段长度(默认 60 天,365d → 6 段)")
    ap.add_argument("--baseline", action="store_true",
                    help="用 baseline outcome/pnl_r 而不是 hybrid_*")
    args = ap.parse_args()

    rows_all = load_csv(args.csv_path)
    use_hybrid = not args.baseline
    rows = filter_notified_closed(rows_all, use_hybrid)

    md = render_markdown(rows_all, rows, use_hybrid, args.seg_days)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"已写入 {args.out}", file=sys.stderr)
    else:
        print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
