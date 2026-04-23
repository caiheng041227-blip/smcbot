"""信号特征重要性分析。

输入:signals CSV(scripts/backtest.py --export-csv 导出)
输出:
  1. 每个特征 vs win 的 Pearson 相关系数 + 分箱胜率
  2. 逻辑回归系数(标准化后)
  3. RandomForest feature importance(gini)
  4. 每个 C 维度 命中/未命中的胜率对比(核心问题:评分有没有判别力)

用法:
  python scripts/analyze_signals.py data/analysis/signals_187d.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler


CAT_COLS = ["direction", "poi_source", "poi_type", "h4_structure"]
NUMERIC_FEATURES = [
    "hour_et", "weekday",
    "rr", "atr_4h", "atr_1h", "atr_15m", "atr_ratio_4h_1h",
    "poi_height_pct", "poi_distance_pct", "poi_age_hours",
    "poi_displacement_mult", "sweep_bar_delta", "sweep_wick_strong",
    "score_total",
    "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9",
]


def load_and_prepare(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # 只保留已闭合的(win ∈ {0,1})
    df = df[df["win"].isin([0, 1])].copy()
    df["win"] = df["win"].astype(int)
    return df


def print_header(title: str) -> None:
    print(f"\n{'='*70}\n{title}\n{'='*70}")


def c_dimension_hitrate(df: pd.DataFrame) -> None:
    """核心质检:每个 C 维度命中 vs 未命中 的胜率差,找"是否 predictive"的最直接证据。"""
    print_header("每个 C 维度 命中 vs 未命中 的胜率对比")
    print(f"{'维度':<5} {'命中':>5} {'胜率':>7} {'未命中':>6} {'胜率':>7} {'差值(pp)':>9} {'样本量':>6}")
    print("-" * 70)
    rows = []
    for c in [f"C{i}" for i in range(1, 10)]:
        hit_mask = df[c] > 0
        n_hit = int(hit_mask.sum())
        n_miss = int((~hit_mask).sum())
        if n_hit == 0 or n_miss == 0:
            continue
        wr_hit = df.loc[hit_mask, "win"].mean()
        wr_miss = df.loc[~hit_mask, "win"].mean()
        diff = (wr_hit - wr_miss) * 100
        rows.append((c, n_hit, wr_hit, n_miss, wr_miss, diff))
    # 按|差值|排序
    rows.sort(key=lambda r: abs(r[5]), reverse=True)
    for c, nh, wh, nm, wm, d in rows:
        print(f"{c:<5} {nh:>5} {wh:>6.1%} {nm:>6} {wm:>6.1%} {d:>+8.1f}  {nh+nm:>6}")


def score_total_quantile(df: pd.DataFrame) -> None:
    """score_total 分位 vs 胜率 —— 看总评分有无判别力。"""
    print_header("score_total 分位 vs 胜率")
    df = df.copy()
    # 等频分箱(5 个分位)
    try:
        df["score_q"] = pd.qcut(df["score_total"], q=5, duplicates="drop")
    except ValueError:
        df["score_q"] = pd.cut(df["score_total"], bins=5)
    agg = df.groupby("score_q", observed=True)["win"].agg(["count", "mean"]).rename(
        columns={"count": "n", "mean": "winrate"}
    )
    print(agg.to_string())


def poi_source_breakdown(df: pd.DataFrame) -> None:
    print_header("POI source 分组胜率 / 平均 R:R / 平均 pnl")
    grp = df.groupby("poi_source").agg(
        n=("win", "count"),
        winrate=("win", "mean"),
        avg_rr=("rr", "mean"),
        cum_r=("pnl_r", "sum"),
        avg_r=("pnl_r", "mean"),
    ).sort_values("n", ascending=False)
    print(grp.to_string())


def numeric_correlations(df: pd.DataFrame) -> None:
    print_header("数值特征与 win 的相关性(|r| > 0.05 为起点)")
    corrs = []
    for c in NUMERIC_FEATURES:
        if c not in df.columns:
            continue
        col = pd.to_numeric(df[c], errors="coerce")
        if col.notna().sum() < 20:
            continue
        r = np.corrcoef(col.fillna(col.median()), df["win"])[0, 1]
        corrs.append((c, r, int(col.notna().sum())))
    corrs.sort(key=lambda x: abs(x[1]), reverse=True)
    print(f"{'特征':<24} {'corr(r)':>10} {'样本':>6}")
    print("-" * 50)
    for feat, r, n in corrs:
        marker = "⭐" if abs(r) >= 0.10 else ("." if abs(r) >= 0.05 else "")
        print(f"{feat:<24} {r:>+10.4f} {n:>6}  {marker}")


def build_feature_matrix(df: pd.DataFrame) -> tuple:
    """数值特征 + 类别 one-hot。返回 (X, feature_names, y)。"""
    numeric = [c for c in NUMERIC_FEATURES if c in df.columns]
    X_num = df[numeric].apply(pd.to_numeric, errors="coerce")
    X_num = X_num.fillna(X_num.median())
    X_cat = pd.get_dummies(df[CAT_COLS].astype(str), prefix=CAT_COLS, drop_first=False)
    X = pd.concat([X_num, X_cat], axis=1)
    feat_names = list(X.columns)
    y = df["win"].values
    return X.values, feat_names, y


def logistic_coefficients(df: pd.DataFrame) -> None:
    print_header("逻辑回归系数(标准化后,越大绝对值越重要)")
    X, names, y = build_feature_matrix(df)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    model = LogisticRegression(max_iter=2000, C=1.0, penalty="l2")
    model.fit(Xs, y)
    # 交叉验证 AUC
    try:
        auc = cross_val_score(
            LogisticRegression(max_iter=2000, C=1.0, penalty="l2"),
            Xs, y, cv=5, scoring="roc_auc",
        ).mean()
        print(f"5-fold CV AUC = {auc:.3f}(0.5=随机,>0.6 才算有信号)")
    except Exception as e:
        print(f"CV AUC 计算失败: {e}")
    coefs = list(zip(names, model.coef_[0]))
    coefs.sort(key=lambda x: abs(x[1]), reverse=True)
    print(f"\n{'特征':<32} {'系数':>10}")
    print("-" * 50)
    for name, c in coefs[:20]:
        marker = "⭐" if abs(c) >= 0.3 else ("." if abs(c) >= 0.15 else "")
        print(f"{name:<32} {c:>+10.4f}  {marker}")


def random_forest_importance(df: pd.DataFrame) -> None:
    print_header("RandomForest feature importance(gini)")
    X, names, y = build_feature_matrix(df)
    rf = RandomForestClassifier(n_estimators=500, max_depth=5, random_state=42, n_jobs=-1)
    rf.fit(X, y)
    try:
        auc = cross_val_score(
            RandomForestClassifier(n_estimators=500, max_depth=5, random_state=42, n_jobs=-1),
            X, y, cv=5, scoring="roc_auc",
        ).mean()
        print(f"5-fold CV AUC = {auc:.3f}")
    except Exception as e:
        print(f"CV AUC 计算失败: {e}")
    imps = list(zip(names, rf.feature_importances_))
    imps.sort(key=lambda x: x[1], reverse=True)
    print(f"\n{'特征':<32} {'重要性':>10}")
    print("-" * 50)
    for name, imp in imps[:20]:
        marker = "⭐" if imp >= 0.05 else ("." if imp >= 0.025 else "")
        print(f"{name:<32} {imp:>10.4f}  {marker}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", nargs="?", default="data/analysis/signals_187d.csv")
    args = ap.parse_args()
    path = Path(args.csv_path)
    if not path.exists():
        print(f"CSV 不存在: {path}")
        sys.exit(1)
    df = load_and_prepare(path)
    print(f"加载 {len(df)} 条已闭合信号(win=1:{int(df['win'].sum())} / loss=0:{int((df['win']==0).sum())})")

    c_dimension_hitrate(df)
    score_total_quantile(df)
    poi_source_breakdown(df)
    numeric_correlations(df)
    logistic_coefficients(df)
    random_forest_importance(df)


if __name__ == "__main__":
    main()
