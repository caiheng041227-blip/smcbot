# smcbot — an ICT signal bot for ETH perps, built to *not* fool itself

**English** · [中文](README.zh-CN.md)

A single-symbol (ETHUSDT perp) trading **signal** bot implementing a small set of
ICT / Smart-Money-Concepts setups. It watches the market, and when a setup triggers
it pushes a signal to Telegram — **you** place the trade manually (advisory, never auto-executes).

What makes this repo worth a look isn't the strategy (it's deliberately simple). It's the
**evaluation discipline** wrapped around it — the part most retail trading repos skip.

> ⚠️ **Not financial advice.** This is a personal research project. Past backtest performance
> says little about the future, and a published edge tends to decay. Trade your own risk.

---

## The honest numbers

Backtest over **1095 days (2023 → mid-2026, ~3.5y out-of-sample)**, **net of real trading costs**
(round-trip fees + slippage — the thing most retail backtests quietly omit):

| net @8bp | net @12bp | net @16bp |
|---:|---:|---:|
| +38.4R | **+33.1R** | +27.9R |

- ~218 trades, ~34% win rate, ~2.3 payoff — the edge is asymmetry, not hit rate. Before costs it's
  +48.8R; the gap is fees + slippage, priced in via `--cost-bp`.
- **Per-year (net @12bp):** `2023 +7.6 · 2024 −6.3 · 2025 +7.4 · 2026 +24.3`.
  **Regime-dependent** — 2024 loses, 2026 carries it. Shown, not hidden.
- **Per-trade Sharpe ≈ 0.12, p ≈ 0.07** — *borderline*, not confirmed. A small positive edge,
  **not** a money printer.

Reproduce it yourself:

```bash
python3 scripts/backtest.py --days 1095 --cost-bp 12
```

---

## Why the methodology, not the signals

This project's whole history is *"most ideas fail out-of-sample."* The tooling exists to catch
that **before** deploying, not after a bad year:

- **Costs subtracted by default** — a backtest that ignores fees and slippage is fiction.
  `--cost-bp` prices in round-trip cost per trade, so the headline is net, not gross.
- **Per-year OOS as a hard gate** — a change ships only if it doesn't hurt *any* year
  (2023/24/25/26). This single rule has killed most "improvements" I've tried.
- **Deflated Sharpe (DSR) + Probability of Backtest Overfitting (PBO)** — price in the fact
  that I tried dozens of parameter sets. Selection bias is subtracted, not ignored.
- **Minimum Track Record Length (MinTRL)** — how many trades until an edge is statistically
  confirmable. Per detector: OTE `p=0.03` (significant), liquidity-raid `p=0.15` (nearly),
  order-block `p=0.89` (statistically dead — being removed).

References: López de Prado (*Advances in Financial Machine Learning*), Bailey & López de Prado
(*Deflated Sharpe Ratio*; *Sharpe Ratio Efficient Frontier / MinTRL*), Lo (*Statistics of Sharpe
Ratios*), Kaminski & Lo (*When Do Stop-Loss Rules Stop Losses?*).

---

## The setups

| Detector | Idea | Status |
|---|---|---|
| **liquidity_raid** | sweep of an equal-highs/lows pool + textbook displacement confirmation, then reversal | the real edge (+24R gross) |
| **OTE** | 62–79% retracement entry, gated to trade *with* the 30-day momentum trend, fixed 2.5R target | main P&L driver |
| **order_block** | OB at premium/discount | statistically dead, deprecating |

HTF gates: daily MSS bias + premium/discount (dealing range) + a 30-day momentum trend gate on OTE.

## Architecture

```
ict_signal/state_machine.py   ICT state machine (4h/1h driven, HTF gates, dedup)
engine/ict_pois.py            the POI detectors
engine/ict.py                 HTF-gate primitives (bias / dealing range / zone)
scripts/backtest.py           backtest harness (costs, single-position, DD/PF, per-year)
scripts/_eval_robustness.py   DSR / PBO / PSR / MinTRL
notify/telegram_bot.py        Telegram push + commands
```

Python 3.11 + asyncio · OKX WebSocket (live) · SQLite · python-telegram-bot.

## Quickstart

```bash
cp .env.example .env          # add your Telegram bot token + chat id
pip install -r requirements.txt
python3 scripts/backtest.py --days 365 --cost-bp 12   # try the backtest first
python3 main.py                                        # run live (pushes to Telegram)
```

Secrets live only in `.env` (git-ignored). No keys are committed.

---

*Built and stress-tested in the open. If you find a hole in the methodology, open an issue —
that's the most useful contribution here.*
