"""
backtest.py  –  Offline backtest using historical API data
============================================================
Usage:
  python backtest.py                    # runs on all UNIVERSE tickers
  python backtest.py --tickers OPENAI ANTHROPIC

Fetches candles + index history from the live API (no auth needed),
then simulates the strategy with realistic fill assumptions.
"""

from __future__ import annotations

import argparse
import logging
from typing import Dict, List, Optional

import numpy as np

from config import UNIVERSE
from core.client import ForumClient
from core.market_enrichment import candle_time_key
from core.portfolio import Portfolio
from strategies.signals import (
    FundingHarvest,
    MeanReversion,
    IndexMomentum,
    combine_signals,
)
from execution.executor import Executor

log = logging.getLogger("backtest")
logging.basicConfig(level=logging.WARNING)


def _index_asof(index_hist: List[dict], candle_start: str) -> Optional[float]:
    """Latest index `value` at or before candle `start` (both ISO strings)."""
    if not index_hist:
        return None
    last: Optional[float] = None
    for row in index_hist:
        if row["timestamp"] <= candle_start:
            last = row["value"]
        else:
            break
    return last


def run_backtest(tickers: List[str], resolution_min: int = 30, bars: int = 200):
    client = ForumClient(sync_time=True)
    portfolio = Portfolio()
    executor = Executor(client, portfolio, dry_run=True)

    funding_strat = FundingHarvest()
    mr_strat = MeanReversion()
    mom_strat = IndexMomentum()

    data: Dict[str, dict] = {}
    for ticker in tickers:
        try:
            mkt = client.get_market(ticker)
            candles = client.get_candles_compat(ticker, resolution_min, bars)
            candles = sorted(candles, key=candle_time_key)
            idx_name = mkt.get("index")
            idx_hist: List[dict] = []
            if idx_name and candles:
                start = candles[0]["start"]
                end = candles[-1]["start"]
                idx_hist = client.get_index_history(
                    idx_name,
                    start=start,
                    end=end,
                    limit=2500,
                    interval="raw",
                )
                idx_hist = sorted(idx_hist, key=lambda r: r["timestamp"])
            data[ticker] = {
                "candles": candles,
                "market": mkt,
                "index_hist": idx_hist,
            }
            log.warning(f"Loaded {ticker}: {len(candles)} bars, {len(idx_hist)} index points")
        except Exception as e:
            log.warning(f"Skipping {ticker}: {e}")

    if not data:
        print("No data loaded. Check API connectivity.")
        return

    min_bars = min(len(v["candles"]) for v in data.values())
    print(f"\nBacktesting {len(data)} tickers × {min_bars} bars "
          f"(~{resolution_min}min target resolution)\n")

    navs = []

    for i in range(min_bars):
        marks = {}
        synthetic_markets = []

        for ticker, td in data.items():
            candle = td["candles"][-(min_bars - i)]
            market = td["market"]
            ix = _index_asof(td["index_hist"], candle["start"])
            if ix is None:
                ix = market.get("lastIndexValue")

            synthetic = {
                "ticker":            ticker,
                "lastPrice":         candle.get("close"),
                "lastIndexValue":    ix,
                "movingFundingRate": market.get("movingFundingRate", 0),
                "cumFunding":        market.get("cumFunding", 0),
                "live":              True,
            }
            synthetic_markets.append(synthetic)
            if candle.get("close"):
                marks[ticker] = float(candle["close"])

        for sm in synthetic_markets:
            ticker = sm["ticker"]
            sigs = []

            fs = funding_strat.generate(sm)
            if fs:
                sigs.append(fs)

            mrs = mr_strat.generate(sm)
            if mrs:
                sigs.append(mrs)

            moms = mom_strat.generate(sm)
            if moms:
                sigs.append(moms)

            if mr_strat.should_exit(ticker) and ticker in portfolio.positions:
                executor.close_position(ticker, marks)
                continue

            combined = combine_signals(sigs)
            if combined:
                executor.execute_signal(
                    ticker, combined.direction, combined.strength, marks
                )

        nav = portfolio.nav(marks)
        navs.append(nav)

    navs_arr = np.array(navs)
    returns = np.diff(navs_arr) / navs_arr[:-1]
    total_ret = (navs[-1] - navs[0]) / navs[0]
    bars_per_day = (24 * 60) / max(resolution_min, 1)
    sharpe = returns.mean() / (returns.std() + 1e-10) * np.sqrt(252 * bars_per_day)
    max_dd = np.max(np.maximum.accumulate(navs_arr) - navs_arr) / np.max(navs_arr)

    print("=" * 50)
    print(f"  Total return      : {total_ret:+.2%}")
    print(f"  Annualised Sharpe : {sharpe:.2f}")
    print(f"  Max drawdown      : {max_dd:.2%}")
    print(f"  Final NAV         : ${navs[-1]/100:,.2f}")
    print(f"  Open positions    : {len(portfolio.positions)}")
    print("=" * 50)

    fills = [(t, p.qty, p.avg_price / 100) for t, p in portfolio.positions.items()]
    if fills:
        print("\nRemaining positions:")
        for t, q, px in fills:
            arrow = "▲" if q > 0 else "▼"
            print(f"  {arrow} {t}  qty={q}  avg=${px:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+", default=UNIVERSE)
    parser.add_argument("--bars", type=int, default=200)
    parser.add_argument("--res", type=int, default=30,
                        help="Target minute resolution (maps to Forum 1m/5m/1d candles)")
    args = parser.parse_args()
    run_backtest(args.tickers, args.res, args.bars)
