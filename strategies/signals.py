"""
strategies/signals.py
=====================
Three independent alpha sources:

  1. FundingHarvest   – collect carry when funding rate is extreme
  2. MeanReversion    – fade large price-vs-index deviations (z-score)
  3. Momentum         – ride index trend with EWM crossover

Each strategy returns a Signal dataclass.
The Combiner weights them into a single position directive.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Sequence
import math
import numpy as np
import logging

from config import (
    FUNDING_HARVEST_THRESH,
    MR_ZSCORE_ENTRY, MR_ZSCORE_EXIT, MR_LOOKBACK_BARS,
    MOM_FAST_BARS, MOM_SLOW_BARS, MOM_SIGNAL_THRESH,
    MIN_EDGE_THRESHOLD,
    MIN_COMBINED_STRENGTH,
    REQUIRE_MULTI_STRATEGY_CONFIRMATION,
    SINGLE_STRATEGY_MIN_STRENGTH,
    ALLOW_SINGLE_STRATEGY_SIGNALS,
    SINGLE_STRATEGY_ALLOWLIST,
    SINGLE_STRATEGY_ALLOWLIST_MIN_STRENGTH,
)

log = logging.getLogger(__name__)


def _index_in_price_units(price: Optional[float], index: Optional[float]) -> Optional[float]:
    """
    Normalize index units to match trade price units.

    Forum `lastPrice` is typically in cents (e.g. 5053), while `lastIndexValue`
    is often dollar-like (e.g. 51.17). Mean-reversion needs both on the same
    scale before computing spread/z-score.
    """
    if price is None or index is None:
        return None
    # Typical live shape: price in cents, index in dollars.
    if price >= 1000 and index < 200:
        return index * 100.0
    # Defensive fallback for the opposite mismatch.
    if price < 200 and index > 1000:
        return index / 100.0
    return index


@dataclass
class Signal:
    ticker:     str
    direction:  int        # +1 long, -1 short, 0 flat
    strength:   float      # 0–1, confidence / sizing scale
    strategy:   str        # label for logging
    edge:       float      # estimated edge in decimal (e.g., 0.005 = 0.5%)

    def __repr__(self):
        arrow = "▲" if self.direction > 0 else ("▼" if self.direction < 0 else "─")
        return (f"[{self.strategy}] {self.ticker} {arrow}  "
                f"strength={self.strength:.2f}  edge={self.edge:.4f}")


# ─── 1. Funding Harvest ───────────────────────────────────────────────────────

class FundingHarvest:
    """
    Core insight (Jane Street carry logic):
      If |movingFundingRate| is large, the side *paying* funding is overextended.
      Take the opposite side, collect the rate, close before next settlement.

    Edge = |fundingRate| (guaranteed carry if you're on the receiving side).
    Risk = price moves against you before settlement.

    Key rule: funding is NOT prorated.  Enter just after settlement,
    exit just before the next one.  This maximises time holding carry
    while minimising market exposure.
    """

    def generate(self, market: dict) -> Optional[Signal]:
        rate = market.get("movingFundingRate")
        if rate is None:
            return None

        abs_rate = abs(rate)
        if abs_rate < FUNDING_HARVEST_THRESH:
            return None

        # If rate > 0 → longs are paying → go SHORT to receive
        # If rate < 0 → shorts are paying → go LONG to receive
        direction = -1 if rate > 0 else 1
        strength  = min(1.0, abs_rate / (FUNDING_HARVEST_THRESH * 3))
        edge      = abs_rate  # the carry we collect

        return Signal(
            ticker    = market["ticker"],
            direction = direction,
            strength  = strength,
            strategy  = "FundingHarvest",
            edge      = edge,
        )


# ─── 2. Mean Reversion (Price vs Index) ──────────────────────────────────────

class MeanReversion:
    """
    Forum prices can diverge from the underlying attention index.
    The funding mechanism mean-reverts them, but slowly.

    Strategy (classic stat-arb):
      spread[t] = lastPrice[t] - lastIndexValue[t]  (in cents)
      z[t]      = (spread[t] - μ) / σ   (rolling window)
      Enter when |z| > MR_ZSCORE_ENTRY
      Exit  when |z| < MR_ZSCORE_EXIT

    This is equivalent to pairs trading the futures vs its index.
    """

    def __init__(self):
        self._history: dict[str, list[float]] = {}   # ticker → spread history

    def update(self, ticker: str, price: Optional[float],
               index: Optional[float]):
        idx_px = _index_in_price_units(price, index)
        if price is None or idx_px is None:
            return
        spread = price - idx_px
        hist = self._history.setdefault(ticker, [])
        hist.append(spread)
        if len(hist) > MR_LOOKBACK_BARS:
            hist.pop(0)

    def generate(self, market: dict) -> Optional[Signal]:
        ticker = market["ticker"]
        price  = market.get("lastPrice")
        index  = market.get("lastIndexValue")
        self.update(ticker, price, index)

        hist = self._history.get(ticker, [])
        if len(hist) < max(10, MR_LOOKBACK_BARS // 2):
            return None

        arr   = np.array(hist, dtype=float)
        mu    = arr.mean()
        sigma = arr.std()
        if sigma < 1e-6:
            return None

        current_spread = hist[-1]
        z = (current_spread - mu) / sigma

        if abs(z) < MR_ZSCORE_ENTRY:
            return None

        # Positive spread → price above index → go short (expect reversion down)
        direction = -1 if z > 0 else 1
        strength  = min(1.0, (abs(z) - MR_ZSCORE_ENTRY) /
                            (MR_ZSCORE_ENTRY * 2))
        # Edge approximation: spread/price as fraction.
        edge = abs(current_spread) / price if price else 0
        # Safety: absurdly large edge usually means malformed market data.
        if edge > 0.20:
            log.debug("%s: skip MR signal — implausible edge %.3f", ticker, edge)
            return None

        if edge < MIN_EDGE_THRESHOLD:
            return None

        return Signal(
            ticker    = ticker,
            direction = direction,
            strength  = strength,
            strategy  = "MeanReversion",
            edge      = edge,
        )

    def should_exit(self, ticker: str) -> bool:
        """Return True when spread has reverted enough to close."""
        hist = self._history.get(ticker, [])
        if len(hist) < 5:
            return False
        arr   = np.array(hist, dtype=float)
        mu, sigma = arr.mean(), arr.std()
        if sigma < 1e-6:
            return True
        z = (hist[-1] - mu) / sigma
        return abs(z) < MR_ZSCORE_EXIT

    def current_abs_zscore(self, ticker: str) -> Optional[float]:
        """Current |z| of spread history for exit gating/logging."""
        hist = self._history.get(ticker, [])
        if len(hist) < 5:
            return None
        arr = np.array(hist, dtype=float)
        mu, sigma = arr.mean(), arr.std()
        if sigma < 1e-6:
            return 0.0
        z = (hist[-1] - mu) / sigma
        return abs(float(z))


# ─── 3. Index Momentum ───────────────────────────────────────────────────────

class IndexMomentum:
    """
    The attention index is an exogenous signal – it can't be directly traded.
    But futures *follow* it with lag.

    Strategy (trend following, EWM crossover à la Renaissance):
      fast_ewm[t]  = EWM(index_value, span=MOM_FAST_BARS)
      slow_ewm[t]  = EWM(index_value, span=MOM_SLOW_BARS)
      signal = fast_ewm - slow_ewm
      Buy when signal > threshold, sell when < -threshold

    We use the index (not price) to avoid endogenous feedback.
    """

    def __init__(self):
        self._index_hist: dict[str, list[float]] = {}

    def update(self, ticker: str, index_value: Optional[float]):
        if index_value is None:
            return
        try:
            v = float(index_value)
        except (TypeError, ValueError):
            return
        hist = self._index_hist.setdefault(ticker, [])
        # Same poll often calls generate() multiple times per ticker (exit check, funding gate,
        # signal). Do not stack duplicate index prints — it poisons EWM length and drift.
        if hist and math.isclose(v, hist[-1], rel_tol=0.0, abs_tol=1e-9):
            return
        hist.append(v)
        if len(hist) > MOM_SLOW_BARS * 3:
            hist.pop(0)

    def warmup_status(self, tickers: Sequence[str]) -> dict:
        """
        Diagnostics: how many names have enough index samples for IndexMomentum.generate().
        Call after markets have been processed so update() has run for each ticker.
        """
        need = MOM_SLOW_BARS + 1
        ready = 0
        empty = 0
        lengths: list[int] = []
        for t in tickers:
            ln = len(self._index_hist.get(t, []))
            lengths.append(ln)
            if ln == 0:
                empty += 1
            elif ln >= need:
                ready += 1
        return {
            "need_bars": need,
            "ready": ready,
            "total": len(tickers),
            "empty_hist": empty,
            "min_len": min(lengths) if lengths else 0,
            "max_len": max(lengths) if lengths else 0,
        }

    def _ewm(self, values: list[float], span: int) -> float:
        alpha = 2.0 / (span + 1)
        ewm = values[0]
        for v in values[1:]:
            ewm = alpha * v + (1 - alpha) * ewm
        return ewm

    def generate(self, market: dict) -> Optional[Signal]:
        ticker = market["ticker"]
        idx_val = market.get("lastIndexValue")
        self.update(ticker, idx_val)

        hist = self._index_hist.get(ticker, [])
        if len(hist) < MOM_SLOW_BARS + 1:
            return None

        fast = self._ewm(hist, MOM_FAST_BARS)
        slow = self._ewm(hist, MOM_SLOW_BARS)
        diff = fast - slow

        # Normalise by slow EWM to get a % signal
        if slow < 1e-6:
            return None
        norm_diff = diff / slow

        if abs(norm_diff) < MOM_SIGNAL_THRESH:
            return None

        direction = 1 if norm_diff > 0 else -1
        strength  = min(1.0, abs(norm_diff) / (MOM_SIGNAL_THRESH * 3))

        # Use full normalized divergence as edge so we clear MIN_EDGE_THRESHOLD once
        # |norm_diff| >= MOM_SIGNAL_THRESH (0.5× was routinely filtered out by the combiner).
        edge = abs(norm_diff)

        if edge < MIN_EDGE_THRESHOLD:
            return None

        return Signal(
            ticker    = ticker,
            direction = direction,
            strength  = strength,
            strategy  = "IndexMomentum",
            edge      = edge,
        )


# ─── Signal Combiner ─────────────────────────────────────────────────────────

# Strategy weights (must sum to 1.0). Funding-first: perp carry is the cleanest edge.
WEIGHTS = {
    # Competition mode: favor carry + trend; MR is disabled for entries by default.
    "FundingHarvest": 0.30,
    "MeanReversion":  0.00,
    "IndexMomentum":  0.70,
}

# |combined_dir| above this → long/short; MR-only often ~0.10–0.14 — higher = quieter.
COMBINED_DIRECTION_THRESH = 0.14


def combine_signals(signals: List[Signal]) -> Optional[Signal]:
    """
    Weighted vote across strategies for a single ticker.
    Returns None if no consensus or insufficient edge.
    """
    if not signals:
        return None

    ticker = signals[0].ticker
    weighted_dir = 0.0
    total_weight = 0.0
    total_edge   = 0.0

    for sig in signals:
        w = WEIGHTS.get(sig.strategy, 0.1)
        weighted_dir += sig.direction * sig.strength * w
        total_weight += w
        total_edge   += sig.edge * w

    if total_weight == 0:
        return None

    combined_dir = weighted_dir / total_weight
    avg_edge     = total_edge   / total_weight

    if avg_edge < MIN_EDGE_THRESHOLD:
        log.debug(f"{ticker}: combined edge {avg_edge:.4f} below threshold, skip")
        return None

    # Quality gates to reduce noisy, low-conviction entries.
    if len(signals) == 1 and REQUIRE_MULTI_STRATEGY_CONFIRMATION:
        only = signals[0]
        if not ALLOW_SINGLE_STRATEGY_SIGNALS:
            if only.strategy in SINGLE_STRATEGY_ALLOWLIST and only.strength >= SINGLE_STRATEGY_ALLOWLIST_MIN_STRENGTH:
                pass
            else:
                log.debug("%s: single-strategy signals disabled (strategy=%s), skip", ticker, only.strategy)
                return None
        if only.strength < SINGLE_STRATEGY_MIN_STRENGTH:
            log.debug(
                "%s: single-strategy signal too weak (%.2f < %.2f), skip",
                ticker,
                only.strength,
                SINGLE_STRATEGY_MIN_STRENGTH,
            )
            return None

    t = COMBINED_DIRECTION_THRESH
    if -t <= combined_dir <= t:
        log.debug(
            f"{ticker}: combined_dir={combined_dir:.3f} in dead zone ±{t}, no trade"
        )
        return None

    direction = 1 if combined_dir > 0 else -1
    strength  = min(1.0, abs(combined_dir))
    if strength < MIN_COMBINED_STRENGTH:
        log.debug(
            "%s: combined strength %.2f below minimum %.2f, skip",
            ticker,
            strength,
            MIN_COMBINED_STRENGTH,
        )
        return None

    strats = ", ".join(s.strategy for s in signals)
    return Signal(
        ticker    = ticker,
        direction = direction,
        strength  = strength,
        strategy  = f"Combined({strats})",
        edge      = avg_edge,
    )
