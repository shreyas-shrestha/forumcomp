"""
core/portfolio.py  –  NAV tracking, position sizing, risk checks
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional
import time
import logging
from config import (
    INITIAL_CAPITAL, MAX_POSITION_PCT, MAX_GROSS_LEVERAGE,
    DAILY_LOSS_LIMIT_PCT, MAX_DRAWDOWN_PCT,
)

log = logging.getLogger(__name__)


@dataclass
class Position:
    ticker:         str
    qty:            int        # positive = long, negative = short
    avg_price:      float      # cents
    last_cum_funding: float = 0.0

    @property
    def notional(self) -> float:
        return abs(self.qty) * self.avg_price

    def mark_pnl(self, mark: float) -> float:
        return self.qty * (mark - self.avg_price)

    def funding_pnl(self, cum_funding: float) -> float:
        """accrued funding = qty * (cumFunding - lastCumFunding)"""
        return self.qty * (cum_funding - self.last_cum_funding)


class Portfolio:
    """
    Tracks NAV, positions, and enforces risk limits.
    All prices in cents.
    """

    def __init__(self):
        self.cash: float              = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.peak_nav: float          = INITIAL_CAPITAL
        self.day_start_nav: float     = INITIAL_CAPITAL
        self.day_start_ts: float      = time.time()
        self._halted: bool            = False

    # ── NAV ─────────────────────────────────────────────────────────────────

    def nav(self, marks: Dict[str, float]) -> float:
        """Current NAV = cash + sum of mark-to-market position values."""
        unrealized = sum(
            p.mark_pnl(marks[t])
            for t, p in self.positions.items()
            if t in marks
        )
        return self.cash + unrealized

    def gross_exposure(self, marks: Dict[str, float]) -> float:
        return sum(abs(p.qty) * marks.get(t, p.avg_price)
                   for t, p in self.positions.items())

    # ── Position sizing ──────────────────────────────────────────────────────

    def max_order_notional(self, ticker: str, marks: Dict[str, float]) -> float:
        """
        Returns max additional notional we can put on in `ticker`
        given per-name and gross leverage limits.
        """
        current_nav = self.nav(marks)
        per_name_cap = current_nav * MAX_POSITION_PCT
        current_notional = abs(self.positions.get(ticker, Position(ticker, 0, 0)).qty) \
                           * marks.get(ticker, 0)
        remaining_name  = max(0, per_name_cap - current_notional)

        gross_cap = current_nav * MAX_GROSS_LEVERAGE
        remaining_gross = max(0, gross_cap - self.gross_exposure(marks))

        return min(remaining_name, remaining_gross)

    def target_qty(self, ticker: str, side: str,
                   marks: Dict[str, float], fraction: float = 1.0) -> int:
        """
        Compute integer contracts for a desired notional fraction of the per-name cap.
        side: 'buy' | 'sell'
        fraction: 0–1, how much of the allowed notional to use
        """
        price = marks.get(ticker)
        if not price:
            return 0
        max_notional = self.max_order_notional(ticker, marks) * fraction
        qty = int(max_notional / price)
        return qty  # caller applies sign

    def explain_zero_target_qty(
        self, ticker: str, marks: Dict[str, float], fraction: float
    ) -> str:
        """
        Human-readable reason when target_qty(...) == 0 (per-ticker cap, gross cap, or <1 contract).
        """
        price = marks.get(ticker)
        if not price:
            return "no mark price for sizing"
        nav = self.nav(marks)
        per_name_cap = nav * MAX_POSITION_PCT
        pos = self.positions.get(ticker)
        q = pos.qty if pos else 0
        cur_notional = abs(q) * float(marks.get(ticker, 0))
        remaining_name = per_name_cap - cur_notional
        gross_cap = nav * MAX_GROSS_LEVERAGE
        gex = self.gross_exposure(marks)
        remaining_gross = gross_cap - gex
        raw_cap = min(max(0.0, remaining_name), max(0.0, remaining_gross))
        max_notional = raw_cap * fraction
        if raw_cap <= 0:
            if remaining_name <= 0:
                return (
                    f"per-ticker cap (NAV≈${nav / 100:.0f} × {MAX_POSITION_PCT:.0%} "
                    f"≈${per_name_cap / 100:.0f}; {ticker} already ≈${cur_notional / 100:.0f})"
                )
            return (
                f"gross cap (gross≈${gex / 100:.0f} vs max≈${gross_cap / 100:.0f})"
            )
        if max_notional < price:
            return (
                f"below 1 contract after sizing (room≈${max_notional / 100:.2f} "
                f"vs px≈${price / 100:.2f}, strength×{fraction:.2f})"
            )
        return "qty rounded to 0 (unexpected — check sizing)"

    # ── Trade accounting ─────────────────────────────────────────────────────

    def record_fill(self, ticker: str, side: str, qty: int,
                    price: float, fee: float = 0.0):
        """
        Update position and cash after a fill.
        qty is always positive; side determines direction.
        """
        signed_qty = qty if side == "buy" else -qty
        cost = signed_qty * price + fee

        if ticker in self.positions:
            pos = self.positions[ticker]
            new_qty = pos.qty + signed_qty
            if new_qty == 0:
                del self.positions[ticker]
            elif (pos.qty > 0) == (new_qty > 0):
                # same side – update avg
                total_cost = pos.qty * pos.avg_price + signed_qty * price
                pos.qty      = new_qty
                pos.avg_price = total_cost / new_qty
            else:
                # flipped
                pos.qty       = new_qty
                pos.avg_price = price
        else:
            if signed_qty != 0:
                self.positions[ticker] = Position(ticker, signed_qty, price)

        self.cash -= cost
        log.info(f"FILL {side.upper()} {qty}x {ticker} @ {price/100:.2f}  "
                 f"cash={self.cash/100:.2f}")

    def apply_exchange_snapshot(
        self,
        position_rows: list,
        account: Optional[dict],
        marks: Dict[str, float],
    ) -> None:
        """
        Overwrite open positions from GET /positions and align cash with GET /account equity.

        Sets cash so that nav(marks) matches exchange equity when marks match the exchange
        mark; between polls, NAV moves with lastPrice like a normal MTM book.
        """
        self.positions.clear()
        for row in position_rows:
            if not isinstance(row, dict):
                continue
            t = row.get("ticker")
            if not t:
                continue
            raw_qty = row.get("qty", 0)
            try:
                q = float(raw_qty)
            except (TypeError, ValueError):
                continue
            if abs(q) < 1e-9:
                continue
            qty_i = int(round(q))
            if qty_i == 0:
                continue
            try:
                avg = float(row["avgEntryPrice"])
            except (KeyError, TypeError, ValueError):
                continue
            lcf = row.get("lastCumFunding")
            try:
                lcf_f = float(lcf) if lcf is not None else 0.0
            except (TypeError, ValueError):
                lcf_f = 0.0
            self.positions[str(t)] = Position(str(t), qty_i, avg, last_cum_funding=lcf_f)

        if account and isinstance(account, dict):
            eq = account.get("equity")
            if eq is not None:
                try:
                    equity = float(eq)
                except (TypeError, ValueError):
                    equity = None
                if equity is not None:
                    local_u = sum(
                        p.mark_pnl(marks[t] if t in marks else p.avg_price)
                        for t, p in self.positions.items()
                    )
                    self.cash = equity - local_u

    def update_funding(self, ticker: str, cum_funding: float):
        """Apply funding PnL and update position's lastCumFunding."""
        if ticker not in self.positions:
            return
        pos = self.positions[ticker]
        pnl = pos.funding_pnl(cum_funding)
        self.cash += pnl
        pos.last_cum_funding = cum_funding
        if abs(pnl) > 10:
            log.info(f"FUNDING {ticker}  pnl={pnl/100:+.4f}  rate={'(calc)':>8}")

    # ── Risk checks ──────────────────────────────────────────────────────────

    def check_risk(self, marks: Dict[str, float]) -> bool:
        """Returns False (and halts bot) if any risk limit is breached."""
        if self._halted:
            return False

        current_nav = self.nav(marks)

        # Reset daily baseline if new day
        if time.time() - self.day_start_ts > 86400:
            self.day_start_nav = current_nav
            self.day_start_ts  = time.time()

        # Update peak
        if current_nav > self.peak_nav:
            self.peak_nav = current_nav

        daily_loss = (self.day_start_nav - current_nav) / self.day_start_nav
        drawdown   = (self.peak_nav - current_nav) / self.peak_nav

        if daily_loss > DAILY_LOSS_LIMIT_PCT:
            log.critical(f"HALT: daily loss {daily_loss:.1%} > {DAILY_LOSS_LIMIT_PCT:.1%}")
            self._halted = True
        if drawdown > MAX_DRAWDOWN_PCT:
            log.critical(f"HALT: drawdown {drawdown:.1%} > {MAX_DRAWDOWN_PCT:.1%}")
            self._halted = True

        return not self._halted

    @property
    def halted(self) -> bool:
        return self._halted
