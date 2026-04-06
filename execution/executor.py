"""
execution/executor.py
======================
Smart order execution:
  - Passive limit orders (join best bid/ask) for low-urgency signals
  - Aggressive limits (cross spread slightly) for funding-harvest timing
  - Auto-cancel stale orders
  - Reduce-only closes
"""

from __future__ import annotations
import time
import logging
from datetime import datetime, timezone
from typing import Optional

from core.client import ForumClient
from core.portfolio import Portfolio
from config import SLIPPAGE_BUFFER, ORDER_TIMEOUT_SECS

log = logging.getLogger(__name__)


class Executor:
    def __init__(self, client: ForumClient, portfolio: Portfolio, dry_run: bool = True):
        self.client    = client
        self.portfolio = portfolio
        self.dry_run   = dry_run      # set False to actually trade
        self._pending: dict[str, dict] = {}   # order_id (str) → meta
        self._seen_trade_ids: set[int] = set()
        self._fill_sync_start = datetime.now(timezone.utc)

    # ── Main entry point ─────────────────────────────────────────────────────

    def execute_signal(self, ticker: str, direction: int, strength: float,
                       marks: dict[str, float], urgent: bool = False):
        """
        Convert a signal into an order.

        urgent=True  → aggressive limit (slightly cross spread) for time-sensitive
                        signals like funding harvest where timing matters.
        urgent=False → passive limit at best bid/ask.
        """
        if direction == 0:
            self._close_position(ticker, marks)
            return

        side = "buy" if direction > 0 else "sell"
        qty  = self.portfolio.target_qty(ticker, side, marks, fraction=strength)

        if qty <= 0:
            log.debug(f"{ticker}: qty=0, skip order")
            return

        # Check if we already hold a position in the same direction
        pos = self.portfolio.positions.get(ticker)
        if pos and ((pos.qty > 0 and direction > 0) or
                    (pos.qty < 0 and direction < 0)):
            log.debug(f"{ticker}: already positioned in direction {direction}, skip")
            return

        price = self._get_order_price(ticker, side, marks, urgent)
        if price is None:
            return

        self._place(ticker, side, qty, price)

    def close_position(self, ticker: str, marks: dict[str, float]):
        self._close_position(ticker, marks)

    # ── Internals ────────────────────────────────────────────────────────────

    def _close_position(self, ticker: str, marks: dict[str, float]):
        pos = self.portfolio.positions.get(ticker)
        if not pos or pos.qty == 0:
            return
        side  = "sell" if pos.qty > 0 else "buy"
        qty   = abs(pos.qty)
        price = self._get_order_price(ticker, side, marks, urgent=True)
        if price:
            self._place(ticker, side, qty, price, reduce_only=True)

    def _get_order_price(self, ticker: str, side: str,
                         marks: dict[str, float], urgent: bool) -> Optional[int]:
        """
        Returns limit price in cents.
        Passive: join the queue (best bid for buys, best ask for sells).
        Aggressive: cross slightly (best ask + buffer for buys).
        """
        try:
            ob = self.client.get_orderbook(ticker)
        except Exception as e:
            log.warning(f"Orderbook fetch failed for {ticker}: {e}")
            lp = marks.get(ticker)
            if lp is None:
                return None
            return int(lp + SLIPPAGE_BUFFER) if side == "buy" \
                   else int(lp - SLIPPAGE_BUFFER)

        bids = ob.get("bids", [])
        asks = ob.get("asks", [])

        if urgent:
            if side == "buy"  and asks:
                return int(asks[0]["price"]) + SLIPPAGE_BUFFER
            if side == "sell" and bids:
                return int(bids[0]["price"]) - SLIPPAGE_BUFFER
        else:
            if side == "buy"  and bids:
                return int(bids[0]["price"])
            if side == "sell" and asks:
                return int(asks[0]["price"])

        lp = marks.get(ticker)
        return int(lp) if lp else None

    def _place(self, ticker: str, side: str, qty: int, price: int,
               reduce_only: bool = False):
        log.info(f"{'[DRY]' if self.dry_run else '[LIVE]'} "
                 f"ORDER {side.upper()} {qty}x {ticker} @ {price/100:.2f}"
                 + (" [reduce-only]" if reduce_only else ""))
        if self.dry_run:
            self.portfolio.record_fill(ticker, side, qty, float(price))
            return

        try:
            resp = self.client.place_order(ticker, side, qty,
                                           order_type="limit", price=price,
                                           reduce_only=reduce_only)
            oid = resp.get("orderId")
            if oid is None and isinstance(resp.get("id"), int):
                oid = resp.get("id")
            if oid is not None:
                oid_str = str(oid)
                self._pending[oid_str] = {
                    "ticker":     ticker,
                    "side":       side,
                    "qty":        qty,
                    "placed_at":  time.time(),
                }
        except Exception as e:
            detail = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    detail = (resp.text or "")[:400]
                except Exception:
                    pass
            log.error("Order placement failed: %s %s", e, detail or "")

    # ── Stale order cleanup ──────────────────────────────────────────────────

    def cancel_stale_orders(self):
        now = time.time()
        to_kill = [
            oid for oid, meta in self._pending.items()
            if now - meta["placed_at"] > ORDER_TIMEOUT_SECS
        ]
        if not to_kill:
            return
        if self.dry_run:
            for oid in to_kill:
                del self._pending[oid]
            return
        try:
            ids = [int(oid) for oid in to_kill]
            if len(ids) > 1:
                log.info(f"Batch cancelling {len(ids)} stale orders")
                self.client.cancel_orders_batch(ids)
            else:
                log.info(f"Cancelling stale order {ids[0]}")
                self.client.cancel_order(ids[0])
        except Exception as e:
            log.warning(f"Stale cancel batch failed, falling back per-order: {e}")
            for oid in to_kill:
                try:
                    self.client.cancel_order(oid)
                except Exception as e2:
                    log.warning(f"Cancel failed {oid}: {e2}")
        finally:
            for oid in to_kill:
                self._pending.pop(oid, None)

    def ingest_exchange_fill(
        self,
        trade_id: int,
        ticker: str,
        side: str,
        qty: int,
        price: float,
    ) -> None:
        """Idempotent fill ingestion (REST List fills or userFills WebSocket)."""
        if self.dry_run:
            return
        if trade_id in self._seen_trade_ids:
            return
        self._seen_trade_ids.add(trade_id)
        self.portfolio.record_fill(ticker, side, qty, price)

    def ingest_exchange_fill_payload(self, data: dict) -> None:
        """Map WebSocket userFills `data` object to portfolio."""
        if not data:
            return
        tid = data.get("tradeId")
        if tid is None:
            return
        self.ingest_exchange_fill(
            int(tid),
            str(data["ticker"]),
            str(data["side"]),
            int(data["qty"]),
            float(data["price"]),
        )

    def release_pending_order_from_ws(self, order_id: int, status: str) -> None:
        """
        Clear _pending when userOrders reports a terminal state.
        Keeps resting / partiallyFilled so stale-cancel still applies.
        """
        if status not in ("filled", "cancelled", "rejected"):
            return
        self._pending.pop(str(order_id), None)

    def sync_fills(self):
        """
        Live: poll List fills for new tradeIds; update portfolio from executions.
        Drops pending order ids once they disappear from open orders (filled/cancelled).
        """
        if self.dry_run:
            return
        try:
            fills = self.client.list_fills(
                start=self._fill_sync_start,
                limit=100,
            )
            for fill in fills:
                tid = fill.get("tradeId")
                if tid is None:
                    continue
                self.ingest_exchange_fill(
                    int(tid),
                    fill["ticker"],
                    fill["side"],
                    int(fill["qty"]),
                    float(fill["price"]),
                )

            open_ids = {o["id"] for o in self.client.get_open_orders()
                        if o.get("id") is not None}
            for oid in list(self._pending.keys()):
                if int(oid) not in open_ids:
                    del self._pending[oid]
        except Exception as e:
            log.warning(f"Fill sync error: {e}")
