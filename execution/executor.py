"""
execution/executor.py
======================
Smart order execution:
  - Passive / improve / aggressive limit pricing (see config ENTRY_OPEN_PRICE_MODE)
  - Aggressive limits (cross spread slightly) for funding-harvest timing only
  - Auto-cancel stale orders
  - Reduce-only closes
"""

from __future__ import annotations
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from collections import defaultdict, deque

BOT_CLIENT_ORDER_PREFIX = "fb-"

import requests

from core.client import ForumClient
from core.portfolio import Portfolio
from config import (
    SLIPPAGE_BUFFER,
    ORDER_TIMEOUT_SECS,
    POLL_INTERVAL_SECS,
    ENTRY_OPEN_PRICE_MODE,
    LOW_PRICE_THRESHOLD_CENTS,
    MAX_CONTRACTS_LOW_PRICE,
    STRATEGY_THROTTLE_ENABLED,
    STRATEGY_THROTTLE_LOOKBACK,
    STRATEGY_THROTTLE_LOSS_PCT,
    STRATEGY_THROTTLE_REDUCTION,
    STRATEGY_THROTTLE_MIN_MULTIPLIER,
    MAX_PENDING_BOT_ORDERS,
    MAX_PENDING_BOT_ORDERS_PER_TICKER,
    MARGIN_ERROR_COOLDOWN_SECS,
    UNWIND_ESCALATE_AFTER_REFRESHES,
    UNWIND_MIN_PROFIT_CENTS,
    UNWIND_FORCE_IF_FREE_MARGIN_BELOW_CENTS,
    MAX_ALLOC_FREE_MARGIN_FRAC,
)

log = logging.getLogger(__name__)


def _summarize_api_error(response: Optional[Any]) -> str:
    """Parse Forum JSON error or raw text for logs."""
    if response is None:
        return ""
    try:
        j = response.json()
        err = j.get("error")
        if isinstance(err, dict):
            parts = [
                str(err.get("code") or ""),
                str(err.get("message") or ""),
            ]
            d = err.get("details")
            if d is not None:
                parts.append(json.dumps(d, default=str)[:200])
            return " | ".join(p for p in parts if p) or str(j)[:350]
        return str(j)[:350]
    except Exception:
        try:
            return (response.text or "")[:400]
        except Exception:
            return ""


class Executor:
    def __init__(self, client: ForumClient, portfolio: Portfolio, dry_run: bool = True):
        self.client    = client
        self.portfolio = portfolio
        self.dry_run   = dry_run      # set False to actually trade
        self._pending: dict[str, dict] = {}   # order_id (str) → meta
        self._seen_trade_ids: set[int] = set()
        self._fill_sync_start = datetime.now(timezone.utc)
        self._pos_opened_at: dict[str, float] = {}
        self._ticker_last_open_strategy: dict[str, str] = {}
        self._strategy_score_hist: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=STRATEGY_THROTTLE_LOOKBACK)
        )
        self._margin_cooldown_until: float = 0.0
        self._last_mm_quote_at: dict[str, float] = {}
        # Latest exchange account snapshot (cents) from reconcile. Used for MM hygiene.
        self._last_equity_cents: Optional[float] = None
        self._last_free_margin_cents: Optional[float] = None
        self._last_unwind_at: dict[str, float] = {}
        # Consecutive unwind refreshes per ticker since last flatten.
        # Used to escalate pricing when reduce-only limits never fill.
        self._unwind_refresh_count: dict[str, int] = {}

    @property
    def pending_order_count(self) -> int:
        return len(self._pending)

    @property
    def pending_bot_managed_count(self) -> int:
        return sum(1 for m in self._pending.values() if m.get("bot_managed"))

    @property
    def pending_manual_count(self) -> int:
        return self.pending_order_count - self.pending_bot_managed_count

    def reconcile_from_exchange(self, marks: dict[str, float]) -> bool:
        """
        Live: snapshot positions, cash (from account equity), and all open orders from REST.

        Portfolio updates come from this path so manual trades and restarts stay aligned.
        Open orders with clientOrderId starting with ``fb-`` are treated as bot-managed for
        stale cancellation; others are tracked but not auto-cancelled.
        """
        if self.dry_run:
            return True
        acct: Optional[dict] = None
        try:
            pos_rows = self.client.get_positions()
        except Exception as e:
            log.warning("Exchange reconcile: get_positions failed: %s", e)
            return False
        try:
            raw_acct = self.client.get_account()
            if isinstance(raw_acct, dict) and "data" in raw_acct and isinstance(raw_acct["data"], dict):
                acct = raw_acct["data"]
            elif isinstance(raw_acct, dict):
                acct = raw_acct
        except Exception as e:
            log.warning("Exchange reconcile: get_account failed (positions still synced): %s", e)

        try:
            self.portfolio.apply_exchange_snapshot(pos_rows, acct, marks)
        except Exception as e:
            log.warning("Exchange reconcile: apply snapshot failed: %s", e)
            return False

        try:
            open_orders = self.client.list_all_open_orders()
        except Exception as e:
            log.warning("Exchange reconcile: list open orders failed: %s", e)
            return False

        # Stale-cancel uses placed_at vs ORDER_TIMEOUT_SECS. Do not use exchange
        # createdAt for first-time adoption (e.g. after restart) or old GTC orders
        # would be cancelled immediately. Preserve placed_at while an id stays open.
        prev_pending = dict(self._pending)
        new_pending: dict[str, dict] = {}
        for o in open_orders:
            if not isinstance(o, dict):
                continue
            oid = o.get("id")
            if oid is None:
                continue
            st = str(o.get("status") or "")
            if st in ("filled", "cancelled", "rejected"):
                continue
            cid = str(o.get("clientOrderId") or "")
            rem = o.get("remainingQuantity")
            if rem is None:
                rem = o.get("quantity")
            try:
                rq = int(rem) if rem is not None else 0
            except (TypeError, ValueError):
                rq = 0
            oid_str = str(oid)
            if oid_str in prev_pending:
                placed_at = prev_pending[oid_str]["placed_at"]
                session_placed = prev_pending[oid_str].get("session_placed", False)
            else:
                # Adopted from exchange (e.g. after restart): do not stale-cancel; only
                # orders placed by this process with session_placed=True use ORDER_TIMEOUT.
                placed_at = time.time()
                session_placed = False
            new_pending[oid_str] = {
                "ticker": o.get("ticker"),
                "side": o.get("side"),
                "qty": rq,
                "placed_at": placed_at,
                "bot_managed": cid.startswith(BOT_CLIENT_ORDER_PREFIX),
                "session_placed": session_placed,
            }
        self._pending = new_pending

        now_ts = time.time()
        for t in self.portfolio.positions.keys():
            self._pos_opened_at.setdefault(t, now_ts)
        for t in list(self._pos_opened_at.keys()):
            if t not in self.portfolio.positions:
                self._pos_opened_at.pop(t, None)

        eq = None
        if isinstance(acct, dict) and acct.get("equity") is not None:
            try:
                eq = float(acct["equity"])
            except (TypeError, ValueError):
                eq = None
        fm = None
        if isinstance(acct, dict) and acct.get("freeMargin") is not None:
            try:
                fm = float(acct["freeMargin"])
            except (TypeError, ValueError):
                fm = None
        self._last_equity_cents = eq
        self._last_free_margin_cents = fm
        log.info(
            "reconcile | positions=%d open_orders=%d (bot=%d manual=%d)%s",
            len(self.portfolio.positions),
            len(self._pending),
            self.pending_bot_managed_count,
            self.pending_manual_count,
            f" equity≈${eq / 100.0:.2f}" if eq is not None else "",
        )
        return True

    # ── Main entry point ─────────────────────────────────────────────────────

    def execute_signal(
        self,
        ticker: str,
        direction: int,
        strength: float,
        marks: dict[str, float],
        *,
        price_mode: Optional[str] = None,
        strategy_label: Optional[str] = None,
    ) -> bool:
        """
        Convert a signal into an order.

        price_mode: "passive" | "improve" | "aggressive" (default: config ENTRY_OPEN_PRICE_MODE).
        Bot passes aggressive only for funding-urgent windows; normal opens use ENTRY_OPEN_PRICE_MODE.
        """
        mode = price_mode if price_mode is not None else ENTRY_OPEN_PRICE_MODE
        if mode not in ("passive", "improve", "aggressive"):
            log.warning("%s: unknown price_mode %r — using passive", ticker, mode)
            mode = "passive"
        if direction == 0:
            return self._close_position(ticker, marks)

        # Margin hygiene: if we're in a cooldown (recent INSUFFICIENT_MARGIN / conflicts),
        # do not open new risk. Still allow reduce-only closes via direction==0 above.
        if not self.dry_run and time.time() < self._margin_cooldown_until:
            log.info("%s: skip open — margin cooldown active (%.0fs left)", ticker, self._margin_cooldown_until - time.time())
            return False

        side = "buy" if direction > 0 else "sell"
        eff_strength = self.apply_strategy_throttle(strategy_label, strength)
        qty  = self.portfolio.target_qty(ticker, side, marks, fraction=eff_strength)
        mpx = marks.get(ticker)
        if mpx is not None and mpx <= LOW_PRICE_THRESHOLD_CENTS:
            qty = min(qty, MAX_CONTRACTS_LOW_PRICE)

        if qty <= 0:
            log.info(
                "%s: skip order — target qty=0 (side=%s strength=%.2f; check NAV vs per-ticker cap)",
                ticker,
                side,
                eff_strength,
            )
            return False

        # Dynamic sizing: cap OPEN risk as a fraction of freeMargin.
        # This prevents repeated 409 INSUFFICIENT_MARGIN loops when signals fire during low headroom.
        if not self.dry_run and side in ("buy", "sell"):
            fm = self._last_free_margin_cents
            if fm is not None and fm > 0:
                # Use the intended order price when available, else last mark.
                est_px = None
                try:
                    est_px = int(mpx) if mpx is not None else None
                except Exception:
                    est_px = None
                # We'll refine after we compute the actual limit price below; for now, cap by mark.
                if est_px is not None and est_px > 0:
                    max_alloc = float(fm) * float(MAX_ALLOC_FREE_MARGIN_FRAC)
                    max_qty = int(max_alloc // float(est_px))
                    if max_qty >= 0:
                        qty = max(0, min(int(qty), int(max_qty)))
                        if qty == 0:
                            log.info(
                                "%s: skip open — freeMargin cap (free≈$%.2f alloc=%.0f%% px=%.2f)",
                                ticker,
                                fm / 100.0,
                                float(MAX_ALLOC_FREE_MARGIN_FRAC) * 100.0,
                                est_px / 100.0,
                            )
                            return False

        # Check if we already hold a position in the same direction
        pos = self.portfolio.positions.get(ticker)
        if pos and ((pos.qty > 0 and direction > 0) or
                    (pos.qty < 0 and direction < 0)):
            log.info(
                "%s: skip order — already long/short in same direction (qty=%s)",
                ticker,
                pos.qty,
            )
            return False

        # Keep queue priority and reduce spam: if we already have a same-side
        # bot-managed resting order on this ticker, do not cancel/repost.
        same_side_resting = any(
            meta.get("bot_managed")
            and meta.get("ticker") == ticker
            and meta.get("side") == side
            for meta in self._pending.values()
        )
        if same_side_resting:
            log.info("%s: skip order — same-side bot limit already resting", ticker)
            return False

        # Global pending-order budget (prevents margin being locked by too many rests).
        bot_pending = [m for m in self._pending.values() if m.get("bot_managed")]
        if len(bot_pending) >= MAX_PENDING_BOT_ORDERS:
            log.info(
                "%s: skip open — bot pending budget hit (%d/%d)",
                ticker,
                len(bot_pending),
                MAX_PENDING_BOT_ORDERS,
            )
            return False
        per_t = sum(1 for m in bot_pending if m.get("ticker") == ticker)
        if per_t >= MAX_PENDING_BOT_ORDERS_PER_TICKER:
            log.info(
                "%s: skip open — per-ticker pending budget hit (%d/%d)",
                ticker,
                per_t,
                MAX_PENDING_BOT_ORDERS_PER_TICKER,
            )
            return False

        price = self._get_order_price(ticker, side, marks, mode)
        if price is None:
            log.warning("%s: skip order — no limit price (orderbook empty and no mark)", ticker)
            return False

        # Re-apply freeMargin cap using the actual order price (more accurate than mark).
        if not self.dry_run:
            fm = self._last_free_margin_cents
            if fm is not None and fm > 0 and price > 0:
                max_alloc = float(fm) * float(MAX_ALLOC_FREE_MARGIN_FRAC)
                max_qty = int(max_alloc // float(price))
                qty = max(0, min(int(qty), int(max_qty)))
                if qty == 0:
                    log.info(
                        "%s: skip open — freeMargin cap (free≈$%.2f alloc=%.0f%% px=%.2f)",
                        ticker,
                        fm / 100.0,
                        float(MAX_ALLOC_FREE_MARGIN_FRAC) * 100.0,
                        price / 100.0,
                    )
                    return False

        self._cancel_bot_managed_orders_for_ticker(ticker)
        return self._place(ticker, side, qty, price, price_mode=mode, strategy_label=strategy_label)

    def close_position(self, ticker: str, marks: dict[str, float]):
        self._close_position(ticker, marks)

    def quote_market_make(
        self,
        ticker: str,
        marks: dict[str, float],
        *,
        qty: int,
        max_spread_cents: int,
        max_inventory: int,
        refresh_secs: int,
    ) -> None:
        """
        Two-sided quoting for spread capture (low frequency).

        - Quotes inside the spread when wide enough (bb+1 / ba-1).
        - Respects inventory caps: if |pos| >= max_inventory, only quote the reducing side.
        - Places at most one bid and one ask per ticker (uses MAX_PENDING_BOT_ORDERS_PER_TICKER=2).
        - Refreshes no more often than refresh_secs per ticker.
        """
        if qty <= 0:
            return
        now = time.time()
        last = self._last_mm_quote_at.get(ticker, 0.0)
        if now - last < max(1, refresh_secs):
            return

        # Margin hygiene: do not open new risk during cooldown. Still allow reduce-only
        # quotes (if holding inventory) by proceeding; reduce-only is enforced per-side below.
        if not self.dry_run and now < self._margin_cooldown_until:
            pos = self.portfolio.positions.get(ticker)
            if not pos or pos.qty == 0:
                return

        try:
            ob = self.client.get_orderbook(ticker)
        except Exception as e:
            log.debug("%s: market-make skip — orderbook fetch failed: %s", ticker, e)
            return

        bids = ob.get("bids", []) or []
        asks = ob.get("asks", []) or []
        if not bids or not asks:
            return
        bb = int(bids[0]["price"])
        ba = int(asks[0]["price"])
        if ba <= bb:
            return
        spread = ba - bb
        if spread > max_spread_cents:
            log.debug("%s: market-make skip — spread %d > %d", ticker, spread, max_spread_cents)
            return

        # Quote inside when possible.
        bid_px = bb + 1 if spread >= 3 else bb
        ask_px = ba - 1 if spread >= 3 else ba
        if ask_px <= bid_px:
            bid_px = bb
            ask_px = ba

        pos = self.portfolio.positions.get(ticker)
        pos_qty = int(pos.qty) if pos else 0
        want_bid = True
        want_ask = True

        # Inventory cap: if we're too long, stop adding longs; only quote sells (ideally reduce-only).
        if pos_qty >= max_inventory:
            want_bid = False
        if pos_qty <= -max_inventory:
            want_ask = False

        # Free-margin gating: if we don't have enough headroom, don't spam quotes.
        # This avoids 409 INSUFFICIENT_MARGIN and prevents one-sided exposure when only one leg is accepted.
        if not self.dry_run and (want_bid or want_ask):
            fm = self._last_free_margin_cents
            if fm is not None:
                # Roughly estimate additional margin requirement as notional; use a safety factor.
                est_mid = (bid_px + ask_px) / 2.0
                est_notional = float(qty) * est_mid
                need_legs = (1 if want_bid else 0) + (1 if want_ask else 0)
                est_need = est_notional * max(1, need_legs) * 1.35
                # If we're already carrying big exposure (e.g. manual ALTMAN), require more slack.
                if fm < max(10_000.0, est_need):
                    log.info(
                        "%s: market-make skip — low freeMargin (free≈$%.2f need≈$%.2f)",
                        ticker,
                        fm / 100.0,
                        est_need / 100.0,
                    )
                    return

        # If already have a bot-managed resting order on that side, don't repost.
        have_bid = any(
            meta.get("bot_managed") and meta.get("ticker") == ticker and meta.get("side") == "buy"
            for meta in self._pending.values()
        )
        have_ask = any(
            meta.get("bot_managed") and meta.get("ticker") == ticker and meta.get("side") == "sell"
            for meta in self._pending.values()
        )

        # Refresh: cancel only the side we plan to replace.
        if want_bid and have_bid:
            self._cancel_bot_managed_orders_for_ticker_side(ticker, "buy")
            have_bid = False
        if want_ask and have_ask:
            self._cancel_bot_managed_orders_for_ticker_side(ticker, "sell")
            have_ask = False

        # Respect global/per-ticker pending budgets.
        bot_pending = [m for m in self._pending.values() if m.get("bot_managed")]
        per_t = sum(1 for m in bot_pending if m.get("ticker") == ticker)
        budget_left_global = max(0, MAX_PENDING_BOT_ORDERS - len(bot_pending))
        budget_left_ticker = max(0, MAX_PENDING_BOT_ORDERS_PER_TICKER - per_t)

        def _try_place(side: str, price: int) -> bool:
            nonlocal budget_left_global, budget_left_ticker
            if budget_left_global <= 0 or budget_left_ticker <= 0:
                return False
            reduce_only = False
            if pos_qty > 0 and side == "sell":
                reduce_only = True
            if pos_qty < 0 and side == "buy":
                reduce_only = True
            q = int(qty)
            if reduce_only:
                # Never exceed current position when reduce-only (exchange rejects it).
                q = min(q, abs(int(pos_qty)))
                if q <= 0:
                    return False
            ok = self._place(
                ticker,
                side,
                q,
                int(price),
                reduce_only=reduce_only,
                price_mode="passive",
                strategy_label="MarketMaking",
            )
            if ok:
                budget_left_global -= 1
                budget_left_ticker -= 1
            return ok

        # If we're flat (or within inventory), the only "real" market-making is two-sided.
        # If we can’t afford both sides in the budgets, do nothing (prevents drifting directional).
        want_two_sided = want_bid and want_ask and pos_qty == 0
        if want_two_sided:
            if budget_left_global < 2 or budget_left_ticker < 2:
                return

        # Atomic two-sided behavior: if we intend to quote both sides and one fails, pull the other.
        placed_buy = False
        placed_sell = False
        # Place the reducing side first when we have inventory (more important for risk control).
        if pos_qty > 0:
            if want_ask and not have_ask:
                placed_sell = _try_place("sell", ask_px)
            if want_bid and not have_bid:
                placed_buy = _try_place("buy", bid_px)
        elif pos_qty < 0:
            if want_bid and not have_bid:
                placed_buy = _try_place("buy", bid_px)
            if want_ask and not have_ask:
                placed_sell = _try_place("sell", ask_px)
        else:
            if want_bid and not have_bid:
                placed_buy = _try_place("buy", bid_px)
            if want_ask and not have_ask:
                placed_sell = _try_place("sell", ask_px)

        if (want_bid and want_ask) and (placed_buy ^ placed_sell):
            # One side failed (often due to margin). Cancel the surviving side so we don't take accidental exposure.
            surviving_side = "buy" if placed_buy else "sell"
            log.info("%s: MM imbalance — cancelling surviving %s quote (other leg failed)", ticker, surviving_side)
            self._cancel_bot_managed_orders_for_ticker_side(ticker, surviving_side)

        self._last_mm_quote_at[ticker] = now

    def work_unwind_position(
        self,
        ticker: str,
        marks: dict[str, float],
        *,
        max_qty_per_order: int,
        refresh_secs: int,
        price_mode: str = "improve",
    ) -> None:
        """
        Reduce-only position unwind worker.

        Places small reduce-only limit orders to work inventory down without opening new risk.
        Designed for manual/discretionary positions that consume margin and interfere with MM.
        """
        if max_qty_per_order <= 0:
            return
        if price_mode not in ("passive", "improve"):
            price_mode = "improve"

        pos = self.portfolio.positions.get(ticker)
        if not pos or pos.qty == 0:
            self._unwind_refresh_count.pop(ticker, None)
            return

        now = time.time()
        last = self._last_unwind_at.get(ticker, 0.0)
        if now - last < max(1, refresh_secs):
            return

        side = "sell" if pos.qty > 0 else "buy"
        reduce_qty = min(abs(int(pos.qty)), int(max_qty_per_order))
        if reduce_qty <= 0:
            return

        # Compute a non-crossing price from the live book.
        px: Optional[int] = None
        try:
            ob = self.client.get_orderbook(ticker)
            bids = ob.get("bids", []) or []
            asks = ob.get("asks", []) or []
            if bids and asks:
                bb = int(bids[0]["price"])
                ba = int(asks[0]["price"])
                if side == "sell":
                    # sell: prefer inside spread when possible, but never cross below best bid.
                    if price_mode == "improve" and ba - bb >= 3:
                        px = max(bb + 1, ba - 1)
                    else:
                        px = ba
                else:
                    # buy: prefer inside spread when possible, but never cross above best ask.
                    if price_mode == "improve" and ba - bb >= 3:
                        px = min(ba - 1, bb + 1)
                    else:
                        px = bb
        except Exception as e:
            log.debug("%s: unwind orderbook fetch failed: %s", ticker, e)

        if px is None:
            m = marks.get(ticker)
            if m is None:
                return
            px = int(m)

        # Profit guard: do not unwind at a losing price unless margin is dangerously low.
        # This prevents "panic" reduction that crystallizes spread/slippage losses.
        try:
            avg = int(pos.avg_price)
        except Exception:
            avg = 0
        fm = self._last_free_margin_cents
        force = (fm is not None and fm < float(UNWIND_FORCE_IF_FREE_MARGIN_BELOW_CENTS))
        min_profit = int(UNWIND_MIN_PROFIT_CENTS)
        if not force and avg > 0 and min_profit > 0:
            if side == "sell":
                # Selling a long: require px >= avg + min_profit.
                if px < avg + min_profit:
                    log.info(
                        "%s: unwind skip — profit guard (px=%.2f < avg+buf=%.2f; freeMargin≈%s)",
                        ticker,
                        px / 100.0,
                        (avg + min_profit) / 100.0,
                        f"${fm/100.0:.2f}" if fm is not None else "n/a",
                    )
                    self._last_unwind_at[ticker] = now
                    return
            else:
                # Buying to cover a short: require px <= avg - min_profit.
                if px > avg - min_profit:
                    log.info(
                        "%s: unwind skip — profit guard (px=%.2f > avg-buf=%.2f; freeMargin≈%s)",
                        ticker,
                        px / 100.0,
                        (avg - min_profit) / 100.0,
                        f"${fm/100.0:.2f}" if fm is not None else "n/a",
                    )
                    self._last_unwind_at[ticker] = now
                    return

        # Escalation: if we've refreshed unwind orders repeatedly without flattening,
        # cross the spread to actually get out and clear unwind lockout.
        # This is separate from the freeMargin-based "force" path.
        refresh_count = int(self._unwind_refresh_count.get(ticker, 0))
        escalate = (not force) and (int(UNWIND_ESCALATE_AFTER_REFRESHES) > 0) and (refresh_count >= int(UNWIND_ESCALATE_AFTER_REFRESHES))

        # If we're in a true margin squeeze, take liquidity (cross-spread) to free margin now.
        if force or escalate:
            try:
                # "aggressive" crosses (best ask/bid ± buffer) via the shared pricing function.
                cross_px = self._get_order_price(ticker, side, marks, "aggressive")
                if cross_px is not None:
                    px = int(cross_px)
                    price_mode = "aggressive"
            except Exception:
                pass

        # Replace only our prior reduce-only orders on that side (avoid stacking).
        self._cancel_bot_managed_orders_for_ticker_side(ticker, side)
        ok = self._place(
            ticker,
            side,
            reduce_qty,
            int(px),
            reduce_only=True,
            price_mode=price_mode,
            strategy_label="Unwind",
        )
        if ok:
            log.info(
                "%s: unwind working %s %sx @ %.2f (pos=%s)",
                ticker,
                side.upper(),
                reduce_qty,
                px / 100.0,
                pos.qty,
            )
            # Only count "attempts" when we actually submit an unwind order.
            self._unwind_refresh_count[ticker] = refresh_count + 1
        self._last_unwind_at[ticker] = now

    def held_for_seconds(self, ticker: str) -> float:
        ts = self._pos_opened_at.get(ticker)
        if ts is None:
            return 0.0
        return max(0.0, time.time() - ts)

    def position_unrealized_pct(self, ticker: str, marks: dict[str, float]) -> Optional[float]:
        pos = self.portfolio.positions.get(ticker)
        if not pos:
            return None
        mark = marks.get(ticker)
        if mark is None or pos.avg_price == 0:
            return None
        pnl_cents = pos.mark_pnl(mark)
        denom = abs(pos.qty) * pos.avg_price
        if denom <= 0:
            return None
        return pnl_cents / denom

    # ── Internals ────────────────────────────────────────────────────────────

    def _close_position(self, ticker: str, marks: dict[str, float]) -> bool:
        pos = self.portfolio.positions.get(ticker)
        if not pos or pos.qty == 0:
            return False
        side  = "sell" if pos.qty > 0 else "buy"
        qty   = abs(pos.qty)
        price = self._get_order_price(ticker, side, marks, "aggressive")
        if price:
            self._cancel_bot_managed_orders_for_ticker(ticker)
            open_strategy = self._ticker_last_open_strategy.get(ticker)
            ok = self._place(
                ticker,
                side,
                qty,
                price,
                reduce_only=True,
                price_mode="aggressive",
                strategy_label=open_strategy,
            )
            if self.dry_run:
                self._pos_opened_at.pop(ticker, None)
                self._ticker_last_open_strategy.pop(ticker, None)
            return ok
        return False

    def _cancel_bot_managed_orders_for_ticker(self, ticker: str) -> None:
        """
        Live: cancel any bot-managed resting orders on this ticker before a new submit.
        Prevents stacking duplicate limits (margin lock, INSUFFICIENT_MARGIN) on repeated signals.
        """
        if self.dry_run:
            return
        oids = [
            oid for oid, meta in self._pending.items()
            if meta.get("bot_managed") and meta.get("ticker") == ticker
        ]
        if not oids:
            return
        int_ids = [int(oid) for oid in oids]
        try:
            if len(int_ids) > 1:
                self.client.cancel_orders_batch(int_ids)
            else:
                self.client.cancel_order(int_ids[0])
            log.info(
                "Cancelled %d prior bot order(s) on %s before new submit",
                len(int_ids),
                ticker,
            )
        except Exception as e:
            log.warning("Cancel-before-place failed for %s: %s", ticker, e)
        finally:
            for oid in oids:
                self._pending.pop(oid, None)

    def _cancel_bot_managed_orders_for_ticker_side(self, ticker: str, side: str) -> None:
        """
        Cancel bot-managed resting orders for a single (ticker, side).
        Used by market making so we don't cancel the opposite quote.
        """
        if self.dry_run:
            return
        oids = [
            oid for oid, meta in self._pending.items()
            if meta.get("bot_managed") and meta.get("ticker") == ticker and meta.get("side") == side
        ]
        if not oids:
            return
        int_ids = [int(oid) for oid in oids]
        try:
            if len(int_ids) > 1:
                self.client.cancel_orders_batch(int_ids)
            else:
                self.client.cancel_order(int_ids[0])
            log.info(
                "Cancelled %d prior bot order(s) on %s side=%s before refresh",
                len(int_ids),
                ticker,
                side,
            )
        except Exception as e:
            log.warning("Cancel refresh failed for %s side=%s: %s", ticker, side, e)
        finally:
            for oid in oids:
                self._pending.pop(oid, None)

    def _get_order_price(
        self,
        ticker: str,
        side: str,
        marks: dict[str, float],
        mode: str,
    ) -> Optional[int]:
        """
        Returns limit price in cents.
        passive: join best bid (buy) / best ask (sell).
        improve: inside spread toward mid when possible; else same as passive.
        aggressive: cross slightly (best ask + buffer for buys, etc.).
        """
        try:
            ob = self.client.get_orderbook(ticker)
        except Exception as e:
            log.warning("%s: orderbook fetch failed — using lastPrice mark: %s", ticker, e)
            lp = marks.get(ticker)
            if lp is None:
                return None
            if mode in ("passive", "improve"):
                return int(lp)
            return int(lp + SLIPPAGE_BUFFER) if side == "buy" \
                   else int(lp - SLIPPAGE_BUFFER)

        bids = ob.get("bids", [])
        asks = ob.get("asks", [])

        if mode == "aggressive":
            if side == "buy"  and asks:
                return int(asks[0]["price"]) + SLIPPAGE_BUFFER
            if side == "sell" and bids:
                return int(bids[0]["price"]) - SLIPPAGE_BUFFER

        elif mode == "improve" and bids and asks:
            bb = int(bids[0]["price"])
            ba = int(asks[0]["price"])
            if ba > bb:
                mid = (bb + ba) // 2
                if side == "buy":
                    return max(bb, min(mid, ba - 1))
                return min(ba, max(mid, bb + 1))

        if side == "buy"  and bids:
            return int(bids[0]["price"])
        if side == "sell" and asks:
            return int(asks[0]["price"])

        lp = marks.get(ticker)
        return int(lp) if lp else None

    def _place(
        self,
        ticker: str,
        side: str,
        qty: int,
        price: int,
        reduce_only: bool = False,
        price_mode: str = "passive",
        strategy_label: Optional[str] = None,
    ) -> bool:
        tag = {"aggressive": "cross-spread", "improve": "improve", "passive": "passive"}.get(
            price_mode, price_mode
        )
        px_mode = f"reduce-only {tag}" if reduce_only else tag
        log.info(
            "%s submit %s %sx %s @ %.2f%s [%s]",
            "[DRY]" if self.dry_run else "[LIVE]",
            side.upper(),
            qty,
            ticker,
            price / 100.0,
            " [reduce-only]" if reduce_only else "",
            px_mode,
        )
        if self.dry_run:
            self._record_strategy_attribution(
                ticker=ticker,
                side=side,
                qty=qty,
                price=float(price),
                reduce_only=reduce_only,
                strategy_label=strategy_label,
            )
            self.portfolio.record_fill(ticker, side, qty, float(price))
            return True

        try:
            resp = self.client.place_order(ticker, side, qty,
                                           order_type="limit", price=price,
                                           reduce_only=reduce_only)
            oid = resp.get("orderId")
            if oid is None and isinstance(resp.get("id"), int):
                oid = resp.get("id")
            st = resp.get("status", "?")
            cid = resp.get("clientOrderId", "?")
            log.info(
                "[LIVE] order accepted | exchange_order_id=%s client_order_id=%s ticker=%s status=%s qty=%s price=%.2f",
                oid,
                cid,
                ticker,
                st,
                qty,
                price / 100.0,
            )
            if oid is not None:
                oid_str = str(oid)
                self._pending[oid_str] = {
                    "ticker":     ticker,
                    "side":       side,
                    "qty":        qty,
                    "placed_at":  time.time(),
                    "bot_managed": True,
                    "session_placed": True,
                }
            return True
        except requests.HTTPError as e:
            summ = _summarize_api_error(getattr(e, "response", None))
            log.error(
                "Order rejected | HTTP %s | %s | api=%s",
                e.response.status_code if e.response else "?",
                e,
                summ or "(no body)",
            )
            if (summ and "INSUFFICIENT_MARGIN" in summ) or (summ and "Insufficient margin" in summ):
                self._margin_cooldown_until = max(self._margin_cooldown_until, time.time() + MARGIN_ERROR_COOLDOWN_SECS)
            return False
        except Exception as e:
            resp = getattr(e, "response", None)
            summ = _summarize_api_error(resp) if resp is not None else ""
            log.error("Order placement failed: %s %s", e, summ or "")
            return False

    def _record_strategy_attribution(
        self,
        *,
        ticker: str,
        side: str,
        qty: int,
        price: float,
        reduce_only: bool,
        strategy_label: Optional[str],
    ) -> None:
        """
        Attribute realized dry-run PnL to the strategy that opened the position.
        """
        if qty <= 0:
            return
        if not reduce_only:
            if strategy_label:
                self._ticker_last_open_strategy[ticker] = strategy_label
            return
        pos = self.portfolio.positions.get(ticker)
        if not pos:
            return
        closing_qty = min(abs(pos.qty), qty)
        if closing_qty <= 0:
            return
        # side='sell' closes long; side='buy' closes short
        sign = 1.0 if side == "sell" else -1.0
        realized_cents = sign * (price - pos.avg_price) * closing_qty
        denom = abs(pos.avg_price) * closing_qty
        realized_pct = (realized_cents / denom) if denom > 0 else 0.0
        owner = self._ticker_last_open_strategy.get(ticker) or strategy_label
        if owner:
            self._strategy_score_hist[owner].append(realized_pct)
            log.info(
                "strategy_pnl | strategy=%s ticker=%s realized=%.2f%%",
                owner,
                ticker,
                realized_pct * 100.0,
            )
        if closing_qty >= abs(pos.qty):
            self._ticker_last_open_strategy.pop(ticker, None)

    def record_mark_to_market_scores(self, marks: dict[str, float]) -> None:
        """
        Append unrealized PnL% snapshots to each owning strategy score stream.
        """
        if not STRATEGY_THROTTLE_ENABLED:
            return
        for ticker, pos in self.portfolio.positions.items():
            owner = self._ticker_last_open_strategy.get(ticker)
            if not owner:
                continue
            mark = marks.get(ticker)
            if mark is None or pos.avg_price == 0:
                continue
            pct = (pos.mark_pnl(mark) / (abs(pos.qty) * pos.avg_price))
            self._strategy_score_hist[owner].append(float(pct))

    def strategy_multiplier(self, strategy_label: Optional[str]) -> float:
        if not STRATEGY_THROTTLE_ENABLED or not strategy_label:
            return 1.0
        hist = self._strategy_score_hist.get(strategy_label)
        if not hist:
            return 1.0
        avg = sum(hist) / max(1, len(hist))
        if avg > STRATEGY_THROTTLE_LOSS_PCT:
            return 1.0
        gap = STRATEGY_THROTTLE_LOSS_PCT - avg
        scale = max(0.0, min(1.0, gap / max(1e-9, abs(STRATEGY_THROTTLE_LOSS_PCT))))
        m = 1.0 - (1.0 - STRATEGY_THROTTLE_REDUCTION) * scale
        return max(STRATEGY_THROTTLE_MIN_MULTIPLIER, m)

    def apply_strategy_throttle(self, strategy_label: Optional[str], strength: float) -> float:
        m = self.strategy_multiplier(strategy_label)
        return max(0.0, min(1.0, strength * m))

    def strategy_perf_snapshot(self) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for strat, hist in self._strategy_score_hist.items():
            if not hist:
                continue
            avg = float(sum(hist) / len(hist))
            out[strat] = {
                "n": float(len(hist)),
                "avg_pnl_pct": avg,
                "multiplier": self.strategy_multiplier(strat),
            }
        return out

    # ── Stale order cleanup ──────────────────────────────────────────────────

    def cancel_stale_orders(self):
        now = time.time()
        # Must be > poll interval: otherwise every resting limit is "stale" on the
        # next tick (e.g. timeout 30s with 60s loop cancels all working orders).
        stale_secs = max(ORDER_TIMEOUT_SECS, POLL_INTERVAL_SECS + 15)
        to_kill = [
            oid for oid, meta in self._pending.items()
            if meta.get("bot_managed")
            and meta.get("session_placed")
            and now - meta["placed_at"] > stale_secs
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
        """
        Idempotent fill notification (REST List fills or userFills WebSocket).

        Live book is updated each iteration via ``reconcile_from_exchange``; fills are logged
        here for visibility and deduped by ``tradeId`` (does not double-apply to ``Portfolio``).
        """
        if self.dry_run:
            return
        if trade_id in self._seen_trade_ids:
            return
        self._seen_trade_ids.add(trade_id)
        log.info(
            "Fill observed | tradeId=%s ticker=%s side=%s qty=%s px=$%.2f (next REST reconcile is authoritative)",
            trade_id,
            ticker,
            side,
            qty,
            price / 100.0,
        )

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
        had = str(order_id) in self._pending
        self._pending.pop(str(order_id), None)
        if had:
            log.info("WS orderUpdate | order_id=%s status=%s (removed from local pending)", order_id, status)

    def sync_fills(self):
        """
        Live: poll recent fills for logging / dedup; portfolio comes from ``reconcile_from_exchange``.
        """
        if self.dry_run:
            return
        try:
            fills = self.client.list_fills(
                start=self._fill_sync_start,
                limit=100,
            )
            new_trade_log = 0
            for fill in fills:
                tid = fill.get("tradeId")
                if tid is None:
                    continue
                tid_i = int(tid)
                if tid_i not in self._seen_trade_ids:
                    new_trade_log += 1
                self.ingest_exchange_fill(
                    tid_i,
                    fill["ticker"],
                    fill["side"],
                    int(fill["qty"]),
                    float(fill["price"]),
                )

            if new_trade_log:
                log.info(
                    "sync_fills | fills_page=%d new_trade_ids=%d",
                    len(fills),
                    new_trade_log,
                )
            else:
                log.debug("sync_fills | fills_page=%d", len(fills))
        except Exception as e:
            log.warning("Fill sync error: %s", e)
