"""
bot.py  –  Main trading loop
=============================
Run one line at a time (do not paste ``# comments`` — the shell passes them as args).

Dry-run (default, no real orders)::

    python3 bot.py

Live trading + private WebSocket::

    python3 bot.py --live --ws

Ctrl+C stops the loop and **does not** flatten by default (positions stay on the exchange;
next run reconciles and MR exit logic applies). To flatten everything on exit::

    python3 bot.py --live --flatten-on-exit

Optional public heartbeat subscription::

    python3 bot.py --live --ws --ws-heartbeat

Verbose DEBUG (executor, REST client, WebSocket, strategies)::

    python3 bot.py -v

``--ws`` uses userFills, userOrders, userPositions, userAccount (needs websocket-client).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import warnings

# macOS system Python often uses LibreSSL; urllib3 v2 emits a noisy warning.
try:
    from urllib3.exceptions import NotOpenSSLWarning

    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
except Exception:
    pass

import requests

from config import (
    UNIVERSE,
    POLL_INTERVAL_SECS,
    FUNDING_AVOID_SECS_BEFORE,
    MARKETS_CACHE_TTL_SEC,
    FUNDING_REFRESH_INTERVAL_SEC,
    ENTRY_OPEN_PRICE_MODE,
    MAX_NEW_OPENS_PER_LOOP,
    FUNDING_AGGRESSIVE_ENTRY_SECS_BEFORE,
    MIN_HOLD_SECS,
    MR_EXIT_MIN_PNL_PCT,
    MR_FORCE_EXIT_Z,
    ENABLE_MEAN_REVERSION_ENTRY,
    ENABLE_MEAN_REVERSION_EXIT,
    FUNDING_EXIT_SECS_BEFORE,
    MOM_EXIT_MIN_STRENGTH,
    FUNDING_BLOCK_IF_MOMENTUM_OPPOSES,
    FUNDING_MOMENTUM_OPPOSE_BLOCK_STRENGTH,
    ENABLE_MARKET_MAKING,
    MARKET_MAKING_TICKERS,
    MM_MAX_SPREAD_CENTS,
    MM_QUOTE_QTY,
    MM_MAX_INVENTORY,
    MM_REFRESH_SECS,
    ENABLE_UNWIND_WORKER,
    UNWIND_TICKERS,
    UNWIND_BLOCK_DIRECTIONAL_OPENS_ON_LIST,
    UNWIND_MAX_QTY_PER_ORDER,
    UNWIND_REFRESH_SECS,
    UNWIND_PRICE_MODE,
    API_KEY,
    API_SECRET,
)
from core.client import ForumClient
from core.market_enrichment import enrich_market_with_funding, seconds_until_iso
from core.portfolio import Portfolio
from strategies.signals import (
    FundingHarvest,
    MeanReversion,
    IndexMomentum,
    Signal,
    combine_signals,
    WEIGHTS,
)
from execution.executor import Executor

# ─── Logging ─────────────────────────────────────────────────────────────────
log = logging.getLogger("bot")


def setup_logging(*, verbose: bool = False) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    h_out = logging.StreamHandler(sys.stdout)
    h_out.setLevel(level)
    h_out.setFormatter(logging.Formatter(fmt))
    h_file = logging.FileHandler("forum_bot.log")
    h_file.setLevel(level)
    h_file.setFormatter(logging.Formatter(fmt))
    root.addHandler(h_out)
    root.addHandler(h_file)
    root.setLevel(level)
    if verbose:
        for name in (
            "execution.executor",
            "core.client",
            "core.ws_client",
            "strategies.signals",
        ):
            logging.getLogger(name).setLevel(logging.DEBUG)
        log.debug("Verbose logging enabled for executor, client, ws_client, strategies.signals")


setup_logging(verbose=False)


# ─── Bot ─────────────────────────────────────────────────────────────────────

class ForumBot:

    def __init__(
        self,
        dry_run: bool = True,
        use_ws: bool = False,
        ws_heartbeat: bool = False,
        *,
        flatten_on_exit: bool = False,
    ):
        self.client    = ForumClient(sync_time=True)
        self.portfolio = Portfolio()
        self.executor  = Executor(self.client, self.portfolio, dry_run=dry_run)

        self.funding_strat   = FundingHarvest()
        # Instantiate MR strategy only when enabled for entry/exit. This prevents
        # MR state from accumulating silently and makes logs match expectations.
        self.mr_strat        = MeanReversion() if (ENABLE_MEAN_REVERSION_ENTRY or ENABLE_MEAN_REVERSION_EXIT) else None
        self.mom_strat       = IndexMomentum()

        self.dry_run = dry_run
        self.use_ws = use_ws
        self.ws_heartbeat = ws_heartbeat
        self.flatten_on_exit = flatten_on_exit
        self._ws = None
        self._last_account_status = None
        self._markets_cache_list: list | None = None
        self._markets_cache_mono: float = 0.0
        self._last_markets_429: bool = False
        self._funding_overlay: dict[str, dict] = {}
        self._funding_refresh_mono: float = 0.0
        self._extra_poll_sleep_sec: float = 0.0
        log.info(
            "ForumBot started | dry_run=%s | ws=%s | ws_heartbeat=%s | "
            "flatten_on_exit=%s | universe=%s",
            dry_run,
            use_ws,
            ws_heartbeat,
            flatten_on_exit,
            len(UNIVERSE),
        )
        log.info(
            "config | MR_entry=%s MR_exit=%s | funding_block_if_mom_opposes=%s (thr=%.2f) | "
            "funding_exit_secs_before=%s mom_exit_min_strength=%.2f | weights=%s | "
            "mm=%s tickers=%s max_spread=%s qty=%s inv=%s refresh=%ss",
            ENABLE_MEAN_REVERSION_ENTRY,
            ENABLE_MEAN_REVERSION_EXIT,
            FUNDING_BLOCK_IF_MOMENTUM_OPPOSES,
            float(FUNDING_MOMENTUM_OPPOSE_BLOCK_STRENGTH),
            FUNDING_EXIT_SECS_BEFORE,
            float(MOM_EXIT_MIN_STRENGTH),
            WEIGHTS,
            ENABLE_MARKET_MAKING,
            MARKET_MAKING_TICKERS,
            MM_MAX_SPREAD_CENTS,
            MM_QUOTE_QTY,
            MM_MAX_INVENTORY,
            MM_REFRESH_SECS,
        )

    def _dispatch_ws(self, msg: dict) -> None:
        ch = msg.get("channel")
        t = msg.get("type")
        if ch == "userFills" and t == "fill":
            d = msg.get("data") or {}
            log.info(
                "WS fill event | ticker=%s side=%s qty=%s tradeId=%s",
                d.get("ticker"),
                d.get("side"),
                d.get("qty"),
                d.get("tradeId"),
            )
            self.executor.ingest_exchange_fill_payload(d)
        elif ch == "userOrders" and t == "orderUpdate":
            d = msg.get("data") or {}
            oid, st = d.get("id"), d.get("status")
            if st in ("filled", "cancelled", "rejected"):
                log.info(
                    "WS orderUpdate | id=%s ticker=%s status=%s",
                    oid,
                    d.get("ticker"),
                    st,
                )
            else:
                log.debug(
                    "WS orderUpdate | id=%s ticker=%s status=%s remaining=%s",
                    oid,
                    d.get("ticker"),
                    st,
                    d.get("remainingQuantity"),
                )
            if oid is not None and st:
                self.executor.release_pending_order_from_ws(int(oid), str(st))
        elif ch == "userPositions" and t == "positionUpdate":
            d = msg.get("data") or {}
            log.info(
                "WS positionUpdate | ticker=%s qty=%s prevQty=%s",
                d.get("ticker"),
                d.get("qty"),
                d.get("prevQty"),
            )
        elif ch == "userAccount" and t == "accountUpdate":
            self._on_account_update_ws(msg.get("data") or {})
        elif t == "heartbeat" or msg.get("event") in ("authOk", "subscribed", "error"):
            log.debug("WS ctl %s", msg)

    def _start_ws(self) -> None:
        if self.dry_run:
            return
        if not API_KEY or not API_SECRET:
            log.warning("WebSocket private channels need FORUM_API_KEY / FORUM_API_SECRET")
            return
        from core.ws_client import ForumWebSocket

        self._ws = ForumWebSocket(
            tickers=list(UNIVERSE),
            url=self.client.ws_url,
            api_key=API_KEY,
            api_secret=API_SECRET,
            clock_skew_sec=self.client.clock_skew_sec,
            public_channels=["tickerUpdates"],
            private_channels=[
                "userFills",
                "userOrders",
                "userPositions",
                "userAccount",
            ],
            on_event=self._dispatch_ws,
            subscribe_heartbeat=self.ws_heartbeat,
            forward_heartbeat=False,
        )
        self._ws.start()
        log.info(
            "WebSocket started (tickerUpdates%s; userFills, userOrders, userPositions, userAccount)",
            ", heartbeat" if self.ws_heartbeat else "",
        )

    def _on_account_update_ws(self, d: dict) -> None:
        if not d:
            return
        st = d.get("status")
        if st == self._last_account_status:
            return
        self._last_account_status = str(st) if st is not None else None
        if st == "liquidating":
            log.error(
                "WS account status=liquidating equity=%s freeMargin=%s marginRatio=%s",
                d.get("equity"),
                d.get("freeMargin"),
                d.get("marginRatio"),
            )
        elif st == "reduce_only":
            log.warning(
                "WS account status=reduce_only equity=%s freeMargin=%s marginRatio=%s",
                d.get("equity"),
                d.get("freeMargin"),
                d.get("marginRatio"),
            )
        elif st == "healthy":
            log.info(
                "WS account status=healthy equity=%s freeMargin=%s unrealizedPnl=%s",
                d.get("equity"),
                d.get("freeMargin"),
                d.get("unrealizedPnl"),
            )
        else:
            log.info("WS accountUpdate status=%s keys=%s", st, list(d.keys()))

    def run(self):
        """Main event loop."""
        if self.use_ws:
            if self.dry_run:
                log.warning("--ws ignored in dry-run mode")
            else:
                self._start_ws()
        while True:
            extra = self._extra_poll_sleep_sec
            self._extra_poll_sleep_sec = 0.0
            try:
                self._iteration()
            except KeyboardInterrupt:
                log.info("Shutdown requested.")
                if self._ws:
                    self._ws.stop()
                if self.flatten_on_exit:
                    self._emergency_close()
                elif not self.dry_run and self.portfolio.positions:
                    log.info(
                        "Stopping without flatten — %d position(s) remain on the exchange; "
                        "restart the bot or use --flatten-on-exit next time to close on exit.",
                        len(self.portfolio.positions),
                    )
                break
            except Exception as e:
                log.error(f"Loop error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL_SECS + extra)

    def _exchange_ok(self) -> bool:
        try:
            st = self.client.get_exchange_status()
            if st.get("inMaintenance"):
                log.warning("Exchange in maintenance — skipping iteration.")
                return False
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning(
                    "Exchange status rate-limited (429) — skipping iteration; not calling /markets this tick"
                )
                self._extra_poll_sleep_sec = max(self._extra_poll_sleep_sec, 30.0)
                return False
            log.debug("Exchange status check: %s", e)
        except Exception as e:
            log.debug("Exchange status check: %s", e)
        return True

    def _iteration(self):
        if not self._exchange_ok():
            return

        markets = self._fetch_markets()
        if not markets:
            reason = []
            if self._last_markets_429:
                reason.append("rate_limited(429)")
            if not self._markets_cache_list:
                reason.append("no_cache_yet")
            log.warning(
                "No market data — skip trading tick (%s)",
                ", ".join(reason) if reason else "empty_universe_or_all_offline",
            )
            if self._last_markets_429:
                self._extra_poll_sleep_sec = max(self._extra_poll_sleep_sec, 45.0)
            return

        marks = {m["ticker"]: m["lastPrice"]
                 for m in markets if m.get("lastPrice")}

        if not self.dry_run:
            ok = self.executor.reconcile_from_exchange(marks)
            if not ok:
                log.warning("Skip trading tick: exchange reconcile failed")
                return

        # Reduce-only unwind worker: clean up manual/discretionary inventory without crossing.
        if ENABLE_UNWIND_WORKER and UNWIND_TICKERS:
            for t in list(UNWIND_TICKERS):
                if t in self.portfolio.positions:
                    self.executor.work_unwind_position(
                        t,
                        marks,
                        max_qty_per_order=int(UNWIND_MAX_QTY_PER_ORDER),
                        refresh_secs=int(UNWIND_REFRESH_SECS),
                        price_mode=str(UNWIND_PRICE_MODE),
                    )

        if not self.portfolio.check_risk(marks):
            log.critical("Risk limits hit – bot halted.")
            self._emergency_close()
            return

        for m in markets:
            ticker = m["ticker"]
            cum_funding = m.get("cumFunding", 0)
            self.portfolio.update_funding(ticker, cum_funding)

        signals_fired = 0
        execute_signal_calls = 0
        orders_placed = 0
        # When unwind is active and any unwind ticker is non-flat, do not open new risk.
        unwind_active = bool(
            ENABLE_UNWIND_WORKER
            and UNWIND_TICKERS
            and any((t in self.portfolio.positions and self.portfolio.positions[t].qty != 0) for t in UNWIND_TICKERS)
        )
        unwind_list = set(UNWIND_TICKERS)
        mm_tickers = set(MARKET_MAKING_TICKERS)
        first_mm = MARKET_MAKING_TICKERS[0] if MARKET_MAKING_TICKERS else None
        open_candidates: list[tuple[float, str, dict, list[Signal], Signal]] = []

        for m in markets:
            ticker = m["ticker"]
            signals = []

            # Funding-settlement hygiene: close any open exposure shortly before settlement.
            pos = self.portfolio.positions.get(ticker)
            if pos:
                until = seconds_until_iso(m.get("nextFundingTime"))
                if until is not None and 0 < until <= FUNDING_EXIT_SECS_BEFORE:
                    log.info("%s: funding window exit (nextFunding in %.0fs)", ticker, until)
                    self.executor.close_position(ticker, marks)
                    continue

                # Momentum flip exit: if index momentum strongly reverses vs our position, close.
                mom_exit = self.mom_strat.generate(m)
                if mom_exit and mom_exit.strength >= MOM_EXIT_MIN_STRENGTH:
                    if (pos.qty > 0 and mom_exit.direction < 0) or (pos.qty < 0 and mom_exit.direction > 0):
                        log.info(
                            "%s: momentum flip exit (pos=%s mom_dir=%s strength=%.2f)",
                            ticker,
                            pos.qty,
                            mom_exit.direction,
                            mom_exit.strength,
                        )
                        self.executor.close_position(ticker, marks)
                        continue

            fs = self.funding_strat.generate(m)
            if fs:
                if FUNDING_BLOCK_IF_MOMENTUM_OPPOSES:
                    mom_now = self.mom_strat.generate(m)
                    if (
                        mom_now
                        and mom_now.strength >= FUNDING_MOMENTUM_OPPOSE_BLOCK_STRENGTH
                        and mom_now.direction == -fs.direction
                    ):
                        log.info(
                            "%s: skip funding entry — momentum opposes (fund_dir=%s mom_dir=%s strength=%.2f)",
                            ticker,
                            fs.direction,
                            mom_now.direction,
                            mom_now.strength,
                        )
                    else:
                        signals.append(fs)
                        if mom_now:
                            signals.append(mom_now)
                else:
                    signals.append(fs)

            if ENABLE_MEAN_REVERSION_ENTRY:
                if self.mr_strat is not None:
                    mrs = self.mr_strat.generate(m)
                    if mrs:
                        signals.append(mrs)

            moms = self.mom_strat.generate(m)
            if moms:
                signals.append(moms)

            pos = self.portfolio.positions.get(ticker)
            if pos and ENABLE_MEAN_REVERSION_EXIT and self.mr_strat is not None and self._should_exit_position(ticker, marks):
                log.info("MR exit: %s", ticker)
                self.executor.close_position(ticker, marks)
                continue

            combined = combine_signals(signals)
            if combined:
                signals_fired += 1
                if (
                    combined.direction != 0
                    and ENABLE_UNWIND_WORKER
                    and UNWIND_BLOCK_DIRECTIONAL_OPENS_ON_LIST
                    and ticker in unwind_list
                ):
                    log.info(
                        "%s: skip open — ticker on UNWIND_TICKERS (no new risk until removed from list)",
                        ticker,
                    )
                    continue
                if unwind_active:
                    log.info("%s: skip open — unwind lockout active (cleaning inventory)", ticker)
                    continue
                if self._skip_funding_open(ticker, m, signals, combined):
                    log.info(
                        "%s: combined signal skipped — funding open blocked (near nextFundingTime)",
                        ticker,
                    )
                    continue
                open_candidates.append((combined.edge, ticker, m, signals, combined))
            else:
                # If directional signals are quiet (common when funding is small), run a
                # conservative market-making loop on selected, tighter-spread tickers.
                if ENABLE_MARKET_MAKING and ticker in mm_tickers:
                    if (
                        ENABLE_UNWIND_WORKER
                        and UNWIND_BLOCK_DIRECTIONAL_OPENS_ON_LIST
                        and ticker in unwind_list
                    ):
                        continue
                    # When you have a manual/discretionary position on the book (e.g. ALTMAN),
                    # margin headroom is the binding constraint. In that regime, quote only the
                    # first configured MM ticker to avoid margin conflicts + one-sided exposure.
                    if self.portfolio.positions:
                        # Always allow quoting tickers where we already have inventory so we can
                        # work back toward flat (reduce-only is enforced inside the executor).
                        have_inv = (ticker in self.portfolio.positions and self.portfolio.positions[ticker].qty != 0)
                        if not have_inv and first_mm is not None and ticker != first_mm:
                            continue
                    self.executor.quote_market_make(
                        ticker,
                        marks,
                        qty=int(MM_QUOTE_QTY),
                        max_spread_cents=int(MM_MAX_SPREAD_CENTS),
                        max_inventory=int(MM_MAX_INVENTORY),
                        refresh_secs=int(MM_REFRESH_SECS),
                    )

        open_candidates.sort(key=lambda x: -x[0])
        opens_budget = int(MAX_NEW_OPENS_PER_LOOP)
        if len(open_candidates) > opens_budget:
            log.info(
                "Directional open budget: up to %d execution(s) this tick (%d candidate(s) by edge)",
                opens_budget,
                len(open_candidates),
            )
        for _edge, ticker, m, signals, combined in open_candidates:
            if opens_budget <= 0:
                break
            log.info(combined)
            until = seconds_until_iso(m.get("nextFundingTime"))
            funding_near = (
                until is not None
                and 0 < until <= FUNDING_AGGRESSIVE_ENTRY_SECS_BEFORE
            )
            funding_urgent = funding_near and any(s.strategy == "FundingHarvest" for s in signals)
            price_mode = "aggressive" if funding_urgent else ENTRY_OPEN_PRICE_MODE
            ok = self.executor.execute_signal(
                ticker, combined.direction, combined.strength, marks,
                price_mode=price_mode,
                strategy_label=combined.strategy,
            )
            execute_signal_calls += 1
            if ok:
                orders_placed += 1
                opens_budget -= 1

        self.executor.record_mark_to_market_scores(marks)
        self.executor.cancel_stale_orders()
        self.executor.sync_fills()

        nav = self.portfolio.nav(marks)
        rem = getattr(self.client, "_rate_limit_remaining", None)
        lim = getattr(self.client, "_rate_limit_limit", None)
        rate_s = f"{rem}/{lim}" if rem is not None else "n/a"
        if self.portfolio.positions:
            pos_detail = ",".join(
                f"{t}:{p.qty}" for t, p in sorted(self.portfolio.positions.items())
            )
        elif self.executor.pending_bot_managed_count:
            # Limits on the book are not positions; long = qty>0, short = qty<0 after a fill.
            pos_detail = (
                f"flat ({self.executor.pending_bot_managed_count} bot limits resting — "
                "no long/short until a fill)"
            )
        else:
            pos_detail = "flat"
        log.info(
            "NAV=$%.2f | pos=%d (%s) | gross=$%.0f | pending=%d (bot=%d manual=%d) | "
            "tickers_live=%d | signals=%d | execute_signal_calls=%d | orders_placed=%d | api_remaining≈%s",
            nav / 100.0,
            len(self.portfolio.positions),
            pos_detail,
            self.portfolio.gross_exposure(marks) / 100.0,
            self.executor.pending_order_count,
            self.executor.pending_bot_managed_count,
            self.executor.pending_manual_count,
            len(markets),
            signals_fired,
            execute_signal_calls,
            orders_placed,
            rate_s,
        )
        snap = self.executor.strategy_perf_snapshot()
        if snap:
            parts = []
            for strat, s in sorted(
                snap.items(), key=lambda kv: kv[1]["avg_pnl_pct"], reverse=True
            ):
                parts.append(
                    f"{strat}:avg={s['avg_pnl_pct']*100:+.2f}% n={int(s['n'])} x{s['multiplier']:.2f}"
                )
            log.info("strategy_throttle | %s", " | ".join(parts))

    def _should_exit_position(self, ticker: str, marks: dict[str, float]) -> bool:
        """
        PnL-aware MR exit:
          1) require minimum hold time,
          2) allow standard MR exit when not materially losing,
          3) if losing, wait for stronger reversion (very low |z|) before exit.
        """
        if self.executor.held_for_seconds(ticker) < MIN_HOLD_SECS:
            return False
        if self.mr_strat is None:
            return False
        if not self.mr_strat.should_exit(ticker):
            return False
        pnl_pct = self.executor.position_unrealized_pct(ticker, marks)
        if pnl_pct is None or pnl_pct >= MR_EXIT_MIN_PNL_PCT:
            return True
        z_now = self.mr_strat.current_abs_zscore(ticker)
        if z_now is not None and z_now <= MR_FORCE_EXIT_Z:
            return True
        log.info(
            "%s: defer MR exit (pnl=%.2f%% < %.2f%%, |z|=%.3f > %.3f)",
            ticker,
            pnl_pct * 100.0,
            MR_EXIT_MIN_PNL_PCT * 100.0,
            z_now if z_now is not None else float("nan"),
            MR_FORCE_EXIT_Z,
        )
        return False

    def _skip_funding_open(self, ticker: str, market: dict, signals, combined) -> bool:
        """
        Avoid opening a new funding carry just before nextFundingTime (full-notional settle).
        Does not block mean-reversion exits, MR/momentum-only trades, or adds if already positioned.
        """
        if not any(s.strategy == "FundingHarvest" for s in signals):
            return False
        pos = self.portfolio.positions.get(ticker)
        if pos and pos.qty != 0:
            return False
        until = seconds_until_iso(market.get("nextFundingTime"))
        if until is None:
            return False
        # only skip when settlement is *soon* and still in the future
        if 0 < until <= FUNDING_AVOID_SECS_BEFORE:
            return True
        return False

    def _fetch_markets(self) -> list:
        """
        Prefer one GET /markets (with 429 retries in client), cache snapshot briefly.
        After a 429, do not hammer N× GET /markets/{{ticker}} — use cache if fresh enough.
        """
        markets: list = []
        all_mkts: list = []
        got_429 = False
        try:
            all_mkts = self.client.get_all_markets()
            self._last_markets_429 = False
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                got_429 = True
                self._last_markets_429 = True
            log.warning("get_all_markets failed: %s", e)
        except Exception as e:
            log.warning("get_all_markets failed: %s", e)

        if not all_mkts and self._markets_cache_list:
            age = time.monotonic() - self._markets_cache_mono
            if age < MARKETS_CACHE_TTL_SEC * 3:
                log.info(
                    "Using cached markets snapshot (age=%.0fs, %d rows)",
                    age,
                    len(self._markets_cache_list),
                )
                all_mkts = self._markets_cache_list

        if all_mkts:
            self._markets_cache_list = list(all_mkts)
            self._markets_cache_mono = time.monotonic()

        by_ticker = {
            m["ticker"]: m for m in all_mkts
            if isinstance(m, dict) and m.get("ticker")
        }
        now_m = time.monotonic()
        refresh_funding = (now_m - self._funding_refresh_mono) >= FUNDING_REFRESH_INTERVAL_SEC
        if refresh_funding:
            self._funding_refresh_mono = now_m

        for ticker in UNIVERSE:
            try:
                m = by_ticker.get(ticker)
                if m is None:
                    if got_429 or self._last_markets_429:
                        log.debug("%s: skip get_market (rate-limit cooldown)", ticker)
                        continue
                    m = self.client.get_market(ticker)
                if not m.get("live", False):
                    continue
                needs_fr = (
                    m.get("movingFundingRate") is None
                    or m.get("nextFundingTime") is None
                )
                if needs_fr:
                    if refresh_funding:
                        try:
                            fr = self.client.get_funding_rate(ticker)
                            self._funding_overlay[ticker] = fr
                            m = enrich_market_with_funding(m, fr)
                        except Exception as e:
                            log.debug("Funding rate fetch %s: %s", ticker, e)
                    else:
                        cached = self._funding_overlay.get(ticker)
                        if cached:
                            m = enrich_market_with_funding(m, cached)
                markets.append(m)
            except Exception as e:
                log.debug("Fetch error %s: %s", ticker, e)
        log.info(
            "markets | tradable_in_loop=%d snapshot_rows=%d funding_bulk_refresh=%s",
            len(markets),
            len(all_mkts),
            refresh_funding,
        )
        return markets

    def _emergency_close(self):
        log.warning("EMERGENCY CLOSE – flattening all positions")
        try:
            markets = self._fetch_markets()
            marks = {m["ticker"]: m["lastPrice"]
                     for m in markets if m.get("lastPrice")}
            for ticker in list(self.portfolio.positions.keys()):
                self.executor.close_position(ticker, marks)
        except Exception as e:
            log.error(f"Emergency close error: {e}")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true",
                        help="Send real orders (default: dry-run)")
    parser.add_argument("--ws", action="store_true",
                        help="Private user WS channels (use with --live)")
    parser.add_argument("--ws-heartbeat", action="store_true",
                        help="Also subscribe to public heartbeat (~1 Hz per ticker)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="DEBUG logs for executor, API client, WebSocket, strategies")
    parser.add_argument(
        "--flatten-on-exit",
        action="store_true",
        help="On Ctrl+C, submit reduce-only closes for all positions (default: leave positions open)",
    )
    args = parser.parse_args()
    setup_logging(verbose=args.verbose)
    ForumBot(
        dry_run=not args.live,
        use_ws=args.ws,
        ws_heartbeat=args.ws_heartbeat,
        flatten_on_exit=args.flatten_on_exit,
    ).run()
