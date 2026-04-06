"""
bot.py  –  Main trading loop
=============================
Run one line at a time (do not paste ``# comments`` — the shell passes them as args).

Dry-run (default, no real orders)::

    python3 bot.py

Live trading + private WebSocket::

    python3 bot.py --live --ws

Optional public heartbeat subscription::

    python3 bot.py --live --ws --ws-heartbeat

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
    API_KEY,
    API_SECRET,
)
from core.client import ForumClient
from core.market_enrichment import enrich_market_with_funding, seconds_until_iso
from core.portfolio import Portfolio
from strategies.signals import (
    FundingHarvest, MeanReversion, IndexMomentum, combine_signals,
)
from execution.executor import Executor

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("forum_bot.log"),
    ],
)
log = logging.getLogger("bot")


# ─── Bot ─────────────────────────────────────────────────────────────────────

class ForumBot:

    def __init__(
        self,
        dry_run: bool = True,
        use_ws: bool = False,
        ws_heartbeat: bool = False,
    ):
        self.client    = ForumClient(sync_time=True)
        self.portfolio = Portfolio()
        self.executor  = Executor(self.client, self.portfolio, dry_run=dry_run)

        self.funding_strat   = FundingHarvest()
        self.mr_strat        = MeanReversion()
        self.mom_strat       = IndexMomentum()

        self.dry_run = dry_run
        self.use_ws = use_ws
        self.ws_heartbeat = ws_heartbeat
        self._ws = None
        self._last_account_status = None
        self._markets_cache_list: list | None = None
        self._markets_cache_mono: float = 0.0
        self._last_markets_429: bool = False
        self._funding_overlay: dict[str, dict] = {}
        self._funding_refresh_mono: float = 0.0
        self._extra_poll_sleep_sec: float = 0.0
        log.info(
            "ForumBot started | dry_run=%s | ws=%s | ws_heartbeat=%s | universe=%s",
            dry_run,
            use_ws,
            ws_heartbeat,
            len(UNIVERSE),
        )

    def _dispatch_ws(self, msg: dict) -> None:
        ch = msg.get("channel")
        t = msg.get("type")
        if ch == "userFills" and t == "fill":
            self.executor.ingest_exchange_fill_payload(msg.get("data") or {})
        elif ch == "userOrders" and t == "orderUpdate":
            d = msg.get("data") or {}
            oid, st = d.get("id"), d.get("status")
            if oid is not None and st:
                self.executor.release_pending_order_from_ws(int(oid), str(st))
        elif ch == "userPositions" and t == "positionUpdate":
            log.debug("WS positionUpdate ticker=%s", (msg.get("data") or {}).get("ticker"))
        elif ch == "userAccount" and t == "accountUpdate":
            self._on_account_update_ws(msg.get("data") or {})

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
                self._emergency_close()
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
            log.warning("No market data received.")
            if self._last_markets_429:
                self._extra_poll_sleep_sec = max(self._extra_poll_sleep_sec, 45.0)
            return

        marks = {m["ticker"]: m["lastPrice"]
                 for m in markets if m.get("lastPrice")}

        if not self.portfolio.check_risk(marks):
            log.critical("Risk limits hit – bot halted.")
            self._emergency_close()
            return

        for m in markets:
            ticker = m["ticker"]
            cum_funding = m.get("cumFunding", 0)
            self.portfolio.update_funding(ticker, cum_funding)

        for m in markets:
            ticker = m["ticker"]
            signals = []

            fs = self.funding_strat.generate(m)
            if fs:
                signals.append(fs)

            mrs = self.mr_strat.generate(m)
            if mrs:
                signals.append(mrs)

            moms = self.mom_strat.generate(m)
            if moms:
                signals.append(moms)

            pos = self.portfolio.positions.get(ticker)
            if pos and self.mr_strat.should_exit(ticker):
                log.info(f"MR exit: {ticker}")
                self.executor.close_position(ticker, marks)
                continue

            combined = combine_signals(signals)
            if combined:
                if self._skip_funding_open(ticker, m, signals, combined):
                    log.debug(f"{ticker}: skip funding entry (near settlement window)")
                    continue
                log.info(combined)
                urgent = combined.strategy.startswith("Combined") and \
                         any(s.strategy == "FundingHarvest" for s in signals)
                self.executor.execute_signal(
                    ticker, combined.direction, combined.strength, marks,
                    urgent=urgent,
                )

        self.executor.cancel_stale_orders()
        self.executor.sync_fills()

        nav = self.portfolio.nav(marks)
        log.info(f"NAV=${nav/100:,.2f}  "
                 f"positions={len(self.portfolio.positions)}  "
                 f"gross_exp=${self.portfolio.gross_exposure(marks)/100:,.0f}")

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
    args = parser.parse_args()
    ForumBot(
        dry_run=not args.live,
        use_ws=args.ws,
        ws_heartbeat=args.ws_heartbeat,
    ).run()
