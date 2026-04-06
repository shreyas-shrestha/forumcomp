"""
core/client.py  –  Authenticated Forum API client

Rate limits (sliding 60s window): see https://comp-docs.forum.market/api-reference/concepts/rate-limits.md
We record X-RateLimit-* headers, honor Retry-After on GET 429 (one retry), and log POST/DELETE 429 without reposting.
"""

from __future__ import annotations

import hashlib
import hmac
import base64
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Union

import requests

from config import API_BASE, API_KEY, API_SECRET, WS_URL

log = logging.getLogger(__name__)


def _iso_utc(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _unwrap_list(resp: Any, key: str = "data") -> List[Any]:
    """Unwrap `{data: [...]}` paginated envelopes; pass through raw lists."""
    if isinstance(resp, dict) and key in resp and isinstance(resp[key], list):
        return resp[key]
    if isinstance(resp, list):
        return resp
    return []


def _hmac_path(request_path: str) -> str:
    """
    Forum signing: prehash uses path *including* /v1, without query string.
    e.g. GET /v1/account — not GET /account.
    """
    if request_path.startswith("/v1"):
        return request_path
    if request_path.startswith("/"):
        return "/v1" + request_path
    return "/v1/" + request_path


def _clamp_limit(limit: int, maximum: int) -> int:
    """Clamp request `limit` to OpenAPI maximum (comp-api OpenAPI)."""
    return min(max(1, limit), maximum)


class ForumClient:
    """
    Thin wrapper around the Forum REST API.
    Public endpoints: no auth needed.
    Private endpoints: HMAC-SHA256 signed (key + timestamp + sign headers).
    Timestamps are aligned to server time when sync_server_time() has been run.
    """

    def __init__(self, sync_time: bool = True):
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._clock_skew_sec: float = 0.0
        # Last X-RateLimit-* from any response (Forum sliding 60s window; see rate-limits doc).
        self._rate_limit_remaining: Optional[int] = None
        self._rate_limit_limit: Optional[int] = None
        self._last_low_rate_log_mono: float = 0.0
        if sync_time:
            try:
                self.sync_server_time()
            except Exception:
                pass

    # ── Clock / auth ──────────────────────────────────────────────────────────

    def sync_server_time(self) -> dict:
        """
        GET /time — refresh HMAC clock skew.
        Does not use _get() retry loop: on 429 we skip sync and keep skew=0 to avoid
        burning the public IP bucket at process start (see rate limits doc).
        """
        url = f"{API_BASE}/time"
        r = self.session.get(url, timeout=10)
        self._update_rate_limit_state(r)
        if r.status_code == 429:
            log.warning(
                "GET /time 429 — leaving clock skew at 0 (HMAC may still work); will retry on next sync"
            )
            return {}
        r.raise_for_status()
        body = r.json()
        srv = int(body["epoch"])
        self._clock_skew_sec = float(srv - int(time.time()))
        return body

    def _now_ts(self) -> str:
        return str(int(time.time() + self._clock_skew_sec))

    def auth_timestamp(self) -> str:
        """Unix seconds string for REST or WebSocket HMAC (includes clock skew)."""
        return self._now_ts()

    def ws_sign_challenge(self, timestamp: Optional[str] = None) -> tuple[str, str]:
        """
        WebSocket HMAC: Base64(HMAC-SHA256(secret, timestamp + "GET" + "/ws/v1")).
        Returns (timestamp, signature).
        """
        ts = timestamp or self.auth_timestamp()
        prehash = ts + "GET" + "/ws/v1"
        sig = base64.b64encode(
            hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()
        return ts, sig

    @property
    def ws_url(self) -> str:
        return WS_URL

    @property
    def clock_skew_sec(self) -> float:
        return self._clock_skew_sec

    def _sign(self, method: str, request_path: str, body: str = "") -> dict:
        ts = self._now_ts()
        prehash = ts + method.upper() + _hmac_path(request_path) + body
        sig = base64.b64encode(
            hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()
        return {
            "FORUM-ACCESS-KEY":       API_KEY,
            "FORUM-ACCESS-TIMESTAMP": ts,
            "FORUM-ACCESS-SIGN":      sig,
        }

    # ── Request helpers ─────────────────────────────────────────────────────

    def _update_rate_limit_state(self, r: requests.Response) -> None:
        """Track X-RateLimit-*; warn when remaining is low (throttled)."""
        try:
            rem = r.headers.get("X-RateLimit-Remaining")
            lim = r.headers.get("X-RateLimit-Limit")
            if rem is not None:
                self._rate_limit_remaining = int(rem)
            if lim is not None:
                self._rate_limit_limit = int(lim)
            if self._rate_limit_remaining is not None and self._rate_limit_remaining < 15:
                now = time.monotonic()
                if now - self._last_low_rate_log_mono > 30.0:
                    self._last_low_rate_log_mono = now
                    log.warning(
                        "Rate limit headroom low: %s/%s requests left in window",
                        self._rate_limit_remaining,
                        self._rate_limit_limit or "?",
                    )
        except (TypeError, ValueError):
            pass

    @staticmethod
    def _retry_after_seconds(r: requests.Response) -> float:
        """Forum 429: Retry-After header or error.details.retryAfter (cap 0.5–120s)."""
        h = r.headers.get("Retry-After")
        if h is not None:
            try:
                return min(max(float(h), 0.5), 120.0)
            except ValueError:
                pass
        try:
            body = r.json()
            details = (body.get("error") or {}).get("details") or {}
            ra = details.get("retryAfter")
            if ra is not None:
                return min(max(float(ra), 0.5), 120.0)
        except Exception:
            pass
        return 5.0

    def _get(self, path: str, params: Optional[dict] = None, auth: bool = False) -> Any:
        url = API_BASE + path
        headers = self._sign("GET", path) if auth else {}
        last: Optional[requests.Response] = None
        max_attempts = 5
        for attempt in range(max_attempts):
            r = self.session.get(url, params=params or {}, headers=headers, timeout=15)
            last = r
            self._update_rate_limit_state(r)
            if r.status_code == 429:
                hint = self._retry_after_seconds(r)
                # Retry-After is often 1s; stay under 120/min we need longer gaps.
                floor = min(45.0, 3.0 * (2**attempt))
                wait = min(max(hint, floor), 60.0)
                if attempt < max_attempts - 1:
                    log.warning(
                        "GET %s 429 (attempt %d/%d) — sleeping %.1fs",
                        path,
                        attempt + 1,
                        max_attempts,
                        wait,
                    )
                    time.sleep(wait)
                    continue
            r.raise_for_status()
            return r.json()
        if last is not None:
            last.raise_for_status()
        raise RuntimeError("unreachable")

    def _post(self, path: str, body: dict) -> Any:
        raw = json.dumps(body, separators=(",", ":"))
        headers = self._sign("POST", path, raw)
        r = self.session.post(API_BASE + path, data=raw, headers=headers, timeout=15)
        self._update_rate_limit_state(r)
        if r.status_code == 429:
            wait = self._retry_after_seconds(r)
            log.warning(
                "POST %s returned 429 — wait %.1fs before retry; not auto-reposting",
                path,
                wait,
            )
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str, params: Optional[dict] = None) -> Any:
        """DELETE; sign `path` only (no query string in HMAC per auth spec)."""
        headers = self._sign("DELETE", path)
        r = self.session.delete(
            API_BASE + path, params=params or {}, headers=headers, timeout=15
        )
        self._update_rate_limit_state(r)
        if r.status_code == 429:
            log.warning(
                "DELETE %s returned 429 — wait %s before retry",
                path,
                self._retry_after_seconds(r),
            )
        r.raise_for_status()
        return r.json()

    def _delete_json(self, path: str, body: dict) -> Any:
        raw = json.dumps(body, separators=(",", ":"))
        headers = self._sign("DELETE", path, raw)
        r = self.session.request(
            "DELETE", API_BASE + path, data=raw, headers=headers, timeout=15
        )
        self._update_rate_limit_state(r)
        if r.status_code == 429:
            log.warning(
                "DELETE %s returned 429 — wait %s before retry",
                path,
                self._retry_after_seconds(r),
            )
        r.raise_for_status()
        return r.json()

    # ── Exchange ─────────────────────────────────────────────────────────────

    def get_server_time(self) -> dict:
        return self._get("/time")

    def get_exchange_status(self) -> dict:
        return self._get("/exchange/status")

    # ── Markets ──────────────────────────────────────────────────────────────

    def get_market(self, ticker: str) -> dict:
        return self._get(f"/markets/{ticker}")

    def get_all_markets(self, category: Optional[str] = None) -> list:
        """
        GET /markets. On 429 we do not retry here — immediate retries burn the same
        IP bucket and worsen sustained rate limits; callers should back off and use cache.
        """
        params: dict = {}
        if category:
            params["category"] = category
        resp = self._get("/markets", params=params)
        if isinstance(resp, list):
            return resp
        return _unwrap_list(resp, "data")

    def get_orderbook(self, ticker: str, depth: int = 20) -> dict:
        return self._get(f"/markets/{ticker}/book", params={"depth": depth})

    def get_recent_trades(self, ticker: str, limit: int = 100,
                          cursor: Optional[str] = None) -> dict:
        """Returns `{data: [...], nextCursor}` per OpenAPI (limit max 500)."""
        params: dict = {"limit": _clamp_limit(limit, 500)}
        if cursor:
            params["cursor"] = cursor
        return self._get(f"/markets/{ticker}/trades", params=params)

    def get_candles(
        self,
        ticker: str,
        *,
        interval: str = "5m",
        start: Optional[Union[str, datetime]] = None,
        end: Optional[Union[str, datetime]] = None,
        limit: int = 500,
    ) -> list:
        """
        GET /markets/{ticker}/candles
        interval: '1m' | '5m' | '1d'
        If start is omitted, uses a window of roughly `limit` bars ending at `end` (or now).
        OpenAPI: limit max 2500.
        """
        limit = _clamp_limit(limit, 2500)
        minutes_per = {"1m": 1, "5m": 5, "1d": 1440}
        if interval not in minutes_per:
            raise ValueError(f"interval must be one of {list(minutes_per.keys())}")

        if end is None:
            end_dt = datetime.now(timezone.utc)
            end_s = None
        elif isinstance(end, datetime):
            end_dt = end.astimezone(timezone.utc)
            end_s = _iso_utc(end_dt)
        else:
            end_s = str(end)
            end_dt = datetime.fromisoformat(end_s.replace("Z", "+00:00"))

        if start is None:
            start_dt = end_dt - timedelta(minutes=minutes_per[interval] * limit)
            start_s = _iso_utc(start_dt)
        elif isinstance(start, datetime):
            start_s = _iso_utc(start.astimezone(timezone.utc))
        else:
            start_s = str(start)

        params: dict = {"interval": interval, "start": start_s, "limit": limit}
        if end_s:
            params["end"] = end_s
        return self._get(f"/markets/{ticker}/candles", params=params)

    def get_candles_compat(self, ticker: str, resolution_min: int = 30,
                           bars: int = 200) -> list:
        """
        Map older `resolution_min` + `bars` style to Forum candle intervals.
        Uses 5m candles for typical intraday resolutions, 1d for >= 1440.
        """
        if resolution_min >= 1440:
            interval = "1d"
        elif resolution_min <= 1:
            interval = "1m"
        else:
            interval = "5m"
        return self.get_candles(ticker, interval=interval, limit=bars)

    # ── Indices ──────────────────────────────────────────────────────────────

    def get_index_details(self, name: str) -> dict:
        return self._get(f"/indices/{name}")

    def get_index_history(
        self,
        name: str,
        *,
        start: Union[str, datetime],
        end: Optional[Union[str, datetime]] = None,
        limit: int = 2500,
        interval: str = "raw",
    ) -> list:
        limit = _clamp_limit(limit, 2500)
        if isinstance(start, datetime):
            start = _iso_utc(start)
        params: dict = {"start": start, "limit": limit, "interval": interval}
        if end is not None:
            params["end"] = _iso_utc(end) if isinstance(end, datetime) else end
        return self._get(f"/indices/{name}/history", params=params)

    # ── Funding ──────────────────────────────────────────────────────────────

    def get_funding_rate(self, ticker: str) -> dict:
        """Last / moving rate, index, last price, next funding time."""
        return self._get(f"/markets/{ticker}/funding-rate")

    def get_funding_history(
        self,
        ticker: str,
        *,
        start: Union[str, datetime],
        end: Optional[Union[str, datetime]] = None,
        limit: int = 2500,
    ) -> list:
        limit = _clamp_limit(limit, 2500)
        if isinstance(start, datetime):
            start = _iso_utc(start)
        params: dict = {"start": start, "limit": limit}
        if end is not None:
            params["end"] = _iso_utc(end) if isinstance(end, datetime) else end
        return self._get(f"/markets/{ticker}/funding-history", params=params)

    # ── Private ──────────────────────────────────────────────────────────────

    def get_account(self) -> dict:
        return self._get("/account", auth=True)

    def get_positions(self) -> list:
        resp = self._get("/positions", auth=True)
        return _unwrap_list(resp, "data") or ([] if isinstance(resp, dict) else resp)

    def get_position(self, ticker: str) -> dict:
        """GET /positions/{ticker}"""
        return self._get(f"/positions/{ticker}", auth=True)

    def get_order(self, order_id: Union[int, str]) -> dict:
        """GET /orders/{orderId}"""
        return self._get(f"/orders/{order_id}", auth=True)

    def get_open_orders(self, ticker: Optional[str] = None) -> list:
        params = {"ticker": ticker} if ticker else {}
        resp = self._get("/orders", params=params, auth=True)
        return _unwrap_list(resp, "data") or ([] if isinstance(resp, dict) else resp)

    def list_fills(
        self,
        ticker: Optional[str] = None,
        order_id: Optional[int] = None,
        start: Optional[Union[str, datetime]] = None,
        end: Optional[Union[str, datetime]] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> list:
        params: dict = {"limit": _clamp_limit(limit, 500)}
        if ticker:
            params["ticker"] = ticker
        if order_id is not None:
            params["orderId"] = order_id
        if start is not None:
            params["start"] = _iso_utc(start) if isinstance(start, datetime) else start
        if end is not None:
            params["end"] = _iso_utc(end) if isinstance(end, datetime) else end
        if cursor:
            params["cursor"] = cursor
        resp = self._get("/fills", params=params, auth=True)
        return _unwrap_list(resp, "data")

    def get_fills_page(
        self,
        *,
        ticker: Optional[str] = None,
        order_id: Optional[int] = None,
        start: Optional[Union[str, datetime]] = None,
        end: Optional[Union[str, datetime]] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> dict:
        """
        GET /fills (paginated envelope).
        Returns the raw response including `nextCursor`.
        """
        params: dict = {"limit": _clamp_limit(limit, 500)}
        if ticker:
            params["ticker"] = ticker
        if order_id is not None:
            params["orderId"] = order_id
        if start is not None:
            params["start"] = _iso_utc(start) if isinstance(start, datetime) else start
        if end is not None:
            params["end"] = _iso_utc(end) if isinstance(end, datetime) else end
        if cursor:
            params["cursor"] = cursor
        resp = self._get("/fills", params=params, auth=True)
        return resp if isinstance(resp, dict) else {"data": resp, "nextCursor": None}

    def list_orders(
        self,
        *,
        ticker: Optional[str] = None,
        side: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> list:
        """
        GET /orders (paginated envelope).
        OpenAPI: limit max 500, supports cursor pagination and optional filters.
        """
        params: dict = {"limit": _clamp_limit(limit, 500)}
        if ticker:
            params["ticker"] = ticker
        if side:
            params["side"] = side
        if cursor:
            params["cursor"] = cursor
        resp = self._get("/orders", params=params, auth=True)
        return _unwrap_list(resp, "data") or ([] if isinstance(resp, dict) else resp)

    def place_order(
        self,
        ticker: str,
        side: str,
        qty: int,
        order_type: str = "limit",
        price: Optional[int] = None,
        reduce_only: bool = False,
        time_in_force: Optional[str] = None,
        client_order_id: Optional[str] = None,
    ) -> dict:
        body = {
            "ticker":     ticker,
            "side":       side,
            "qty":        qty,
            "orderType":  order_type,
            "reduceOnly": reduce_only,
            "clientOrderId": client_order_id or f"fb-{uuid.uuid4().hex}",
        }
        if price is not None:
            body["price"] = price
        if order_type == "limit":
            body["timeInForce"] = time_in_force or "goodTillCancel"
        elif time_in_force:
            body["timeInForce"] = time_in_force
        return self._post("/orders", body)

    def place_orders_batch(self, orders: List[dict]) -> dict:
        """
        POST /orders/batch — up to 10 orders per request.
        Each element is a CreateOrderRequest dict (ticker, side, qty, orderType, ...).
        Adds timeInForce=goodTillCancel for limit orders if omitted (competition API).
        """
        if len(orders) > 10:
            raise ValueError("batch supports at most 10 orders")
        normalized: List[dict] = []
        for o in orders:
            o2 = dict(o)
            if o2.get("orderType") == "limit" and "timeInForce" not in o2:
                o2["timeInForce"] = "goodTillCancel"
            normalized.append(o2)
        return self._post("/orders/batch", {"orders": normalized})

    def cancel_order(self, order_id: Union[int, str]) -> dict:
        return self._delete(f"/orders/{order_id}")

    def get_order_by_client_id(self, client_order_id: str) -> dict:
        """GET /orders/client/{clientOrderId}"""
        return self._get(f"/orders/client/{client_order_id}", auth=True)

    def cancel_order_by_client_id(self, client_order_id: str) -> dict:
        """DELETE /orders/client/{clientOrderId}"""
        return self._delete(f"/orders/client/{client_order_id}")

    def cancel_orders_batch(self, order_ids: List[int]) -> dict:
        """DELETE /orders/batch — up to 20 order IDs."""
        if len(order_ids) > 20:
            raise ValueError("batch cancel supports at most 20 order IDs")
        return self._delete_json("/orders/batch", {"orderIds": order_ids})

    def cancel_all_orders(self, ticker: Optional[str] = None) -> dict:
        params = {"ticker": ticker} if ticker else None
        return self._delete("/orders", params=params)
