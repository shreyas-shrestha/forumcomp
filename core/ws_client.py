"""
Forum WebSocket client — real-time market + private channels.

Requires: pip install websocket-client

Connect to wss://comp-api.forum.market/ws/v1 (or FORUM_WS_URL), subscribe within 10s.

Inbound shapes (AsyncAPI / docs): ``heartbeat``, ``orderUpdate`` (userOrders),
``fill`` (userFills), ``positionUpdate`` (userPositions), ``accountUpdate`` (userAccount).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Any, Callable, List, Optional

from config import API_KEY, API_SECRET, WS_URL

log = logging.getLogger(__name__)

OnEvent = Optional[Callable[[dict], None]]


def _sign_ws(secret: str, timestamp: str) -> str:
    prehash = timestamp + "GET" + "/ws/v1"
    return base64.b64encode(
        hmac.new(secret.encode(), prehash.encode(), hashlib.sha256).digest()
    ).decode()


class ForumWebSocket:
    """
    Background WebSocket connection with additive subscriptions.

    Typical flow: public subscribe → auth → private subscribe (after authOk).
    """

    def __init__(
        self,
        *,
        tickers: List[str],
        url: str = WS_URL,
        api_key: str = API_KEY,
        api_secret: str = API_SECRET,
        clock_skew_sec: float = 0.0,
        public_channels: Optional[List[str]] = None,
        private_channels: Optional[List[str]] = None,
        on_event: OnEvent = None,
        subscribe_heartbeat: bool = False,
        forward_heartbeat: bool = False,
    ):
        try:
            import websocket as ws_mod  # type: ignore[import-untyped]
        except ImportError as e:
            raise RuntimeError(
                "Install websocket-client: pip install websocket-client"
            ) from e

        self._ws_mod = ws_mod
        self.url = url
        self.tickers = tickers
        self.api_key = api_key
        self.api_secret = api_secret
        self._skew = clock_skew_sec
        pub = list(public_channels or ["tickerUpdates"])
        if subscribe_heartbeat and "heartbeat" not in pub:
            pub.append("heartbeat")
        self.public_channels = pub
        self.private_channels = private_channels or []
        self.on_event = on_event
        self._forward_heartbeat = forward_heartbeat

        self._ws_app: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._auth_pending = bool(self.private_channels and api_key and api_secret)
        self._msg_id = 0
        self._lock = threading.Lock()
        self._backoff_sec = 1.0
        self._last_close_code: Optional[int] = None

    def _ts(self) -> str:
        return str(int(time.time() + self._skew))

    def _next_cmd_id(self) -> int:
        with self._lock:
            self._msg_id += 1
            return self._msg_id

    def _send_cmd(self, ws, cmd: str, params: dict) -> None:
        payload = {"id": self._next_cmd_id(), "cmd": cmd, "params": params}
        raw = json.dumps(payload, separators=(",", ":"))
        ws.send(raw)

    def _on_open(self, ws) -> None:
        log.info("WS connected, subscribing public channels")
        self._backoff_sec = 1.0
        self._send_cmd(
            ws,
            "subscribe",
            {"channels": self.public_channels, "tickers": self.tickers},
        )
        if self._auth_pending:
            ts = self._ts()
            sig = _sign_ws(self.api_secret, ts)
            self._send_cmd(
                ws,
                "auth",
                {"key": self.api_key, "timestamp": ts, "signature": sig},
            )

    def _on_message(self, ws, message: str) -> None:
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            log.warning("WS non-JSON message: %s", message[:200])
            return

        if msg.get("event") == "authOk":
            log.info("WS auth OK, subscribing private channels")
            self._send_cmd(
                ws,
                "subscribe",
                # Per comp docs: private channels do not require tickers; use [].
                {"channels": self.private_channels, "tickers": []},
            )
            self._auth_pending = False

        if self.on_event:
            if msg.get("type") == "heartbeat" and not self._forward_heartbeat:
                return
            try:
                self.on_event(msg)
            except Exception as e:
                log.error("WS on_event error: %s", e, exc_info=True)

    def _on_error(self, ws, error) -> None:
        if error is not None:
            log.warning("WS error: %s", error)

    def _on_close(self, ws, code, reason) -> None:
        try:
            self._last_close_code = int(code) if code is not None else None
        except Exception:
            self._last_close_code = None
        log.info("WS closed code=%s reason=%s", code, reason)

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            self._ws_app = self._ws_mod.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            try:
                self._ws_app.run_forever()
            except Exception as e:
                log.warning("WS run_forever: %s", e)
            if self._stop.is_set():
                break
            # Reconnect with exponential backoff, capped at 30s (docs suggestion).
            sleep_s = min(self._backoff_sec, 30.0)
            log.info("WS reconnecting in %.1fs (last_close=%s)", sleep_s, self._last_close_code)
            time.sleep(sleep_s)
            self._backoff_sec = min(self._backoff_sec * 2.0, 30.0)

    def start(self, daemon: bool = True) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._auth_pending = bool(self.private_channels and self.api_key and self.api_secret)
        self._thread = threading.Thread(target=self._run_loop, daemon=daemon)
        self._thread.start()
        log.info("WS thread started")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._ws_app:
            try:
                self._ws_app.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None
        self._ws_app = None
