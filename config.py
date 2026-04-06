"""
Forum Trading Bot - Configuration
=================================
Edit this file before running. All tunable parameters live here.
"""

import os
from pathlib import Path


def _load_dotenv() -> None:
    """Load KEY=VALUE lines from repo-root .env if present (does not override env)."""
    path = Path(__file__).resolve().parent / ".env"
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

# ─── API ──────────────────────────────────────────────────────────────────────
# Competition defaults (https://comp-docs.forum.market). Override for prod:
#   export FORUM_API_BASE=https://api.forum.market/v1
#   export FORUM_WS_URL=wss://api.forum.market/ws/v1
API_BASE        = os.getenv("FORUM_API_BASE", "https://comp-api.forum.market/v1")
WS_URL          = os.getenv("FORUM_WS_URL", "wss://comp-api.forum.market/ws/v1")
API_KEY         = os.getenv("FORUM_API_KEY", "")          # fk_... (read+trade for --live)
API_SECRET      = os.getenv("FORUM_API_SECRET", "")       # HMAC secret from Forum

# HMAC prehash path must include "/v1" (see Authentication — path is /v1/orders not /orders).
# Rate limits (comp-docs): public 120/min IP, private read 300/min user, order ops 500/min user.
# Candles / index history: OpenAPI limit max 2500 (not the older 1000 pagination note).

# ─── UNIVERSE ─────────────────────────────────────────────────────────────────
# All tradeable tickers (edit to add/remove)
UNIVERSE = [
    "OPENAI", "ANTHROPIC", "OZEMPIC", "PEPTIDES", "MRBEAST",
    "CLUELY", "MUSK", "ALTMAN", "KANYE", "DRAKE", "GTASIX",
    "KALSHI", "POLYMARKET", "ICE", "TRUMP", "CLAWD", "SPACEX",
    "UBER", "WAYMO", "F1",
]

# ─── CAPITAL ──────────────────────────────────────────────────────────────────
# 100_000 cents = $1,000 (typical comp notional). Portfolio NAV is local until fills sync.
INITIAL_CAPITAL     = 100_000
MAX_POSITION_PCT    = 0.18          # per-ticker cap (slightly higher for $1k book)
MAX_GROSS_LEVERAGE  = 2.5           # cross-margin style cap on total exposure
MIN_EDGE_THRESHOLD  = 0.0025        # combiner min avg edge (0.25%) — slightly more active

# ─── FUNDING STRATEGY ─────────────────────────────────────────────────────────
# Funding is settled every 8h; we compare movingFundingRate vs threshold
FUNDING_HARVEST_THRESH   = 0.0012   # enter carry a bit earlier than 0.15%
FUNDING_INTERVAL_HOURS   = 8        # settlement cadence (informative; use API nextFundingTime live)
# Skip opening new funding-carry positions inside this window before nextFundingTime
FUNDING_AVOID_SECS_BEFORE = 120

# ─── MEAN REVERSION ───────────────────────────────────────────────────────────
MR_ZSCORE_ENTRY     = 1.65        # slightly more responsive than 1.8 for attention vol
MR_ZSCORE_EXIT      = 0.4           # exit when spread reverts near 0
MR_LOOKBACK_BARS    = 48            # bars (30 min each → 24h lookback)

# ─── MOMENTUM ─────────────────────────────────────────────────────────────────
MOM_FAST_BARS       = 6             # fast EWM (3h)
MOM_SLOW_BARS       = 24            # slow EWM (12h)
MOM_SIGNAL_THRESH   = 0.005         # min fast-slow divergence to trade

# ─── EXECUTION ────────────────────────────────────────────────────────────────
POLL_INTERVAL_SECS  = 60            # main loop frequency
# Reuse GET /markets snapshot on 429 / blips (avoid 20× per-ticker hammer).
MARKETS_CACHE_TTL_SEC = 90
# Refresh per-ticker funding at most this often (20 tickers × /funding-rate spikes public limits).
FUNDING_REFRESH_INTERVAL_SEC = 120
ORDER_TIMEOUT_SECS  = 30            # cancel unfilled limit orders after this
SLIPPAGE_BUFFER     = 5             # extra cents on aggressive limit orders

# ─── RISK ─────────────────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT_PCT = 0.05         # halt if NAV drops >5% in a day
MAX_DRAWDOWN_PCT     = 0.12         # halt if drawdown from peak > 12%
