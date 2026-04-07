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
MAX_POSITION_PCT    = 0.14          # per-ticker cap (full 20-name universe; avoids margin pile-up)
MAX_GROSS_LEVERAGE  = 2.0           # total resting + positions cap in notional terms
MIN_EDGE_THRESHOLD  = 0.0040        # combiner min avg edge — MR edge is coarse; higher = fewer spurious trades
# Combined-signal quality gates: push toward fewer, higher-conviction trades.
MIN_COMBINED_STRENGTH = 0.22
REQUIRE_MULTI_STRATEGY_CONFIRMATION = True
SINGLE_STRATEGY_MIN_STRENGTH = 0.60
ALLOW_SINGLE_STRATEGY_SIGNALS = False  # if False: require >=2 strategies to agree

# ─── COMPETITION MODE ─────────────────────────────────────────────────────────
# Market fit: Forum comp behaves more like carry+trend than tight MR-to-index arb.
ENABLE_MEAN_REVERSION_ENTRY = False
ENABLE_MEAN_REVERSION_EXIT  = False
# Allow single-strategy entries only for these strategies (others require confirmation).
SINGLE_STRATEGY_ALLOWLIST = {"FundingHarvest"}
# Extra strength required for allowlisted single-strategy entries.
SINGLE_STRATEGY_ALLOWLIST_MIN_STRENGTH = 0.70

# Close any open position shortly before settlement (carry is not prorated).
FUNDING_EXIT_SECS_BEFORE = 45

# Momentum exit: if momentum strongly flips against your position, close.
MOM_EXIT_MIN_STRENGTH = 0.65

# Funding entries: block carry trades that fight strong momentum.
FUNDING_BLOCK_IF_MOMENTUM_OPPOSES = True
FUNDING_MOMENTUM_OPPOSE_BLOCK_STRENGTH = 0.55

# ─── FUNDING STRATEGY ─────────────────────────────────────────────────────────
# Funding is settled every 8h; we compare movingFundingRate vs threshold
FUNDING_HARVEST_THRESH   = 0.0012   # enter carry a bit earlier than 0.15%
FUNDING_INTERVAL_HOURS   = 8        # settlement cadence (informative; use API nextFundingTime live)
# Skip opening new funding-carry positions inside this window before nextFundingTime
FUNDING_AVOID_SECS_BEFORE = 120

# ─── MEAN REVERSION ───────────────────────────────────────────────────────────
MR_ZSCORE_ENTRY     = 1.95        # stricter dislocation vs index (fewer marginal MR entries)
MR_ZSCORE_EXIT      = 0.4           # exit when spread reverts near 0
MR_LOOKBACK_BARS    = 48            # bars (30 min each → 24h lookback)
# Exit discipline: avoid flipping out too early on small adverse noise.
MIN_HOLD_SECS       = 900           # minimum hold before MR-driven exits (15m)
MR_EXIT_MIN_PNL_PCT = -0.003        # avoid MR exits below -0.30% unless fully reverted
MR_FORCE_EXIT_Z     = 0.12          # allow exit at a loss when z-score is near flat

# ─── ADAPTIVE STRATEGY ALLOCATION ─────────────────────────────────────────────
# Shift capital away from underperforming signal families (rolling PnL% by strategy).
STRATEGY_THROTTLE_ENABLED       = True
STRATEGY_THROTTLE_LOOKBACK      = 40      # strategy score memory in ticks/events
STRATEGY_THROTTLE_LOSS_PCT      = -0.0015 # if avg score <= -0.15%, throttle
STRATEGY_THROTTLE_REDUCTION     = 0.55    # multiplier at/below loss threshold
STRATEGY_THROTTLE_MIN_MULTIPLIER = 0.35   # never size below this multiplier

# ─── ORDER / MARGIN HYGIENE ───────────────────────────────────────────────────
# Prevent tying up margin in too many resting limits (a common cause of 409 INSUFFICIENT_MARGIN).
MAX_PENDING_BOT_ORDERS = 8
# Allow 2 when running market making (one bid + one ask).
MAX_PENDING_BOT_ORDERS_PER_TICKER = 2
# On margin errors, pause new opens briefly (still allows reduce-only closes).
MARGIN_ERROR_COOLDOWN_SECS = 180

# ─── MARKET MAKING (SPREAD CAPTURE) ───────────────────────────────────────────
# Thin books mean spreads dominate signal edges. This mode places small two-sided quotes
# on a small subset of tickers with relatively tight spreads (e.g. SPACEX).
ENABLE_MARKET_MAKING = True
# Only quote these tickers (empty list => disabled by filter, even if ENABLE_MARKET_MAKING).
MARKET_MAKING_TICKERS = ["SPACEX", "CLUELY", "PEPTIDES", "OPENAI"]
# Only quote when top-of-book spread is at/below this many cents.
MM_MAX_SPREAD_CENTS = 60
# Quote size (contracts) per side.
MM_QUOTE_QTY = 2
# Inventory cap per ticker (absolute position size). If exceeded, only quote to reduce.
MM_MAX_INVENTORY = 6
# How often to refresh quotes (seconds). Should be >= loop interval to avoid spam.
MM_REFRESH_SECS = 60

# ─── POSITION UNWIND (REDUCE-ONLY WORKER) ─────────────────────────────────────
# When you have a discretionary/manual position, it can consume freeMargin and break MM.
# This worker slowly works positions down using reduce-only limit orders (no new risk).
ENABLE_UNWIND_WORKER = True
# Only unwind these tickers (empty => disabled). Useful for cleaning up manual mistakes.
# Set this to ALL currently-held tickers when you need to fully reset inventory + free margin.
# Based on your `forum_bot.log (1–5435)` tail, you had open positions in:
#   ALTMAN, GTASIX, ICE, KALSHI, KANYE, SPACEX, UBER
UNWIND_TICKERS = ["ALTMAN", "GTASIX", "ICE", "KALSHI", "KANYE", "SPACEX", "UBER"]
# Max contracts per reduce-only order.
UNWIND_MAX_QTY_PER_ORDER = 1
# How often to refresh unwind orders per ticker.
UNWIND_REFRESH_SECS = 90
# Price style for unwind:
# - "passive": join best bid/ask (slow)
# - "improve": step inside spread when possible (faster, still non-crossing)
UNWIND_PRICE_MODE = "passive"
# Profit guard for unwind: avoid selling longs below avg entry (and avoid buying shorts above avg entry)
# unless margin is dangerously low.
UNWIND_MIN_PROFIT_CENTS = 2
# If freeMargin (cents) drops below this, unwind is allowed even at a loss to reduce risk.
UNWIND_FORCE_IF_FREE_MARGIN_BELOW_CENTS = 5_000

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
# Bot session-placed limits: cancelled after max(ORDER_TIMEOUT_SECS, POLL_INTERVAL_SECS+15).
# Keep ORDER_TIMEOUT_SECS meaningful once above that floor (e.g. 300 = refresh quotes every 5m).
ORDER_TIMEOUT_SECS  = 300
SLIPPAGE_BUFFER     = 5             # extra cents on aggressive limit orders (funding-urgent only by default)
# Normal entry pricing (still gated by strategy thresholds — this is how we quote, not how often).
#   passive     — join best bid/ask (cheapest; often sits unfilled)
#   improve     — inside spread toward mid (balanced: better fill odds, little extra vs passive)
#   aggressive  — cross spread + SLIPPAGE_BUFFER (use sparingly; enable for all opens only if you accept cost)
ENTRY_OPEN_PRICE_MODE = "improve"
# Contract-count cap for cheap tickers (prevents oversized fills on low-price names).
LOW_PRICE_THRESHOLD_CENTS = 1500
MAX_CONTRACTS_LOW_PRICE   = 5

# ─── RISK ─────────────────────────────────────────────────────────────────────
DAILY_LOSS_LIMIT_PCT = 0.05         # halt if NAV drops >5% in a day
MAX_DRAWDOWN_PCT     = 0.12         # halt if drawdown from peak > 12%
