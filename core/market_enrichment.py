"""Helpers to merge REST payloads for strategy consumption."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def enrich_market_with_funding(mkt: dict, funding: dict) -> dict:
    """Overlay get_funding_rate() fields onto get_market() for a single snapshot."""
    out = dict(mkt)
    if funding.get("movingFundingRate") is not None:
        out["movingFundingRate"] = funding["movingFundingRate"]
    if funding.get("fundingRate") is not None:
        out["lastSettledFundingRate"] = funding["fundingRate"]
    if funding.get("indexValue") is not None:
        out["lastIndexValue"] = funding["indexValue"]
    if funding.get("lastPrice") is not None:
        out["lastPrice"] = funding["lastPrice"]
    if funding.get("nextFundingTime") is not None:
        out["nextFundingTime"] = funding["nextFundingTime"]
    return out


def seconds_until_iso(iso_ts: Optional[str]) -> Optional[float]:
    """Seconds until an ISO-8601 instant (negative if already passed)."""
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return (dt - datetime.now(timezone.utc)).total_seconds()
    except (ValueError, TypeError):
        return None


def candle_time_key(c: dict) -> str:
    """Sort key for CandlestickRecord."""
    return c.get("start") or ""
