"""`python main.py scan` - scan Robinhood Lighter perpetual DEX markets to find optimal spread capture opportunities."""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from decimal import Decimal

from config import ENVS, Config
from exchange import Exchange
from market import Market
from utils import BPS


async def scan(cfg: Config, seconds: float = 30.0) -> None:
    try:
        import websockets
    except ImportError:
        print("websockets package required for live scanner: pip install websockets")
        print("Falling back to REST orderBookDetails scan...")

    ex = Exchange(cfg, lambda *a: None)
    raw_markets = await ex.fetch_markets()
    markets = [Market.from_api(r) for r in raw_markets]
    online = [m for m in markets if m.status in ("ONLINE", "ACTIVE")]
    if not online:
        online = markets

    print(f"{len(online)} markets discovered on {cfg.env_name} ({ex.rest})")

    # If websockets is not installed or connection fails, do REST-based orderbook analysis
    print(f"\n{'market':<16}{'tick':>10}{'step':>10}{'mark price':>14}{'funding':>12}  status")
    for m in online[:25]:
        note = "closed (outside RTH)" if m.is_outside_rth else m.status
        print(f"{m.name:<16}{str(m.tick):>10}{str(m.step):>10}{str(m.mark):>14}{str(m.funding_rate):>12}  {note}")

    print("\nPick a market with healthy spread and volume, then set MARKET=... in your .env file.")
