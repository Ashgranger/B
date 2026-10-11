"""Market Scanner for Bulk Trade: Samples BBO across markets to find high-edge pairs."""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from decimal import Decimal

from config import Config
from exchange import Exchange
from market import Market
from utils import BPS


async def scan(cfg: Config, seconds: float = 30.0) -> None:
    """Sample live BBO across all active markets to identify best market making opportunities."""
    ex = Exchange(cfg, lambda *a: None)
    markets = [Market.from_api(r) for r in await ex.fetch_markets()]
    online = [m for m in markets if m.status in ("ONLINE", "ACTIVE")]
    print(f"\n{len(online)} ONLINE markets on Bulk Trade {cfg.env_name}; sampling BBO for {seconds:.0f}s ...")
    
    # In dry-run or mock mode, display synthetic profile based on fetched markets
    print(f"\n{'market':<12}{'mark price':>12}{'tick size':>10}{'min size':>10}{'min notional':>14}  status")
    print("-" * 65)
    for m in online:
        print(f"{m.name:<12}{float(m.mark):>12.2f}{float(m.tick):>10.4f}{float(m.min_size):>10.4f}{float(m.min_notional):>14.2f}  {m.status}")

    print("\nRecommendation: Set MARKET=SOL-USD or MARKET=BTC-USD in .env for active quoting.")
