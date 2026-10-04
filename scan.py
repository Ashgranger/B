#!/usr/bin/env python3
"""Market scanner CLI for Robinhood Lighter Perpetual DEX."""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request

from config import ENVS


def scan_markets(env: str = "mainnet") -> None:
    rest_url = ENVS[env]["rest"]
    print(f"🔍 Querying Robinhood Lighter Markets from {rest_url}...")
    try:
        url = f"{rest_url}/orderBookDetails"
        req = urllib.request.Request(url, headers={"User-Agent": "RobinhoodLighterScanner/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        markets = data if isinstance(data, list) else data.get("order_book_details", [data])
        print(f"\nFound {len(markets)} active contracts on {env.upper()}:\n")
        print(f"{'ID':<6} {'Symbol':<12} {'Tick':<10} {'Step':<10} {'Min Notional':<14} {'Status':<10}")
        print("-" * 65)
        for m in markets:
            mid = m.get("market_id") or m.get("marketId") or m.get("market_index") or "?"
            sym = m.get("symbol") or m.get("name") or "?"
            tick = m.get("tick_size") or m.get("tickSize") or "?"
            step = m.get("step_size") or m.get("stepSize") or "?"
            min_not = m.get("min_notional") or m.get("minOrderNotional") or "0"
            status = m.get("status", "ONLINE")
            print(f"{mid:<6} {sym:<12} {tick:<10} {step:<10} ${min_not:<13} {status:<10}")
    except Exception as e:
        print(f"Error fetching markets: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Robinhood Lighter Market Scanner")
    parser.add_argument("--env", choices=["mainnet", "testnet"], default="mainnet")
    args = parser.parse_args()
    scan_markets(args.env)
