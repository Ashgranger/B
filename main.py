#!/usr/bin/env python3
"""Bulk Trade Perp DEX High-Frequency Market Maker (Level 7/8 Quantitative Engine).

Usage:
  python main.py                 # paper-trade (DRY_RUN=1): simulated fills, real data
  python main.py --live          # live trading on Bulk Trade Solana Perp DEX
  python main.py scan            # scan markets for highest spread & liquidity opportunities
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from utils import Fatal, setup_logging

log = logging.getLogger("main")


def main() -> None:
    ap = argparse.ArgumentParser(description="Bulk Trade Perp DEX Market Maker")
    ap.add_argument("cmd", nargs="?", default="run", choices=["run", "scan"])
    ap.add_argument("--live", action="store_true", help="Send live signed orders (overrides DRY_RUN=1)")
    ap.add_argument("--env", choices=["mainnet", "testnet", "devnet"], help="Override BULK_ENV")
    ap.add_argument("--market", help="Override MARKET (e.g. SOL-USD, BTC-USD)")
    ap.add_argument("--env-file", default=".env")
    ap.add_argument("--seconds", type=float, default=30, help="Scan sampling duration")
    args = ap.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(args.env_file)
    except ImportError:
        pass

    if args.env:
        os.environ["BULK_ENV"] = args.env
    if args.market:
        os.environ["MARKET"] = args.market
    if args.live:
        os.environ["DRY_RUN"] = "0"

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    from config import Config
    try:
        cfg = Config.from_env()
    except Fatal as e:
        sys.exit(f"Configuration error: {e}")

    if args.cmd == "scan":
        from scan import scan
        try:
            asyncio.run(scan(cfg, args.seconds))
        except KeyboardInterrupt:
            pass
        return

    from bot import MarketMaker
    mm = MarketMaker(cfg)

    def _sig_handler(sig, frame):
        log.info("Shutdown signal received (%s), terminating...", sig)
        mm.stop_evt.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    try:
        asyncio.run(mm.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
