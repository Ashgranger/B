#!/usr/bin/env python3
"""Robinhood Lighter Perp DEX Level 8+ Institutional Market Maker.

Usage:
  python main.py                 # paper-trade (DRY_RUN=1): real market data, simulated fills, no orders
  python main.py --live          # send real post-only limit orders over WebSocket
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
    ap = argparse.ArgumentParser(description="Robinhood Lighter Perp DEX Level 8+ Market Maker")
    ap.add_argument("--live", action="store_true", help="send real orders (overrides DRY_RUN=1)")
    ap.add_argument("--env", choices=["mainnet", "testnet"], help="override LIGHTER_ENV")
    ap.add_argument("--market", help="override MARKET (e.g. BTC-USD)")
    ap.add_argument("--env-file", default=".env")
    args = ap.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(args.env_file)
    except ImportError:
        pass

    if args.env:
        os.environ["LIGHTER_ENV"] = args.env
    if args.market:
        os.environ["MARKET"] = args.market
    if args.live:
        os.environ["DRY_RUN"] = "0"

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    from config import Config
    try:
        cfg = Config.from_env()
    except Fatal as e:
        sys.exit(f"config error: {e}")

    from bot import LighterMarketMaker
    log.info(
        "⚡ [ROBINHOOD LIGHTER MM] env=%s market=%s %s | ChainID=%d APIKeyIdx=%d | "
        "order=$%s max_pos=$%s | MakerFee=%.3f%% (%.1fbps) TakerFee=%.3f%% (%.1fbps) | "
        "MinEdge=%sbps Skew=%sbps LadderLevels=%d",
        cfg.env_name, cfg.market,
        "PAPER (no orders sent)" if cfg.dry_run else "LIVE REAL ORDERS",
        cfg.chain_id, cfg.api_key_index,
        cfg.order_usd, cfg.max_position_usd,
        float(cfg.maker_fee_bps) / 100.0, cfg.maker_fee_bps,
        float(cfg.taker_fee_bps) / 100.0, cfg.taker_fee_bps,
        cfg.min_edge_bps, cfg.skew_bps, cfg.extra_levels
    )

    async def _main() -> None:
        bot = LighterMarketMaker(cfg)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, bot.stop_evt.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(bot.stop_evt.set))
        await bot.run()

    asyncio.run(_main())


if __name__ == "__main__":
    main()
