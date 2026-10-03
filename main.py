"""Entry point for Bulk Trade Perpetual DEX Market Maker Bot."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from bot import BulkMarketMakerBot
from config import Config
from scan import scan_markets
from utils import Fatal


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def main_async(args: argparse.Namespace) -> None:
    cfg = Config.load(args.config)

    # CLI parameter overrides
    if args.env:
        cfg.env_name = args.env.lower()
    if args.market:
        cfg.market = args.market.upper()
    if args.dry_run is not None:
        cfg.dry_run = bool(args.dry_run)

    bot = BulkMarketMakerBot(cfg)
    loop = asyncio.get_running_loop()

    # Graceful shutdown handlers
    def _sig_handler():
        logging.info("Received termination signal. Cancelling tasks...")
        asyncio.create_task(bot.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    try:
        await bot.start()
    except asyncio.CancelledError:
        pass
    finally:
        await bot.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk Trade Perpetual DEX Level 8+ Market Maker")
    parser.add_argument("--env", choices=["mainnet", "testnet", "devnet"], help="Environment")
    parser.add_argument("--market", help="Market symbol (e.g. BTC-USD, SOL-USD)")
    parser.add_argument("--dry-run", type=int, choices=[0, 1], help="1=Paper Trading, 0=Live Real Orders")
    parser.add_argument("--config", default=".env", help="Path to .env configuration file")
    parser.add_argument("--scan", action="store_true", help="Scan active markets on Bulk Trade and exit")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    if args.scan:
        scan_markets(args.env or "mainnet")
        return

    try:
        asyncio.run(main_async(args))
    except (KeyboardInterrupt, Fatal) as e:
        logging.info("Exiting: %s", e)
        sys.exit(0)


if __name__ == "__main__":
    main()
