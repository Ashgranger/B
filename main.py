#!/usr/bin/env python3
"""Arcus Full Quantitative Market Maker on Robinhood Lighter Perp DEX (Level 7/8 Engine).

  python main.py                 # paper-trade (DRY_RUN=1): real Robinhood Lighter market data, simulated fills
  python main.py --live          # send real post-only limit orders on Robinhood Lighter Perp DEX
  python main.py scan            # rank Robinhood Lighter perp markets by spread vs movement
  python main.py sim             # run standalone synthetic quantitative simulation
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
    ap = argparse.ArgumentParser(description="Arcus perp market maker on Robinhood Lighter Perp DEX (Level 7/8 Engine)")
    ap.add_argument("cmd", nargs="?", default="run", choices=["run", "scan", "sim"])
    ap.add_argument("--live", action="store_true", help="send real orders on Robinhood Lighter (overrides DRY_RUN=1)")
    ap.add_argument(
        "--env",
        choices=["mainnet", "testnet", "rh_mainnet", "rh_testnet", "lighter_mainnet", "lighter_testnet"],
        help="override LIGHTER_ENV / ARCUS_ENV",
    )
    ap.add_argument("--market", help="override MARKET (e.g. BTC, ETH, NVDA, CRCL, or BTC-USD)")
    ap.add_argument("--env-file", default=".env")
    ap.add_argument("--seconds", type=float, default=30, help="scan/sim sampling time")
    args = ap.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(args.env_file)
    except ImportError:
        pass
    if args.env:
        os.environ["LIGHTER_ENV"] = args.env
        os.environ["ARCUS_ENV"] = args.env
    if args.market:
        os.environ["MARKET"] = args.market
    if args.live:
        os.environ["DRY_RUN"] = "0"
    elif args.cmd in ("sim", "scan"):
        os.environ.setdefault("LIGHTER_WALLET_ADDRESS", "0x" + "11" * 20)
        os.environ.setdefault("LIGHTER_API_PRIVATE_KEY", "11" * 40)
        os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    from config import Config
    try:
        cfg = Config.from_env()
    except Fatal as e:
        sys.exit(f"config error: {e}")

    if args.cmd == "scan":
        from scan import scan
        asyncio.run(scan(cfg, args.seconds))
        return
    if args.cmd == "sim":
        from sim import run_standalone_sim
        asyncio.run(run_standalone_sim(steps=max(60, int(args.seconds * 10)), market_name=cfg.market))
        return

    from bot import MarketMaker
    log.info(
        "venue=Robinhood-Lighter env=%s (chain_id=%d, collateral=%s) market=%s %s | "
        "order=$%s max_pos=$%s min_edge=%sbps skew=%sbps ladder_levels=%d min_ev=%sbps tox_mult=%s",
        cfg.env_name, cfg.chain_id, cfg.settlement_asset, cfg.market,
        "PAPER (no orders sent)" if cfg.dry_run else "LIVE", cfg.order_usd, cfg.max_position_usd,
        cfg.min_edge_bps, cfg.skew_bps, cfg.extra_levels, cfg.min_ev_bps, cfg.tox_mult,
    )
    if not cfg.dry_run and cfg.env_name in ("mainnet", "rh_mainnet", "lighter_mainnet"):
        log.warning("LIVE ON MAINNET (%s) - real %s funds. Only post-only limit orders. Ctrl+C cancels all and exits.",
                    cfg.rest_url, cfg.settlement_asset)

    async def _main() -> None:
        bot = MarketMaker(cfg)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, bot.stop_evt.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(bot.stop_evt.set))
        await bot.run()

    try:
        import uvloop
        asyncio.run(_main(), loop_factory=uvloop.new_event_loop)
    except ImportError:
        asyncio.run(_main())


if __name__ == "__main__":
    main()
