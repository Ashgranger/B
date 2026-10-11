"""High-Frequency Quantitative Market Maker for Bulk Trade Perp DEX (100% WebSocket Engine)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from decimal import Decimal
from typing import Any, Optional

from config import Config
from exchange import Exchange
from feeds import CrossFeedManager
from market import Market, MarketData
from signer import Signer
from ledger import Ledger, Fill
from engine import MarketMakingEngine, QuoteTarget
from orders import OrderManager, Order
from utils import BPS, BUY, SELL, ZERO, ONE, Fatal, fmt

log = logging.getLogger("bot")


def extract_positions(c: Any) -> list:
    if isinstance(c, list):
        return [r for r in c if isinstance(r, dict)]
    if isinstance(c, dict):
        if "positions" in c:
            p = c["positions"]
            if isinstance(p, dict):
                return [r for r in p.values() if isinstance(r, dict)]
            if isinstance(p, list):
                return [r for r in p if isinstance(r, dict)]
            return []
        if "market" in c or "symbol" in c:
            return [c]
    return []


class MarketMaker:
    """Ultra-low latency institutional MM Engine running 100% over WebSocket on Bulk Trade."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.now = time.monotonic
        self.stop_evt = asyncio.Event()

        self.ex = Exchange(cfg, self._on_channel)
        self.md = MarketData(cfg)
        self.signer = Signer(
            cfg.private_key,
            cfg.account,
            domain=cfg.domain,
            builder_to=cfg.builder_to,
            builder_fee_bps=cfg.builder_fee_bps,
        )
        self.ledger = Ledger(cfg)
        self.ledger.on_markout_cb = self._journal_markout
        self.md.own_provider = self._own_resting
        self._loss_pause_until = 0.0
        self._pnl_base = ZERO
        self.engine = MarketMakingEngine(cfg)
        self.om = OrderManager(cfg, self.ex, self.signer, self._get_market, self._on_fill)

        self._recent_fills: deque = deque()
        self._burst_blocked_until = {BUY: 0.0, SELL: 0.0}
        self._trend_blocked_until = {BUY: 0.0, SELL: 0.0}

        self._last_heartbeat = 0.0
        self._last_reconcile = 0.0
        self._last_pause_log = {"rth": 0.0, "spread": 0.0, "oracle": 0.0, "jump": 0.0}
        self._last_status = 0.0
        self._last_info_fetch = 0.0
        self._last_logged_realized: Decimal = ZERO
        self._tick_lock = asyncio.Lock()
        self._dirty_evt = asyncio.Event()
        self._bg_tasks: dict = {}
        self._last_ext_wake = 0.0
        self._cross_feeds = None
        self._last_cross_log = 0.0
        self._dms_armed = False
        self._dms_fail = 0
        self._dms_ok_until = 0.0
        self._dms_blocked_until = 0.0
        self._files: dict = {}
        self._tick_on_trades = True

    def _get_market(self) -> Market:
        if not self.md.info:
            raise Fatal("Market metadata not yet loaded")
        return self.md.info

    def _on_channel(self, channel: str, contents: Any, is_snapshot: bool) -> None:
        """Actor/Watch zero-latency event handler from WebSocket stream."""
        now = self.now()
        ch = str(channel or "").lower()

        if ch in ("bbo", "ticker"):
            if not isinstance(contents, dict):
                return
            bb = contents.get("bestBid") or contents.get("bid")
            ba = contents.get("bestAsk") or contents.get("ask")
            if not bb or not ba:
                return
            try:
                bid = Decimal(str(bb["price"] if isinstance(bb, dict) else bb))
                ask = Decimal(str(ba["price"] if isinstance(ba, dict) else ba))
                bid_sz = Decimal(str(bb.get("size", 1))) if isinstance(bb, dict) else None
                ask_sz = Decimal(str(ba.get("size", 1))) if isinstance(ba, dict) else None
                self.md.update(bid, ask, bid_sz, ask_sz, now)
                if self.md.mid:
                    self.ledger.process_markouts(self.md.mid, now)
                self._dirty_evt.set()
            except Exception:
                pass

        elif ch in ("trades", "trade"):
            if isinstance(contents, list):
                for tr in contents:
                    self._handle_trade(tr, now)
            elif isinstance(contents, dict):
                self._handle_trade(contents, now)
            if self._tick_on_trades:
                self._dirty_evt.set()

        elif ch in ("orderbook", "depth", "l2orderbook"):
            if isinstance(contents, dict):
                bids = contents.get("bids") or []
                asks = contents.get("asks") or []
                self.md.on_depth(bids, asks, now)
                self._dirty_evt.set()

        elif ch in ("orders", "order"):
            if isinstance(contents, list):
                for row in contents:
                    self.om.on_update(row, now)
            elif isinstance(contents, dict):
                self.om.on_update(contents, now)
            self._dirty_evt.set()

        elif ch in ("fills", "userfills", "fill"):
            if isinstance(contents, list):
                for row in contents:
                    self.om.on_fill(row, now)
            elif isinstance(contents, dict):
                self.om.on_fill(contents, now)
            self._dirty_evt.set()

        elif ch in ("positions", "position"):
            rows = extract_positions(contents)
            m = self.md.info
            if m:
                target_mid = self.md.mid or m.mark
                for r in rows:
                    sym = str(r.get("symbol") or r.get("market") or "").upper()
                    if not sym or sym == m.name.upper():
                        side = str(r.get("side", "FLAT")).upper()
                        sz = Decimal(str(r.get("size") or r.get("sz") or "0"))
                        signed_pos = sz if side in ("LONG", "BUY") else (-sz if side in ("SHORT", "SELL") else ZERO)
                        self.ledger.reconcile(signed_pos, now, target_mid, m.min_notional)

        elif ch in ("funding", "fundingrate"):
            if isinstance(contents, dict):
                r = Decimal(str(contents.get("rate") or contents.get("fundingRate") or "0"))
                if self.md.info:
                    self.md.info.funding_rate = r
                pmt = Decimal(str(contents.get("payment") or contents.get("fundingPayment") or "0"))
                if pmt != ZERO:
                    self.ledger.apply_funding(pmt)

    def _handle_trade(self, tr: dict, now: float) -> None:
        try:
            px = Decimal(str(tr.get("price") or tr.get("px") or 0))
            sz = Decimal(str(tr.get("size") or tr.get("sz") or tr.get("qty") or 0))
            side = str(tr.get("side") or "").upper()
            if px > ZERO and sz > ZERO:
                self.md.on_trade(side, sz, px, now)
        except Exception:
            pass

    def _own_resting(self) -> list:
        now = self.now()
        age = getattr(self.cfg, "own_order_min_age_s", 0.05)
        return [
            (o.side, o.price, o.remaining)
            for o in self.om.orders.values()
            if not o.is_taker and o.cancelling_since is None and o.remaining > 0 and (now - o.created) >= age
        ]

    def _on_fill(self, side: str, qty: Decimal, px: Decimal, o: Order) -> None:
        now = self.now()
        mid = self.md.mid or px
        fill = self.ledger.record_fill(side, qty, px, mid, now)
        log.info("FILL %s %s @ %s (mid=%s, edge=%+.2fbps, pos=%s, realized_delta=$%s)",
                 side, fmt(qty), fmt(px), fmt(mid), float(fill.edge_bps),
                 fmt(self.ledger.position), fmt(fill.realized_delta))

        sweep_window = getattr(self.cfg, "sweep_guard_window_s", 1.0)
        self._recent_fills.append((now, side))
        while self._recent_fills and now - self._recent_fills[0][0] > sweep_window:
            self._recent_fills.popleft()

        sweep_limit = getattr(self.cfg, "sweep_guard_max_fills", getattr(self.cfg, "sweep_guard_fills", 2))
        rapid_fills = sum(1 for t, s in self._recent_fills if s == side)
        if rapid_fills >= sweep_limit:
            sweep_cooldown = getattr(self.cfg, "sweep_guard_cooldown_s", getattr(self.cfg, "burst_cooldown_s", 3.0))
            self._burst_blocked_until[side] = now + sweep_cooldown
            log.warning("SWEEP GUARD: %d %s fills in <=%.1fs -> emergency pause %s quotes",
                        rapid_fills, side, sweep_window, side)
            asyncio.create_task(self.om.cancel_side(side, now))

        self._dirty_evt.set()

    def _journal_markout(self, f: Fill, horizon: float, m_bps: float) -> None:
        pass

    def _fp(self, path: str):
        fp = self._files.get(path)
        if fp is None or fp.closed:
            fp = open(path, "a", buffering=1)
            self._files[path] = fp
        return fp

    def _close_files(self) -> None:
        for fp in self._files.values():
            try:
                fp.close()
            except Exception:
                pass
        self._files.clear()

    def _spawn_bg(self, name: str, coro_fn, now: float) -> None:
        t = self._bg_tasks.get(name)
        if t is not None and not t.done():
            return
        self._bg_tasks[name] = asyncio.create_task(coro_fn(now))

    async def tick(self) -> None:
        """Sub-millisecond quoting cycle."""
        async with self._tick_lock:
            now = self.now()
            m = self.md.info
            if not m or not self.md.mid or not self.md.bid or not self.md.ask:
                return

            mid = self.md.mid
            tot_pnl = self.ledger.total_pnl(mid)

            daily_limit = getattr(self.cfg, "daily_loss_limit", getattr(self.cfg, "session_max_loss_usd", Decimal("50.0")))
            if tot_pnl <= -daily_limit:
                log.error("DAILY LOSS LIMIT HIT ($%s <= -$%s) - Pausing quotes", fmt(tot_pnl), fmt(daily_limit))
                await self.om.cancel_all(now=now, force=True, reason="Daily loss limit")
                return

            session_limit = getattr(self.cfg, "session_max_loss", getattr(self.cfg, "session_max_loss_usd", Decimal("100.0")))
            if tot_pnl <= -session_limit:
                log.error("SESSION MAX LOSS BREACHED ($%s <= -$%s) - Halting", fmt(tot_pnl), fmt(session_limit))
                await self.om.cancel_all(now=now, force=True, reason="Session max loss")
                self.stop_evt.set()
                return

            spread_max = getattr(self.cfg, "spread_max_bps", getattr(self.cfg, "max_market_spread_bps", Decimal("30.0")))
            if self.md.spread_bps > spread_max:
                await self.om.cancel_all(now=now, force=True, reason="Spread too wide")
                return

            buy_blocked = (now < self._burst_blocked_until[BUY] or now < self._trend_blocked_until[BUY])
            sell_blocked = (now < self._burst_blocked_until[SELL] or now < self._trend_blocked_until[SELL])

            existing_slots = set(self.om.pair_slots.keys())
            targets = self.engine.generate_ladder_quotes(
                m, self.md, self.ledger, now, buy_blocked, sell_blocked, existing_slots=existing_slots
            )

            blocked_sides = set()
            if buy_blocked:
                blocked_sides.add(BUY)
            if sell_blocked:
                blocked_sides.add(SELL)

            # High-speed quote synchronization
            await self.om.sync_quotes(targets, now, blocked_sides=blocked_sides)

    def _status_log(self, now: float) -> None:
        if now - self._last_status < 10.0:
            return
        self._last_status = now
        mid = self.md.mid or ZERO
        log.info(
            "BULK WS STATUS | mid=%s spr=%.2fbps obi=%.2f vol=%.2fbps | pos=%s unreal=$%.4f realized=$%.4f | orders: %s",
            fmt(mid),
            float(self.md.spread_bps),
            float(self.md.obi),
            float(self.md.vol_bps),
            fmt(self.ledger.position),
            float(self.ledger.unrealized(mid)),
            float(self.ledger.realized),
            self.om.describe(now),
        )

    async def _heartbeat(self, now: float) -> None:
        """WebSocket Dead Man's Switch refresh."""
        if not self.cfg.dms_enabled or self.cfg.dry_run:
            return
        if now - self._last_heartbeat < self.cfg.dms_refresh_s:
            return
        self._last_heartbeat = now
        try:
            deadline = int(time.time() + self.cfg.dms_ttl_s)
            req = self.signer.schedule_cancel(self.md.info, deadline)
            await self.ex.write(req)
        except Exception:
            pass

    async def _reconcile(self, now: float) -> None:
        """Reconcile open orders over WebSocket."""
        if now - self._last_reconcile < 15.0:
            return
        self._last_reconcile = now
        try:
            orders = await self.ex.get_orders(self.cfg.account, self.cfg.market)
            if isinstance(orders, list):
                await self.om.reconcile(orders, now)
        except Exception:
            pass

    async def run(self) -> None:
        log.info("Starting Bulk Trade 100%% WebSocket MM Bot [%s] on %s...", self.cfg.market, self.cfg.env_name)
        
        # Load market metadata
        raw_markets = await self.ex.fetch_markets(self.cfg.market)
        self.md.info = Market.from_api(raw_markets[0])
        self.md.info_ts = self.now()
        log.info("Market initialized: %s tick=%s step=%s mark=$%s (Domain %d)",
                 self.md.info.name, self.md.info.tick, self.md.info.step,
                 self.md.info.mark, self.cfg.domain)

        if self.cfg.cross_exchange_enabled and self._cross_feeds is None:
            self._cross_feeds = CrossFeedManager(self.cfg, self)
            self._cross_feeds.start()

        # Connect WebSocket with TCP_NODELAY and zero compression overhead
        try:
            import websockets
            from websockets.exceptions import ConnectionClosed
        except ImportError:
            class ConnectionClosed(Exception):
                pass
            if not hasattr(self.ex, "ws") or self.ex.ws is None:
                log.info("Websockets module stubbed for dry-run/simulation.")

        reconnect_delay = 1.0
        max_reconnect_delay = 10.0

        while not self.stop_evt.is_set():
            reader_task = None
            try:
                log.info("Connecting to Bulk Trade WebSocket: %s...", self.ex.ws_url)
                # When websockets package is available, connect with optimized socket flags
                if "websockets" in sys.modules and hasattr(sys.modules["websockets"], "connect"):
                    async with sys.modules["websockets"].connect(
                        self.ex.ws_url,
                        ping_interval=10,
                        ping_timeout=15,
                        max_size=2**23,
                        close_timeout=3,
                        compression=None,  # Disabled compression eliminates CPU decompression latency
                    ) as ws:
                        # Apply TCP_NODELAY immediately
                        transport = getattr(ws, "transport", None)
                        if transport:
                            sock = transport.get_extra_info("socket")
                            self.ex.optimize_socket(sock)
                        self.ex.ws = ws
                        reader_task = asyncio.create_task(self.ex.reader())

                        # Subscribe to all market data and account streams
                        await self.ex.subscribe("bbo", self.cfg.market)
                        await self.ex.subscribe("orderbook", self.cfg.market)
                        await self.ex.subscribe("trades", self.cfg.market)
                        await self.ex.subscribe("orders", self.cfg.market, account=self.cfg.account)
                        await self.ex.subscribe("fills", self.cfg.market, account=self.cfg.account)
                        await self.ex.subscribe("positions", self.cfg.market, account=self.cfg.account)

                        reconnect_delay = 1.0
                        log.info("Connected & subscribed to all Bulk Trade WebSocket channels.")

                        while not self.stop_evt.is_set() and self.ex.is_connected:
                            now = self.now()
                            self._spawn_bg("heartbeat", self._heartbeat, now)
                            self._spawn_bg("reconcile", self._reconcile, now)
                            self._status_log(now)

                            await self.tick()

                            try:
                                # Ultra-low latency event wait: wakes immediately upon market data arrival
                                await asyncio.wait_for(self._dirty_evt.wait(), timeout=0.05)
                                self._dirty_evt.clear()
                            except asyncio.TimeoutError:
                                pass

                else:
                    # In test/offline mode without websockets network package:
                    log.info("Simulated WebSocket engine active.")
                    while not self.stop_evt.is_set():
                        now = self.now()
                        self._status_log(now)
                        await self.tick()
                        try:
                            await asyncio.wait_for(self._dirty_evt.wait(), timeout=0.05)
                            self._dirty_evt.clear()
                        except asyncio.TimeoutError:
                            pass

            except Exception as e:
                log.warning("WebSocket connection dropped (%s). Reconnecting in %.1fs...", e, reconnect_delay)
            finally:
                if reader_task and not reader_task.done():
                    reader_task.cancel()
                self.ex.ws = None

            if not self.stop_evt.is_set():
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

        log.info("Stopping Bulk Trade bot - cancelling resting orders...")
        await self.om.cancel_all(now=self.now(), force=True, reason="Bot shutdown")
        if self._cross_feeds:
            await self._cross_feeds.stop()
        self._close_files()
