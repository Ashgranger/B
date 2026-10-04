"""Level 8+ Market Maker Engine for Robinhood Lighter Perp DEX (apidocs.rh.lighter.xyz)."""
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
from typing import Any, Optional, Dict, List

from config import Config
from exchange import LighterExchange
from market import Market, MarketData
from signer import LighterSigner
from ledger import Ledger, Fill
from engine import MarketMakingEngine, QuoteTarget
from orders import OrderManager, Order
from utils import BPS, BUY, SELL, ZERO, ONE, Fatal, fmt, bps_diff

log = logging.getLogger("bot")


class LighterMarketMaker:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.now = time.monotonic
        self.stop_evt = asyncio.Event()

        self.ex = LighterExchange(cfg, self._on_channel)
        self.md = MarketData(cfg)
        self.signer = LighterSigner(
            cfg.signing_key, cfg.address, cfg.account_index,
            api_key_index=cfg.api_key_index, chain_id=cfg.chain_id
        )
        self.ledger = Ledger(cfg)
        self.engine = MarketMakingEngine(cfg)
        self.om = OrderManager(cfg, self.ex, self.signer, self._get_market, self._on_fill)

        self._recent_fills: deque = deque()
        self._burst_blocked_until = {BUY: 0.0, SELL: 0.0}
        self._trend_blocked_until = {BUY: 0.0, SELL: 0.0}

        self._last_heartbeat = 0.0
        self._last_reconcile = 0.0
        self._last_status = 0.0
        self._last_pause_log = {"rth": 0.0, "spread": 0.0, "jump": 0.0, "session_loss": 0.0}
        self._tick_lock = asyncio.Lock()
        self._dirty_evt = asyncio.Event()

    def on_external_venue_bbo(self, venue: str, bid: Decimal, ask: Decimal,
                              bid_sz: Decimal = Decimal("1"), ask_sz: Decimal = Decimal("1")) -> None:
        self.md.update_cross_venue(venue, bid, ask, bid_sz, ask_sz, self.now())
        self._dirty_evt.set()

    def _log_quote_opportunity(self, targets: List[QuoteTarget], now: float) -> None:
        if not getattr(self.cfg, "enable_quote_dataset", False) or not self.cfg.quote_dataset_path:
            return
        if self.cfg.quote_dataset_path == os.devnull:
            return
        try:
            snapshot = self.md.get_microstructure_snapshot(self.md.info, now)
            record = {
                "ts": now,
                "snapshot": snapshot,
                "position": float(self.ledger.position),
                "avg_cost": float(self.ledger.avg_cost),
                "unrealized_pnl": float(self.ledger.unrealized(self.md.mid or Decimal(0))),
                "realized_pnl": float(self.ledger.realized),
                "quotes": [
                    {
                        "pair_index": q.pair_index,
                        "side": q.side,
                        "price": float(q.price),
                        "qty": float(q.qty),
                        "ev_bps": float(q.expected_value_bps),
                        "p_fill": float(q.fill_probability),
                        "is_exit": q.is_exit_quote,
                        "is_taker": getattr(q, "is_taker", False),
                        "is_reduce_only": getattr(q, "is_reduce_only", False)
                    }
                    for q in targets
                ]
            }
            with open(self.cfg.quote_dataset_path, "a") as fp:
                fp.write(json.dumps(record) + chr(10))
        except Exception:
            pass

    async def tick(self) -> None:
        await self._tick()

    def _get_market(self) -> Market:
        if not self.md.info:
            raise Fatal("Market metadata not yet loaded")
        return self.md.info

    def _on_channel(self, channel: str, contents: Any, is_snapshot: bool) -> None:
        now = self.now()
        ch_clean = str(channel).lower()

        # 1. Order Book Stream
        if "order_book" in ch_clean or ch_clean == "bbo":
            if not isinstance(contents, dict):
                return
            bids = contents.get("bids") or []
            asks = contents.get("asks") or []

            bb, ba, bsz, asz = None, None, None, None
            if bids:
                first_bid = bids[0]
                bb = Decimal(str(first_bid[0] if isinstance(first_bid, (list, tuple)) else first_bid.get("price")))
                bsz = Decimal(str(first_bid[1] if isinstance(first_bid, (list, tuple)) else first_bid.get("size", "1")))
            if asks:
                first_ask = asks[0]
                ba = Decimal(str(first_ask[0] if isinstance(first_ask, (list, tuple)) else first_ask.get("price")))
                asz = Decimal(str(first_ask[1] if isinstance(first_ask, (list, tuple)) else first_ask.get("size", "1")))

            if bb is not None and ba is not None and bb < ba:
                self.md.update(bb, ba, bsz, asz, now)
                self.md.on_depth(bids[:15], asks[:15], now)
                self._dirty_evt.set()

        # 2. Public Trade Stream
        elif "trade" in ch_clean:
            trades_list = contents if isinstance(contents, list) else [contents]
            for t in trades_list:
                if not isinstance(t, dict):
                    continue
                px = Decimal(str(t.get("price") or "0"))
                sz = Decimal(str(t.get("size") or t.get("amount") or "0"))
                is_maker_buyer = t.get("is_maker_buyer")
                if is_maker_buyer is not None:
                    side = SELL if is_maker_buyer else BUY
                else:
                    side = str(t.get("side", BUY)).upper()
                if px > 0 and sz > 0:
                    self.md.on_trade(side, sz, px, now)
                    self._dirty_evt.set()

        # 3. Market Stats Stream (funding rate & oracle index price)
        elif "market_stats" in ch_clean:
            if isinstance(contents, dict):
                fr = contents.get("current_funding_rate") or contents.get("funding_rate") or "0"
                idx_px = contents.get("index_price") or contents.get("mark_price") or "0"
                self.md.funding_rate = Decimal(str(fr))
                self.md.index_price = Decimal(str(idx_px))

        # 4. Account Orders Stream
        elif "account_orders" in ch_clean or "account_all_orders" in ch_clean:
            orders_list = contents if isinstance(contents, list) else [contents]
            for o_evt in orders_list:
                if isinstance(o_evt, dict):
                    self.om.on_account_order_event(o_evt, now)

        # 5. Account Positions Stream
        elif "account_all_positions" in ch_clean or "positions" in ch_clean:
            positions_list = contents if isinstance(contents, list) else [contents]
            for pos in positions_list:
                if isinstance(pos, dict):
                    mid = int(pos.get("market_id") or pos.get("marketId") or pos.get("market_index") or 0)
                    if mid == self.cfg.market_id or mid == 0:
                        size_raw = pos.get("position_size") or pos.get("size") or "0"
                        remote_pos = Decimal(str(size_raw))
                        if self.md.mid:
                            min_notional = self.md.info.min_notional if self.md.info else Decimal("1")
                            self.ledger.reconcile(remote_pos, now, self.md.mid, min_notional)

    def _on_fill(self, side: str, qty: Decimal, price: Decimal, o: Order) -> None:
        now = self.now()
        mid = self.md.mid or price
        m = self.md.info
        min_notional = m.min_notional if m else Decimal("1")
        is_maker = not o.is_taker
        regime = self.md.detect_regime(now, self.ledger.tox_bps)

        f = self.ledger.on_fill(side, qty, price, mid, now, min_notional, is_maker=is_maker,
                                regime=regime, level=o.pair_index)

        self._recent_fills.append((now, side, qty))
        while self._recent_fills and now - self._recent_fills[0][0] > self.cfg.burst_window_s:
            self._recent_fills.popleft()

        side_fills = [sz for t, s, sz in self._recent_fills if s == side]
        if len(side_fills) >= self.cfg.burst_fills:
            log.warning("🚨 [BURST GUARD] Triggered on %s side! Halting quotes for %.1fs",
                        side, self.cfg.burst_cooldown_s)
            self._burst_blocked_until[side] = now + self.cfg.burst_cooldown_s

        sweep_fills = [sz for t, s, sz in self._recent_fills if s == side and now - t <= self.cfg.sweep_guard_window_s]
        if len(sweep_fills) >= self.cfg.sweep_guard_fills:
            log.warning("🛡️ [SWEEP GUARD] %d rapid taker fills on %s within %.1fs! Pulling side.",
                        len(sweep_fills), side, self.cfg.sweep_guard_window_s)
            self._burst_blocked_until[side] = now + 4.0

        self._journal_fill(f)

    def _journal_fill(self, f: Fill) -> None:
        row = {
            "ts": f.ts, "side": f.side, "qty": fmt(f.qty), "price": fmt(f.price),
            "mid": fmt(f.mid), "edge_bps": fmt(f.edge_bps), "position": fmt(f.position),
            "realized_delta": fmt(f.realized_delta), "is_maker": f.is_maker,
            "total_realized": fmt(self.ledger.realized), "total_volume": fmt(self.ledger.volume_usd),
        }
        try:
            with open(self.cfg.journal_path, "a") as fp:
                fp.write(json.dumps(row) + "\n")
        except Exception as e:
            log.warning("Journal write error: %s", e)

    async def _tick(self) -> None:
        async with self._tick_lock:
            now = self.now()
            md = self.md
            if not md.bid or not md.ask or not md.mid or not md.info:
                return

            mid = md.mid

            # 1. Process multi-horizon Bayesian markouts
            self.ledger.process_markouts(mid, now)

            # 2. Check Session Max Loss circuit breaker
            tot_pnl = self.ledger.total_pnl(mid)
            if self.cfg.session_max_loss_usd > 0 and tot_pnl < -self.cfg.session_max_loss_usd:
                if now - self._last_pause_log["session_loss"] > 15.0:
                    log.error("🛑 Session loss limit hit: %s USD < -%s USD! HALTING BOT.",
                              fmt(tot_pnl), fmt(self.cfg.session_max_loss_usd))
                    self._last_pause_log["session_loss"] = now
                await self.om.cancel_all(now)
                return

            # 3. Check Volatility Pause & Price Jump Cooldown
            if md.jump_active(now):
                await self.om.cancel_all(now)
                return

            if md.vol_bps > self.cfg.vol_pause_bps:
                await self.om.cancel_all(now)
                return

            # 4. Check Market Spread Anomaly
            if md.spread_bps > self.cfg.max_market_spread_bps:
                await self.om.cancel_all(now)
                return

            # 5. Check Blocked Sides
            buy_blocked = (now < self._burst_blocked_until[BUY]) or (now < self._trend_blocked_until[BUY])
            sell_blocked = (now < self._burst_blocked_until[SELL]) or (now < self._trend_blocked_until[SELL])

            existing_slots = set(self.om.pair_slots.keys())

            # 6. Generate Level 8+ Multi-Ladder Quotes with Positive Unrealized PnL Management
            targets = self.engine.generate_ladder_quotes(
                m=md.info, md=md, ledger=self.ledger, now=now,
                buy_blocked=buy_blocked, sell_blocked=sell_blocked,
                existing_slots=existing_slots
            )

            self._log_quote_opportunity(targets, now)
            target_map: Dict[tuple, QuoteTarget] = {(t.pair_index, t.side): t for t in targets}

            # 7. Cancel removed or obsolete ladder slots
            for (p_idx, side), oid in list(self.om.pair_slots.items()):
                if (p_idx, side) not in target_map:
                    o = self.om.orders.get(oid)
                    if o:
                        await self.om.cancel(o, now)

            # 8. Requote or Place Active Targets
            for (p_idx, side), target in target_map.items():
                existing = self.om.get_order_by_slot(p_idx, side)

                if target.is_taker:
                    if existing:
                        await self.om.cancel(existing, now)
                    await self.om.place(
                        pair_index=p_idx, side=side, px=target.price, qty=target.qty,
                        now=now, time_in_force="IOC", reduce_only=True, quote_mid=mid
                    )
                    continue

                if existing is None:
                    await self.om.place(
                        pair_index=p_idx, side=side, px=target.price, qty=target.qty,
                        now=now, time_in_force="ALO", reduce_only=target.is_exit_quote, quote_mid=mid
                    )
                else:
                    drift = bps_diff(target.price, existing.price)
                    is_retreat = (drift < 0) if side == BUY else (drift > 0)
                    is_advance = (drift > 0) if side == BUY else (drift < 0)

                    time_since_action = now - existing.last_action

                    should_requote = False
                    if is_retreat and abs(drift) >= self.cfg.retreat_bps:
                        should_requote = True
                    elif is_advance and abs(drift) >= self.cfg.requote_bps and time_since_action >= self.cfg.min_requote_s:
                        should_requote = True
                    elif time_since_action >= self.cfg.stale_s:
                        should_requote = True

                    if should_requote:
                        await self.om.cancel(existing, now)
                        await self.om.place(
                            pair_index=p_idx, side=side, px=target.price, qty=target.qty,
                            now=now, time_in_force="ALO", reduce_only=target.is_exit_quote, quote_mid=mid
                        )

    async def _status_loop(self) -> None:
        while not self.stop_evt.is_set():
            await asyncio.sleep(self.cfg.status_s)
            mid = self.md.mid or Decimal("0")
            unreal = self.ledger.unrealized(mid)
            tot = self.ledger.total_pnl(mid)
            u_bps = self.ledger.unrealized_bps(mid)
            regime = self.md.detect_regime(self.now(), self.ledger.tox_bps)

            log.info(
                "📊 [ROBINHOOD LIGHTER MM] Mid: %s | Pos: %s USD (%s) | Unreal: %s USD (%s bps) | "
                "Realized: %s USD | Fees: %s USD | SpreadCap: %s USD | Fills: %d | Regime: %s | Orders: %s",
                fmt(mid), fmt(self.ledger.position * mid), fmt(self.ledger.position),
                fmt(unreal), fmt(u_bps), fmt(self.ledger.realized), fmt(self.ledger.fees),
                fmt(self.ledger.spread_capture), self.ledger.n_fills, regime,
                self.om.describe(self.now())
            )

    async def run(self) -> None:
        log.info("🚀 Starting Robinhood Lighter Perp DEX Level 8+ Market Maker...")
        log.info("⚙️ Network: %s | Chain ID: %d | API Key Index: %d | Maker: %.3f%% (%.1fbps) | Taker: %.3f%% (%.1fbps)",
                 self.cfg.env_name, self.cfg.chain_id, self.cfg.api_key_index,
                 float(self.cfg.maker_fee_bps)/100.0, self.cfg.maker_fee_bps,
                 float(self.cfg.taker_fee_bps)/100.0, self.cfg.taker_fee_bps)

        market_details = await self.ex.fetch_market_details(self.cfg.market_id)
        self.md.info = Market.from_api(market_details)
        log.info("✅ Market initialized: %s (tick=%s, step=%s)",
                 self.md.info.name, self.md.info.tick_size, self.md.info.step_size)

        nonce = await self.ex.fetch_next_nonce(self.cfg.account_index, self.cfg.api_key_index)
        self.signer.set_nonce(nonce)
        log.info("🔢 Synchronized sequencer starting nonce: %d", nonce)

        asyncio.create_task(self._status_loop())

        while not self.stop_evt.is_set():
            try:
                try:
                    await asyncio.wait_for(self._dirty_evt.wait(), timeout=self.cfg.loop_s)
                    self._dirty_evt.clear()
                except asyncio.TimeoutError:
                    pass

                await self._tick()
            except Exception as e:
                log.exception("Error in main MM loop: %s", e)
                await asyncio.sleep(1.0)
