"""Order management for Bulk Trade: Atomic batching, pre-computed IDs, and ALO quoting."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Set

from exchange import Exchange
from market import Market
from signer import Signer
from utils import (
    BUY,
    SELL,
    ZERO,
    ONE,
    bps_diff,
    fmt,
    q_down,
    q_up,
)

log = logging.getLogger("orders")


@dataclass
class Order:
    order_id: str
    pair_index: int
    side: str
    price: Decimal
    qty: Decimal
    remaining: Decimal
    created: float
    last_action: float
    filled_any: bool = False
    cancelling_since: Optional[float] = None
    is_taker: bool = False
    is_reduce_only: bool = False
    ev_bps: Decimal = Decimal(0)
    quote_mid: Optional[Decimal] = None
    est_px: Optional[Decimal] = None
    client_order_id: Optional[str] = None


class OrderManager:
    """Manages local order lifecycle and atomic Bulk Trade mutations."""

    def __init__(
        self,
        cfg,
        ex: Exchange,
        signer: Signer,
        get_market: Callable[[], Market],
        on_fill: Callable[[str, Decimal, Decimal, Order], None],
    ):
        self.cfg = cfg
        self.ex = ex
        self.signer = signer
        self.get_market = get_market
        self.on_fill = on_fill

        self.orders: Dict[str, Order] = {}
        self.pair_slots: Dict[Tuple[int, str], str] = {}  # (pair_index, side) -> order_id
        self._last_taker: Dict[str, float] = {}
        self._unmatched: Dict[str, tuple] = {}
        self.reject_until = {BUY: 0.0, SELL: 0.0}
        self._reject_n = {BUY: 0, SELL: 0}
        self._actions: deque = deque()
        self.paused_until = 0.0
        self._recently_closed: deque = deque(maxlen=200)
        self._consec_errors = 0
        self.last_place_ts = 0.0
        self.maybe_orders = True

        self.n_place = 0
        self.n_modify = 0
        self.n_cancel = 0
        self.n_reject = 0
        self.n_actions = 0

    def side_orders(self, side: str) -> List[Order]:
        return sorted(
            (o for o in self.orders.values() if o.side == side),
            key=lambda o: o.price,
            reverse=(side == BUY),
        )

    def get_order_by_slot(self, pair_index: int, side: str) -> Optional[Order]:
        oid = self.pair_slots.get((pair_index, side))
        return self.orders.get(oid) if oid else None

    def open_qty(self, side: str) -> Decimal:
        return sum((o.remaining for o in self.orders.values() if o.side == side), Decimal(0))

    def describe(self, now: float) -> str:
        if not self.orders:
            return "none"
        desc = []
        for (pair_idx, side), oid in sorted(self.pair_slots.items()):
            o = self.orders.get(oid)
            if o:
                desc.append(f"L{pair_idx}{'B' if side == BUY else 'S'} {fmt(o.remaining)}@{fmt(o.price)}")
        return " | ".join(desc) if desc else "none"

    def _budget(self, now: float) -> bool:
        max_rate = getattr(self.cfg, "max_actions_per_min", 240)
        while self._actions and now - self._actions[0] > 60:
            self._actions.popleft()
        if len(self._actions) >= max_rate:
            return False
        self._actions.append(now)
        self.n_actions += 1
        return True

    @staticmethod
    def _ok(resp: dict) -> bool:
        return resp.get("status") in (200, 202) and "error" not in resp

    def _error(self, what: str, resp: dict, now: float) -> None:
        err = resp.get("error")
        log.warning("%s failed: status=%s %s", what, resp.get("status"), json.dumps(err)[:200])
        self._consec_errors += 1
        retry = err.get("retryAfterMs") if isinstance(err, dict) else None
        if resp.get("status") == 429 and retry:
            self.paused_until = now + float(retry) / 1000 + 0.1
        if self._consec_errors >= 8:
            log.error("Too many consecutive errors - pausing 30s")
            self.paused_until = now + 30
            self._consec_errors = 0

    def _backoff(self, side: str, now: float) -> None:
        self._reject_n[side] += 1
        self.reject_until[side] = now + min(0.25 * 2 ** (self._reject_n[side] - 1), 4.0)

    async def place(
        self,
        pair_index: int,
        side: str,
        px: Decimal,
        qty: Decimal,
        now: float,
        time_in_force: str = "ALO",
        reduce_only: bool = False,
        quote_mid: Optional[Decimal] = None,
        est_px: Optional[Decimal] = None,
    ) -> Optional[Order]:
        """Place single order on Bulk Trade."""
        if now < self.paused_until or not self._budget(now):
            return None
        m = self.get_market()
        tick = m.tick_for(px)
        is_taker = (time_in_force == "IOC")
        if is_taker:
            px = q_up(px, tick) if side == BUY else q_down(px, tick)
        else:
            px = q_down(px, tick) if side == BUY else q_up(px, tick)
        qty = q_down(qty, m.step)
        if qty < m.min_size or (m.min_notional > 0 and px * qty < m.min_notional):
            return None

        act = self.signer.build_limit_action(
            m.name, side, px, qty, time_in_force=time_in_force, reduce_only=reduce_only
        )
        envelope, oids = self.signer.sign_transaction([act])
        pre_oid = oids[0]

        req = {"type": "order", "payload": envelope}
        self.maybe_orders = True
        self.last_place_ts = now
        resp = await self.ex.write(req)
        res = resp.get("result") or {}

        if not self._ok(resp) or str(res.get("status")).upper() == "REJECTED":
            self._error(f"place L{pair_index} {side}", resp, now)
            if not is_taker:
                self._backoff(side, now)
            return None

        self._consec_errors = 0
        self.n_place += 1
        oid = str(res.get("orderId") or pre_oid)
        o = Order(
            oid, pair_index, side, px, qty, qty, now, now,
            is_taker=is_taker, is_reduce_only=reduce_only, quote_mid=quote_mid, est_px=est_px
        )
        self.orders[oid] = o
        if not is_taker:
            self.pair_slots[(pair_index, side)] = oid
        log.info("PLACE L%d %s %s @ %s (id=%s, taker=%s)", pair_index, side, fmt(qty), fmt(px), oid[:8], is_taker)
        
        early = self._unmatched.pop(oid, None)
        if early:
            self._apply(o, early[1], now)
        return o

    async def place_taker(
        self, side: str, px: Decimal, qty: Decimal, now: float, reduce_only: bool = True
    ) -> Optional[Order]:
        """Execute an immediate IOC taker order (unwind or liquidation cut)."""
        return await self.place(
            pair_index=-1,
            side=side,
            px=px,
            qty=qty,
            now=now,
            time_in_force="IOC",
            reduce_only=reduce_only,
        )

    async def cancel(self, o: Order, now: float) -> None:
        """Cancel a resting order on Bulk Trade."""
        if o.cancelling_since is not None and now - o.cancelling_since < 5:
            return
        o.cancelling_since = now
        self._recently_closed.append((now, o.order_id))
        self._budget(now)

        m = self.get_market()
        act = self.signer.build_cancel_action(o.order_id, m.name)
        envelope, _ = self.signer.sign_transaction([act])
        req = {"type": "cancel", "payload": envelope}
        resp = await self.ex.write(req)

        if self.cfg.dry_run or "ORDER_NOT_FOUND" in json.dumps(resp):
            self._remove_order(o.order_id)
        elif not self._ok(resp):
            o.cancelling_since = None
            self._error(f"cancel L{o.pair_index} {o.side}", resp, now)
        else:
            self.n_cancel += 1

    async def cancel_side(self, side: str, now: float) -> None:
        """Cancel all orders on a given side."""
        for o in self.side_orders(side):
            await self.cancel(o, now)

    async def cancel_all(self, now: float = 0.0, force: bool = False, reason: str = "") -> None:
        """Cancel all resting orders on Bulk Trade."""
        if not self.orders:
            return
        m = self.get_market()
        log.info("CANCEL ALL: %s (%d active orders)", reason, len(self.orders))
        act = self.signer.build_cancel_all_action(m.name)
        envelope, _ = self.signer.sign_transaction([act])
        req = {"type": "cancelAll", "payload": envelope}
        await self.ex.write(req)
        for oid in list(self.orders.keys()):
            self._remove_order(oid)
        self.n_cancel += 1

    async def modify(
        self, o: Order, px: Decimal, now: float, urgent: bool = False, reduce_only: Optional[bool] = None
    ) -> bool:
        """Modify/replace an existing order."""
        if now < self.paused_until or not self._budget(now):
            if urgent:
                await self.cancel(o, now)
            return False
        
        # In Bulk Trade, we can perform an atomic cancel + place
        m = self.get_market()
        tick = m.tick_for(px)
        px = q_down(px, tick) if o.side == BUY else q_up(px, tick)
        r_only = o.is_reduce_only if reduce_only is None else reduce_only

        if self.cfg.batch_replace_enabled:
            replaced = await self.batch_replace([o], [(o.pair_index, o.side, px, o.remaining, r_only)], now)
            return len(replaced) > 0
        else:
            await self.cancel(o, now)
            new_o = await self.place(o.pair_index, o.side, px, o.remaining, now, "ALO", r_only)
            return new_o is not None

    async def batch_replace(
        self,
        cancels: List[Order],
        places: List[Tuple[int, str, Decimal, Decimal, bool]],  # (pair_idx, side, px, qty, reduce_only)
        now: float,
    ) -> List[Order]:
        """
        Execute atomic Bulk Trade batch transaction: bundle cancels and places together.
        Sign once, transmit once, executed in a single atomic match step!
        """
        if not cancels and not places:
            return []
        if now < self.paused_until or not self._budget(now):
            return []

        m = self.get_market()
        actions = []
        # Add cancel actions first
        for c in cancels:
            c.cancelling_since = now
            actions.append(self.signer.build_cancel_action(c.order_id, m.name))

        # Add place actions
        place_meta = []
        for pair_idx, side, px, qty, r_only in places:
            tick = m.tick_for(px)
            px = q_down(px, tick) if side == BUY else q_up(px, tick)
            qty = q_down(qty, m.step)
            if qty >= m.min_size:
                act = self.signer.build_limit_action(m.name, side, px, qty, "ALO", r_only)
                actions.append(act)
                place_meta.append((pair_idx, side, px, qty, r_only))

        if not actions:
            return []

        envelope, oids = self.signer.sign_transaction(actions)
        req = {"type": "batch_order", "payload": envelope}
        resp = await self.ex.write(req)

        if not self._ok(resp):
            self._error("batch_replace", resp, now)
            return []

        # Cleanup cancels locally
        for c in cancels:
            self._remove_order(c.order_id)
            self.n_cancel += 1

        # Register placed orders
        placed_orders = []
        place_oids = [oid for oid in oids if oid]
        for (pair_idx, side, px, qty, r_only), oid in zip(place_meta, place_oids):
            o = Order(
                order_id=oid,
                pair_index=pair_idx,
                side=side,
                price=px,
                qty=qty,
                remaining=qty,
                created=now,
                last_action=now,
                is_taker=False,
                is_reduce_only=r_only,
            )
            self.orders[oid] = o
            self.pair_slots[(pair_idx, side)] = oid
            placed_orders.append(o)
            self.n_place += 1
            log.info("BATCH PLACE L%d %s %s @ %s (id=%s)", pair_idx, side, fmt(qty), fmt(px), oid[:8])

        return placed_orders

    async def sync_quotes(self, targets: list, now: float, blocked_sides: Optional[Set[str]] = None) -> None:
        """Synchronize active market maker quotes with generated ladder targets."""
        active_slots = set()
        groups: Dict[Tuple[int, str], list] = {}
        for t in targets:
            slot = (t.pair_index, t.side)
            groups.setdefault(slot, []).append(t)
            if not getattr(t, "is_taker", False):
                active_slots.add(slot)

        coros = []
        for slot, ts in groups.items():
            for t in ts:
                coros.append(self._sync_one(t, now))

        for slot, oid in list(self.pair_slots.items()):
            if slot not in active_slots:
                o = self.orders.get(oid)
                if o:
                    coros.append(self.cancel(o, now))

        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    async def _sync_one(self, t, now: float) -> None:
        slot = (t.pair_index, t.side)
        existing = self.get_order_by_slot(t.pair_index, t.side)

        if getattr(t, "is_taker", False):
            last = self._last_taker.get(t.side)
            if last is not None and now - last < 1.5:
                return
            self._last_taker[t.side] = now
            if existing:
                await self.cancel(existing, now)
                self.pair_slots.pop(slot, None)
            await self.place(
                t.pair_index, t.side, t.price, t.qty, now,
                time_in_force="IOC", reduce_only=True,
                quote_mid=getattr(t, "quote_mid", None),
                est_px=getattr(t, "est_px", None),
            )
            return

        is_exit = bool(getattr(t, "is_exit_quote", False))
        if existing is None:
            if now >= self.reject_until[t.side]:
                o_new = await self.place(
                    t.pair_index, t.side, t.price, t.qty, now,
                    time_in_force="ALO", reduce_only=is_exit,
                    quote_mid=getattr(t, "quote_mid", None),
                )
                if o_new:
                    o_new.ev_bps = getattr(t, "expected_value_bps", Decimal(0))
            return

        drift = abs(bps_diff(t.price, existing.price))
        is_advancing = (t.side == BUY and t.price > existing.price) or (t.side == SELL and t.price < existing.price)
        is_retreating = not is_advancing

        should_modify = False
        urgent = False

        if getattr(existing, "is_reduce_only", False) != is_exit:
            should_modify = True
            urgent = True

        m = self.get_market()
        tick_bps = (m.tick / existing.price) * Decimal("10000") if existing.price > 0 else Decimal("0.1")
        eff_retreat = min(self.cfg.spread_min_bps, tick_bps * Decimal("0.9"))
        eff_requote = min(self.cfg.spread_base_bps, tick_bps * Decimal("0.9"))

        if is_retreating and (drift >= eff_retreat or abs(t.price - existing.price) >= m.tick):
            should_modify = True
            urgent = True
        elif is_advancing and (drift >= eff_requote or abs(t.price - existing.price) >= m.tick):
            should_modify = True

        if should_modify:
            if await self.modify(existing, t.price, now, urgent=urgent, reduce_only=is_exit):
                existing.ev_bps = getattr(t, "expected_value_bps", Decimal(0))
                existing.quote_mid = getattr(t, "quote_mid", None)

    def on_update(self, c: dict, now: float) -> None:
        """Handle incoming order update from exchange stream."""
        if not isinstance(c, dict) or not (c.get("orderId") or c.get("id")):
            return
        oid = str(c.get("orderId") or c.get("id"))
        o = self.orders.get(oid)
        if o is None:
            self._unmatched[oid] = (now, c)
            if len(self._unmatched) > 200:
                self._unmatched = {k: v for k, v in self._unmatched.items() if v[0] > now - 30}
            return
        self._apply(o, c, now)

    def _apply(self, o: Order, c: dict, now: float) -> None:
        state = str(c.get("state") or c.get("status") or "").upper()
        filled = (state == "FILLED")
        px = o.price
        try:
            if c.get("price") or c.get("px"):
                px = Decimal(str(c.get("price") or c.get("px")))
            for k in ("avgPrice", "averagePrice", "avgFillPrice", "lastFillPrice", "fillPrice"):
                v = c.get(k)
                if v and Decimal(str(v)) > 0:
                    px = Decimal(str(v))
                    break
        except Exception:
            pass

        fill_qty = Decimal(0)
        rem = c.get("remainingSize") or c.get("remaining_size")
        if rem is not None:
            try:
                new_rem = Decimal(str(rem))
                if new_rem < o.remaining:
                    fill_qty = o.remaining - new_rem
                o.remaining = new_rem
            except Exception:
                pass
        elif filled:
            fill_qty = o.remaining
            o.remaining = Decimal(0)

        if fill_qty > 0:
            o.filled_any = True
            self.on_fill(o.side, fill_qty, px, o)

        if state in ("FILLED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED"):
            self._remove_order(o.order_id)

    async def reconcile(self, rows: list, now: float) -> None:
        """Reconcile local order cache with exchange open order snapshot."""
        m = self.get_market()
        open_ids = {str(r.get("orderId") or r.get("id")) for r in rows if isinstance(r, dict)}
        for o in list(self.orders.values()):
            if o.order_id not in open_ids and now - o.last_action > 5 and o.cancelling_since is None:
                log.warning("Dropping ghost L%d %s order %s", o.pair_index, o.side, o.order_id)
                self._remove_order(o.order_id)
        
        mine = set(self.orders)
        if now - self.last_place_ts < 3:
            return
        recent_closed = {oid for ts, oid in self._recently_closed if now - ts < 15.0}
        reqs = []
        for oid in (open_ids - mine - recent_closed):
            if oid and oid != "None":
                self._recently_closed.append((now, oid))
                log.warning("Cancelling orphan order %s for market %s", oid, m.name)
                act = self.signer.build_cancel_action(oid, m.name)
                envelope, _ = self.signer.sign_transaction([act])
                reqs.append(self.ex.write({"type": "cancel", "payload": envelope}))
        if reqs:
            await asyncio.gather(*reqs, return_exceptions=True)

    def _remove_order(self, order_id: str) -> None:
        o = self.orders.pop(order_id, None)
        if o:
            self._recently_closed.append((time.time(), order_id))
            slot = (o.pair_index, o.side)
            if self.pair_slots.get(slot) == order_id:
                self.pair_slots.pop(slot, None)
