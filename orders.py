"""Order Management System for Bulk Trade Perpetual DEX with Multi-Ladder Slots."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from config import Config
from exchange import BulkExchange
from market import Market
from signer import BulkSigner
from utils import BUY, SELL, BPS, ZERO, ONE, bps_diff, clamp, fmt, q_down, q_up

log = logging.getLogger("bulk_orders")


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
    ev_bps: Decimal = Decimal("0")
    quote_mid: Optional[Decimal] = None


class OrderManager:
    """Manages active resting and taker orders on Bulk Trade Perpetual DEX."""

    def __init__(
        self,
        cfg: Config,
        ex: BulkExchange,
        signer: BulkSigner,
        get_market: Callable[[], Market],
        on_fill_cb: Callable[[str, Decimal, Decimal, Order], None],
    ):
        self.cfg = cfg
        self.ex = ex
        self.signer = signer
        self.get_market = get_market
        self.on_fill_cb = on_fill_cb

        self.orders: Dict[str, Order] = {}
        self.pair_slots: Dict[Tuple[int, str], str] = {}
        self._unmatched_fills: Dict[str, Tuple[Decimal, Decimal]] = {}
        self.reject_until: Dict[str, float] = {BUY: 0.0, SELL: 0.0}
        self._reject_n: Dict[str, int] = {BUY: 0, SELL: 0}
        self._actions: deque = deque()
        self.paused_until: float = 0.0
        self._consec_errors: int = 0
        self.last_place_ts: float = 0.0
        self.maybe_orders: bool = True

        # Diagnostics counters
        self.n_place: int = 0
        self.n_modify: int = 0
        self.n_cancel: int = 0
        self.n_reject: int = 0
        self.n_actions: int = 0

    def side_orders(self, side: str) -> List[Order]:
        return [o for o in self.orders.values() if o.side == side and o.remaining > ZERO]

    def get_order_by_slot(self, pair_index: int, side: str) -> Optional[Order]:
        oid = self.pair_slots.get((pair_index, side))
        return self.orders.get(oid) if oid else None

    def describe(self, now: float) -> str:
        if not self.orders:
            return "none"
        desc = []
        for (pair_idx, side), oid in sorted(self.pair_slots.items()):
            o = self.orders.get(oid)
            if o and o.remaining > ZERO:
                tag = f"L{pair_idx}{'B' if side == BUY else 'S'}"
                desc.append(f"{tag} {fmt(o.remaining)}@{fmt(o.price)}")
        return " | ".join(desc) if desc else "none"

    def _budget(self, now: float) -> bool:
        while self._actions and now - self._actions[0] > 60.0:
            self._actions.popleft()
        if len(self._actions) >= self.cfg.max_actions_per_min:
            return False
        self._actions.append(now)
        self.n_actions += 1
        return True

    def _backoff(self, side: str, now: float) -> None:
        self._reject_n[side] += 1
        self.reject_until[side] = now + min(0.25 * (2 ** (self._reject_n[side] - 1)), 4.0)

    async def place(
        self,
        pair_index: int,
        side: str,
        px: Decimal,
        qty: Decimal,
        now: float,
        time_in_force: str = "ALO",
        reduce_only: bool = False,
        quote_mid: Optional[Decimal] = None
    ) -> Optional[Order]:
        if now < self.paused_until or not self._budget(now):
            return None

        m = self.get_market()
        is_buy = (side == BUY)
        order_type = "market" if time_in_force == "IOC" and px <= ZERO else "limit"

        signed = self.signer.sign_order(
            symbol=m.name,
            is_buy=is_buy,
            price=px,
            size=qty,
            order_type=order_type,
            tif=time_in_force,
            reduce_only=reduce_only,
        )
        self.maybe_orders = True
        self.last_place_ts = now

        resp = await self.ex.send_transaction(signed)
        res = resp.get("result") or {}
        status_ok = resp.get("status") in (200, 202) and "error" not in resp

        if not status_ok or str(res.get("status")).upper() == "REJECTED":
            log.warning("Place failed: status=%s res=%s", resp.get("status"), str(res)[:200])
            self._backoff(side, now)
            self._consec_errors += 1
            if resp.get("status") == 429:
                self.paused_until = now + 5.0
            return None

        self._consec_errors = 0
        self._reject_n[side] = 0
        self.n_place += 1

        oid = str(res.get("order_id") or signed.get("order_id") or f"bulk-{int(now*1000)}")
        o = Order(
            order_id=oid,
            pair_index=pair_index,
            side=side,
            price=px,
            qty=qty,
            remaining=qty,
            created=now,
            last_action=now,
            is_taker=(time_in_force == "IOC"),
            is_reduce_only=reduce_only,
            quote_mid=quote_mid,
        )
        self.orders[oid] = o
        self.pair_slots[(pair_index, side)] = oid
        log.info("🎯 [BULK PLACE] L%d %s %s @ %s (id=%s tif=%s)", pair_index, side, fmt(qty), fmt(px), oid[:12], time_in_force)

        # Process any fill that arrived before placement response
        early = self._unmatched_fills.pop(oid, None)
        if early:
            self._apply_fill(o, early[0], early[1], now)
        return o

    async def modify(
        self,
        existing: Order,
        new_px: Decimal,
        now: float,
        urgent: bool = False,
        reduce_only: bool = False
    ) -> bool:
        """Atomic modify: executes cancel old order + place new order in a single signed batch transaction."""
        if not urgent and (now < self.paused_until or not self._budget(now)):
            return False

        m = self.get_market()
        cancel_action = {
            "type": "cancel",
            "symbol": m.name,
            "order_id": existing.order_id,
        }
        place_action = {
            "type": "order",
            "symbol": m.name,
            "is_buy": (existing.side == BUY),
            "price": float(new_px),
            "size": float(existing.remaining),
            "order_type": {"type": "limit", "tif": "ALO"},
            "reduce_only": reduce_only,
        }

        signed = self.signer.sign_group([cancel_action, place_action])
        resp = await self.ex.send_transaction(signed)
        res = resp.get("result") or {}
        status_ok = resp.get("status") in (200, 202) and "error" not in resp

        if not status_ok:
            log.warning("Atomic modify failed: status=%s res=%s", resp.get("status"), str(res)[:200])
            self._consec_errors += 1
            return False

        self._consec_errors = 0
        self.n_modify += 1

        old_oid = existing.order_id
        new_oid = None
        if isinstance(res.get("order_ids"), list) and len(res["order_ids"]) > 1:
            new_oid = str(res["order_ids"][1])
        elif signed.get("order_ids") and len(signed["order_ids"]) > 1:
            new_oid = str(signed["order_ids"][1])
        else:
            new_oid = f"bulk-mod-{int(now*1000)}"

        # Re-key order in internal tables
        self.orders.pop(old_oid, None)
        existing.order_id = new_oid
        existing.price = new_px
        existing.last_action = now
        existing.is_reduce_only = reduce_only
        self.orders[new_oid] = existing
        self.pair_slots[(existing.pair_index, existing.side)] = new_oid

        log.info("✏️ [BULK ATOMIC MODIFY] L%d %s to %s (id=%s -> %s)",
                 existing.pair_index, existing.side, fmt(new_px), old_oid[:8], new_oid[:8])
        return True

    async def cancel(self, o: Order, now: float) -> None:
        if o.cancelling_since is not None and now - o.cancelling_since < 5.0:
            return
        o.cancelling_since = now

        m = self.get_market()
        signed = self.signer.sign_cancel(symbol=m.name, order_id=o.order_id)
        resp = await self.ex.send_transaction(signed)

        self.n_cancel += 1
        self.orders.pop(o.order_id, None)
        self.pair_slots.pop((o.pair_index, o.side), None)
        log.info("❌ [BULK CANCEL] L%d %s @ %s (id=%s)", o.pair_index, o.side, fmt(o.price), o.order_id[:12])

    async def cancel_all(self, force: bool = False) -> None:
        if not force and not self.orders and not self.maybe_orders:
            return
        now = time.time()
        for o in list(self.orders.values()):
            await self.cancel(o, now)

        if (force or self.maybe_orders) and not self.cfg.dry_run:
            try:
                m = self.get_market()
                signed = self.signer.sign_cancel_all(symbols=[m.name])
                await self.ex.send_transaction(signed)
            except Exception as e:
                log.warning("Cancel all broadcast error: %s", e)

        self.orders.clear()
        self.pair_slots.clear()
        self.maybe_orders = False

    async def sync_quotes(self, targets: list, now: float, blocked_sides: Optional[Set[str]] = None) -> None:
        """Reconcile resting and taker order slots with engine targets."""
        active_slots = set()
        m = self.get_market()

        for t in targets:
            slot = (t.pair_index, t.side)
            existing = self.get_order_by_slot(t.pair_index, t.side)

            # Taker aggressive execution (e.g. emergency unwind or locked take profit)
            if getattr(t, "is_taker", False):
                if existing:
                    await self.cancel(existing, now)
                    self.pair_slots.pop(slot, None)
                await self.place(
                    pair_index=t.pair_index,
                    side=t.side,
                    px=t.price,
                    qty=t.qty,
                    now=now,
                    time_in_force="IOC",
                    reduce_only=True,
                    quote_mid=getattr(t, "quote_mid", None),
                )
                continue

            active_slots.add(slot)
            is_exit = bool(getattr(t, "is_exit_quote", False))

            if existing is None:
                if now >= self.reject_until[t.side]:
                    o_new = await self.place(
                        pair_index=t.pair_index,
                        side=t.side,
                        px=t.price,
                        qty=t.qty,
                        now=now,
                        time_in_force="ALO",
                        reduce_only=is_exit,
                        quote_mid=getattr(t, "quote_mid", None),
                    )
                    if o_new:
                        o_new.ev_bps = getattr(t, "expected_value_bps", Decimal("0"))
            else:
                drift = abs(bps_diff(t.price, existing.price))
                is_advancing = (t.side == BUY and t.price > existing.price) or (t.side == SELL and t.price < existing.price)
                is_retreating = not is_advancing

                should_modify = False
                urgent = False

                if getattr(existing, "is_reduce_only", False) != is_exit:
                    should_modify = True
                    urgent = True

                tick_bps = (m.tick / existing.price * BPS) if existing.price > ZERO else Decimal("0.1")
                eff_retreat = min(self.cfg.retreat_bps, tick_bps * Decimal("0.9"))
                eff_requote = min(self.cfg.requote_bps, tick_bps * Decimal("0.9"))

                if is_retreating and (drift >= eff_retreat or abs(t.price - existing.price) >= m.tick):
                    should_modify = True
                    urgent = True
                elif is_advancing and (drift >= eff_requote or abs(t.price - existing.price) >= m.tick):
                    min_cooldown = self.cfg.touch_min_requote_s if existing.pair_index == 0 else self.cfg.min_requote_s
                    if now - existing.last_action >= min_cooldown:
                        queue_cost = getattr(self.cfg, "queue_reset_cost_bps", Decimal("0.20"))
                        ev_gain = getattr(t, "expected_value_bps", Decimal("0")) - getattr(existing, "ev_bps", Decimal("0"))
                        if ev_gain >= queue_cost or drift >= (self.cfg.requote_bps * Decimal("1.5")):
                            should_modify = True

                if should_modify:
                    if await self.modify(existing, t.price, now, urgent=urgent, reduce_only=is_exit):
                        existing.ev_bps = getattr(t, "expected_value_bps", Decimal("0"))
                        existing.quote_mid = getattr(t, "quote_mid", None)

        # Cancel any resting slots no longer desired
        for slot, oid in list(self.pair_slots.items()):
            if slot not in active_slots:
                o = self.orders.get(oid)
                if o:
                    await self.cancel(o, now)

    def on_fill(self, order_id: str, fill_qty: Decimal, fill_price: Decimal, now: float) -> None:
        """Handle incoming fill execution event."""
        o = self.orders.get(order_id)
        if o:
            self._apply_fill(o, fill_qty, fill_price, now)
        else:
            self._unmatched_fills[order_id] = (fill_qty, fill_price)

    def _apply_fill(self, o: Order, fill_qty: Decimal, fill_price: Decimal, now: float) -> None:
        o.filled_any = True
        o.remaining = max(ZERO, o.remaining - fill_qty)
        self.on_fill_cb(o.side, fill_qty, fill_price, o)
        if o.remaining <= ZERO:
            self.orders.pop(o.order_id, None)
            self.pair_slots.pop((o.pair_index, o.side), None)
