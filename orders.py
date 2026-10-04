"""Order book of our own quotes with Multi-Pair Individual Order Management on Robinhood Lighter DEX."""
from __future__ import annotations

import itertools
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional, Dict, Tuple, List, Set

from exchange import LighterExchange
from market import Market
from signer import LighterSigner
from utils import BUY, SELL, fmt, bps_diff, q_down, q_up

log = logging.getLogger("orders")


@dataclass
class Order:
    order_id: str
    client_order_id: int
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


class OrderManager:
    def __init__(self, cfg, ex: LighterExchange, signer: LighterSigner,
                 get_market: Callable[[], Market],
                 on_fill: Callable[[str, Decimal, Decimal, Order], None]):
        self.cfg = cfg
        self.ex = ex
        self.signer = signer
        self.get_market = get_market
        self.on_fill = on_fill

        self.orders: Dict[str, Order] = {}
        self.pair_slots: Dict[Tuple[int, str], str] = {}
        self._unmatched_fills: Dict[str, tuple] = {}
        self.reject_until = {BUY: 0.0, SELL: 0.0}
        self._reject_n = {BUY: 0, SELL: 0}
        self._actions: deque = deque()
        self.paused_until = 0.0
        self._recently_closed: deque = deque(maxlen=200)
        self._consec_errors = 0
        self.last_place_ts = 0.0
        self.maybe_orders = True
        self._client_id_gen = itertools.count(1001)

        self.n_place = 0
        self.n_modify = 0
        self.n_cancel = 0
        self.n_reject = 0
        self.n_actions = 0

    def side_orders(self, side: str) -> List[Order]:
        return sorted((o for o in self.orders.values() if o.side == side),
                      key=lambda o: o.price, reverse=(side == BUY))

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
        while self._actions and now - self._actions[0] > 60:
            self._actions.popleft()
        if len(self._actions) >= self.cfg.max_actions_per_min:
            return False
        self._actions.append(now)
        self.n_actions += 1
        return True

    @staticmethod
    def _ok(resp: dict) -> bool:
        return resp.get("status") in (200, 202) and "error" not in resp

    def _error(self, what: str, resp: dict, now: float) -> None:
        err = resp.get("error")
        log.warning("%s failed: status=%s %s", what, resp.get("status"), json.dumps(err)[:300])
        self._consec_errors += 1
        if resp.get("status") == 429:
            self.paused_until = now + 5.0
        if self._consec_errors >= 8:
            log.error("Too many consecutive errors - pausing 30s")
            self.paused_until = now + 30.0
            self._consec_errors = 0

    def _backoff(self, side: str, now: float) -> None:
        self._reject_n[side] += 1
        self.reject_until[side] = now + min(0.25 * 2 ** (self._reject_n[side] - 1), 4.0)

    async def place(self, pair_index: int, side: str, px: Decimal, qty: Decimal, now: float,
                    time_in_force: str = "ALO", reduce_only: bool = False,
                    quote_mid: Optional[Decimal] = None) -> Optional[Order]:
        if now < self.paused_until or not self._budget(now):
            return None

        m = self.get_market()
        cid = next(self._client_id_gen)
        post_only = (time_in_force == "ALO")
        signed = self.signer.sign_create_order(
            m, side, px, qty, client_order_id=cid, post_only=post_only, reduce_only=reduce_only
        )
        self.maybe_orders = True
        self.last_place_ts = now

        resp = await self.ex.send_tx_ws(signed["tx_type"], signed["tx_info"])
        res = resp.get("result") or {}
        if not self._ok(resp) or str(res.get("status")).upper() == "REJECTED":
            self._error(f"place L{pair_index} {side}", resp, now)
            if not post_only:
                self._backoff(side, now)
            return None

        self._consec_errors = 0
        self.n_place += 1
        oid = str(res.get("order_id") or res.get("orderId") or f"ord-{cid}")
        o = Order(
            order_id=oid, client_order_id=cid, pair_index=pair_index, side=side,
            price=px, qty=qty, remaining=qty, created=now, last_action=now,
            is_taker=(time_in_force == "IOC"), is_reduce_only=reduce_only, quote_mid=quote_mid
        )
        self.orders[oid] = o
        self.pair_slots[(pair_index, side)] = oid
        log.info("🎯 [ROBINHOOD LIGHTER WS PLACE] L%d %s %s @ %s (id=%s, reduce_only=%s)",
                 pair_index, side, fmt(qty), fmt(px), oid, reduce_only)

        early = self._unmatched_fills.pop(oid, None)
        if early:
            self._apply_fill(o, early[0], early[1], now)
        return o

    async def modify(self, o: Order, px: Decimal, now: float, urgent: bool = False,
                     reduce_only: Optional[bool] = None) -> bool:
        if now < self.paused_until or not self._budget(now):
            if urgent:
                await self.cancel(o, now)
            return False

        m = self.get_market()
        tick = m.tick_size
        px = q_down(px, tick) if o.side == BUY else q_up(px, tick)
        r_only = o.is_reduce_only if reduce_only is None else reduce_only

        signed = self.signer.sign_modify_order(
            m, order_id=o.order_id, side=o.side, price=px, size=o.qty,
            client_order_id=o.client_order_id, reduce_only=r_only
        )
        resp = await self.ex.send_tx_ws(signed["tx_type"], signed["tx_info"])
        if not self._ok(resp):
            self._error(f"modify L{o.pair_index} {o.side}", resp, now)
            await self.cancel(o, now)
            return False

        self._consec_errors = 0
        self.n_modify += 1
        log.info("MODIFY L%d %s %s -> %s (reduce_only=%s)", o.pair_index, o.side, fmt(o.price), fmt(px), r_only)
        o.price, o.last_action, o.is_reduce_only = px, now, r_only
        return True

    async def cancel(self, o: Order, now: float) -> None:
        if o.cancelling_since is not None and now - o.cancelling_since < 5:
            return
        o.cancelling_since = now
        self._recently_closed.append((now, o.order_id))
        self._budget(now)

        m = self.get_market()
        signed = self.signer.sign_cancel_order(m, o.order_id, o.client_order_id)
        resp = await self.ex.send_tx_ws(signed["tx_type"], signed["tx_info"])
        if self.cfg.dry_run or "ORDER_NOT_FOUND" in json.dumps(resp):
            self._remove(o)
        self.n_cancel += 1

    async def cancel_all(self, now: float) -> None:
        to_cancel = list(self.orders.values())
        if not to_cancel:
            return
        log.info("🚨 Cancelling all open orders (%d total)...", len(to_cancel))
        m = self.get_market()
        batch_items = [self.signer.sign_cancel_order(m, o.order_id, o.client_order_id) for o in to_cancel]
        await self.ex.send_batch_tx_ws([it["tx_type"] for it in batch_items], [it["tx_info"] for it in batch_items])
        for o in to_cancel:
            self._remove(o)

    def _remove(self, o: Order) -> None:
        self.orders.pop(o.order_id, None)
        slot = (o.pair_index, o.side)
        if self.pair_slots.get(slot) == o.order_id:
            self.pair_slots.pop(slot, None)

    def _apply_fill(self, o: Order, fill_qty: Decimal, fill_px: Decimal, now: float) -> None:
        o.filled_any = True
        o.remaining = max(Decimal(0), o.remaining - fill_qty)
        self.on_fill(o.side, fill_qty, fill_px, o)
        if o.remaining == Decimal(0):
            self._remove(o)

    def on_account_order_event(self, event: dict, now: float) -> None:
        oid = str(event.get("order_id") or event.get("orderId") or "")
        status = str(event.get("status") or "").upper()
        filled_qty = Decimal(str(event.get("filled_size") or event.get("filled_amount") or "0"))
        fill_px = Decimal(str(event.get("price") or "0"))

        o = self.orders.get(oid)
        if not o:
            if filled_qty > 0:
                self._unmatched_fills[oid] = (filled_qty, fill_px)
            return

        if filled_qty > 0:
            delta = filled_qty - (o.qty - o.remaining)
            if delta > 0:
                self._apply_fill(o, delta, fill_px, now)

        if status in ("FILLED", "CANCELLED", "EXPIRED", "REJECTED"):
            self._remove(o)
