"""Bulk Trade Perpetual DEX WebSocket simulator driving the Level 8+ Market Maker."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import types
from decimal import Decimal as D
from typing import Dict, List, Optional, Tuple, Any

from bot import BulkMarketMakerBot
from config import Config
from market import Market
from utils import BUY, SELL, BPS, ZERO, ONE, fmt

ADDR = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
MKT = Market(
    market_id=1,
    name="BTC-USD",
    status="ONLINE",
    tick=D("0.1"),
    step=D("0.00000001"),
    tiers=[],
    min_notional=D("5.0"),
    min_size=D("0.00001"),
    max_size=D("1000.0"),
    mark=D("0.0"),
    is_outside_rth=False,
)


def mkcfg(**env) -> Config:
    base = {
        "BULK_ENV": "devnet",
        "BULK_WALLET_ADDRESS": ADDR,
        "BULK_PRIVATE_KEY": "11" * 32,
        "DRY_RUN": "1",
        "MARKET": "BTC-USD",
        "JOURNAL_PATH": os.devnull,
        "LEARNING_STATE_PATH": os.devnull,
        "MAX_ACTIONS_PER_MIN": "100000",
        "MIN_REQUOTE_S": "0.5",
        "TOUCH_MIN_REQUOTE_S": "0.2",
        "EXTRA_LEVELS": "1",
        "ENABLE_ADAPTIVE_EV": "1",
        "ENABLE_ORDERBOOK_INTEL": "1",
        "ENABLE_ONLINE_LEARNING": "1",
        "MAKER_FEE_BPS": "0.0",
        "TAKER_FEE_BPS": "3.5",
        "MIN_EDGE_BPS": "2.0",
        "MAX_EDGE_BPS": "30.0",
        "ORDER_USD": "20.0",
        "MAX_POSITION_USD": "100.0",
        "SKEW_BPS": "3.0",
    }
    base.update({k: str(v) for k, v in env.items()})
    old = {k: os.environ.get(k) for k in base}
    os.environ.update(base)
    try:
        return Config.load()
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class Clock:
    def __init__(self, start: float = 1000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> float:
        self.t += dt
        return self.t


class SimBulkWS:
    """Mock WebSocket actor & matching engine for Bulk Trade Perpetual DEX."""
    def __init__(self, bot: BulkMarketMakerBot):
        self.bot = bot
        self.orders: Dict[str, dict] = {}
        self.seq = 0
        self.bid: Optional[D] = None
        self.ask: Optional[D] = None
        self.bid_sz: D = D("1.0")
        self.ask_sz: D = D("1.0")
        self.position = D(0)
        self.cash = D(0)
        self.rejects = 0
        self.max_open_per_side = 0

    def _reply(self, obj: dict):
        raw = json.dumps(obj)
        try:
            self.bot.ex.handle_message(raw)
        except Exception:
            asyncio.get_running_loop().call_soon(self.bot.ex.handle_message, raw)

    def _crosses(self, side: str, px: D) -> bool:
        return (side == BUY and self.ask is not None and px >= self.ask) or \
               (side == SELL and self.bid is not None and px <= self.bid)

    async def send(self, raw: str):
        msg = json.loads(raw)
        mtype = msg.get("type")
        rid = msg.get("id")
        data = msg.get("data", {})

        if mtype == "order":
            signed_tx = data
            actions = signed_tx.get("actions", [])
            order_ids = []
            status = "ACK"

            for i, a in enumerate(actions):
                atype = a.get("type")
                if atype == "order":
                    self.seq += 1
                    oid = signed_tx.get("order_id") or (
                        signed_tx.get("order_ids")[i] if signed_tx.get("order_ids") else f"bulk-sim-{self.seq}"
                    )
                    side = BUY if a.get("is_buy") else SELL
                    price = D(str(a.get("price", 0)))
                    qty = D(str(a.get("size", 0)))
                    tif = a.get("order_type", {}).get("tif", "ALO")

                    o = {"id": oid, "side": side, "price": price, "rem": qty, "orig": qty, "tif": tif}

                    if self._crosses(side, price):
                        if tif == "IOC":
                            signed = qty if side == BUY else -qty
                            self.position += signed
                            self.cash -= signed * price
                            o["rem"] = D(0)
                            order_ids.append(oid)
                            self._reply({"type": "account", "data": {"type": "fill", "order_id": oid, "size": fmt(qty), "price": fmt(price)}})
                        else:
                            self.rejects += 1
                            status = "REJECTED"
                            order_ids.append(oid)
                    else:
                        self.orders[oid] = o
                        order_ids.append(oid)
                        for s in (BUY, SELL):
                            self.max_open_per_side = max(
                                self.max_open_per_side,
                                sum(1 for x in self.orders.values() if x["side"] == s)
                            )
                elif atype == "cancel":
                    oid = str(a.get("order_id"))
                    self.orders.pop(oid, None)
                    order_ids.append(oid)
                elif atype == "cancelAll":
                    self.orders.clear()

            res = {
                "order_id": order_ids[0] if order_ids else None,
                "order_ids": order_ids,
                "status": status,
            }
            self._reply({"id": rid, "status": 200, "result": res})

        elif mtype == "ping":
            self._reply({"type": "pong"})

    def set_book(self, bid: str, ask: str, bsz: str = "1.0", asz: str = "1.0"):
        self.bid, self.ask = D(str(bid)), D(str(ask))
        self.bid_sz, self.ask_sz = D(str(bsz)), D(str(asz))

        for o in list(self.orders.values()):
            if (o["side"] == BUY and self.ask <= o["price"]) or (o["side"] == SELL and self.bid >= o["price"]):
                q = o["rem"]
                signed = q if o["side"] == BUY else -q
                self.position += signed
                self.cash -= signed * o["price"]
                self.orders.pop(o["id"], None)
                o["rem"] = D(0)
                self.bot.om.on_fill(o["id"], q, o["price"], self.bot.now())

    def taker(self, side: str, qty: Optional[D] = None) -> Optional[D]:
        """Taker fill: matches against bot resting orders."""
        target_side = SELL if side == BUY else BUY
        active = [o for o in self.bot.om.orders.values() if o.side == target_side and o.remaining > ZERO]
        if not active:
            return None
        active.sort(key=lambda o: o.price, reverse=(target_side == BUY))
        best = active[0]
        fill_sz = min(best.remaining, qty) if qty else best.remaining
        fill_px = best.price

        self.orders.pop(best.order_id, None)
        self.bot.om.on_fill(best.order_id, fill_sz, fill_px, self.bot.now())
        return fill_sz


def make(**env) -> Tuple[BulkMarketMakerBot, SimBulkWS, Clock]:
    cfg = mkcfg(**env)
    bot = BulkMarketMakerBot(cfg)
    clock = Clock()
    bot.now = clock
    bot.md.info = MKT
    bot.md.info_ts = clock.t
    sim_ws = SimBulkWS(bot)
    bot.ex.ws = sim_ws
    bot.ex._is_connected = True
    bot.om.maybe_orders = False
    return bot, sim_ws, clock


async def step(
    bot: BulkMarketMakerBot,
    sim_ws: SimBulkWS,
    clock: Clock,
    bid: str,
    ask: str,
    bsz: str = "1.0",
    asz: str = "1.0",
    dt: float = 0.25
) -> None:
    clock.advance(dt)
    now = clock.t
    bot.md.info_ts = now
    sim_ws.set_book(bid, ask, bsz, asz)
    bot.md.update(D(bid), D(ask), D(bsz), D(asz), now)
    for _ in range(3):
        await asyncio.sleep(0)
    await bot.tick()
    for _ in range(3):
        await asyncio.sleep(0)
