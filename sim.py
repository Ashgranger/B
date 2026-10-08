"""Robinhood Lighter perpetual DEX simulator driving the quantitative Market Maker."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import types
from decimal import Decimal as D

# Stub websockets if not installed in offline environment
if "websockets" not in sys.modules:
    try:
        import websockets  # noqa
    except ImportError:
        stub = types.ModuleType("websockets")
        class ConnectionClosed(Exception): pass
        stub.ConnectionClosed = ConnectionClosed
        sys.modules["websockets"] = stub

import config as C
from bot import MarketMaker
from market import Market
from utils import fmt

ADDR = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
MKT = Market(1, "BTC-USD", "ONLINE", D("0.1"), D("0.00000001"), [], D("5"), D("0.00001"), D("100"), D("0"), False)


def mkcfg(**env):
    base = {
        "ROBINHOOD_LIGHTER_ENV": "testnet",
        "LIGHTER_ENV": "testnet",
        "ARCUS_ENV": "testnet",
        "ROBINHOOD_WALLET_ADDRESS": ADDR,
        "LIGHTER_L1_ADDRESS": ADDR,
        "ARCUS_WALLET_ADDRESS": ADDR,
        "ROBINHOOD_API_PRIVATE_KEY": "11" * 32,
        "LIGHTER_API_PRIVATE_KEY": "11" * 32,
        "ARCUS_API_SIGNING_KEY": "11" * 32,
        "DRY_RUN": "0",
        "JOURNAL_PATH": os.devnull,
        "LEARNING_STATE_PATH": os.devnull,
        "MAX_ACTIONS_PER_MIN": "100000",
        "MIN_REQUOTE_S": "0.5",
        "EXTRA_LEVELS": "1",
        "ENABLE_ADAPTIVE_EV": "1",
        "ENABLE_ORDERBOOK_INTEL": "1",
        "ENABLE_ONLINE_LEARNING": "1",
    }
    base.update({k: str(v) for k, v in env.items()})
    old = {k: os.environ.get(k) for k in base}
    os.environ.update(base)
    try:
        return C.Config.from_env()
    finally:
        for k, v in old.items():
            if v is None: os.environ.pop(k, None)
            else: os.environ[k] = v


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class SimWS:
    def __init__(self, bot):
        self.bot = bot
        self.orders = {}
        self.seq = 0
        self.bid = self.ask = None
        self.bid_sz = self.ask_sz = D("1")
        self.position = D(0)
        self.cash = D(0)
        self.rejects = 0
        self.posts = []
        self.max_open_per_side = 0

    def _reply(self, obj):
        asyncio.get_running_loop().call_soon(self.bot.ex.handle_message, json.dumps(obj))

    def _push(self, o, state, status, reason=None, rem=None):
        c = {
            "orderId": o["id"], "client_order_id": o["id"], "client_order_index": o["id"],
            "state": state, "status": status, "side": o["side"],
            "price": fmt(o["price"]), "remainingSize": fmt(o["rem"] if rem is None else rem),
            "remaining_base_amount": fmt(o["rem"] if rem is None else rem)
        }
        if reason:
            c["rejectionReason" if state == "REJECTED" else "cancelReason"] = reason
        self._reply({"type": "channel_data", "channel": "orders", "id": ADDR, "contents": c})

    def _crosses(self, side, px):
        return (side == "BUY" and px >= self.ask) or (side == "SELL" and px <= self.bid)

    async def send(self, raw):
        m = json.loads(raw)
        mtype = m.get("type")
        if mtype in ("post", "jsonapi/sendtx"):
            self._post(m)
        elif mtype in ("get",):
            self._get(m)
        elif mtype == "ping":
            self._reply({"type": "pong"})

    def _post(self, m):
        r = m.get("request", {})
        t = r.get("type")
        p = r.get("payload", {})

        # If data is present from Lighter jsonapi
        if not t and "tx_type" in m.get("data", {}):
            tx_type = m["data"]["tx_type"]
            t = {14: "placeOrder", 17: "modifyOrder", 15: "cancelOrder", 16: "scheduleCancel"}.get(tx_type, "order")
            tx_info = m["data"].get("tx_info", {})
            if isinstance(tx_info, str):
                try:
                    tx_info = json.loads(tx_info)
                except Exception:
                    pass
            p = tx_info

        self.posts.append(t)
        if t == "placeOrder":
            self.seq += 1
            oid = str(p.get("orderId") or p.get("clientOrderIndex") or f"sim-{self.seq}")
            side = p.get("orderSide") or ("SELL" if p.get("IsAsk") == 1 else "BUY")

            price_decimals = getattr(self.bot.md.info, "price_decimals", 2)
            size_decimals = getattr(self.bot.md.info, "size_decimals", 4)

            if "price" in p and p["price"] is not None:
                price = D(str(p["price"]))
            elif "Price" in p and p["Price"] is not None:
                price = D(str(p["Price"])) / D(10**price_decimals)
            else:
                price = D("0")

            if "quantity" in p and p["quantity"] is not None:
                qty = D(str(p["quantity"]))
            elif "BaseAmount" in p and p["BaseAmount"] is not None:
                qty = D(str(p["BaseAmount"])) / D(10**size_decimals)
            else:
                qty = D("1")

            o = {"id": oid, "side": side, "price": price, "rem": qty}
            self._reply({"id": m["id"], "status": 202, "code": 200, "result": {"orderId": oid, "status": "ACK"}})
            if self._crosses(o["side"], o["price"]):
                tif = str(p.get("timeInForce", "ALO")).upper()
                is_ioc = ("IOC" in tif or "IMMEDIATE" in tif or tif == "0" or p.get("TimeInForce") == 0 or r.get("tx_info", {}).get("TimeInForce") == 0)
                if is_ioc:
                    q = o["rem"]
                    signed = q if o["side"] == "BUY" else -q
                    self.position += signed
                    self.cash -= signed * o["price"]
                    o["rem"] = D(0)
                    self._push(o, "FILLED", "FILLED", rem=D(0))
                    self._reply({"type": "channel_data", "channel": "positions", "id": ADDR,
                                 "contents": {"positions": [{"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}]}})
                else:
                    self.rejects += 1
                    self._push(o, "REJECTED", "REJECTED", "POST_ONLY_WOULD_CROSS")
            else:
                self.orders[oid] = o
                self._push(o, "OPEN", "OPEN")
                for s in ("BUY", "SELL"):
                    self.max_open_per_side = max(self.max_open_per_side, sum(1 for x in self.orders.values() if x["side"] == s))

        elif t == "modifyOrder":
            oid = str(p.get("orderId") or p.get("ClientOrderIndex"))
            o = self.orders.get(oid)
            if o is None:
                self._reply({"id": m["id"], "status": 404, "error": {"type": "ORDER_NOT_FOUND"}})
                return
            self._reply({"id": m["id"], "status": 202, "code": 200, "result": {"status": "ACK"}})

            if "price" in p and p["price"] is not None:
                o["price"] = D(str(p["price"]))
            elif "Price" in p and p["Price"] is not None:
                price_decimals = getattr(self.bot.md.info, "price_decimals", 2)
                o["price"] = D(str(p["Price"])) / D(10**price_decimals)

            if self._crosses(o["side"], o["price"]):
                self.rejects += 1
                self.orders.pop(o["id"])
                self._push(o, "REJECTED", "REJECTED", "POST_ONLY_WOULD_CROSS")
            else:
                self._push(o, "OPEN", "OPEN")

        elif t == "cancelOrder":
            oid = str(p.get("orderId") or p.get("ClientOrderIndex"))
            o = self.orders.pop(oid, None)
            if o is None:
                self._reply({"id": m["id"], "status": 404, "error": {"type": "ORDER_NOT_FOUND"}})
                return
            self._reply({"id": m["id"], "status": 202, "code": 200, "result": {"status": "ACK"}})
            self._push(o, "CANCELED", "CANCELED", rem=o["rem"])

        elif t == "cancelAllOrders":
            for o in list(self.orders.values()):
                self._push(o, "CANCELED", "CANCELED")
            self.orders.clear()
            self._reply({"id": m["id"], "status": 202, "code": 200, "result": {"status": "ACK"}})

        elif t == "scheduleCancel":
            self._reply({"id": m["id"], "status": 202, "code": 200, "result": {"status": "ACK"}})

        else:
            self._reply({"id": m["id"], "status": 202, "code": 200, "result": {"status": "ACK"}})

    def _get(self, m):
        t = m["request"]["type"]
        if t == "bbo":
            res = self._bbo()
        elif t == "positions":
            res = {"positions": {"1": {"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}}}
        else:
            res = {"openOrders": [{"orderId": o["id"]} for o in self.orders.values()], "recentClosedOrders": []}
        self._reply({"id": m["id"], "status": 200, "result": res})

    def _bbo(self):
        return {"bestBid": {"price": fmt(self.bid), "size": fmt(self.bid_sz)},
                "bestAsk": {"price": fmt(self.ask), "size": fmt(self.ask_sz)}}

    def set_book(self, bid, ask, bsz="1", asz="1"):
        self.bid, self.ask = D(str(bid)), D(str(ask))
        self.bid_sz, self.ask_sz = D(str(bsz)), D(str(asz))
        for o in list(self.orders.values()):
            if (o["side"] == "BUY" and self.ask <= o["price"]) or (o["side"] == "SELL" and self.bid >= o["price"]):
                q = o["rem"]
                signed = q if o["side"] == "BUY" else -q
                self.position += signed
                self.cash -= signed * o["price"]
                self.orders.pop(o["id"])
                o["rem"] = D(0)
                self._push(o, "FILLED", "FILLED", rem=D(0))
                self._reply({"type": "channel_data", "channel": "positions", "id": ADDR,
                             "contents": {"positions": [{"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}]}})
        self._reply({"type": "channel_data", "channel": "bbo", "id": "BTC-USD",
                     "contents": {"bestBid": {"price": fmt(self.bid), "size": fmt(self.bid_sz)},
                                  "bestAsk": {"price": fmt(self.ask), "size": fmt(self.ask_sz)}}})

    def push_trade(self, side: str, size: str, price: str):
        self._reply({"type": "channel_data", "channel": "trades", "id": "BTC-USD",
                     "contents": {"side": side, "quantity": size, "price": price}})

    def taker(self, side: str):
        if side == "BUY":
            cand = sorted([o for o in self.orders.values() if o["side"] == "SELL" and o["price"] <= self.ask],
                          key=lambda o: o["price"])
        else:
            cand = sorted([o for o in self.orders.values() if o["side"] == "BUY" and o["price"] >= self.bid],
                          key=lambda o: o["price"], reverse=True)
        for o in cand:
            q = o["rem"]
            signed = q if o["side"] == "BUY" else -q
            self.position += signed
            self.cash -= signed * o["price"]
            self.orders.pop(o["id"])
            o["rem"] = D(0)
            self._push(o, "FILLED", "FILLED", rem=D(0))
            self._reply({"type": "channel_data", "channel": "positions", "id": ADDR,
                         "contents": {"positions": [{"marketId": 1, "side": "LONG" if self.position >= 0 else "SHORT", "size": fmt(self.position)}]}})

    def pnl(self, mid):
        return self.cash + self.position * mid


def make(**env):
    cfg = mkcfg(**env)
    bot = MarketMaker(cfg)
    clock = Clock()
    bot.now = clock
    bot.md.info = MKT
    bot.md.info_ts = clock.t
    sim = SimWS(bot)
    bot.ex.ws = sim
    bot.om.maybe_orders = False
    return bot, sim, clock


async def step(bot, sim, clock, bid, ask, bsz="1", asz="1", dt=0.25, tick=True, keep_info=True):
    clock.t += dt
    if keep_info:
        bot.md.info_ts = clock.t
    sim.set_book(bid, ask, bsz, asz)
    for _ in range(3):
        await asyncio.sleep(0)
    if tick:
        await bot.tick()
    for _ in range(3):
        await asyncio.sleep(0)
