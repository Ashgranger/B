"""Robinhood Lighter Perp DEX WebSocket simulator driving the Level 8+ Market Maker."""
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
from bot import LighterMarketMaker
from market import Market
from utils import fmt, BUY, SELL

ADDR = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
MKT = Market(1, "BTC-USD", "ONLINE", D("0.1"), D("0.00000001"), [], D("5"), D("0.00001"), D("100"), D("0"), False, D("0"))


def mkcfg(**env):
    base = {
        "LIGHTER_ENV": "mainnet",
        "LIGHTER_WALLET_ADDRESS": ADDR,
        "LIGHTER_API_SIGNING_KEY": "11" * 32,
        "LIGHTER_CHAIN_ID": "466324",
        "LIGHTER_API_KEY_INDEX": "4",
        "DRY_RUN": "0",
        "JOURNAL_PATH": os.devnull,
        "LEARNING_STATE_PATH": os.devnull,
        "MAX_ACTIONS_PER_MIN": "100000",
        "MIN_REQUOTE_S": "0.5",
        "EXTRA_LEVELS": "1",
        "ENABLE_ADAPTIVE_EV": "1",
        "ENABLE_ORDERBOOK_INTEL": "1",
        "ENABLE_ONLINE_LEARNING": "1",
        "MAKER_FEE_BPS": "1.2",  # 0.012%
        "TAKER_FEE_BPS": "3.5",  # 0.035%
        "MIN_EDGE_BPS": "2.9",
        "ENABLE_SELECTIVE_TOUCH": "1",
        "ENABLE_QUEUE_MODEL": "1",
        "ENABLE_ONESIDED_TOUCH": "1",
        "ENABLE_QUOTE_DATASET": "0",
    }
    base.update({k: str(v) for k, v in env.items()})
    old = {k: os.environ.get(k) for k in base}
    os.environ.update(base)
    try:
        return C.Config.from_env()
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class SimRobinhoodLighterWS:
    def __init__(self, bot: LighterMarketMaker):
        self.bot = bot
        self.orders = {}
        self.seq = 0
        self.bid = self.ask = None
        self.bid_sz = self.ask_sz = D("1")
        self.depth_bids = []
        self.depth_asks = []
        self.position = D(0)
        self.cash = D(0)
        self.rejects = 0
        self.posts = []
        self.max_open_per_side = 0

    def _reply(self, obj):
        raw = json.dumps(obj)
        try:
            self.bot.ex.handle_message(raw)
        except Exception:
            asyncio.get_running_loop().call_soon(self.bot.ex.handle_message, raw)

    def _push_order_event(self, o, status, filled_size=D(0)):
        event = {
            "order_id": o["id"],
            "status": status,
            "side": o["side"],
            "price": fmt(o["price"]),
            "filled_size": fmt(filled_size),
            "remaining": fmt(o["rem"]),
            "reduce_only": 1 if o.get("is_reduce_only") else 0
        }
        self._reply({
            "type": "update/account_orders",
            "channel": f"account_orders/1/{ADDR}",
            "data": event
        })

    def _crosses(self, side, px):
        return (side == BUY and self.ask is not None and px >= self.ask) or \
               (side == SELL and self.bid is not None and px <= self.bid)

    async def send(self, raw: str):
        msg = json.loads(raw)
        mtype = msg.get("type")
        data = msg.get("data", {})
        rid = data.get("id") or msg.get("id")

        if mtype == "jsonapi/sendtx":
            tx_type = data.get("tx_type")
            tx_info = data.get("tx_info", {})
            self._handle_send_tx(rid, tx_type, tx_info)
        elif mtype == "jsonapi/sendtxbatch":
            tx_types = data.get("tx_types", [])
            tx_infos = data.get("tx_infos", [])
            results = []
            for tt, ti in zip(tx_types, tx_infos):
                sub_res = self._process_single_tx(tt, ti)
                results.append(sub_res)
            self._reply({"id": rid, "status": 200, "result": results})

    def _process_single_tx(self, tx_type, tx_info):
        if tx_type == 1:  # CreateOrder
            self.seq += 1
            oid = f"sim-{self.seq}"
            side = SELL if tx_info.get("is_ask") == 1 else BUY
            tick = self.bot.md.info.tick_size if self.bot.md.info else MKT.tick_size
            step = self.bot.md.info.step_size if self.bot.md.info else MKT.step_size
            price = D(str(tx_info.get("price", 0))) * tick
            qty = D(str(tx_info.get("base_amount", 0))) * step
            r_only = bool(tx_info.get("reduce_only"))

            o = {"id": oid, "side": side, "price": price, "rem": qty, "orig": qty, "is_reduce_only": r_only}

            if self._crosses(side, price):
                tif = tx_info.get("time_in_force", 2)
                if tif == 0:  # TIF_IOC = 0
                    signed = qty if side == BUY else -qty
                    self.position += signed
                    self.cash -= signed * price
                    o["rem"] = D(0)
                    self._push_order_event(o, "FILLED", filled_size=qty)
                    return {"order_id": oid, "status": "FILLED"}
                else:  # TIF_ALO = 2 (Post-only would cross)
                    self.rejects += 1
                    self._push_order_event(o, "REJECTED")
                    return {"order_id": oid, "status": "REJECTED", "error": "POST_ONLY_WOULD_CROSS"}
            else:
                self.orders[oid] = o
                self._push_order_event(o, "OPEN")
                for s in (BUY, SELL):
                    self.max_open_per_side = max(
                        self.max_open_per_side,
                        sum(1 for x in self.orders.values() if x["side"] == s)
                    )
                return {"order_id": oid, "status": "OPEN"}

        elif tx_type == 4:  # ModifyOrder
            oid = str(tx_info.get("order_id"))
            o = self.orders.get(oid)
            if not o:
                return {"status": 404, "error": "ORDER_NOT_FOUND"}
            tick = self.bot.md.info.tick_size if self.bot.md.info else MKT.tick_size
            new_price = D(str(tx_info.get("price", 0))) * tick
            if "reduce_only" in tx_info:
                o["is_reduce_only"] = bool(tx_info.get("reduce_only"))
            o["price"] = new_price
            self._push_order_event(o, "MODIFIED")
            return {"order_id": oid, "status": "MODIFIED"}

        elif tx_type == 2:  # CancelOrder
            oid = str(tx_info.get("order_id"))
            o = self.orders.pop(oid, None)
            if o:
                self._push_order_event(o, "CANCELLED")
                return {"order_id": oid, "status": "CANCELLED"}
            return {"status": 404, "error": "ORDER_NOT_FOUND"}

        return {"status": 200, "result": "ACK"}

    def _handle_send_tx(self, rid, tx_type, tx_info):
        res = self._process_single_tx(tx_type, tx_info)
        self._reply({"id": rid, "status": 200, "result": res})

    def set_book(self, bid, ask, bsz="1", asz="1", depth_bids=None, depth_asks=None):
        self.bid, self.ask = D(str(bid)), D(str(ask))
        self.bid_sz, self.ask_sz = D(str(bsz)), D(str(asz))

        if depth_bids is not None:
            self.depth_bids = depth_bids
        else:
            self.depth_bids = [[fmt(self.bid), fmt(self.bid_sz)]]

        if depth_asks is not None:
            self.depth_asks = depth_asks
        else:
            self.depth_asks = [[fmt(self.ask), fmt(self.ask_sz)]]

        for o in list(self.orders.values()):
            if (o["side"] == BUY and self.ask <= o["price"]) or (o["side"] == SELL and self.bid >= o["price"]):
                q = o["rem"]
                signed = q if o["side"] == BUY else -q
                self.position += signed
                self.cash -= signed * o["price"]
                self.orders.pop(o["id"])
                o["rem"] = D(0)
                self._push_order_event(o, "FILLED", filled_size=q)
                self._reply({
                    "type": "update/account_all_positions",
                    "channel": f"account_all_positions/{ADDR}",
                    "data": [{"market_id": 1, "position_size": fmt(self.position)}]
                })

        self._reply({
            "type": "update/order_book",
            "channel": "order_book/1",
            "data": {
                "bids": self.depth_bids,
                "asks": self.depth_asks,
                "nonce": self.seq,
            }
        })

    def push_trade(self, side: str, size: str, price: str):
        is_maker_buyer = (side.upper() == SELL)
        self._reply({
            "type": "update/trade",
            "channel": "trade/1",
            "data": {
                "price": price,
                "size": size,
                "is_maker_buyer": is_maker_buyer,
                "timestamp": int(self.bot.now() * 1000)
            }
        })

    def taker(self, side: str):
        if side == BUY:
            cand = sorted([o for o in self.orders.values() if o["side"] == SELL and o["price"] <= self.ask],
                          key=lambda o: o["price"])
        else:
            cand = sorted([o for o in self.orders.values() if o["side"] == BUY and o["price"] >= self.bid],
                          key=lambda o: o["price"], reverse=True)
        for o in cand:
            q = o["rem"]
            signed = q if o["side"] == BUY else -q
            self.position += signed
            self.cash -= signed * o["price"]
            self.orders.pop(o["id"])
            o["rem"] = D(0)
            self._push_order_event(o, "FILLED", filled_size=q)
            self._reply({
                "type": "update/account_all_positions",
                "channel": f"account_all_positions/{ADDR}",
                "data": [{"market_id": 1, "position_size": fmt(self.position)}]
            })


def make(**env):
    cfg = mkcfg(**env)
    bot = LighterMarketMaker(cfg)
    clock = Clock()
    bot.now = clock
    bot.md.info = MKT
    bot.md.info_ts = clock.t
    sim = SimRobinhoodLighterWS(bot)
    bot.ex.ws = sim
    bot.om.maybe_orders = False
    return bot, sim, clock


async def step(bot, sim, clock, bid, ask, bsz="1", asz="1", dt=0.25, tick=True, keep_info=True,
               depth_bids=None, depth_asks=None):
    clock.t += dt
    if keep_info:
        bot.md.info_ts = clock.t
    sim.set_book(bid, ask, bsz, asz, depth_bids=depth_bids, depth_asks=depth_asks)
    for _ in range(3):
        await asyncio.sleep(0)
    if tick:
        await bot._tick()
    for _ in range(3):
        await asyncio.sleep(0)
