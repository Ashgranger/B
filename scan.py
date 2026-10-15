"""`python main.py scan` - rank Robinhood Lighter Perp DEX markets by spread vs movement."""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from decimal import Decimal

from config import ENVS, Config
from exchange import Exchange
from market import Market
from utils import BPS


async def scan(cfg: Config, seconds: float = 30.0) -> None:
    try:
        import websockets
    except ImportError:
        print("websockets package required for scanner: pip install websockets")
        return

    ex = Exchange(cfg, lambda *a: None)
    markets = [Market.from_api(r) for r in await ex.fetch_markets()]
    online = [m for m in markets if m.status == "ONLINE"]
    ws_url = getattr(cfg, "ws_url", "") or ENVS[cfg.env_name]["ws"]
    print(f"{len(online)} ACTIVE perp markets on Robinhood Lighter ({cfg.env_name} - {ws_url}); sampling BBO for {seconds:.0f}s ...")
    by_id: dict[int, dict] = {m.market_id: {"spread": [], "mid": [], "n": 0, "m": m} for m in online}
    by_name: dict[str, dict] = {m.name: by_id[m.market_id] for m in online}

    async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None, max_size=2 ** 23) as ws:
        for m in online:
            # Subscribe to Lighter ticker/{market_id} and order_book/{market_id}
            await ws.send(json.dumps({"type": "subscribe", "channel": f"ticker/{m.market_id}"}))
            await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{m.market_id}"}))
        end = time.monotonic() + seconds
        last_ping = time.monotonic()
        while (left := end - time.monotonic()) > 0:
            if time.monotonic() - last_ping >= 25.0:
                last_ping = time.monotonic()
                await ws.send(json.dumps({"type": "ping"}))
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), left))
            except asyncio.TimeoutError:
                break
            if not isinstance(msg, dict):
                continue
            mtype = str(msg.get("type") or "")
            if mtype == "ping":
                await ws.send(json.dumps({"type": "pong"}))
                continue

            d = None
            bid = ask = None

            # Lighter native ticker message: channel="ticker:0", ticker={"b": {"price": ...}, "a": {"price": ...}}
            if mtype in ("subscribed/ticker", "update/ticker"):
                ch = str(msg.get("channel") or "")
                if ":" in ch or "/" in ch:
                    sep = ":" if ":" in ch else "/"
                    try:
                        mid_int = int(ch.split(sep, 1)[1])
                        d = by_id.get(mid_int)
                    except ValueError:
                        pass
                t = msg.get("ticker") or {}
                b, a = t.get("b"), t.get("a")
                if isinstance(b, dict) and isinstance(a, dict) and b.get("price") and a.get("price"):
                    bid, ask = Decimal(str(b["price"])), Decimal(str(a["price"]))

            # Lighter native order_book snapshot message
            elif mtype == "subscribed/order_book":
                ch = str(msg.get("channel") or "")
                if ":" in ch or "/" in ch:
                    sep = ":" if ":" in ch else "/"
                    try:
                        mid_int = int(ch.split(sep, 1)[1])
                        d = by_id.get(mid_int)
                    except ValueError:
                        pass
                ob = msg.get("order_book") or {}
                bids, asks = ob.get("bids") or [], ob.get("asks") or []
                if bids and asks and isinstance(bids[0], dict) and isinstance(asks[0], dict):
                    bid, ask = Decimal(str(bids[0]["price"])), Decimal(str(asks[0]["price"]))

            # Legacy Arcus fallback
            elif mtype in ("channel_data", "subscribed") and msg.get("channel") == "bbo":
                d = by_name.get(msg.get("id"))
                c = msg.get("contents") or {}
                if c.get("bestBid") and c.get("bestAsk"):
                    bid, ask = Decimal(str(c["bestBid"]["price"])), Decimal(str(c["bestAsk"]["price"]))

            if not d or bid is None or ask is None or bid <= 0 or bid >= ask:
                continue
            mid = (bid + ask) / 2
            d["spread"].append(float((ask - bid) / mid * BPS))
            d["mid"].append(float(mid))
            d["n"] += 1

    rows = []
    for mkt_id, d in by_id.items():
        if len(d["mid"]) < 2:
            continue
        mid0 = statistics.median(d["mid"])
        move = (max(d["mid"]) - min(d["mid"])) / mid0 * 1e4
        spr = statistics.median(d["spread"])
        tick_bps = float(d["m"].tick) / mid0 * 1e4
        rows.append((spr / max(move, 0.1), d["m"].name, mkt_id, spr, tick_bps, move, d["n"], d["m"].is_outside_rth))
    rows.sort(reverse=True)
    print(f"\n{'market':<14}{'id':>4}{'spread bps':>12}{'tick bps':>10}{'range bps':>11}{'updates':>9}{'spr/range':>10}  note")
    for score, name, mkt_id, spr, tick_bps, move, n, rth in rows[:25]:
        note = "closed (outside RTH)" if rth else ("spread <= 1 tick: nothing to capture at touch" if spr <= tick_bps * 1.05 else "")
        print(f"{name:<14}{mkt_id:>4}{spr:>12.2f}{tick_bps:>10.3f}{move:>11.2f}{n:>9}{score:>10.2f}  {note}")
    print("\nPick a market where spread bps >= ~2x your MIN_EDGE_BPS and spr/range is high, then set MARKET=... in .env")
