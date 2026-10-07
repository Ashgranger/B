"""Transport client for Robinhood Lighter Perpetual DEX: WebSocket streaming & zk-transactions."""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import urllib.request
import urllib.parse
from typing import Any, Callable, Optional, Dict, List

from config import ENVS, Config
from utils import Fatal

log = logging.getLogger("exchange")

try:
    import orjson as _orjson
    _loads = _orjson.loads
    _dumps = lambda o: _orjson.dumps(o).decode()
except ImportError:
    _loads = json.loads
    _dumps = json.dumps

try:
    from websockets.exceptions import ConnectionClosed
except ImportError:
    class ConnectionClosed(Exception):
        pass


class Exchange:
    def __init__(self, cfg: Config, on_channel: Callable[[str, Any, bool], None]):
        self.cfg = cfg
        self.rest = ENVS[cfg.env_name]["rest"]
        self.ws_url = ENVS[cfg.env_name]["ws"]
        self.on_channel = on_channel
        self.ws = None
        self._ids = itertools.count(1)
        self._pending: dict[Any, asyncio.Future] = {}
        self._dry_seq = itertools.count(1)
        self._ping_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        return self.ws is not None and not getattr(self.ws, "closed", False)

    async def _send(self, obj: dict) -> bool:
        if not self.is_connected:
            return False
        try:
            await self.ws.send(_dumps(obj))
            return True
        except (ConnectionClosed, ConnectionResetError, BrokenPipeError, OSError) as e:
            log.warning("WebSocket send failed (closed): %s", e)
            self.ws = None
            self._fail_all_pending("CONNECTION_CLOSED")
            return False
        except Exception as e:
            log.warning("WebSocket send error: %s", e)
            return False

    def _fail_all_pending(self, reason: str = "CONNECTION_CLOSED") -> None:
        for rid, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_result({"status": 503, "error": {"type": reason, "message": "WebSocket disconnected"}})
        self._pending.clear()

    async def _ping_loop(self) -> None:
        """Keepalive required by Lighter: send ping frame every 30-45s (must be < 120s)."""
        while self.is_connected:
            try:
                await asyncio.sleep(30)
                if self.is_connected:
                    await self._send({"type": "ping"})
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("Ping error: %s", e)
                break

    def start_ping_loop(self) -> None:
        self.stop_ping_loop()
        try:
            self._ping_task = asyncio.create_task(self._ping_loop())
        except Exception:
            pass

    def stop_ping_loop(self) -> None:
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            self._ping_task = None

    async def call(self, kind: str, request: dict, timeout: float = 10.0) -> dict:
        if not self.is_connected and not self.cfg.dry_run:
            return {"status": 503, "error": {"type": "NOT_CONNECTED", "message": "WebSocket not connected"}}
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        self._pending[str(rid)] = fut
        try:
            ws_msg = {
                "type": kind,
                "id": rid,
                "request": request,
            }
            if "tx_type" in request:
                ws_msg["data"] = {
                    "id": str(rid),
                    "tx_type": request["tx_type"],
                    "tx_info": request.get("tx_info", request.get("payload", {})),
                }

            sent = await self._send(ws_msg)
            if not sent and not self.cfg.dry_run:
                return {"status": 503, "error": {"type": "CONNECTION_CLOSED", "message": "Failed to send over WebSocket"}}
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return {"status": 408, "error": {"type": "TIMEOUT", "message": f"Request {kind} timed out after {timeout}s"}}
        except Exception as e:
            return {"status": 500, "error": {"type": "SEND_ERROR", "message": str(e)}}
        finally:
            self._pending.pop(rid, None)
            self._pending.pop(str(rid), None)

    async def write(self, request: dict) -> dict:
        if self.cfg.dry_run:
            rtype = request.get("type", "order")
            log.debug("DRY %s %s", rtype, json.dumps(request.get("payload", request.get("tx_info", {})))[:200])
            if rtype == "placeOrder":
                oid = str(request.get("clientOrderIndex") or f"dry-{next(self._dry_seq)}")
                return {"status": 202, "code": 200, "result": {"orderId": oid, "status": "ACK"}}
            return {"status": 202, "code": 200, "result": {"status": "ACK"}}

        return await self.call("post", request)

    async def write_batch(self, requests: list[dict]) -> dict:
        """Send up to 50 transactions in a single batch over Lighter WebSocket."""
        if not requests:
            return {"status": 200, "code": 200}
        if self.cfg.dry_run:
            return {"status": 202, "code": 200, "result": {"status": "ACK", "count": len(requests)}}

        tx_types = [r.get("tx_type", 14) for r in requests]
        tx_infos = [r.get("tx_info", r.get("payload")) for r in requests]
        return await self.call("jsonapi/sendtxbatch", {"tx_types": tx_types, "tx_infos": tx_infos})

    async def get(self, rtype: str, payload: dict, timeout: float = 8.0) -> Optional[Any]:
        r = await self.call("get", {"type": rtype, "payload": payload}, timeout)
        return r.get("result") if r.get("status") == 200 else None

    async def subscribe(self, channel: str, sub_id: str = "", **extra) -> bool:
        """Subscribe to a Lighter channel (e.g. order_book/{market_id}, ticker/{market_id})."""
        msg = {"type": "subscribe", "channel": channel}
        if sub_id:
            msg["id"] = sub_id
        msg.update(extra)
        return await self._send(msg)

    async def unsubscribe(self, channel: str) -> bool:
        """Unsubscribe from a channel."""
        return await self._send({"type": "unsubscribe", "channel": channel})

    def handle_message(self, raw: Any) -> None:
        try:
            msg = _loads(raw) if isinstance(raw, (str, bytes)) else raw
        except ValueError:
            return
        if not isinstance(msg, dict):
            return

        mtype = str(msg.get("type", ""))

        # 1. Ping / Pong
        if mtype == "pong":
            return
        if mtype == "ping":
            if self.is_connected:
                asyncio.create_task(self._send({"type": "pong"}))
            return

        # 2. Transaction responses
        rid = msg.get("id") or (msg.get("data", {}).get("id") if isinstance(msg.get("data"), dict) else None)
        if rid is not None:
            fut = self._pending.get(rid) or self._pending.get(str(rid))
            if fut and not fut.done():
                fut.set_result(msg)
                return

        # 3. Lighter WebSocket Channel updates
        if mtype.startswith("update/") or mtype.startswith("subscribed/"):
            channel = msg.get("channel", "")
            is_snap = mtype.startswith("subscribed/")

            if mtype in ("update/ticker", "subscribed/ticker") or "ticker" in channel:
                t = msg.get("ticker", {})
                a = t.get("a", {})
                b = t.get("b", {})
                contents = {
                    "bestBid": {"price": b.get("price"), "size": b.get("size")},
                    "bestAsk": {"price": a.get("price"), "size": a.get("size")},
                }
                self.on_channel("bbo", contents, is_snap)
                return

            if mtype in ("update/order_book", "subscribed/order_book") or "order_book" in channel:
                ob = msg.get("order_book", {})
                bids = [[x.get("price"), x.get("size")] for x in ob.get("bids", [])]
                asks = [[x.get("price"), x.get("size")] for x in ob.get("asks", [])]
                self.on_channel("l2Orderbook", {"bids": bids, "asks": asks}, is_snap)
                return

            if mtype in ("update/trade", "subscribed/trade") or "trade" in channel:
                trades_raw = msg.get("trades", [])
                trades_clean = []
                for tr in trades_raw:
                    is_maker_ask = tr.get("is_maker_ask", False)
                    trade_side = "BUY" if is_maker_ask else "SELL"
                    trades_clean.append({
                        "side": tr.get("side", trade_side),
                        "price": tr.get("price"),
                        "size": tr.get("size"),
                        "timestamp": tr.get("timestamp"),
                    })
                self.on_channel("trades", trades_clean, is_snap)
                return

            if mtype in ("update/market_stats", "subscribed/market_stats") or "market_stats" in channel:
                ms = msg.get("market_stats", {})
                self.on_channel("market_stats", ms, is_snap)
                return

            if mtype in ("update/account_orders", "subscribed/account_orders") or "account_orders" in channel:
                orders_map = msg.get("orders", {})
                orders_list = []
                if isinstance(orders_map, dict):
                    for m_orders in orders_map.values():
                        if isinstance(m_orders, list):
                            orders_list.extend(m_orders)
                elif isinstance(orders_map, list):
                    orders_list = orders_map
                self.on_channel("orders", orders_list, is_snap)
                return

            if mtype in ("update/account_market", "subscribed/account_market") or "account_market" in channel:
                if "position" in msg:
                    self.on_channel("positions", msg["position"], is_snap)
                if "orders" in msg:
                    self.on_channel("orders", msg["orders"], is_snap)
                return

            if mtype in ("update/account_all", "subscribed/account_all") or "account_all" in channel:
                if "positions" in msg:
                    positions_list = list(msg["positions"].values()) if isinstance(msg["positions"], dict) else msg["positions"]
                    self.on_channel("positions", positions_list, is_snap)
                return

            self.on_channel(channel, msg, is_snap)
            return

        # 4. Standard Arcus channel compatibility
        if mtype in ("channel_data", "subscribed"):
            try:
                self.on_channel(msg.get("channel"), msg.get("contents"), mtype == "subscribed")
            except Exception:
                log.exception("channel handler error (%s)", msg.get("channel"))
            return

        if mtype in ("error", "degraded") or "error" in msg:
            log.warning("server message: %s", str(raw)[:300])

    async def reader(self) -> None:
        try:
            if not self.ws:
                return
            self.start_ping_loop()
            async for raw in self.ws:
                self.handle_message(raw)
        except (ConnectionClosed, ConnectionResetError, BrokenPipeError, OSError) as e:
            log.warning("WebSocket reader disconnected: %s", e)
        except Exception as e:
            log.warning("WebSocket reader unexpected error: %s", e)
        finally:
            self.stop_ping_loop()
            self._fail_all_pending("CONNECTION_CLOSED")
            self.ws = None

    async def fetch_markets(self, market: Optional[str] = None) -> list:
        def _get():
            url = f"{self.rest}/api/v1/orderBookDetails"
            req = urllib.request.Request(url, headers={"accept": "application/json", "User-Agent": "LighterBot/1.0"})
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    data = json.loads(r.read())
                    if "order_book_details" in data:
                        return data["order_book_details"]
                    if "markets" in data:
                        return data["markets"]
                    return data
            except Exception as e:
                alt_url = f"{self.rest}/v1/markets" + (f"?market={market}" if market else "")
                req2 = urllib.request.Request(alt_url, headers={"accept": "application/json", "User-Agent": "LighterBot/1.0"})
                try:
                    with urllib.request.urlopen(req2, timeout=10) as r2:
                        data2 = json.loads(r2.read())
                        return data2.get("markets", [])
                except Exception:
                    raise Fatal(f"Failed to fetch market details from {self.rest}: {e}")

        rows = await asyncio.to_thread(_get)
        if market:
            norm_mkt = market.upper().replace("/", "-")
            matched = [
                m for m in rows
                if str(m.get("symbol") or m.get("marketDisplayName") or m.get("name") or "").upper().replace("/", "-") == norm_mkt
            ]
            if not matched:
                log.warning("market %s not directly matched in orderBookDetails; using default profile", market)
                return [{"market_id": 0, "symbol": market, "price_decimals": 2, "size_decimals": 4, "status": "active"}]
            return matched
        return rows
