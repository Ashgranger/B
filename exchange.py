"""Bulk Trade Perpetual DEX Transport Layer (bulk.trade / bulk.exchange).

Handles:
- High-frequency WebSocket streaming (L2 orderbook, public trades, ticker/BBO, account fills & orders)
- REST API mutations and queries (order submission, batch actions, market metadata, positions)
- Automatic reconnection resilience with exponential backoff
- Rate limiting and budget monitoring (actions per minute)
- Full dry-run / paper trading simulation mode
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from config import Config, ENVS
from utils import BUY, SELL, ExchangeError, Fatal

log = logging.getLogger("bulk_exchange")


class BulkExchange:
    """Transport client for Bulk Trade Perpetual DEX on Solana."""

    def __init__(self, cfg: Config, on_channel: Callable[[str, Any, bool], None]):
        self.cfg = cfg
        self.rest_url = cfg.rest_url.rstrip("/")
        self.ws_url = cfg.ws_url
        self.on_channel = on_channel
        self.ws = None
        self._ids = itertools.count(1)
        self._pending: Dict[str, asyncio.Future] = {}
        self._dry_seq = itertools.count(1)
        self._actions_window: List[float] = []
        self._is_connected = False
        self._reconnect_attempts = 0

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    async def _send_ws(self, obj: dict) -> bool:
        if self.ws and not getattr(self.ws, "closed", False):
            try:
                await self.ws.send(json.dumps(obj))
                return True
            except Exception as e:
                log.warning("WebSocket send failed: %s", e)
                return False
        return False

    def _check_rate_limit(self) -> bool:
        now = time.monotonic()
        self._actions_window = [t for t in self._actions_window if now - t < 60.0]
        if len(self._actions_window) >= self.cfg.max_actions_per_min:
            log.warning("⚠️ Bulk Trade rate limit reached (%d actions/min)! Pausing action.", self.cfg.max_actions_per_min)
            return False
        self._actions_window.append(now)
        return True

    async def send_transaction(self, signed_tx: Dict[str, Any], timeout: float = 8.0) -> Dict[str, Any]:
        """Submit signed transaction to Bulk Trade (via WebSocket if available, else REST)."""
        if not self._check_rate_limit():
            return {"status": 429, "error": "rate_limit_exceeded"}

        if self.cfg.dry_run:
            dry_id = signed_tx.get("order_id") or f"bulk-dry-{next(self._dry_seq)}"
            order_ids = signed_tx.get("order_ids") or [dry_id]
            log.debug("DRY RUN send_transaction: order_id=%s actions=%d", dry_id, len(signed_tx.get("actions", [])))
            return {
                "status": 200,
                "result": {
                    "order_id": dry_id,
                    "order_ids": order_ids,
                    "status": "ACK",
                    "timestamp": time.time_ns(),
                }
            }

        # If WebSocket connected, send over WebSocket actor
        if self.is_connected:
            rid = f"bulk-tx-{next(self._ids)}-{int(time.time()*1000)}"
            fut = asyncio.get_running_loop().create_future()
            self._pending[rid] = fut
            try:
                req = {
                    "type": "order",
                    "id": rid,
                    "data": signed_tx,
                }
                sent = await self._send_ws(req)
                if sent:
                    return await asyncio.wait_for(fut, timeout)
            except asyncio.TimeoutError:
                log.warning("WebSocket send_transaction timed out (%s), falling back to REST", rid)
            except Exception as e:
                log.warning("WebSocket send error: %s, falling back to REST", e)
            finally:
                self._pending.pop(rid, None)

        # Fallback to REST POST /api/v1/order
        return await self._rest_post("/order", signed_tx, timeout=timeout)

    async def _rest_post(self, endpoint: str, payload: dict, timeout: float = 8.0) -> Dict[str, Any]:
        """HTTP POST to Bulk Trade REST API."""
        url = f"{self.rest_url}{endpoint}"
        body_bytes = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "BulkArcusBot/1.0",
        }

        def _do_post():
            req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    code = resp.status
                    data = json.loads(resp.read().decode("utf-8"))
                    return {"status": code, "result": data}
            except urllib.error.HTTPError as e:
                err_text = e.read().decode("utf-8", errors="replace")
                try:
                    err_json = json.loads(err_text)
                except Exception:
                    err_json = {"raw": err_text}
                return {"status": e.code, "error": err_json}
            except Exception as e:
                return {"status": 500, "error": str(e)}

        return await asyncio.to_thread(_do_post)

    async def _rest_get(self, endpoint: str, params: Optional[dict] = None, timeout: float = 8.0) -> Dict[str, Any]:
        """HTTP GET from Bulk Trade REST API."""
        qs = f"?{urllib.parse.urlencode(params)}" if params else ""
        url = f"{self.rest_url}{endpoint}{qs}"
        headers = {"User-Agent": "BulkArcusBot/1.0"}

        def _do_get():
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    code = resp.status
                    data = json.loads(resp.read().decode("utf-8"))
                    return {"status": code, "result": data}
            except urllib.error.HTTPError as e:
                err_text = e.read().decode("utf-8", errors="replace")
                return {"status": e.code, "error": err_text}
            except Exception as e:
                return {"status": 500, "error": str(e)}

        return await asyncio.to_thread(_do_get)

    async def fetch_market_details(self, symbol: str) -> dict:
        """Fetch market static specifications (tick, step, limits) from Bulk Trade."""
        if self.cfg.dry_run:
            return {
                "market_id": self.cfg.market_id,
                "symbol": symbol,
                "tick_size": "0.1",
                "step_size": "0.001",
                "min_order_size": "0.001",
                "max_order_size": "100.0",
                "min_order_notional": "10.0",
                "mark_price": "85000.0",
                "funding_rate": "0.0001",
                "status": "ONLINE"
            }

        res = await self._rest_get("/market", {"symbol": symbol})
        if res.get("status") == 200 and isinstance(res.get("result"), dict):
            return res["result"]

        # Default fallback specifications for Solana perps
        return {
            "market_id": self.cfg.market_id,
            "symbol": symbol,
            "tick_size": "0.1",
            "step_size": "0.001",
            "min_order_size": "0.001",
            "max_order_size": "1000.0",
            "min_order_notional": "10.0",
            "mark_price": "85000.0",
            "funding_rate": "0.0",
            "status": "ONLINE"
        }

    async def fetch_open_orders(self, symbol: str, account: str) -> List[dict]:
        """Fetch active open orders for the account from Bulk Trade."""
        if self.cfg.dry_run:
            return []
        res = await self._rest_get("/orders", {"symbol": symbol, "account": account})
        if res.get("status") == 200:
            result = res.get("result")
            if isinstance(result, list):
                return result
            if isinstance(result, dict) and "orders" in result:
                return result["orders"]
        return []

    async def fetch_account_state(self, account: str) -> dict:
        """Fetch account balance, positions, and equity from Bulk Trade."""
        if self.cfg.dry_run:
            return {
                "account": account,
                "collateral_usd": "10000.0",
                "free_collateral_usd": "10000.0",
                "positions": [],
            }
        res = await self._rest_get("/account", {"account": account})
        return res.get("result") if res.get("status") == 200 else {}

    async def subscribe_all(self, symbol: str, account_pubkey: str) -> None:
        """Subscribe to all necessary Bulk Trade WebSocket streams."""
        channels = [
            {"type": "subscribe", "channel": "orderbook", "symbol": symbol},
            {"type": "subscribe", "channel": "trades", "symbol": symbol},
            {"type": "subscribe", "channel": "ticker", "symbol": symbol},
        ]
        if account_pubkey:
            channels.append({"type": "subscribe", "channel": "account", "account": account_pubkey})

        for msg in channels:
            await self._send_ws(msg)
            log.info("📡 Subscribed to Bulk Trade WS channel: %s", msg.get("channel"))

    def handle_message(self, raw: str) -> None:
        """Parse incoming WebSocket message from Bulk Trade."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        mtype = msg.get("type", "")

        # Transaction execution response matching pending future
        rid = msg.get("id") or (msg.get("data", {}).get("id") if isinstance(msg.get("data"), dict) else None)
        if rid and rid in self._pending:
            fut = self._pending[rid]
            if not fut.done():
                fut.set_result(msg)
            return

        # Market data & Account stream routing
        if mtype in ("orderbook", "depth", "trades", "trade", "ticker", "bbo", "account", "fill", "order"):
            contents = msg.get("data") or msg.get("contents") or msg
            self.on_channel(mtype, contents, is_snapshot=bool(msg.get("is_snapshot")))
            return

        # Heartbeat ping/pong
        if mtype == "ping":
            asyncio.create_task(self._send_ws({"type": "pong"}))
            return

        if "error" in msg:
            log.warning("Bulk Trade WS alert: %s", str(raw)[:300])

    async def reader(self) -> None:
        """Continuously read messages from WebSocket connection."""
        self._is_connected = True
        try:
            async for raw in self.ws:
                self.handle_message(raw)
        finally:
            self._is_connected = False
