"""WebSocket-First Transport for Robinhood Lighter Perp DEX (apidocs.rh.lighter.xyz).

Handles all exchange communication over WebSocket:
- Market data streaming (orderbook 50ms batching, public trades, BBO, stats)
- User private data streaming (account orders, positions, balances, tx status)
- Transaction mutations (order creation, batch cancels, modifies via jsonapi/sendtx over WS)
- Automatic keepalive, sequence management, and connection resilience
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
import urllib.request
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from config import ENVS, Config
from utils import Fatal, BUY, SELL

log = logging.getLogger("exchange")


class LighterExchange:
    def __init__(self, cfg: Config, on_channel: Callable[[str, Any, bool], None]):
        self.cfg = cfg
        self.rest = ENVS[cfg.env_name]["rest"]
        self.ws_url = ENVS[cfg.env_name]["ws"]
        self.on_channel = on_channel
        self.ws = None
        self._ids = itertools.count(1)
        self._pending: Dict[str, asyncio.Future] = {}
        self._dry_seq = itertools.count(1)
        self._actions_window: list = []
        self._is_connected = False
        self._keepalive_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    async def _send(self, obj: dict) -> None:
        if self.ws:
            await self.ws.send(json.dumps(obj))

    def _check_rate_limit(self) -> bool:
        now = time.monotonic()
        self._actions_window = [t for t in self._actions_window if now - t < 60.0]
        if len(self._actions_window) >= self.cfg.max_actions_per_min:
            log.warning("⚠️ Rate limit reached (%d actions/min)! Pausing action.", self.cfg.max_actions_per_min)
            return False
        self._actions_window.append(now)
        return True

    async def send_tx_ws(self, tx_type: int, tx_info: dict, timeout: float = 8.0) -> dict:
        """Send a single signed transaction over WebSocket using jsonapi/sendtx."""
        if not self._check_rate_limit():
            return {"status": 429, "error": "rate_limit_exceeded"}

        if self.cfg.dry_run:
            dry_id = f"dry-{next(self._dry_seq)}"
            log.debug("DRY sendtx type=%d id=%s", tx_type, dry_id)
            return {"status": 200, "result": {"order_id": dry_id, "status": "ACK"}}

        rid = f"tx-{next(self._ids)}-{int(time.time()*1000)}"
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            req = {
                "type": "jsonapi/sendtx",
                "data": {
                    "id": rid,
                    "tx_type": tx_type,
                    "tx_info": tx_info,
                }
            }
            await self._send(req)
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            log.warning("WebSocket sendtx timeout (%s)", rid)
            return {"status": 408, "error": "timeout"}
        finally:
            self._pending.pop(rid, None)

    async def send_batch_tx_ws(self, tx_types: List[int], tx_infos: List[dict], timeout: float = 8.0) -> dict:
        """Send a batch of signed transactions over WebSocket using jsonapi/sendtxbatch."""
        if not tx_types:
            return {"status": 200, "result": []}

        if not self._check_rate_limit():
            return {"status": 429, "error": "rate_limit_exceeded"}

        if self.cfg.dry_run:
            dry_ids = [f"dry-{next(self._dry_seq)}" for _ in tx_types]
            log.debug("DRY sendtxbatch count=%d ids=%s", len(tx_types), dry_ids)
            return {"status": 200, "result": [{"order_id": oid, "status": "ACK"} for oid in dry_ids]}

        rid = f"batch-{next(self._ids)}-{int(time.time()*1000)}"
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            req = {
                "type": "jsonapi/sendtxbatch",
                "data": {
                    "id": rid,
                    "tx_types": tx_types,
                    "tx_infos": tx_infos,
                }
            }
            await self._send(req)
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            log.warning("WebSocket sendtxbatch timeout (%s)", rid)
            return {"status": 408, "error": "timeout"}
        finally:
            self._pending.pop(rid, None)

    async def subscribe_all(self, market_id: int, account_id: int, auth_token: str = "") -> None:
        """Subscribe to all necessary Robinhood Lighter WebSocket channels."""
        channels = [
            f"order_book/{market_id}",
            f"trade/{market_id}",
            f"bbo/{market_id}",
            f"market_stats/{market_id}",
            f"account_orders/{market_id}/{account_id}",
            f"account_all_positions/{account_id}",
            f"account_all_assets/{account_id}",
            f"account_tx/{account_id}",
        ]
        for ch in channels:
            sub_msg = {"type": "subscribe", "channel": ch}
            if "account" in ch and auth_token:
                sub_msg["auth"] = auth_token
            await self._send(sub_msg)
            log.info("📡 Subscribed to Robinhood Lighter WS channel: %s", ch)

    def handle_message(self, raw: str) -> None:
        """Process incoming WebSocket messages (market data, account events, tx confirmations)."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        mtype = msg.get("type", "")
        # Transaction responses
        rid = msg.get("id") or (msg.get("data", {}).get("id") if isinstance(msg.get("data"), dict) else None)
        if rid and rid in self._pending:
            fut = self._pending[rid]
            if not fut.done():
                fut.set_result(msg)
            return

        # Market data & Account stream updates
        if mtype.startswith("update/") or mtype.startswith("subscribed/") or mtype in ("channel_data", "order_book", "trade"):
            ch = msg.get("channel") or mtype.split("/")[-1]
            contents = msg.get("data") or msg.get("contents") or msg
            self.on_channel(ch, contents, mtype.startswith("subscribed/"))
            return

        if "error" in msg or mtype == "error":
            log.warning("Robinhood Lighter WS message alert: %s", str(raw)[:300])

    async def _keepalive_loop(self) -> None:
        """Keepalive heartbeat sending a ping every 60s (Lighter requires at least 1 frame every 2 mins)."""
        while self._is_connected:
            try:
                await asyncio.sleep(60.0)
                if self.ws:
                    await self._send({"type": "ping"})
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("Keepalive ping exception: %s", e)

    async def reader(self) -> None:
        """Continuously reads frames from WebSocket connection."""
        self._is_connected = True
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        try:
            async for raw in self.ws:
                self.handle_message(raw)
        finally:
            self._is_connected = False
            if self._keepalive_task:
                self._keepalive_task.cancel()

    async def fetch_market_details(self, market_id: int) -> dict:
        """Fetch market static configuration from Robinhood Lighter REST API."""
        def _get():
            url = f"{self.rest}/orderBookDetails?market_id={market_id}"
            try:
                with urllib.request.urlopen(url, timeout=10) as r:
                    return json.loads(r.read())
            except Exception as e:
                log.warning("REST orderBookDetails fetch failed: %s. Using default specifications.", e)
                return {
                    "market_id": market_id,
                    "symbol": self.cfg.market,
                    "tick_size": "0.1",
                    "step_size": "0.001",
                    "min_size": "0.001",
                    "min_notional": "5.0",
                    "status": "ONLINE"
                }

        data = await asyncio.to_thread(_get)
        return data

    async def fetch_next_nonce(self, account_index: int, api_key_index: int) -> int:
        """Fetch next valid sequencer nonce from Robinhood Lighter REST API."""
        def _get():
            url = f"{self.rest}/nextNonce?account_index={account_index}&api_key_index={api_key_index}"
            try:
                with urllib.request.urlopen(url, timeout=10) as r:
                    data = json.loads(r.read())
                    return int(data.get("next_nonce", 0))
            except Exception as e:
                log.warning("REST nextNonce query failed: %s. Starting from nonce 0.", e)
                return 0

        nonce = await asyncio.to_thread(_get)
        return nonce
