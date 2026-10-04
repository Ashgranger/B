"""Request and Transaction Signing for Robinhood Lighter Perp DEX (apidocs.rh.lighter.xyz).

Lighter on Robinhood Chain uses Ed25519 signing keys with L2 Chain ID 466324 (Robinhood Chain).
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Dict, Any, List, Optional
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from market import Market
from utils import canonical, fmt, to_int, BUY, SELL, SigningError


class LighterSigner:
    # Lighter L2 Transaction Types
    TX_TYPE_CREATE_ORDER = 1
    TX_TYPE_CANCEL_ORDER = 2
    TX_TYPE_CANCEL_ALL = 3
    TX_TYPE_MODIFY_ORDER = 4

    # Order Sides
    SIDE_BUY = 0
    SIDE_SELL = 1

    # Time-in-Force (Lighter protocol definitions)
    TIF_IOC = 0  # Immediate-or-Cancel
    TIF_GTC = 1  # Good-till-Time / Good-till-Cancelled
    TIF_ALO = 2  # Post-Only / Add Liquidity Only

    # Order Types
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TYPE_STOP_LOSS = 2
    ORDER_TYPE_STOP_LIMIT = 3
    ORDER_TYPE_TAKE_PROFIT = 4

    def __init__(self, key_hex: str, address: str, account_index: int = 0,
                 api_key_index: int = 4, chain_id: int = 466324):
        try:
            self.priv = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(key_hex))
        except Exception as e:
            raise SigningError(f"Invalid Ed25519 private key hex: {e}")
        pub = self.priv.public_key()
        if hasattr(pub, "public_bytes_raw"):
            self.api_key = pub.public_bytes_raw().hex()
        else:
            self.api_key = pub.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()
        self.address = address
        self.addr_lc = address.lower()
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.chain_id = chain_id
        self._nonce = 0
        self._last_ts = 0

    def set_nonce(self, nonce: int) -> None:
        """Synchronize with Lighter exchange sequencer next_nonce."""
        self._nonce = max(self._nonce, nonce)

    def get_and_increment_nonce(self) -> int:
        n = self._nonce
        self._nonce += 1
        return n

    def next_ts(self) -> int:
        self._last_ts = max(time.time_ns(), self._last_ts + 1)
        return self._last_ts

    def sign_bytes(self, message: bytes) -> str:
        return self.priv.sign(message).hex()

    def sign_json(self, payload: Dict[str, Any]) -> str:
        raw = canonical(payload).encode("utf-8")
        return self.sign_bytes(raw)

    def sign_create_order(
        self,
        m: Market,
        side: str,
        price: Decimal,
        size: Decimal,
        client_order_id: int,
        post_only: bool = True,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        """Sign a CreateOrder transaction for Robinhood Lighter DEX."""
        nonce = self.get_and_increment_nonce()
        ts = self.next_ts()
        is_ask = (side == SELL)
        tif = self.TIF_ALO if post_only else self.TIF_IOC

        price_int = to_int(price, m.tick_size)
        size_int = to_int(size, m.step_size)

        tx_info = {
            "chain_id": self.chain_id,
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "market_id": m.market_id,
            "client_order_id": client_order_id,
            "is_ask": 1 if is_ask else 0,
            "order_type": self.ORDER_TYPE_LIMIT,
            "time_in_force": tif,
            "reduce_only": 1 if reduce_only else 0,
            "price": price_int,
            "base_amount": size_int,
            "trigger_price": 0,
            "order_expiry": 0,
            "nonce": nonce,
            "timestamp": ts,
        }
        sig = self.sign_json(tx_info)
        tx_info["signature"] = sig

        return {
            "tx_type": self.TX_TYPE_CREATE_ORDER,
            "tx_info": tx_info,
            "id": f"ord-{client_order_id}-{nonce}",
        }

    def sign_modify_order(
        self,
        m: Market,
        order_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        client_order_id: Optional[int] = None,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        """Sign a ModifyOrder transaction for Robinhood Lighter DEX."""
        nonce = self.get_and_increment_nonce()
        ts = self.next_ts()
        is_ask = (side == SELL)
        price_int = to_int(price, m.tick_size)
        size_int = to_int(size, m.step_size)

        tx_info = {
            "chain_id": self.chain_id,
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "market_id": m.market_id,
            "order_id": str(order_id),
            "client_order_id": client_order_id or 0,
            "is_ask": 1 if is_ask else 0,
            "price": price_int,
            "base_amount": size_int,
            "reduce_only": 1 if reduce_only else 0,
            "nonce": nonce,
            "timestamp": ts,
        }
        sig = self.sign_json(tx_info)
        tx_info["signature"] = sig

        return {
            "tx_type": self.TX_TYPE_MODIFY_ORDER,
            "tx_info": tx_info,
            "id": f"mod-{order_id}-{nonce}",
        }

    def sign_cancel_order(self, m: Market, order_id: str, client_order_id: Optional[int] = None) -> Dict[str, Any]:
        """Sign a CancelOrder transaction for Robinhood Lighter DEX."""
        nonce = self.get_and_increment_nonce()
        ts = self.next_ts()

        tx_info = {
            "chain_id": self.chain_id,
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "market_id": m.market_id,
            "order_id": str(order_id),
            "client_order_id": client_order_id or 0,
            "nonce": nonce,
            "timestamp": ts,
        }
        sig = self.sign_json(tx_info)
        tx_info["signature"] = sig

        return {
            "tx_type": self.TX_TYPE_CANCEL_ORDER,
            "tx_info": tx_info,
            "id": f"cnc-{order_id}-{nonce}",
        }

    def sign_batch(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Combine multiple signed transactions into a single sendTxBatch envelope."""
        tx_types = [item["tx_type"] for item in items]
        tx_infos = [item["tx_info"] for item in items]
        batch_id = f"batch-{self.next_ts()}"
        return {
            "type": "jsonapi/sendtxbatch",
            "data": {
                "id": batch_id,
                "tx_types": tx_types,
                "tx_infos": tx_infos,
            },
        }
