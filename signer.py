"""Request signing and transaction construction per Lighter Perpetual DEX & Robinhood Chain docs."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from decimal import Decimal
from typing import Any, Optional

from market import Market
from utils import canonical, fmt, to_lighter_int, Fatal

log = logging.getLogger("signer")

# --- Lighter Protocol Constants (ported from lighter-go / txtypes / constants.go) --- #
TX_TYPE_CHANGE_PUB_KEY = 8
TX_TYPE_CREATE_SUB_ACCOUNT = 9
TX_TYPE_CREATE_PUBLIC_POOL = 10
TX_TYPE_UPDATE_PUBLIC_POOL = 11
TX_TYPE_TRANSFER = 12
TX_TYPE_WITHDRAW = 13
TX_TYPE_CREATE_ORDER = 14
TX_TYPE_CANCEL_ORDER = 15
TX_TYPE_CANCEL_ALL_ORDERS = 16
TX_TYPE_MODIFY_ORDER = 17
TX_TYPE_UPDATE_LEVERAGE = 20
TX_TYPE_CREATE_GROUPED_ORDERS = 28
TX_TYPE_UPDATE_MARGIN = 29

# Order Types
ORDER_TYPE_LIMIT = 0
ORDER_TYPE_MARKET = 1
ORDER_TYPE_STOP_LOSS = 2
ORDER_TYPE_STOP_LOSS_LIMIT = 3
ORDER_TYPE_TAKE_PROFIT = 4
ORDER_TYPE_TAKE_PROFIT_LIMIT = 5
ORDER_TYPE_TWAP = 6

# Time In Force (TIF)
TIF_IOC = 0         # Immediate or Cancel
TIF_GTT = 1         # Good till Time
TIF_POST_ONLY = 2   # Post-Only / ALO (Add Liquidity Only / Maker Only)

# Side
SIDE_BUY = 0
SIDE_SELL = 1

# Cancel All Time In Force
CANCEL_ALL_IMMEDIATE = 0
CANCEL_ALL_SCHEDULED = 1   # Dead Man's Switch (Scheduled cancel all)
CANCEL_ALL_ABORT = 2

# Self Trade Behavior
STB_EXPIRE_MAKER = 0
STB_EXPIRE_TAKER = 1
STB_EXPIRE_BOTH = 2
STB_REDUCE = 3

# Order Status
STATUS_IN_PROGRESS = 0
STATUS_PENDING = 1
STATUS_ACTIVE_LIMIT = 2
STATUS_FILLED = 3
STATUS_CANCELED = 4
STATUS_CANCELED_POST_ONLY = 5
STATUS_CANCELED_REDUCE_ONLY = 6
STATUS_CANCELED_EXPIRED = 12


class Signer:
    # Class-level mappings for backward compatibility
    OP_PLACE = TX_TYPE_CREATE_ORDER
    OP_CANCEL = TX_TYPE_CANCEL_ORDER
    OP_MODIFY = TX_TYPE_MODIFY_ORDER
    SIDE = {"BUY": SIDE_BUY, "SELL": SIDE_SELL, "bid": SIDE_BUY, "ask": SIDE_SELL}
    TIF_IOC = TIF_IOC
    TIF_GTT = TIF_GTT
    TIF_POST_ONLY = TIF_POST_ONLY
    TIF_ALO = TIF_POST_ONLY
    TIF_FOK = TIF_IOC
    TIF_GTC = TIF_GTT

    def __init__(self, key_hex: str, address: str, account_index: int = 0, api_key_index: int = 4,
                 url: str = "https://api.rh.lighter.xyz"):
        self.key_hex = key_hex.removeprefix("0x").lower()
        self.address = address
        self.addr_lc = address.lower()
        self.account_index = int(account_index)
        self.api_key_index = int(api_key_index)
        self.url = url
        self.ai = self.account_index

        self._nonce = 0
        self._nonce_initialized = False
        self._client_order_seq = int(time.time() * 1000) % (2 ** 32)
        self._last_ts = 0

        self._pub_key_hex = ""
        self._priv = None
        self._lighter_client = None
        self._init_keys()

    def _init_keys(self) -> None:
        try:
            raw_bytes = bytes.fromhex(self.key_hex)
        except Exception:
            raw_bytes = self.key_hex.encode()

        # 1. Try native lighter SDK SignerClient if available
        try:
            import lighter
            self._lighter_client = lighter.SignerClient(
                url=self.url,
                api_private_keys={self.api_key_index: self.key_hex},
                account_index=self.account_index,
            )
            log.info("Initialized native Lighter SignerClient successfully (API Key Index %d)", self.api_key_index)
        except Exception as e:
            self._lighter_client = None
            log.debug("Native Lighter SignerClient not loaded (%s); using pure-Python engine", e)

        # 2. Key derivation for tests and pure-Python execution
        try:
            from cryptography.hazmat.primitives.asymmetric import ed25519
            from cryptography.hazmat.primitives import serialization
            # If 40 bytes (80 hex chars, Robinhood Lighter native key), derive 32-byte seed
            seed = raw_bytes if len(raw_bytes) == 32 else hashlib.sha256(raw_bytes).digest()
            priv = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            pub = priv.public_key()
            self._pub_key_hex = pub.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()
            self._priv = priv
        except Exception:
            h = hashlib.sha256(raw_bytes).hexdigest()
            self._pub_key_hex = h
            self._priv = None

    @property
    def priv(self):
        """Getter for internal key pair (for verification in tests)."""
        return self._priv

    @property
    def api_key(self) -> str:
        return self._pub_key_hex

    def set_nonce(self, next_nonce: int) -> None:
        """Synchronize with exchange GET /api/v1/nextNonce."""
        self._nonce = max(self._nonce, int(next_nonce))
        self._nonce_initialized = True

    def next_nonce(self) -> int:
        """Get the next sequential nonce for this API key."""
        n = self._nonce
        self._nonce += 1
        return n

    def next_client_order_index(self) -> int:
        """Generate a unique uint48 client order index (≤ 2^48 - 1)."""
        self._client_order_seq += 1
        now_ms = int(time.time() * 1000) & 0xFFFFFFFFFF
        return ((now_ms % 1000000) * 10000 + (self._client_order_seq % 10000)) & 0xFFFFFFFFFFFF

    def next_ts(self) -> int:
        self._last_ts = max(time.time_ns(), self._last_ts + 1)
        return self._last_ts

    def sign_hash(self, message: bytes) -> str:
        """Generate signature for transaction payload."""
        if self._priv is not None:
            try:
                return self._priv.sign(message).hex()
            except Exception:
                pass
        try:
            raw_bytes = bytes.fromhex(self.key_hex)
        except Exception:
            raw_bytes = self.key_hex.encode()
        return hmac.new(raw_bytes, message, hashlib.sha256).hexdigest()

    def create_auth_token(self, deadline_s: int = 28800) -> str:
        """Generate an auth token for private WebSocket channels and REST queries."""
        if self._lighter_client is not None:
            try:
                token, err = self._lighter_client.create_auth_token_with_expiry(
                    deadline=deadline_s,
                    api_key_index=self.api_key_index
                )
                if not err and token:
                    return str(token)
            except Exception as e:
                log.warning("native create_auth_token error: %s", e)

        exp = int(time.time()) + deadline_s
        payload = f"{self.addr_lc}:{self.account_index}:{self.api_key_index}:{exp}"
        sig = self.sign_hash(payload.encode())
        return f"{payload}:{sig}"

    def place(self, m: Market, side: str, px: Decimal, qty: Decimal, good_til_us: int,
              time_in_force: str = "ALO", reduce_only: bool = False,
              client_order_index: Optional[int] = None) -> dict:
        """
        Construct and sign a Limit/Market Order on Robinhood Lighter perpetual DEX.
        Maps time_in_force:
          'IOC' -> TIF_IOC (0)
          'GTT' / 'GTC' -> TIF_GTT (1)
          'ALO' / 'POST_ONLY' -> TIF_POST_ONLY (2)
        """
        tif_str = time_in_force.upper()
        if tif_str in ("IOC", "0"):
            tif_code = TIF_IOC
            order_type = ORDER_TYPE_LIMIT
            order_expiry = 0
        elif tif_str in ("GTT", "GTC", "1"):
            tif_code = TIF_GTT
            order_type = ORDER_TYPE_LIMIT
            order_expiry = max(int(time.time() * 1000) + 300_000, int(good_til_us / 1000))
        else:  # ALO / POST_ONLY / 2
            tif_code = TIF_POST_ONLY
            order_type = ORDER_TYPE_LIMIT
            order_expiry = max(int(time.time() * 1000) + 300_000, int(good_til_us / 1000))

        is_ask = SIDE_SELL if side.upper() in ("SELL", "ASK") else SIDE_BUY
        c_order_idx = client_order_index if client_order_index is not None else self.next_client_order_index()
        nonce = self.next_nonce()

        price_decimals = getattr(m, "price_decimals", 2)
        size_decimals = getattr(m, "size_decimals", 4)
        int_price = to_lighter_int(px, price_decimals)
        int_base_amount = to_lighter_int(qty, size_decimals)

        # Delegate to native Lighter SDK signer if available
        if self._lighter_client is not None:
            try:
                tx_type, tx_info, err = self._lighter_client.sign_create_order(
                    market_index=m.market_id,
                    client_order_index=c_order_idx,
                    base_amount=int_base_amount,
                    price=int_price,
                    is_ask=(1 if is_ask else 0),
                    order_type=order_type,
                    time_in_force=tif_code,
                    reduce_only=(1 if reduce_only else 0),
                    order_expiry=order_expiry,
                    nonce=nonce,
                    api_key_index=self.api_key_index,
                )
                if not err and tx_info:
                    return {
                        "type": "placeOrder",
                        "tx_type": int(tx_type) if tx_type is not None else TX_TYPE_CREATE_ORDER,
                        "tx_info": tx_info,
                        "clientOrderIndex": c_order_idx,
                        "orderId": str(c_order_idx),
                        "payload": {
                            "address": self.address, "accountIndex": self.account_index, "apiKeyIndex": self.api_key_index,
                            "marketId": m.market_id, "clientOrderIndex": c_order_idx, "orderSide": side.upper(),
                            "isAsk": bool(is_ask), "quantity": fmt(qty), "price": fmt(px), "timeInForce": "post-only" if tif_code == TIF_POST_ONLY else "immediate-or-cancel",
                            "reduceOnly": bool(reduce_only), "nonce": nonce, "tx_type": TX_TYPE_CREATE_ORDER, "tx_info": tx_info
                        },
                        "apiKey": self.api_key,
                        "signature": "",
                        "timestamp": str(self.next_ts()),
                    }
            except Exception as e:
                log.debug("Native sign_create_order fallback to python: %s", e)

        # Pure-Python signer
        tx_info = {
            "AccountIndex": self.account_index,
            "ApiKeyIndex": self.api_key_index,
            "MarketIndex": m.market_id,
            "ClientOrderIndex": c_order_idx,
            "BaseAmount": int_base_amount,
            "Price": int_price,
            "IsAsk": is_ask,
            "OrderType": order_type,
            "TimeInForce": tif_code,
            "ReduceOnly": 1 if reduce_only else 0,
            "TriggerPrice": 0,
            "OrderExpiry": order_expiry,
            "Nonce": nonce,
            "SelfTradeBehaviorMode": STB_EXPIRE_MAKER,
            "SelfTradeEqualityMode": 0,
        }

        canonical_info = canonical(tx_info)
        sig = self.sign_hash(canonical_info.encode())
        tx_info["Sig"] = sig

        body = {
            "address": self.address,
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "clientOrderIndex": c_order_idx,
            "orderSide": side.upper(),
            "isAsk": bool(is_ask),
            "orderType": "limit" if order_type == ORDER_TYPE_LIMIT else "market",
            "timeInForce": "post-only" if tif_code == TIF_POST_ONLY else ("immediate-or-cancel" if tif_code == TIF_IOC else "good-till-time"),
            "quantity": fmt(qty),
            "price": fmt(px),
            "reduceOnly": bool(reduce_only),
            "orderExpiry": order_expiry,
            "nonce": nonce,
            "tx_type": TX_TYPE_CREATE_ORDER,
            "tx_info": tx_info,
        }

        return {
            "type": "placeOrder",
            "tx_type": TX_TYPE_CREATE_ORDER,
            "tx_info": tx_info,
            "payload": body,
            "clientOrderIndex": c_order_idx,
            "orderId": str(c_order_idx),
            "apiKey": self.api_key,
            "signature": sig,
            "timestamp": str(self.next_ts()),
        }

    def modify(self, m: Market, order_id: str | int, side: str, px: Decimal, qty: Decimal,
               good_til_us: int, reduce_only: bool = False, order_version: int = 0) -> dict:
        c_order_idx = int(str(order_id).replace("dry-", "").replace("ord-", "").replace("sim-", "") or 0)
        nonce = self.next_nonce()

        price_decimals = getattr(m, "price_decimals", 2)
        size_decimals = getattr(m, "size_decimals", 4)
        int_price = to_lighter_int(px, price_decimals)
        int_base_amount = to_lighter_int(qty, size_decimals)

        if self._lighter_client is not None:
            try:
                tx_type, tx_info, err = self._lighter_client.sign_modify_order(
                    market_index=m.market_id,
                    order_index=c_order_idx,
                    base_amount=int_base_amount,
                    price=int_price,
                    order_version=order_version,
                    nonce=nonce,
                    api_key_index=self.api_key_index,
                )
                if not err and tx_info:
                    return {
                        "type": "modifyOrder",
                        "tx_type": int(tx_type) if tx_type is not None else TX_TYPE_MODIFY_ORDER,
                        "tx_info": tx_info,
                        "payload": {"marketId": m.market_id, "orderId": str(order_id), "price": fmt(px), "quantity": fmt(qty), "tx_type": TX_TYPE_MODIFY_ORDER},
                        "orderId": str(order_id),
                        "clientOrderIndex": c_order_idx,
                        "apiKey": self.api_key,
                        "signature": "",
                        "timestamp": str(self.next_ts()),
                    }
            except Exception as e:
                log.debug("Native sign_modify_order fallback: %s", e)

        tx_info = {
            "AccountIndex": self.account_index,
            "ApiKeyIndex": self.api_key_index,
            "MarketIndex": m.market_id,
            "ClientOrderIndex": c_order_idx,
            "BaseAmount": int_base_amount,
            "Price": int_price,
            "TriggerPrice": 0,
            "OrderVersion": order_version,
            "Nonce": nonce,
        }

        canonical_info = canonical(tx_info)
        sig = self.sign_hash(canonical_info.encode())
        tx_info["Sig"] = sig

        body = {
            "address": self.address,
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "orderId": str(order_id),
            "clientOrderIndex": c_order_idx,
            "side": side.upper(),
            "quantity": fmt(qty),
            "price": fmt(px),
            "timeInForce": "post-only",
            "reduceOnly": bool(reduce_only),
            "nonce": nonce,
            "tx_type": TX_TYPE_MODIFY_ORDER,
            "tx_info": tx_info,
        }

        return {
            "type": "modifyOrder",
            "tx_type": TX_TYPE_MODIFY_ORDER,
            "tx_info": tx_info,
            "payload": body,
            "orderId": str(order_id),
            "clientOrderIndex": c_order_idx,
            "apiKey": self.api_key,
            "signature": sig,
            "timestamp": str(self.next_ts()),
        }

    def cancel(self, m: Market, order_id: str | int) -> dict:
        c_order_idx = int(str(order_id).replace("dry-", "").replace("ord-", "").replace("sim-", "") or 0)
        nonce = self.next_nonce()

        if self._lighter_client is not None:
            try:
                tx_type, tx_info, err = self._lighter_client.sign_cancel_order(
                    market_index=m.market_id,
                    order_index=c_order_idx,
                    nonce=nonce,
                    api_key_index=self.api_key_index,
                )
                if not err and tx_info:
                    return {
                        "type": "cancelOrder",
                        "tx_type": int(tx_type) if tx_type is not None else TX_TYPE_CANCEL_ORDER,
                        "tx_info": tx_info,
                        "payload": {"marketId": m.market_id, "orderId": str(order_id), "clientOrderIndex": c_order_idx, "tx_type": TX_TYPE_CANCEL_ORDER},
                        "orderId": str(order_id),
                        "clientOrderIndex": c_order_idx,
                        "apiKey": self.api_key,
                        "signature": "",
                        "timestamp": str(self.next_ts()),
                    }
            except Exception as e:
                log.debug("Native sign_cancel_order fallback: %s", e)

        tx_info = {
            "AccountIndex": self.account_index,
            "ApiKeyIndex": self.api_key_index,
            "MarketIndex": m.market_id,
            "ClientOrderIndex": c_order_idx,
            "Nonce": nonce,
        }

        canonical_info = canonical(tx_info)
        sig = self.sign_hash(canonical_info.encode())
        tx_info["Sig"] = sig

        body = {
            "address": self.address,
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "orderId": str(order_id),
            "clientOrderIndex": c_order_idx,
            "nonce": nonce,
            "tx_type": TX_TYPE_CANCEL_ORDER,
            "tx_info": tx_info,
        }

        return {
            "type": "cancelOrder",
            "tx_type": TX_TYPE_CANCEL_ORDER,
            "tx_info": tx_info,
            "payload": body,
            "orderId": str(order_id),
            "clientOrderIndex": c_order_idx,
            "apiKey": self.api_key,
            "signature": sig,
            "timestamp": str(self.next_ts()),
        }

    def schedule_cancel(self, m: Market, deadline_us: Optional[int]) -> dict:
        nonce = self.next_nonce()
        if deadline_us is not None:
            tif = CANCEL_ALL_SCHEDULED
            expiry_ms = int(deadline_us / 1000)
        else:
            tif = CANCEL_ALL_ABORT
            expiry_ms = 0

        if self._lighter_client is not None:
            try:
                tx_type, tx_info, err = self._lighter_client.sign_cancel_all_orders(
                    time_in_force=tif,
                    timestamp_ms=expiry_ms,
                    cancel_all_market_index=m.market_id,
                    nonce=nonce,
                    api_key_index=self.api_key_index,
                )
                if not err and tx_info:
                    return {
                        "type": "scheduleCancel",
                        "tx_type": int(tx_type) if tx_type is not None else TX_TYPE_CANCEL_ALL_ORDERS,
                        "tx_info": tx_info,
                        "payload": {"marketId": m.market_id, "timeInForce": tif, "deadline_ms": expiry_ms, "tx_type": TX_TYPE_CANCEL_ALL_ORDERS},
                        "apiKey": self.api_key,
                        "signature": "",
                        "timestamp": str(self.next_ts()),
                    }
            except Exception as e:
                log.debug("Native sign_schedule_cancel fallback: %s", e)

        tx_info = {
            "AccountIndex": self.account_index,
            "ApiKeyIndex": self.api_key_index,
            "MarketIndex": m.market_id,
            "TimeInForce": tif,
            "CancelAllTime": expiry_ms,
            "Nonce": nonce,
        }

        canonical_info = canonical(tx_info)
        sig = self.sign_hash(canonical_info.encode())
        tx_info["Sig"] = sig

        body = {
            "address": self.address,
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "timeInForce": tif,
            "deadline_ms": expiry_ms,
            "nonce": nonce,
            "tx_type": TX_TYPE_CANCEL_ALL_ORDERS,
            "tx_info": tx_info,
        }

        return {
            "type": "scheduleCancel",
            "tx_type": TX_TYPE_CANCEL_ALL_ORDERS,
            "tx_info": tx_info,
            "payload": body,
            "apiKey": self.api_key,
            "signature": sig,
            "timestamp": str(self.next_ts()),
        }

    def cancel_all(self, m: Market, immediate: bool = True) -> dict:
        nonce = self.next_nonce()
        tif = CANCEL_ALL_IMMEDIATE if immediate else CANCEL_ALL_SCHEDULED
        if self._lighter_client is not None:
            try:
                tx_type, tx_info, err = self._lighter_client.sign_cancel_all_orders(
                    time_in_force=tif,
                    timestamp_ms=0,
                    cancel_all_market_index=m.market_id,
                    nonce=nonce,
                    api_key_index=self.api_key_index,
                )
                if not err and tx_info:
                    return {
                        "type": "cancelAllOrders",
                        "tx_type": int(tx_type) if tx_type is not None else TX_TYPE_CANCEL_ALL_ORDERS,
                        "tx_info": tx_info,
                        "payload": {"marketId": m.market_id, "timeInForce": tif, "tx_type": TX_TYPE_CANCEL_ALL_ORDERS},
                        "apiKey": self.api_key,
                        "signature": "",
                        "timestamp": str(self.next_ts()),
                    }
            except Exception as e:
                log.debug("Native sign_cancel_all fallback: %s", e)

        tx_info = {
            "AccountIndex": self.account_index,
            "ApiKeyIndex": self.api_key_index,
            "MarketIndex": m.market_id,
            "TimeInForce": tif,
            "CancelAllTime": 0,
            "Nonce": nonce,
        }
        canonical_info = canonical(tx_info)
        sig = self.sign_hash(canonical_info.encode())
        tx_info["Sig"] = sig

        body = {
            "address": self.address,
            "accountIndex": self.account_index,
            "marketId": m.market_id,
            "timeInForce": CANCEL_ALL_IMMEDIATE,
            "nonce": nonce,
            "tx_type": TX_TYPE_CANCEL_ALL_ORDERS,
            "tx_info": tx_info,
        }

        return {
            "type": "cancelAllOrders",
            "tx_type": TX_TYPE_CANCEL_ALL_ORDERS,
            "tx_info": tx_info,
            "payload": body,
            "apiKey": self.api_key,
            "signature": sig,
            "timestamp": str(self.next_ts()),
        }

    def legacy(self, action: str, body: dict) -> dict:
        ts = self.next_ts()
        message = f"{ts}{action}{canonical(body)}"
        return {
            "type": action,
            "payload": body,
            "apiKey": self.api_key,
            "timestamp": str(ts),
            "signature": self.sign_hash(message.encode()),
        }
