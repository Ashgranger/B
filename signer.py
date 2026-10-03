"""Solana Ed25519 Cryptographic Signer for Bulk Trade Perpetual DEX (bulk.trade).

Supports:
- Canonical Ed25519 transaction signing with network domain separation (Mainnet=1, Testnet=2, Devnet=3)
- Pre-computed deterministic Order IDs matching Bulk node derivation
- Single order and atomic batch/group transaction signing
- Agent-wallet / delegated signing (account != signer)
- Base58 key import/export and validation
"""
from __future__ import annotations

import hashlib
import json
import struct
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from utils import (
    BUY, SELL, SCALE_1E8, ALPHABET, b58encode, b58decode,
    sha256, to_fixed_u64, canonical, SigningError
)

# Bulk Signature Domains
DOMAIN_MAINNET = 1
DOMAIN_TESTNET = 2
DOMAIN_DEVNET = 3

DOMAINS: Dict[str, int] = {
    "mainnet": DOMAIN_MAINNET,
    "testnet": DOMAIN_TESTNET,
    "devnet": DOMAIN_DEVNET,
}


def serialize_action_bincode(action: Dict[str, Any]) -> bytes:
    """Serialize a single action into canonical bincode-compatible binary representation.
    
    Order Action (Variant 0):
      - variant: u32 (0)
      - symbol: length-prefixed UTF-8 string (u64 len + bytes)
      - is_buy: u8 (1 = True, 0 = False)
      - price: u64 fixed-point (1e8 scale)
      - size: u64 fixed-point (1e8 scale)
      - order_type: u32 (0 = Limit, 1 = Market)
      - tif: u32 (0 = GTC, 1 = IOC, 2 = ALO)
      - reduce_only: u8 (1 = True, 0 = False)
    
    Cancel Action (Variant 1):
      - variant: u32 (1)
      - symbol: length-prefixed UTF-8 string (u64 len + bytes)
      - order_id: length-prefixed string / bytes
      
    CancelAll Action (Variant 2):
      - variant: u32 (2)
      - symbol count: u64 + each length-prefixed string
    """
    atype = action.get("type", "order")
    buf = bytearray()
    
    if atype == "order":
        buf.extend(struct.pack("<I", 0))  # Variant 0 = Order
        symbol_bytes = action.get("symbol", "").encode("utf-8")
        buf.extend(struct.pack("<Q", len(symbol_bytes)))
        buf.extend(symbol_bytes)
        
        is_buy = bool(action.get("is_buy", True))
        buf.extend(struct.pack("B", 1 if is_buy else 0))
        
        px_u64 = to_fixed_u64(action.get("price", 0))
        sz_u64 = to_fixed_u64(action.get("size", 0))
        buf.extend(struct.pack("<QQ", px_u64, sz_u64))
        
        ot = action.get("order_type", {})
        ot_type = ot.get("type", "limit").lower()
        if ot_type == "market":
            buf.extend(struct.pack("<I", 1))  # 1 = Market
            buf.extend(struct.pack("<I", 1))  # Default IOC for market
        else:
            buf.extend(struct.pack("<I", 0))  # 0 = Limit
            tif = ot.get("tif", "GTC").upper()
            tif_val = 2 if tif == "ALO" else (1 if tif == "IOC" else 0)
            buf.extend(struct.pack("<I", tif_val))
            
        reduce_only = bool(action.get("reduce_only", False))
        buf.extend(struct.pack("B", 1 if reduce_only else 0))
        
    elif atype == "cancel":
        buf.extend(struct.pack("<I", 1))  # Variant 1 = Cancel
        symbol_bytes = action.get("symbol", "").encode("utf-8")
        buf.extend(struct.pack("<Q", len(symbol_bytes)))
        buf.extend(symbol_bytes)
        
        oid = action.get("order_id", "").encode("utf-8")
        buf.extend(struct.pack("<Q", len(oid)))
        buf.extend(oid)
        
    elif atype == "cancelAll":
        buf.extend(struct.pack("<I", 2))  # Variant 2 = CancelAll
        symbols = action.get("symbols", [])
        buf.extend(struct.pack("<Q", len(symbols)))
        for s in symbols:
            s_bytes = s.encode("utf-8")
            buf.extend(struct.pack("<Q", len(s_bytes)))
            buf.extend(s_bytes)
    else:
        buf.extend(struct.pack("<I", 99))
        c_bytes = canonical(action).encode("utf-8")
        buf.extend(struct.pack("<Q", len(c_bytes)))
        buf.extend(c_bytes)
        
    return bytes(buf)


def compute_order_id(
    action: Dict[str, Any],
    nonce: Union[int, str],
    account_pubkey: str,
    seqno: int = 0
) -> str:
    """Compute optimistic pre-computed Order ID in Base58 matching Bulk node derivation.
    
    Formula: SHA256(seqno_le + bincode(single_action) + account_bytes + nonce_le) (base58)
    """
    seqno_le = struct.pack("<Q", seqno)
    action_bytes = serialize_action_bincode(action)
    
    acc_bytes = b58decode(account_pubkey)
    if len(acc_bytes) != 32:
        acc_bytes = acc_bytes.rjust(32, b"bytes([0])")[:32]
        
    nonce_int = int(nonce)
    nonce_le = struct.pack("<Q", nonce_int)
    
    h = hashlib.sha256()
    h.update(seqno_le)
    h.update(action_bytes)
    h.update(acc_bytes)
    h.update(nonce_le)
    digest = h.digest()
    return b58encode(digest)


class BulkSigner:
    """Ed25519 Cryptographic Signer for Bulk Trade Perpetual DEX."""

    def __init__(
        self,
        private_key: Optional[Union[str, bytes]] = None,
        account_pubkey: Optional[str] = None,
        environment: str = "mainnet"
    ):
        self.env_name = environment.lower()
        self.domain = DOMAINS.get(self.env_name, DOMAIN_MAINNET)
        
        # Load or generate Ed25519 keypair
        if private_key is None or not private_key:
            self._priv = ed25519.Ed25519PrivateKey.generate()
        elif isinstance(private_key, bytes):
            if len(private_key) == 32:
                self._priv = ed25519.Ed25519PrivateKey.from_private_bytes(private_key)
            elif len(private_key) == 64:
                self._priv = ed25519.Ed25519PrivateKey.from_private_bytes(private_key[:32])
            else:
                raise SigningError(f"Invalid private key bytes length: {len(private_key)}")
        elif isinstance(private_key, str):
            priv_str = private_key.strip()
            try:
                raw = b58decode(priv_str)
                if len(raw) == 64:
                    self._priv = ed25519.Ed25519PrivateKey.from_private_bytes(raw[:32])
                elif len(raw) == 32:
                    self._priv = ed25519.Ed25519PrivateKey.from_private_bytes(raw)
                else:
                    raise ValueError(f"Base58 decoded length {len(raw)} is neither 32 nor 64")
            except Exception:
                try:
                    raw = bytes.fromhex(priv_str)
                    if len(raw) == 32:
                        self._priv = ed25519.Ed25519PrivateKey.from_private_bytes(raw)
                    elif len(raw) == 64:
                        self._priv = ed25519.Ed25519PrivateKey.from_private_bytes(raw[:32])
                    else:
                        raise ValueError(f"Hex length {len(raw)} invalid")
                except Exception as ex:
                    raise SigningError(f"Could not parse private key (neither Base58 nor Hex): {ex}")
        else:
            raise SigningError(f"Unsupported private key format: {type(private_key)}")

        self._pub = self._priv.public_key()
        self.signer_pubkey_bytes = self._pub.public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw
        )
        self.signer_pubkey = b58encode(self.signer_pubkey_bytes)
        
        self.account_pubkey = account_pubkey.strip() if account_pubkey else self.signer_pubkey
        self.account_pubkey_bytes = b58decode(self.account_pubkey)
        if len(self.account_pubkey_bytes) != 32:
            self.account_pubkey_bytes = self.account_pubkey_bytes.rjust(32, b"bytes([0])")[:32]

    def get_nonce(self) -> str:
        """Generate nanosecond Unix timestamp nonce."""
        return str(time.time_ns())

    def sign_message(self, message: bytes) -> str:
        """Sign raw message bytes with Ed25519, returning Base58 signature string."""
        sig = self._priv.sign(message)
        return b58encode(sig)

    def verify(self, signature_b58: str, message: bytes) -> bool:
        """Verify signature against signer's public key."""
        try:
            sig = b58decode(signature_b58)
            self._pub.verify(sig, message)
            return True
        except Exception:
            return False

    def build_signing_bytes(self, actions: List[Dict[str, Any]], nonce: str) -> bytes:
        """Build canonical transaction signing bytes:
        serialized_actions + nonce_le (8 bytes) + account_pubkey (32 bytes) + domain (1 byte)
        """
        buf = bytearray()
        buf.extend(struct.pack("<Q", len(actions)))
        for a in actions:
            buf.extend(serialize_action_bincode(a))
            
        nonce_int = int(nonce)
        buf.extend(struct.pack("<Q", nonce_int))
        buf.extend(self.account_pubkey_bytes)
        buf.extend(struct.pack("B", self.domain))
        return bytes(buf)

    def sign_transaction(
        self,
        actions: List[Dict[str, Any]],
        nonce: Optional[str] = None
    ) -> Dict[str, Any]:
        """Sign one or more actions into a complete Bulk transaction payload."""
        if not actions:
            raise SigningError("Cannot sign empty actions list")
            
        nonce_str = nonce or self.get_nonce()
        msg_bytes = self.build_signing_bytes(actions, nonce_str)
        signature = self.sign_message(msg_bytes)
        
        order_ids = []
        for i, a in enumerate(actions):
            if a.get("type") == "order":
                oid = compute_order_id(a, nonce_str, self.account_pubkey, seqno=i)
                order_ids.append(oid)
            else:
                order_ids.append(None)
                
        res: Dict[str, Any] = {
            "actions": actions,
            "nonce": nonce_str,
            "account": self.account_pubkey,
            "signer": self.signer_pubkey,
            "signature": signature,
        }
        if len(order_ids) == 1 and order_ids[0] is not None:
            res["order_id"] = order_ids[0]
        elif any(oid is not None for oid in order_ids):
            res["order_ids"] = order_ids
            
        return res

    def sign_order(
        self,
        symbol: str,
        is_buy: bool,
        price: Union[Decimal, float],
        size: Union[Decimal, float],
        order_type: str = "limit",
        tif: str = "ALO",
        reduce_only: bool = False,
        nonce: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Sign a single limit or market order action."""
        action = {
            "type": "order",
            "symbol": symbol,
            "is_buy": is_buy,
            "price": float(price),
            "size": float(size),
            "order_type": {
                "type": order_type.lower(),
                "tif": tif.upper() if order_type.lower() == "limit" else "IOC",
            },
            "reduce_only": reduce_only,
        }
        return self.sign_transaction([action], nonce=nonce)

    def sign_cancel(
        self,
        symbol: str,
        order_id: str,
        nonce: Optional[str] = None
    ) -> Dict[str, Any]:
        """Sign a cancel action by order_id."""
        action = {
            "type": "cancel",
            "symbol": symbol,
            "order_id": order_id,
        }
        return self.sign_transaction([action], nonce=nonce)

    def sign_cancel_all(
        self,
        symbols: Optional[List[str]] = None,
        nonce: Optional[str] = None
    ) -> Dict[str, Any]:
        """Sign a cancelAll action for specified symbols (or all if empty)."""
        action = {
            "type": "cancelAll",
            "symbols": symbols or [],
        }
        return self.sign_transaction([action], nonce=nonce)

    def sign_group(
        self,
        actions: List[Dict[str, Any]],
        nonce: Optional[str] = None
    ) -> Dict[str, Any]:
        """Sign multiple actions atomically in a single signed transaction."""
        return self.sign_transaction(actions, nonce=nonce)
