"""Bulk Trade Ed25519 Signer & Transaction Envelope Builder.

Implements Solana Ed25519 signing, signature domains, canonical serialization,
pre-computed optimistic Order IDs, Builder Codes, and atomic batch transactions.
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
    b58decode,
    b58encode,
    canonical,
    fmt,
    to_fixed_u64,
    BUY,
    SELL,
    Fatal,
)


class SignatureDomain:
    MAINNET = 1
    TESTNET = 2
    DEVNET = 3

    @classmethod
    def from_str(cls, s: str) -> int:
        clean = (s or "").strip().lower()
        if clean in ("mainnet", "1"):
            return cls.MAINNET
        if clean in ("testnet", "2"):
            return cls.TESTNET
        if clean in ("devnet", "3"):
            return cls.DEVNET
        return cls.MAINNET


class Signer:
    """High-performance Ed25519 signer for Bulk Trade Perp DEX."""

    # Time-In-Force (TIF)
    TIF_ALO = "ALO"  # Add Liquidity Only (Post-Only)
    TIF_GTC = "GTC"  # Good 'Til Cancelled
    TIF_IOC = "IOC"  # Immediate Or Cancel
    TIF_FOK = "FOK"  # Fill Or Kill

    def __init__(
        self,
        private_key: Union[str, bytes],
        account: Optional[str] = None,
        domain: int = SignatureDomain.MAINNET,
        builder_to: Optional[str] = None,
        builder_fee_bps: int = 0,
    ):
        """
        Initialize the Bulk Trade Signer.
        :param private_key: Base58 string or 32-byte seed or 64-byte keypair
        :param account: Base58 Solana address of the trading/sub-account (defaults to signer pubkey)
        :param domain: SignatureDomain (1=Mainnet, 2=Testnet, 3=Devnet)
        :param builder_to: Optional builder code recipient address
        :param builder_fee_bps: Optional builder fee in bps (1..15)
        """
        self.domain = domain
        self._priv = self._load_private_key(private_key)
        self._pub = self._priv.public_key()
        self.pubkey_bytes = self._pub.public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        self.pubkey_b58 = b58encode(self.pubkey_bytes)
        
        # Account is either provided or defaults to the signer public key
        self.account_b58 = (account or "").strip() or self.pubkey_b58
        try:
            self.account_bytes = b58decode(self.account_b58)
        except Exception:
            self.account_bytes = self.pubkey_bytes
            self.account_b58 = self.pubkey_b58

        self.builder_to = builder_to.strip() if builder_to else None
        self.builder_fee_bps = int(builder_fee_bps) if builder_fee_bps else 0
        self._last_nonce = 0

    @classmethod
    def generate(cls, domain: int = SignatureDomain.MAINNET) -> "Signer":
        """Generate a fresh random keypair for testing or initialization."""
        priv = ed25519.Ed25519PrivateKey.generate()
        raw_seed = priv.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
        return cls(raw_seed, domain=domain)

    def _load_private_key(self, k: Union[str, bytes]) -> ed25519.Ed25519PrivateKey:
        raw_bytes: bytes
        if isinstance(k, str):
            clean = k.strip()
            if len(clean) == 64:  # Hex string
                try:
                    raw_bytes = bytes.fromhex(clean)
                except ValueError:
                    raw_bytes = b58decode(clean)
            else:
                raw_bytes = b58decode(clean)
        elif isinstance(k, bytes):
            raw_bytes = k
        else:
            raise Fatal(f"Unsupported private key format: {type(k)}")

        if len(raw_bytes) == 32:
            return ed25519.Ed25519PrivateKey.from_private_bytes(raw_bytes)
        elif len(raw_bytes) == 64:
            # First 32 bytes is seed in Solana/NaCl keypair representation
            seed = raw_bytes[:32]
            return ed25519.Ed25519PrivateKey.from_private_bytes(seed)
        else:
            raise Fatal(f"Invalid private key byte length: {len(raw_bytes)} (expected 32 or 64)")

    def next_nonce(self) -> int:
        """Monotonic nanosecond timestamp nonce."""
        now_ns = time.time_ns()
        self._last_nonce = max(now_ns, self._last_nonce + 1)
        return self._last_nonce

    def compute_order_id(self, action: dict, nonce: int, seqno: int = 0) -> str:
        """
        Compute deterministic pre-computed Order ID for optimistic tracking.
        Hashing: sha256(domain (1B) + account (32B) + nonce (8B LE) + seqno (4B LE) + canonical_action).
        """
        action_bytes = canonical(action).encode("utf-8")
        h = hashlib.sha256()
        h.update(struct.pack("<B", self.domain))
        h.update(self.account_bytes)
        h.update(struct.pack("<Q", nonce))
        h.update(struct.pack("<I", seqno))
        h.update(action_bytes)
        return b58encode(h.digest())

    def attach_builder_code(self, action_body: dict) -> dict:
        """Attach optional builder code attribution if configured."""
        if self.builder_to and 1 <= self.builder_fee_bps <= 15:
            action_body["builderCode"] = {
                "to": self.builder_to,
                "fee": self.builder_fee_bps,
            }
        return action_body

    def build_limit_action(
        self,
        market_symbol: str,
        side: str,
        price: Decimal,
        qty: Decimal,
        time_in_force: str = "ALO",
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> dict:
        """
        Construct a Bulk Trade limit order action payload ('l').
        """
        is_buy = (side == BUY)
        p_float = float(price)
        sz_float = float(qty)
        
        body = {
            "c": market_symbol,
            "b": is_buy,
            "p": p_float,
            "sz": sz_float,
            "r": bool(reduce_only),
            "tif": time_in_force,
        }
        if client_order_id:
            body["cid"] = client_order_id

        self.attach_builder_code(body)
        return {"l": body}

    def build_market_action(
        self,
        market_symbol: str,
        side: str,
        qty: Decimal,
        reduce_only: bool = False,
        ioc: bool = True,
        client_order_id: Optional[str] = None,
    ) -> dict:
        """
        Construct a Bulk Trade market order action payload ('m').
        """
        is_buy = (side == BUY)
        sz_float = float(qty)
        body = {
            "c": market_symbol,
            "b": is_buy,
            "sz": sz_float,
            "r": bool(reduce_only),
            "i": bool(ioc),
        }
        if client_order_id:
            body["cid"] = client_order_id

        self.attach_builder_code(body)
        return {"m": body}

    def build_cancel_action(self, order_id: str, market_symbol: str = "") -> dict:
        """Construct a Bulk Trade cancel action payload ('c')."""
        body = {"order_id": order_id}
        if market_symbol:
            body["c"] = market_symbol
        return {"c": body}

    def build_cancel_all_action(self, market_symbol: str = "") -> dict:
        """Construct a Bulk Trade cancel-all action payload ('cancelAll')."""
        body = {}
        if market_symbol:
            body["c"] = market_symbol
        return {"cancelAll": body}

    def sign_transaction(
        self, actions: List[dict], nonce: Optional[int] = None
    ) -> Tuple[dict, List[str]]:
        """
        Create a fully signed Bulk Trade transaction envelope for single or batch actions.
        Returns: (signed_envelope, precomputed_order_ids)
        """
        if not actions:
            raise ValueError("Actions list cannot be empty")
        
        tx_nonce = nonce if nonce is not None else self.next_nonce()

        # Compute order IDs for order placement actions
        order_ids = []
        for seqno, act in enumerate(actions):
            if "l" in act or "m" in act:
                oid = self.compute_order_id(act, tx_nonce, seqno)
                order_ids.append(oid)
            else:
                order_ids.append("")

        # Create canonical message payload for Ed25519 signing
        # Format conforms to Bulk Trade canonical signing format
        msg_payload = {
            "domain": self.domain,
            "account": self.account_b58,
            "signer": self.pubkey_b58,
            "nonce": tx_nonce,
            "actions": actions,
        }
        canonical_bytes = canonical(msg_payload).encode("utf-8")
        
        # Ed25519 signature
        signature_raw = self._priv.sign(canonical_bytes)
        signature_b58 = b58encode(signature_raw)

        envelope = {
            "actions": actions,
            "nonce": tx_nonce,
            "account": self.account_b58,
            "signer": self.pubkey_b58,
            "signature": signature_b58,
            "order_ids": [oid for oid in order_ids if oid],
        }
        return envelope, order_ids

    def schedule_cancel(self, m: Any, deadline_us: Optional[int]) -> dict:
        """Bulk Trade Dead Man's Switch / Schedule Cancel."""
        act = {"scheduleCancel": {"c": getattr(m, "name", str(m))}}
        if deadline_us is not None:
            act["scheduleCancel"]["deadline"] = int(deadline_us)
        envelope, _ = self.sign_transaction([act])
        return {"type": "scheduleCancel", "payload": envelope}
