"""Utility constants, math functions, Base58 codecs, and binary serialization helpers."""
from __future__ import annotations

import hashlib
import json
import struct
import time
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional, Tuple, Union

# Side Constants
BUY = "BUY"
SELL = "SELL"

# Numerical Constants
BPS = Decimal("10000")
ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
SCALE_1E8 = Decimal("100000000")

# Solana / Bitcoin Base58 Alphabet
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
ALPHABET_MAP = {c: i for i, c in enumerate(ALPHABET)}


class Fatal(Exception):
    """Unrecoverable fatal error requiring bot halt."""
    pass


class ExchangeError(Exception):
    """Recoverable exchange protocol or communication error."""
    pass


class SigningError(Exception):
    """Cryptographic signing or verification error."""
    pass


def b58encode(b: bytes) -> str:
    """Encode raw bytes into Base58 string (Solana/Bitcoin convention)."""
    zero_byte = bytes([0])
    n_pad = len(b) - len(b.lstrip(zero_byte))
    num = int.from_bytes(b, "big")
    res = []
    while num > 0:
        num, rem = divmod(num, 58)
        res.append(ALPHABET[rem])
    return ("1" * n_pad) + ("".join(reversed(res)) if res else ("" if n_pad else "1"))


def b58decode(s: str) -> bytes:
    """Decode Base58 string into raw bytes."""
    zero_byte = bytes([0])
    n_pad = len(s) - len(s.lstrip("1"))
    num = 0
    for char in s:
        if char not in ALPHABET_MAP:
            raise ValueError(f"Invalid Base58 character: '{char}'")
        num = num * 58 + ALPHABET_MAP[char]
    b = num.to_bytes((num.bit_length() + 7) // 8, "big") if num > 0 else b""
    return (zero_byte * n_pad) + b


def sha256(data: bytes) -> bytes:
    """Compute SHA-256 digest."""
    return hashlib.sha256(data).digest()


def to_fixed_u64(val: Union[Decimal, float, int], scale: Decimal = SCALE_1E8) -> int:
    """Convert float/Decimal to fixed-point unsigned 64-bit integer (e.g. 1e8 scale)."""
    d = Decimal(str(val)) if not isinstance(val, Decimal) else val
    return int((d * scale).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def from_fixed_u64(val: int, scale: Decimal = SCALE_1E8) -> Decimal:
    """Convert fixed-point u64 integer back to Decimal."""
    return Decimal(val) / scale


def fmt(val: Optional[Union[Decimal, float]]) -> str:
    """Format decimal or float cleanly without trailing exponential noise."""
    if val is None:
        return "None"
    d = Decimal(str(val)) if not isinstance(val, Decimal) else val
    s = f"{d:f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def to_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def clamp(val: Decimal, low: Decimal, high: Decimal) -> Decimal:
    """Clamp decimal within bounds."""
    if low > high:
        low, high = high, low
    return max(low, min(val, high))


def bps_diff(p1: Decimal, p2: Decimal) -> Decimal:
    """Relative basis points difference (p1 - p2) / p2 * 10,000."""
    if p2 == ZERO:
        return ZERO
    return ((p1 - p2) / p2) * BPS


def q_down(val: Decimal, step: Decimal) -> Decimal:
    """Quantize downward to nearest step."""
    if step <= ZERO:
        return val
    return (val / step).quantize(Decimal("1"), rounding=ROUND_DOWN) * step


def q_up(val: Decimal, step: Decimal) -> Decimal:
    """Quantize upward to nearest step."""
    if step <= ZERO:
        return val
    return (val / step).quantize(Decimal("1"), rounding=ROUND_UP) * step


def canonical(obj: Any) -> str:
    """Deterministic JSON string for hashing and signing verification."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
