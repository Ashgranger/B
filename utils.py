"""Ultra-low latency utility functions, math helpers, fast Base58, and constants for Bulk Trade bot."""
from __future__ import annotations

import json
import logging
import sys
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

log = logging.getLogger("utils")

# Mathematical & protocol constants
BPS = Decimal("0.0001")
ZERO = Decimal("0")
ONE = Decimal("1")
BULK_SCALE = Decimal("100000000")  # 1e8 fixed point scale for Bulk Trade protocol
BULK_SCALE_INT = 100000000
USDC_DECIMALS = 6
USDC_BASE_UNIT = Decimal("1000000")

BUY = "BUY"
SELL = "SELL"

# Fast Base58 table lookups (Solana / Bitcoin standard)
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B58_INDEX = [-1] * 256
for _i, _c in enumerate(B58_ALPHABET):
    B58_INDEX[ord(_c)] = _i


def b58encode(b: bytes) -> str:
    """High-speed Base58 encoding."""
    if not b:
        return ""
    n = int.from_bytes(b, "big")
    chars = []
    while n > 0:
        n, r = divmod(n, 58)
        chars.append(B58_ALPHABET[r])
    pad = 0
    for byte in b:
        if byte == 0:
            pad += 1
        else:
            break
    return "1" * pad + "".join(reversed(chars)) if chars else "1" * pad


def b58decode(s: str) -> bytes:
    """High-speed O(1) lookup table Base58 decoding."""
    if not s:
        return b""
    n = 0
    for c in s:
        idx = B58_INDEX[ord(c)]
        if idx == -1:
            raise ValueError(f"Invalid Base58 character: '{c}'")
        n = n * 58 + idx
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") if n > 0 else b""
    pad = 0
    for c in s:
        if c == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + full


class Fatal(Exception):
    """Unrecoverable startup or operational error."""
    pass


def q_down(x: Decimal, unit: Decimal) -> Decimal:
    """Quantize downward to nearest tick/step unit."""
    return (x / unit).quantize(Decimal("1"), rounding=ROUND_DOWN) * unit


def q_up(x: Decimal, unit: Decimal) -> Decimal:
    """Quantize upward to nearest tick/step unit."""
    return (x / unit).quantize(Decimal("1"), rounding=ROUND_UP) * unit


def to_int(value: Decimal, unit: Decimal) -> int:
    """Scale a decimal value into discrete integer units."""
    return int((value / unit).quantize(Decimal("1"), rounding=ROUND_DOWN))


def to_fixed_u64(val: Decimal) -> int:
    """Fast Bulk Trade fixed-point u64 conversion (1e8 scale)."""
    return int((val * BULK_SCALE).quantize(Decimal("1"), rounding=ROUND_DOWN))


def from_fixed_u64(val: int) -> Decimal:
    """Fast Bulk Trade fixed-point u64 to Decimal conversion (1e8 scale)."""
    return Decimal(val) / BULK_SCALE


def fmt(d: Decimal) -> str:
    """Format decimal cleanly removing trailing zeros."""
    s = f"{d:f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def canonical(obj: Any) -> str:
    """Fast deterministic JSON string for Ed25519 cryptographic signing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def clamp(x: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    """Clamp decimal x within [lo, hi]."""
    return max(lo, min(hi, x))


def bps_diff(a: Decimal, b: Decimal) -> Decimal:
    """Basis points difference: (a - b) / b * 10,000."""
    if b == ZERO:
        return ZERO
    return ((a - b) / b) * Decimal("10000")


def setup_logging(level: str = "INFO") -> None:
    """Configure structured logging."""
    num_level = getattr(logging, level.upper(), logging.INFO)
    fmt_str = "%(asctime)s %(levelname)-5s [%(name)s] %(message)s"
    logging.basicConfig(level=num_level, format=fmt_str, datefmt="%H:%M:%S", stream=sys.stdout)
