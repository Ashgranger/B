"""Utility constants, math functions, and serialization helpers for Robinhood Lighter MM Bot."""
from __future__ import annotations

import hashlib
import json
import logging
import sys
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


class Fatal(Exception):
    """Unrecoverable fatal error requiring bot halt."""
    pass


class ExchangeError(Exception):
    """Recoverable exchange protocol or communication error."""
    pass


class SigningError(Exception):
    """Cryptographic signing or verification error."""
    pass


def setup_logging(level_name: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def fmt(val: Optional[Union[Decimal, float]]) -> str:
    """Format decimal or float cleanly without trailing exponential noise."""
    if val is None:
        return "None"
    d = Decimal(str(val)) if not isinstance(val, Decimal) else val
    s = f"{d:f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def to_int(val: Any, step_or_tick: Decimal) -> int:
    """Convert a float/Decimal value to an integer scaled by tick_size or step_size."""
    d = Decimal(str(val))
    scaled = d / step_or_tick
    return int(scaled.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


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
