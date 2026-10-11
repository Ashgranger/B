"""Bulk Trade market metadata, orderbook tracking, and microstructure intelligence."""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils import BPS, ZERO, ONE, BULK_SCALE, clamp, fmt


def _D(x: Any) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


@dataclass
class Market:
    market_id: int
    name: str
    status: str
    tick: Decimal
    step: Decimal
    tiers: list
    min_notional: Decimal
    min_size: Decimal
    max_size: Decimal
    mark: Decimal
    is_outside_rth: bool
    funding_rate: Decimal = Decimal(0)
    next_funding_time: float = 0.0
    index_price: Decimal = Decimal(0)

    @classmethod
    def from_api(cls, d: dict) -> "Market":
        mid = int(d.get("marketId") or d.get("market_id") or 1)
        name = str(d.get("symbol") or d.get("market") or d.get("marketDisplayName") or "SOL-USD").upper()
        status = str(d.get("status", "ONLINE")).upper()
        tick = _D(d.get("tickSize") or d.get("tick_size") or "0.01")
        step = _D(d.get("stepSize") or d.get("step_size") or d.get("lotSize") or "0.001")
        tiers = list(d.get("tickTiers") or [])
        min_notional = _D(d.get("minOrderNotional") or d.get("min_notional") or "5.0")
        min_size = _D(d.get("minOrderSize") or d.get("min_size") or step)
        max_size = _D(d.get("maxOrderSize") or d.get("max_size") or "1000000")
        mark = _D(d.get("markPrice") or d.get("mark_price") or "0")
        idx_px = _D(d.get("indexPrice") or d.get("index_price") or mark)
        funding = _D(d.get("fundingRate") or d.get("funding_rate") or "0")
        next_funding = float(d.get("nextFundingTime") or d.get("next_funding_time") or 0.0)
        is_rth = bool(d.get("isOutsideRth", False))
        return cls(
            market_id=mid,
            name=name,
            status=status,
            tick=tick,
            step=step,
            tiers=tiers,
            min_notional=min_notional,
            min_size=min_size,
            max_size=max_size,
            mark=mark,
            is_outside_rth=is_rth,
            funding_rate=funding,
            next_funding_time=next_funding,
            index_price=idx_px,
        )

    def tick_for(self, price: Decimal) -> Decimal:
        for t in self.tiers:
            up = t.get("upToPrice")
            if up is None or price < Decimal(str(up)):
                return Decimal(str(t["tick"]))
        return self.tick


@dataclass
class VenueState:
    venue: str
    bid: Decimal
    ask: Decimal
    bid_sz: Decimal
    ask_sz: Decimal
    ts: float
    bids: tuple = ()  # ((px, sz), ...)
    asks: tuple = ()
    depth_ts: float = 0.0

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def obi(self) -> Decimal:
        """Depth-weighted Order Book Imbalance (OBI)."""
        if self.bids and self.asks:
            b = a = 0.0
            for k, (px, sz) in enumerate(self.bids[:10]):
                b += float(px * sz) / (k + 1)
            for k, (px, sz) in enumerate(self.asks[:10]):
                a += float(px * sz) / (k + 1)
            tot = b + a
            return Decimal(str(round((b - a) / tot, 6))) if tot > 0 else ZERO
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total


class _Basis:
    __slots__ = ("value", "last_ts", "first_ts", "n")

    def __init__(self, v: float, ts: float):
        self.value, self.last_ts, self.first_ts, self.n = v, ts, ts, 1


class CrossVenueTracker:
    """External CEX lead-lag intelligence (Binance, Bybit) for Bulk Trade."""

    def __init__(self, cfg=None):
        self.stale_s = float(getattr(cfg, "cross_stale_s", 2.0))
        self.basis_tau_s = float(getattr(cfg, "cross_basis_tau_s", 45.0))
        self.warmup_s = float(getattr(cfg, "cross_warmup_s", 10.0))
        self.max_shift_bps = float(getattr(cfg, "cross_max_shift_bps", 6.0))
        self.weights = dict(getattr(cfg, "cross_exchange_weights", {}) or {})
        self.venues: Dict[str, VenueState] = {}
        self._vhist: Dict[str, deque] = {}
        self._basis: Dict[str, _Basis] = {}
        self._trades: deque = deque()
        self._liqs: deque = deque()
        self._last_now = 0.0
        self._cache: dict = {}

    def update_venue(
        self, venue: str, bid: Decimal, ask: Decimal, bid_sz: Decimal, ask_sz: Decimal, now: float
    ) -> None:
        st = VenueState(venue, bid, ask, bid_sz, ask_sz, now)
        old = self.venues.get(venue)
        if old is not None and old.bids:
            st.bids, st.asks, st.depth_ts = old.bids, old.asks, old.depth_ts
        self.venues[venue] = st
        self._touch(now)
        h = self._vhist.setdefault(venue, deque())
        h.append((now, st.mid))
        while h and now - h[0][0] > 30.0:
            h.popleft()

    def update_depth(self, venue: str, bids: list, asks: list, now: float) -> None:
        st = self.venues.get(venue)
        try:
            b = tuple((_D(r[0]), _D(r[1])) for r in bids[:10])
            a = tuple((_D(r[0]), _D(r[1])) for r in asks[:10])
        except Exception:
            return
        if not b or not a:
            return
        if st is None:
            st = VenueState(venue, b[0][0], a[0][0], b[0][1], a[0][1], now)
            self.venues[venue] = st
        st.bids, st.asks, st.depth_ts = b, a, now
        self._touch(now)

    def update_trade(self, venue: str, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._trades.append((now, venue, side.upper(), float(size * price)))
        while self._trades and now - self._trades[0][0] > 30.0:
            self._trades.popleft()
        self._touch(now)

    def update_liquidation(self, venue: str, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._liqs.append((now, venue, side.upper(), float(size * price)))
        while self._liqs and now - self._liqs[0][0] > 60.0:
            self._liqs.popleft()
        self._touch(now)

    def drop_venue(self, venue: str) -> None:
        self.venues.pop(venue, None)
        self._vhist.pop(venue, None)
        self._cache.clear()

    def observe_local(self, local_mid: Decimal, now: float) -> None:
        lm = float(local_mid)
        if lm <= 0:
            return
        for name, st in self.venues.items():
            if now - st.ts > self.stale_s:
                continue
            sample = (float(st.mid) - lm) / lm * 1e4
            b = self._basis.get(name)
            if b is None:
                self._basis[name] = _Basis(sample, now)
                continue
            dt = now - b.last_ts
            if dt <= 0:
                continue
            a = min(1.0 - math.exp(-dt / self.basis_tau_s), 0.05)
            if abs(sample - b.value) > 3.0:
                a *= 0.1
            b.value += a * (sample - b.value)
            b.last_ts = now
            b.n += 1
        self._cache.clear()

    def _touch(self, now: float) -> None:
        if now > self._last_now:
            self._last_now = now
        self._cache.clear()

    def fresh(self, now: Optional[float] = None) -> list:
        n = self._last_now if now is None else now
        return [v for v in self.venues.values() if n - v.ts <= self.stale_s and v.mid > ZERO]

    def _w(self, venue: str) -> float:
        return float(self.weights.get(venue.lower(), 1.0))

    def warmed(self, venue: str, now: float) -> bool:
        b = self._basis.get(venue)
        return b is not None and b.n >= 15 and (now - b.first_ts) >= self.warmup_s

    def cross_fair_value(self, now: Optional[float] = None) -> Optional[Decimal]:
        fr = self.fresh(now)
        if not fr:
            return None
        return sum(v.mid for v in fr) / Decimal(str(len(fr)))

    def lead_lag_divergence_bps(self, local_mid: Optional[Decimal], now: Optional[float] = None) -> Decimal:
        if local_mid is None or local_mid <= ZERO:
            return ZERO
        n = self._last_now if now is None else now
        lm = float(local_mid)
        w_sum = 0.0
        d_sum = 0.0
        for v in self.fresh(n):
            if not self.warmed(v.venue, n):
                continue
            w = self._w(v.venue)
            basis_val = self._basis[v.venue].value
            div = (float(v.mid) - lm) / lm * 1e4 - basis_val
            d_sum += div * w
            w_sum += w
        if w_sum <= 0:
            return ZERO
        res = d_sum / w_sum
        res = max(-self.max_shift_bps, min(self.max_shift_bps, res))
        return Decimal(str(round(res, 4)))

    def dispersion_bps(self, now: Optional[float] = None) -> Decimal:
        fr = self.fresh(now)
        if len(fr) < 2:
            return ZERO
        mids = [float(v.mid) for v in fr]
        min_m, max_m = min(mids), max(mids)
        mean_m = sum(mids) / len(mids)
        if mean_m <= 0:
            return ZERO
        return Decimal(str(round((max_m - min_m) / mean_m * 1e4, 2)))


class MarketData:
    """Consolidated market data with L2 depth, BBO, and rolling indicators."""

    HISTORY_S = 60.0

    def __init__(self, cfg):
        self.cfg = cfg
        self.info: Optional[Market] = None
        self.bid: Optional[Decimal] = None
        self.ask: Optional[Decimal] = None
        self.bid_sz: Optional[Decimal] = None
        self.ask_sz: Optional[Decimal] = None
        self.ts: float = 0.0
        
        self._book_depth_bids: list[Tuple[Decimal, Decimal]] = []
        self._book_depth_asks: list[Tuple[Decimal, Decimal]] = []
        
        self._hist: deque = deque()
        self._trades: deque = deque()
        self._vol_ewma: Decimal = ZERO
        self._tfi_cache: dict = {}
        self._tfi_now: Optional[float] = None
        
        self.jump_until: float = 0.0
        self.cross = CrossVenueTracker(cfg)
        self.own_provider: Optional[Callable[[], list]] = None
        self._excl = getattr(cfg, "excl_own_quotes", True)

    def _own_levels(self, side: str) -> dict:
        d: dict = {}
        if not self._excl or not self.own_provider:
            return d
        try:
            for s, p, q in self.own_provider():
                if s == side:
                    d[p] = d.get(p, ZERO) + q
        except Exception:
            return {}
        return d

    def depth_levels(self, side: str) -> list[Tuple[Decimal, Decimal]]:
        is_bid = side in ("BUY", "BID")
        raw = self._book_depth_bids if is_bid else self._book_depth_asks
        own = self._own_levels("BUY" if is_bid else "SELL")
        if not own:
            return raw
        out = []
        for row in raw:
            p, sz = _D(row[0]), _D(row[1])
            e = sz - own.get(p, ZERO)
            if e > ZERO:
                out.append((p, e))
        return out

    def top_size(self, side: str) -> Optional[Decimal]:
        is_bid = side in ("BUY", "BID")
        raw = self.bid_sz if is_bid else self.ask_sz
        px = self.bid if is_bid else self.ask
        if raw is None or px is None:
            return raw
        own = self._own_levels("BUY" if is_bid else "SELL").get(px, ZERO)
        if own <= ZERO:
            return raw
        ext = raw - own
        if ext > ZERO:
            return ext
        for p, sz in self.depth_levels(side):
            if (is_bid and p < px) or ((not is_bid) and p > px):
                return sz
        return ZERO

    def update(
        self,
        bid: Decimal,
        ask: Decimal,
        bid_sz: Optional[Decimal],
        ask_sz: Optional[Decimal],
        now: float,
    ) -> None:
        prev = self.mid
        self.bid, self.ask, self.bid_sz, self.ask_sz, self.ts = bid, ask, bid_sz, ask_sz, now
        mid = self.mid
        if prev and mid and bid < ask:
            if abs(mid - prev) / prev * BPS >= getattr(self.cfg, "jump_threshold_bps", getattr(self.cfg, "jump_bps", Decimal("6.0"))):
                self.jump_until = now + self.cfg.jump_cooldown_s
        if bid < ask and self.cross.venues:
            self.cross.observe_local(mid, now)
        if bid < ask:
            self._hist.append((now, mid))
            while self._hist and now - self._hist[0][0] > self.HISTORY_S:
                self._hist.popleft()
            move = self.move_bps(getattr(self.cfg, "volatility_lookback_s", getattr(self.cfg, "vol_window_s", 60.0)), now)
            self._vol_ewma = move if self._vol_ewma == 0 else self._vol_ewma * Decimal("0.9") + move * Decimal("0.1")

    def on_trade(self, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._trades.append((now, side.upper(), size, price))
        self._tfi_cache.clear()
        self._tfi_now = None
        while self._trades and now - self._trades[0][0] > 60.0:
            self._trades.popleft()

    def on_depth(self, bids: list, asks: list, now: float) -> None:
        try:
            self._book_depth_bids = [(_D(r[0]), _D(r[1])) for r in bids]
            self._book_depth_asks = [(_D(r[0]), _D(r[1])) for r in asks]
        except Exception:
            self._book_depth_bids = bids
            self._book_depth_asks = asks

    def clear_book(self) -> None:
        self.bid = self.ask = self.bid_sz = self.ask_sz = None
        self._book_depth_bids.clear()
        self._book_depth_asks.clear()

    @property
    def mid(self) -> Optional[Decimal]:
        return (self.bid + self.ask) / 2 if self.bid is not None and self.ask is not None else None

    @property
    def micro(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        bsz, asz = self.top_size("BUY"), self.top_size("SELL")
        if bsz and asz and (bsz + asz) > 0:
            return (self.bid * asz + self.ask * bsz) / (bsz + asz)
        return self.mid

    @property
    def obi(self) -> Decimal:
        bsz, asz = self.top_size("BUY"), self.top_size("SELL")
        if bsz is None or asz is None:
            return ZERO
        total = bsz + asz
        if total <= 0:
            return ZERO
        return (bsz - asz) / total

    def trade_flow_imbalance(self, window_s: float, now: float) -> Decimal:
        if self._tfi_now != now:
            self._tfi_cache.clear()
            self._tfi_now = now
        else:
            hit = self._tfi_cache.get(window_s)
            if hit is not None:
                return hit
        buy_vol = ZERO
        sell_vol = ZERO
        for t, side, sz, _ in reversed(self._trades):
            if now - t > window_s:
                break
            if side in ("BUY", "BID"):
                buy_vol += sz
            else:
                sell_vol += sz
        total = buy_vol + sell_vol
        res = ZERO if total <= 0 else (buy_vol - sell_vol) / total
        self._tfi_cache[window_s] = res
        return res

    def move_bps(self, window_s: float, now: float) -> Decimal:
        if len(self._hist) < 2:
            return ZERO
        cutoff = now - window_s
        vals = [m for t, m in self._hist if t >= cutoff]
        if len(vals) < 2:
            return ZERO
        lo, hi = min(vals), max(vals)
        mid = (lo + hi) / Decimal("2")
        return ((hi - lo) / mid) * Decimal("10000") if mid > ZERO else ZERO
