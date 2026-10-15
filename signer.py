"""Lighter L2 Transaction & Auth-Token Signer for Robinhood Lighter Perp DEX.

References:
  - https://apidocs.rh.lighter.xyz/docs/get-started
  - https://apidocs.lighter.xyz/docs/lighter-rh
  - https://apidocs.lighter.xyz/docs/trading
  - https://github.com/elliottech/lighter-python/blob/main/lighter/signer_client.py
  - https://github.com/elliottech/lighter-go
"""
from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import json
import logging
import os
import platform
import time
from decimal import Decimal
from typing import Any, Optional, Tuple

from market import Market
from utils import canonical, fmt, to_int

log = logging.getLogger("signer")


class SignedTxResponse(ctypes.Structure):
    _fields_ = [
        ("txType", ctypes.c_uint8),
        ("txInfo", ctypes.c_void_p),
        ("txHash", ctypes.c_void_p),
        ("messageToSign", ctypes.c_void_p),
        ("err", ctypes.c_void_p),
    ]


class StrOrErr(ctypes.Structure):
    _fields_ = [
        ("str", ctypes.c_void_p),
        ("err", ctypes.c_void_p),
    ]


class ApiKeyResponse(ctypes.Structure):
    _fields_ = [
        ("privateKey", ctypes.c_void_p),
        ("publicKey", ctypes.c_void_p),
        ("err", ctypes.c_void_p),
    ]


def _decode_and_free(lib: Any, ptr: Any) -> Optional[str]:
    if not ptr:
        return None
    if isinstance(ptr, bytes):
        return ptr.decode("utf-8", errors="replace")
    if isinstance(ptr, str):
        return ptr
    try:
        c_str = ctypes.cast(ptr, ctypes.c_char_p).value
        if c_str is not None:
            return c_str.decode("utf-8", errors="replace")
        return None
    finally:
        if lib is not None and hasattr(lib, "Free"):
            try:
                lib.Free(ptr)
            except Exception:
                pass


def _populate_c_lib_signatures(lib: Any) -> None:
    """Populate exact ctypes argtypes and restypes matching elliottech/lighter-python signer_client.py."""
    lib.CreateClient.argtypes = [
        ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_int, ctypes.c_longlong
    ]
    lib.CreateClient.restype = ctypes.c_void_p

    if hasattr(lib, "CheckClient"):
        lib.CheckClient.argtypes = [ctypes.c_int, ctypes.c_longlong]
        lib.CheckClient.restype = ctypes.c_void_p

    # 19-argument SignCreateOrder (includes integrator fees & self-trade behavior modes)
    lib.SignCreateOrder.argtypes = [
        ctypes.c_int,       # market_index
        ctypes.c_longlong,  # client_order_index
        ctypes.c_longlong,  # base_amount
        ctypes.c_int,       # price
        ctypes.c_int,       # is_ask
        ctypes.c_int,       # order_type
        ctypes.c_int,       # time_in_force
        ctypes.c_int,       # reduce_only
        ctypes.c_int,       # trigger_price
        ctypes.c_longlong,  # order_expiry
        ctypes.c_longlong,  # integrator_account_index
        ctypes.c_int,       # integrator_taker_fee
        ctypes.c_int,       # integrator_maker_fee
        ctypes.c_uint8,     # self_trade_behavior_mode
        ctypes.c_uint8,     # self_trade_equality_mode
        ctypes.c_uint8,     # skip_nonce
        ctypes.c_longlong,  # nonce
        ctypes.c_int,       # api_key_index
        ctypes.c_longlong,  # account_index
    ]
    lib.SignCreateOrder.restype = SignedTxResponse

    # 15-argument SignModifyOrder (includes integrator fees, self-trade modes & order_version)
    lib.SignModifyOrder.argtypes = [
        ctypes.c_int,       # market_index
        ctypes.c_longlong,  # order_index
        ctypes.c_longlong,  # base_amount
        ctypes.c_longlong,  # price
        ctypes.c_longlong,  # trigger_price
        ctypes.c_longlong,  # integrator_account_index
        ctypes.c_int,       # integrator_taker_fee
        ctypes.c_int,       # integrator_maker_fee
        ctypes.c_uint8,     # self_trade_behavior_mode
        ctypes.c_uint8,     # self_trade_equality_mode
        ctypes.c_uint8,     # skip_nonce
        ctypes.c_longlong,  # nonce
        ctypes.c_longlong,  # order_version
        ctypes.c_int,       # api_key_index
        ctypes.c_longlong,  # account_index
    ]
    lib.SignModifyOrder.restype = SignedTxResponse

    # 6-argument SignCancelOrder
    lib.SignCancelOrder.argtypes = [
        ctypes.c_int,       # market_index
        ctypes.c_longlong,  # order_index
        ctypes.c_uint8,     # skip_nonce
        ctypes.c_longlong,  # nonce
        ctypes.c_int,       # api_key_index
        ctypes.c_longlong,  # account_index
    ]
    lib.SignCancelOrder.restype = SignedTxResponse

    # 7-argument SignCancelAllOrders
    lib.SignCancelAllOrders.argtypes = [
        ctypes.c_int,       # time_in_force
        ctypes.c_longlong,  # timestamp_ms
        ctypes.c_int,       # cancel_all_market_index
        ctypes.c_uint8,     # skip_nonce
        ctypes.c_longlong,  # nonce
        ctypes.c_int,       # api_key_index
        ctypes.c_longlong,  # account_index
    ]
    lib.SignCancelAllOrders.restype = SignedTxResponse

    lib.CreateAuthToken.argtypes = [
        ctypes.c_longlong,  # deadline_unix
        ctypes.c_int,       # api_key_index
        ctypes.c_longlong,  # account_index
    ]
    lib.CreateAuthToken.restype = StrOrErr

    if hasattr(lib, "Free"):
        lib.Free.argtypes = [ctypes.c_void_p]
        lib.Free.restype = None


class Signer:
    # --- Lighter Official Transaction Types (TxTypeL2*) ---
    TX_TYPE_CHANGE_PUB_KEY = 8
    TX_TYPE_CREATE_SUB_ACCOUNT = 9
    TX_TYPE_TRANSFER = 12
    TX_TYPE_WITHDRAW = 13
    TX_TYPE_CREATE_ORDER = 14
    TX_TYPE_CANCEL_ORDER = 15
    TX_TYPE_CANCEL_ALL_ORDERS = 16
    TX_TYPE_MODIFY_ORDER = 17
    TX_TYPE_UPDATE_LEVERAGE = 20
    TX_TYPE_CREATE_GROUPED_ORDERS = 28
    TX_TYPE_UPDATE_MARGIN = 29

    # Legacy op constants preserved for unit test compatibility
    OP_PLACE, OP_CANCEL, OP_MODIFY = 1, 2, 3
    SIDE = {"BUY": 0, "SELL": 1}

    # --- Lighter Official Order Types ---
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TYPE_STOP_LOSS = 2
    ORDER_TYPE_STOP_LOSS_LIMIT = 3
    ORDER_TYPE_TAKE_PROFIT = 4
    ORDER_TYPE_TAKE_PROFIT_LIMIT = 5
    ORDER_TYPE_TWAP = 6

    # --- Lighter Official Time-In-Force Constants ---
    LIGHTER_TIF_IOC = 0
    LIGHTER_TIF_GTT = 1
    LIGHTER_TIF_POST_ONLY = 2
    LIGHTER_TIF_ALO = 2
    LIGHTER_TIF_GTC = 1

    # Legacy TIF constants preserved on class attributes for backward-compatible unit tests
    TIF_GTT, TIF_FOK, TIF_IOC, TIF_ALO = 0, 1, 2, 3
    TIF_GTC = TIF_GTT

    # --- Lighter Official Cancel-All (Dead Man Switch) TIF Constants ---
    CANCEL_ALL_TIF_IMMEDIATE = 0
    CANCEL_ALL_TIF_SCHEDULED = 1
    CANCEL_ALL_TIF_ABORT = 2

    # --- Lighter Sentinel Constants ---
    NIL_TRIGGER_PRICE = 0
    DEFAULT_28_DAY_ORDER_EXPIRY = -1
    DEFAULT_IOC_EXPIRY = 0
    NIL_MARKET_INDEX = 255
    NIL_ORDER_VERSION = 0
    TX_EXPIRY_OFFSET_MS = 10 * 60 * 1000 - 1000

    def __init__(
        self,
        key_hex: str,
        address: str,
        account_index: int,
        api_key_index: int = 4,
        chain_id: int = 466324,
        rest_url: str = "https://api.rh.lighter.xyz",
        skip_nonce: bool = False,
        signer_lib_path: str = "",
    ):
        self.raw_key_hex = key_hex.removeprefix("0x").strip()
        self.address = address
        self.addr_lc = address.lower()
        self.ai = int(account_index)
        self.api_key_index = int(api_key_index)
        self.chain_id = int(chain_id)
        self.rest_url = rest_url.rstrip("/")
        self.skip_nonce = int(bool(skip_nonce))

        # Initialize Ed25519 key from first 32 bytes (64 hex chars) for deterministic fallback & legacy compatibility
        seed_bytes = bytes.fromhex(self.raw_key_hex[:64].ljust(64, "0"))
        from cryptography.hazmat.primitives.asymmetric import ed25519
        from cryptography.hazmat.primitives import serialization
        self.priv = ed25519.Ed25519PrivateKey.from_private_bytes(seed_bytes)
        pub = self.priv.public_key()
        if hasattr(pub, "public_bytes_raw"):
            self.api_key = pub.public_bytes_raw().hex()
        else:
            self.api_key = pub.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()

        self._last_ts = 0
        self._nonce: Optional[int] = None
        self._client_order_seq = int(time.time() * 1000) % 1_000_000_000
        self.client_to_venue_id: dict[str, int] = {}
        self.venue_to_client_id: dict[str, str] = {}

        self._c_lib = None
        self._c_lib_path = ""
        self.c_client_error: Optional[str] = None
        self._init_native_signer(signer_lib_path)

    def _init_native_signer(self, signer_lib_path: str = "") -> None:
        """Load official lighter-signer shared library (via lighter.signer_client or direct CDLL)."""
        # 1. Try lighter.signer_client.get_signer() first so we share the exact SDK ctypes setup
        if not signer_lib_path:
            try:
                import lighter.signer_client as lsc  # type: ignore
                lib = lsc.get_signer()
                if lib is not None:
                    self._c_lib = lib
                    self._c_lib_path = "lighter.signer_client.get_signer()"
                    err_str = self.reinit_client(self.ai, self.api_key_index)
                    if err_str is None:
                        log.info(
                            "Initialized native Lighter C signer via lighter.signer_client "
                            "(chain_id=%d, account_index=%d, api_key_index=%d)",
                            self.chain_id, self.ai, self.api_key_index,
                        )
                    else:
                        log.warning(
                            "Native Lighter C signer loaded, but CreateClient reported: %s "
                            "(check LIGHTER_API_PRIVATE_KEY [80-hex / 40-byte key], LIGHTER_ACCOUNT_INDEX=%d, LIGHTER_API_KEY_INDEX=%d)",
                            err_str, self.ai, self.api_key_index,
                        )
                    return
            except Exception as e:
                log.debug("lighter.signer_client.get_signer() not used directly: %s", e)

        # 2. Fallback: locate shared library binary on disk
        candidates = []
        if signer_lib_path and os.path.exists(signer_lib_path):
            candidates.append(signer_lib_path)

        try:
            import lighter  # type: ignore
            pkg_dir = os.path.dirname(lighter.__file__)
            sys_name = platform.system().lower()
            arch = platform.machine().lower()
            arch_tag = "arm64" if arch in ("arm64", "aarch64") else "amd64"
            ext = ".dylib" if sys_name == "darwin" else (".dll" if sys_name == "windows" else ".so")
            for sub in ("signers", "sharedlib", ""):
                p = os.path.join(pkg_dir, sub, f"lighter-signer-{sys_name}-{arch_tag}{ext}")
                if os.path.exists(p):
                    candidates.append(p)
                p2 = os.path.join(pkg_dir, sub, f"lighter_signer_{sys_name}_{arch_tag}{ext}")
                if os.path.exists(p2):
                    candidates.append(p2)
        except Exception:
            pass

        for path in candidates:
            try:
                lib = ctypes.CDLL(path)
                _populate_c_lib_signatures(lib)
                self._c_lib = lib
                self._c_lib_path = path
                err_str = self.reinit_client(self.ai, self.api_key_index)
                if err_str is None:
                    log.info(
                        "Loaded native Lighter C signer library from %s (chain_id=%d, account_index=%d, api_key_index=%d)",
                        path, self.chain_id, self.ai, self.api_key_index,
                    )
                else:
                    log.warning(
                        "Loaded native Lighter C library from %s, but CreateClient reported: %s",
                        path, err_str,
                    )
                return
            except Exception as e:
                log.debug("Could not initialize native Lighter C library at %s: %s", path, e)

    def reinit_client(
        self,
        account_index: Optional[int] = None,
        api_key_index: Optional[int] = None,
        rest_url: Optional[str] = None,
        chain_id: Optional[int] = None,
    ) -> Optional[str]:
        """(Re-)initialize the C signer client for `(account_index, api_key_index)`.
        Returns `None` on success, or the error string from `CreateClient` on failure.
        """
        if account_index is not None:
            self.ai = int(account_index)
        if api_key_index is not None:
            self.api_key_index = int(api_key_index)
        if rest_url is not None:
            self.rest_url = rest_url.rstrip("/")
        if chain_id is not None:
            self.chain_id = int(chain_id)

        if self._c_lib is None:
            return None
        try:
            err_ptr = self._c_lib.CreateClient(
                self.rest_url.encode("utf-8"),
                self.raw_key_hex.encode("utf-8"),
                int(self.chain_id),
                int(self.api_key_index),
                int(self.ai),
            )
            err_str = _decode_and_free(self._c_lib, err_ptr)
            self.c_client_error = err_str
            return err_str
        except Exception as e:
            self.c_client_error = str(e)
            return str(e)

    def check_client(self) -> Optional[str]:
        """Verify with the Lighter server that `(api_key_index, account_index)` matches `LIGHTER_API_PRIVATE_KEY`.
        Returns `None` if valid, or an error string describing the mismatch.
        """
        if self._c_lib is None:
            return None
        if self.c_client_error:
            return f"CreateClient failed: {self.c_client_error}"
        if not hasattr(self._c_lib, "CheckClient"):
            return None
        try:
            err_ptr = self._c_lib.CheckClient(int(self.api_key_index), int(self.ai))
            return _decode_and_free(self._c_lib, err_ptr)
        except Exception as e:
            return str(e)

    def _decode_signed_tx(self, res: Any) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[str]]:
        """Decode a `SignedTxResponse` returned by the C library and free all C strings."""
        err_str = _decode_and_free(self._c_lib, getattr(res, "err", None))
        tx_info_str = _decode_and_free(self._c_lib, getattr(res, "txInfo", None))
        tx_hash_str = _decode_and_free(self._c_lib, getattr(res, "txHash", None))
        _decode_and_free(self._c_lib, getattr(res, "messageToSign", None))
        if err_str:
            return None, None, None, err_str
        return int(res.txType), tx_info_str, tx_hash_str, None

    @property
    def uses_native_c_signer(self) -> bool:
        return self._c_lib is not None and self.c_client_error is None

    # ------------------------------------------------------------------
    # Nonce & ClientOrderIndex Management
    # ------------------------------------------------------------------
    def next_ts(self) -> int:
        self._last_ts = max(time.time_ns(), self._last_ts + 1)
        return self._last_ts

    def set_nonce(self, nonce: int) -> None:
        """Synchronize local nonce counter from GET /api/v1/nextNonce."""
        self._nonce = int(nonce)

    def next_nonce(self) -> int:
        """Return and increment local nonce. If not yet synchronized, starts at 0."""
        if self._nonce is None:
            self._nonce = 0
        n = self._nonce
        self._nonce += 1
        return n

    def rollback_nonce(self) -> None:
        """Roll back local optimistic nonce by 1 (or reset to None for C signer hard-refresh) on error."""
        if self.uses_native_c_signer:
            self._nonce = None
        elif self._nonce is not None and self._nonce > 0:
            self._nonce -= 1

    def next_client_order_index(self) -> int:
        """Generate a strictly increasing client_order_index in [1, 2^48 - 1]."""
        self._client_order_seq += 1
        return self._client_order_seq

    def register_order_mapping(self, client_order_index: int | str, venue_order_index: int | str) -> None:
        c_str = str(client_order_index)
        v_str = str(venue_order_index)
        try:
            self.client_to_venue_id[c_str] = int(venue_order_index)
        except (ValueError, TypeError):
            pass
        self.venue_to_client_id[v_str] = c_str

    def resolve_order_index(self, order_id: str | int) -> int:
        """Resolve an order_id string (either client_order_index, 'dry-N', or venue order_index) to integer Index."""
        s = str(order_id)
        if s in self.client_to_venue_id:
            return self.client_to_venue_id[s]
        if s.startswith("dry-"):
            try:
                return int(s.split("-", 1)[1])
            except ValueError:
                return 1
        try:
            return int(s)
        except ValueError:
            return (int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:8], 16) & 0x7FFFFFFF) or 1

    # ------------------------------------------------------------------
    # Auth Token Generation (HTTP & WebSocket private channels)
    # ------------------------------------------------------------------
    def create_auth_token(self, expiry_seconds: int = 25200, now_unix: Optional[int] = None) -> str:
        """Generate a Lighter auth token valid for up to 8 hours (default 7 hours = 25,200s)."""
        now_s = int(time.time()) if now_unix is None else int(now_unix)
        expiry_seconds = max(60, min(28500, int(expiry_seconds)))
        deadline = now_s + expiry_seconds

        if self._c_lib is not None and self.c_client_error is None:
            try:
                res = self._c_lib.CreateAuthToken(
                    int(deadline),
                    int(self.api_key_index),
                    int(self.ai),
                )
                auth_str = _decode_and_free(self._c_lib, getattr(res, "str", None))
                err_str = _decode_and_free(self._c_lib, getattr(res, "err", None))
                if not err_str and auth_str:
                    return auth_str
                if err_str:
                    log.warning("CreateAuthToken C error: %s", err_str)
            except Exception as e:
                log.warning("CreateAuthToken exception: %s", e)

        msg = f"{deadline}:{self.ai}:{self.api_key_index}"
        sig_64 = self.priv.sign(f"{self.chain_id}:{msg}".encode("utf-8"))
        digest_16 = hmac.new(self.raw_key_hex.encode("utf-8"), sig_64, hashlib.sha256).digest()[:16]
        sig_hex = (sig_64 + digest_16).hex()
        return f"{deadline}:{self.ai}:{self.api_key_index}:{sig_hex}"

    # ------------------------------------------------------------------
    # Internal Helpers for Legacy + Lighter Dual Envelope
    # ------------------------------------------------------------------
    def _typed(self, op: int, ts: int, market_id: int, **fields) -> str:
        d = {"ad": self.addr_lc, "ai": self.ai, "ct": ts, "m": market_id, "op": op, "v": 1}
        d.update(fields)
        return canonical(d)

    def _sign_l2_tx_info(self, tx_type: int, tx_info_dict: dict) -> Tuple[str, str]:
        """Compute deterministic L2 signature (base64 `Sig`) and `tx_hash` for pure-Python test/sim mode."""
        canonical_payload = canonical({"chainId": self.chain_id, "txType": tx_type, **tx_info_dict})
        sig_64 = self.priv.sign(canonical_payload.encode("utf-8"))
        extra_16 = hmac.new(self.raw_key_hex.encode("utf-8"), sig_64, hashlib.sha256).digest()[:16]
        sig_80 = sig_64 + extra_16
        sig_b64 = base64.b64encode(sig_80).decode("ascii")
        tx_hash = hashlib.sha256(sig_80).hexdigest()
        return sig_b64, tx_hash

    def _envelope(
        self,
        req_type: str,
        body: dict,
        message: str,
        ts: int,
        tx_type: Optional[int] = None,
        tx_info: Optional[str] = None,
        tx_hash: Optional[str] = None,
        client_order_index: Optional[int] = None,
        signer_error: Optional[str] = None,
    ) -> dict:
        env = {
            "type": req_type,
            "payload": body,
            "apiKey": self.api_key,
            "timestamp": str(ts),
            "signature": self.priv.sign(message.encode()).hex(),
        }
        if tx_type is not None:
            env["tx_type"] = int(tx_type)
        if tx_info is not None:
            env["tx_info"] = tx_info
        if tx_hash is not None:
            env["tx_hash"] = tx_hash
        if client_order_index is not None:
            env["client_order_index"] = int(client_order_index)
        if signer_error is not None:
            env["signer_error"] = signer_error
        return env

    @staticmethod
    def _lighter_tif_code(time_in_force: str) -> int:
        tif = time_in_force.upper()
        if tif == "IOC":
            return Signer.LIGHTER_TIF_IOC
        if tif in ("GTT", "GTC"):
            return Signer.LIGHTER_TIF_GTT
        return Signer.LIGHTER_TIF_POST_ONLY

    @staticmethod
    def _scale_price(m: Market, px: Decimal) -> int:
        tick = m.tick_for(px) if hasattr(m, "tick_for") else m.tick
        return to_int(px, tick)

    @staticmethod
    def _scale_size(m: Market, qty: Decimal) -> int:
        return to_int(qty, m.step)

    # ------------------------------------------------------------------
    # Order Signing Methods: place (TxType 14), cancel (TxType 15),
    # modify (TxType 17), schedule_cancel / cancel_all (TxType 16)
    # ------------------------------------------------------------------
    def place(
        self,
        m: Market,
        side: str,
        px: Decimal,
        qty: Decimal,
        good_til_us: int,
        time_in_force: str = "ALO",
        reduce_only: bool = False,
        client_order_index: Optional[int] = None,
        nonce: Optional[int] = None,
    ) -> dict:
        ts = self.next_ts()
        side_u = side.upper()
        if time_in_force == "IOC":
            legacy_tif_code = self.TIF_IOC
        elif time_in_force == "FOK":
            legacy_tif_code = self.TIF_FOK
        elif time_in_force in ("GTT", "GTC"):
            legacy_tif_code = self.TIF_GTT
        else:
            legacy_tif_code = self.TIF_ALO

        lighter_tif = self._lighter_tif_code(time_in_force)
        is_ask = 1 if side_u == "SELL" else 0
        r_val = 1 if reduce_only else 0
        int_price = self._scale_price(m, px)
        int_size = self._scale_size(m, qty)
        coi = int(client_order_index) if client_order_index is not None else self.next_client_order_index()
        was_unsynced = (nonce is None and self._nonce is None)
        tx_nonce = int(nonce) if nonce is not None else self.next_nonce()

        if lighter_tif == self.LIGHTER_TIF_IOC:
            order_expiry_ms = self.DEFAULT_IOC_EXPIRY
        else:
            now_ms = int(time.time() * 1000)
            req_expiry_ms = int(good_til_us // 1000)
            max_28d_ms = now_ms + 28 * 86_400 * 1000
            min_5m_ms = now_ms + 5 * 60 * 1000
            order_expiry_ms = max(min_5m_ms, min(req_expiry_ms, max_28d_ms))

        expired_at_ms = int(time.time() * 1000) + self.TX_EXPIRY_OFFSET_MS

        tx_info_str: Optional[str] = None
        tx_hash_str: Optional[str] = None
        signer_error: Optional[str] = None

        if self._c_lib is not None:
            if self.c_client_error:
                signer_error = f"Lighter C Signer CreateClient failed: {self.c_client_error}"
            else:
                try:
                    c_expiry = (
                        self.DEFAULT_IOC_EXPIRY
                        if lighter_tif == self.LIGHTER_TIF_IOC
                        else self.DEFAULT_28_DAY_ORDER_EXPIRY
                    )
                    c_nonce = -1 if was_unsynced else tx_nonce
                    n_args = len(getattr(self._c_lib.SignCreateOrder, "argtypes", None) or [])
                    if n_args == 14:
                        res = self._c_lib.SignCreateOrder(
                            int(m.market_id),
                            int(coi),
                            int(int_size),
                            int(int_price),
                            int(is_ask),
                            int(self.ORDER_TYPE_LIMIT),
                            int(lighter_tif),
                            int(r_val),
                            int(self.NIL_TRIGGER_PRICE),
                            int(c_expiry),
                            int(self.skip_nonce),
                            int(c_nonce),
                            int(self.api_key_index),
                            int(self.ai),
                        )
                    else:
                        # Official 19-argument SignCreateOrder in lighter-python / lighter-go
                        res = self._c_lib.SignCreateOrder(
                            int(m.market_id),
                            int(coi),
                            int(int_size),
                            int(int_price),
                            int(is_ask),
                            int(self.ORDER_TYPE_LIMIT),
                            int(lighter_tif),
                            int(r_val),
                            int(self.NIL_TRIGGER_PRICE),
                            int(c_expiry),
                            0,  # integrator_account_index
                            0,  # integrator_taker_fee
                            0,  # integrator_maker_fee
                            0,  # self_trade_behavior_mode
                            0,  # self_trade_equality_mode
                            int(self.skip_nonce),
                            int(c_nonce),
                            int(self.api_key_index),
                            int(self.ai),
                        )
                    _, tx_info_str, tx_hash_str, c_err = self._decode_signed_tx(res)
                    if c_err:
                        signer_error = f"SignCreateOrder error: {c_err}"
                        log.error("Lighter C SignCreateOrder failed: %s", c_err)
                    elif tx_info_str:
                        try:
                            parsed_n = json.loads(tx_info_str).get("Nonce")
                            if parsed_n is not None:
                                self._nonce = int(parsed_n) + 1
                        except Exception:
                            pass
                except Exception as e:
                    signer_error = f"SignCreateOrder exception: {e}"
                    log.error("Lighter C SignCreateOrder exception: %s", e)

        if tx_info_str is None:
            tx_info_obj: dict[str, Any] = {
                "AccountIndex": self.ai,
                "ApiKeyIndex": self.api_key_index,
                "MarketIndex": int(m.market_id),
                "ClientOrderIndex": int(coi),
                "BaseAmount": int(int_size),
                "Price": int(int_price),
                "IsAsk": int(is_ask),
                "Type": int(self.ORDER_TYPE_LIMIT),
                "TimeInForce": int(lighter_tif),
                "ReduceOnly": int(r_val),
                "TriggerPrice": int(self.NIL_TRIGGER_PRICE),
                "OrderExpiry": int(order_expiry_ms),
                "ExpiredAt": int(expired_at_ms),
                "Nonce": int(tx_nonce),
            }
            if self.skip_nonce:
                tx_info_obj["L2TxAttributes"] = {"SkipNonce": 1}
            sig_b64, tx_hash_str = self._sign_l2_tx_info(self.TX_TYPE_CREATE_ORDER, tx_info_obj)
            tx_info_obj["Sig"] = sig_b64
            tx_info_str = json.dumps(tx_info_obj, separators=(",", ":"))

        g_val = good_til_us * 1000
        gtt_body = str(good_til_us)
        msg = self._typed(
            self.OP_PLACE, ts, m.market_id, g=g_val,
            p=int_price, q=int_size, r=r_val,
            s=self.SIDE[side_u], t=legacy_tif_code,
        )
        body = {
            "address": self.address,
            "accountIndex": self.ai,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "clientOrderIndex": coi,
            "orderSide": side_u,
            "orderType": "LIMIT",
            "timeInForce": time_in_force,
            "lighterTimeInForce": lighter_tif,
            "goodTilTime": gtt_body,
            "orderExpiry": order_expiry_ms,
            "quantity": fmt(qty),
            "baseAmount": int_size,
            "price": fmt(px),
            "intPrice": int_price,
            "reduceOnly": bool(reduce_only),
            "nonce": tx_nonce,
            "timestamp": ts,
        }
        return self._envelope(
            "placeOrder",
            body,
            msg,
            ts,
            tx_type=self.TX_TYPE_CREATE_ORDER,
            tx_info=tx_info_str,
            tx_hash=tx_hash_str,
            client_order_index=coi,
            signer_error=signer_error,
        )

    def cancel(self, m: Market, order_id: str, nonce: Optional[int] = None) -> dict:
        ts = self.next_ts()
        idx = self.resolve_order_index(order_id)
        was_unsynced = (nonce is None and self._nonce is None)
        tx_nonce = int(nonce) if nonce is not None else self.next_nonce()
        c_nonce = -1 if was_unsynced else tx_nonce
        expired_at_ms = int(time.time() * 1000) + self.TX_EXPIRY_OFFSET_MS

        tx_info_str: Optional[str] = None
        tx_hash_str: Optional[str] = None
        signer_error: Optional[str] = None

        if self._c_lib is not None:
            if self.c_client_error:
                signer_error = f"Lighter C Signer CreateClient failed: {self.c_client_error}"
            else:
                try:
                    res = self._c_lib.SignCancelOrder(
                        int(m.market_id),
                        int(idx),
                        int(self.skip_nonce),
                        int(c_nonce),
                        int(self.api_key_index),
                        int(self.ai),
                    )
                    _, tx_info_str, tx_hash_str, c_err = self._decode_signed_tx(res)
                    if c_err:
                        signer_error = f"SignCancelOrder error: {c_err}"
                        log.error("Lighter C SignCancelOrder failed: %s", c_err)
                except Exception as e:
                    signer_error = f"SignCancelOrder exception: {e}"
                    log.error("Lighter C SignCancelOrder exception: %s", e)

        if tx_info_str is None:
            tx_info_obj: dict[str, Any] = {
                "AccountIndex": self.ai,
                "ApiKeyIndex": self.api_key_index,
                "MarketIndex": int(m.market_id),
                "Index": int(idx),
                "ExpiredAt": int(expired_at_ms),
                "Nonce": int(tx_nonce),
            }
            if self.skip_nonce:
                tx_info_obj["L2TxAttributes"] = {"SkipNonce": 1}
            sig_b64, tx_hash_str = self._sign_l2_tx_info(self.TX_TYPE_CANCEL_ORDER, tx_info_obj)
            tx_info_obj["Sig"] = sig_b64
            tx_info_str = json.dumps(tx_info_obj, separators=(",", ":"))

        msg = self._typed(self.OP_CANCEL, ts, m.market_id, id=order_id)
        body = {
            "address": self.address,
            "accountIndex": self.ai,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "kind": "orderId",
            "orderId": str(order_id),
            "orderIndex": idx,
            "nonce": tx_nonce,
            "timestamp": ts,
        }
        return self._envelope(
            "cancelOrder",
            body,
            msg,
            ts,
            tx_type=self.TX_TYPE_CANCEL_ORDER,
            tx_info=tx_info_str,
            tx_hash=tx_hash_str,
            signer_error=signer_error,
        )

    def modify(
        self,
        m: Market,
        order_id: str,
        side: str,
        px: Decimal,
        qty: Decimal,
        good_til_us: int,
        reduce_only: bool = False,
        nonce: Optional[int] = None,
    ) -> dict:
        ts = self.next_ts()
        side_u = side.upper()
        r_val = 1 if reduce_only else 0
        int_price = self._scale_price(m, px)
        int_size = self._scale_size(m, qty)
        idx = self.resolve_order_index(order_id)
        was_unsynced = (nonce is None and self._nonce is None)
        tx_nonce = int(nonce) if nonce is not None else self.next_nonce()
        c_nonce = -1 if was_unsynced else tx_nonce
        expired_at_ms = int(time.time() * 1000) + self.TX_EXPIRY_OFFSET_MS

        tx_info_str: Optional[str] = None
        tx_hash_str: Optional[str] = None
        signer_error: Optional[str] = None

        if self._c_lib is not None:
            if self.c_client_error:
                signer_error = f"Lighter C Signer CreateClient failed: {self.c_client_error}"
            else:
                try:
                    n_args = len(getattr(self._c_lib.SignModifyOrder, "argtypes", None) or [])
                    if n_args == 9:
                        res = self._c_lib.SignModifyOrder(
                            int(m.market_id),
                            int(idx),
                            int(int_size),
                            int(int_price),
                            int(self.NIL_TRIGGER_PRICE),
                            int(self.skip_nonce),
                            int(tx_nonce),
                            int(self.api_key_index),
                            int(self.ai),
                        )
                    else:
                        # Official 15-argument SignModifyOrder in lighter-python / lighter-go
                        res = self._c_lib.SignModifyOrder(
                            int(m.market_id),
                            int(idx),
                            int(int_size),
                            int(int_price),
                            int(self.NIL_TRIGGER_PRICE),
                            0,  # integrator_account_index
                            0,  # integrator_taker_fee
                            0,  # integrator_maker_fee
                            0,  # self_trade_behavior_mode
                            0,  # self_trade_equality_mode
                            int(self.skip_nonce),
                            int(c_nonce),
                            int(self.NIL_ORDER_VERSION),
                            int(self.api_key_index),
                            int(self.ai),
                        )
                    _, tx_info_str, tx_hash_str, c_err = self._decode_signed_tx(res)
                    if c_err:
                        signer_error = f"SignModifyOrder error: {c_err}"
                        log.error("Lighter C SignModifyOrder failed: %s", c_err)
                except Exception as e:
                    signer_error = f"SignModifyOrder exception: {e}"
                    log.error("Lighter C SignModifyOrder exception: %s", e)

        if tx_info_str is None:
            tx_info_obj: dict[str, Any] = {
                "AccountIndex": self.ai,
                "ApiKeyIndex": self.api_key_index,
                "MarketIndex": int(m.market_id),
                "Index": int(idx),
                "BaseAmount": int(int_size),
                "Price": int(int_price),
                "TriggerPrice": int(self.NIL_TRIGGER_PRICE),
                "ExpiredAt": int(expired_at_ms),
                "Nonce": int(tx_nonce),
            }
            if self.skip_nonce:
                tx_info_obj["L2TxAttributes"] = {"SkipNonce": 1}
            sig_b64, tx_hash_str = self._sign_l2_tx_info(self.TX_TYPE_MODIFY_ORDER, tx_info_obj)
            tx_info_obj["Sig"] = sig_b64
            tx_info_str = json.dumps(tx_info_obj, separators=(",", ":"))

        msg = self._typed(
            self.OP_MODIFY, ts, m.market_id, g=good_til_us * 1000, id=order_id,
            p=int_price, q=int_size, r=r_val,
            s=self.SIDE[side_u], t=self.TIF_ALO,
        )
        body = {
            "address": self.address,
            "accountIndex": self.ai,
            "apiKeyIndex": self.api_key_index,
            "marketId": m.market_id,
            "orderId": str(order_id),
            "orderIndex": idx,
            "side": side_u,
            "quantity": fmt(qty),
            "baseAmount": int_size,
            "price": fmt(px),
            "intPrice": int_price,
            "timeInForce": "ALO",
            "lighterTimeInForce": self.LIGHTER_TIF_POST_ONLY,
            "reduceOnly": bool(reduce_only),
            "goodTilTime": str(good_til_us),
            "nonce": tx_nonce,
        }
        return self._envelope(
            "modifyOrder",
            body,
            msg,
            ts,
            tx_type=self.TX_TYPE_MODIFY_ORDER,
            tx_info=tx_info_str,
            tx_hash=tx_hash_str,
            signer_error=signer_error,
        )

    def schedule_cancel(
        self,
        m: Market,
        deadline_us: "int | None",
        nonce: Optional[int] = None,
    ) -> dict:
        """Dead man switch via Lighter `SignCancelAllOrders` (TxType 16)."""
        ts = self.next_ts()
        tx_nonce = int(nonce) if nonce is not None else self.next_nonce()
        expired_at_ms = int(time.time() * 1000) + self.TX_EXPIRY_OFFSET_MS

        if deadline_us is not None:
            cancel_tif = self.CANCEL_ALL_TIF_SCHEDULED
            time_ms = int(deadline_us) // 1000 if int(deadline_us) > 10_000_000_000_000 else int(deadline_us)
        else:
            cancel_tif = self.CANCEL_ALL_TIF_ABORT
            time_ms = 0

        market_idx = int(m.market_id) if m is not None else self.NIL_MARKET_INDEX
        tx_info_str: Optional[str] = None
        tx_hash_str: Optional[str] = None
        signer_error: Optional[str] = None

        if self._c_lib is not None:
            if self.c_client_error:
                signer_error = f"Lighter C Signer CreateClient failed: {self.c_client_error}"
            else:
                try:
                    res = self._c_lib.SignCancelAllOrders(
                        int(cancel_tif),
                        int(time_ms),
                        int(market_idx),
                        int(self.skip_nonce),
                        int(tx_nonce),
                        int(self.api_key_index),
                        int(self.ai),
                    )
                    _, tx_info_str, tx_hash_str, c_err = self._decode_signed_tx(res)
                    if c_err:
                        signer_error = f"SignCancelAllOrders error: {c_err}"
                        log.error("Lighter C SignCancelAllOrders failed: %s", c_err)
                except Exception as e:
                    signer_error = f"SignCancelAllOrders exception: {e}"
                    log.error("Lighter C SignCancelAllOrders exception: %s", e)

        if tx_info_str is None:
            tx_info_obj: dict[str, Any] = {
                "AccountIndex": self.ai,
                "ApiKeyIndex": self.api_key_index,
                "TimeInForce": int(cancel_tif),
                "Time": int(time_ms),
                "MarketIndex": int(market_idx),
                "ExpiredAt": int(expired_at_ms),
                "Nonce": int(tx_nonce),
            }
            if self.skip_nonce:
                tx_info_obj["L2TxAttributes"] = {"SkipNonce": 1}
            sig_b64, tx_hash_str = self._sign_l2_tx_info(self.TX_TYPE_CANCEL_ALL_ORDERS, tx_info_obj)
            tx_info_obj["Sig"] = sig_b64
            tx_info_str = json.dumps(tx_info_obj, separators=(",", ":"))

        body: dict[str, Any] = {
            "address": self.address,
            "accountIndex": self.ai,
            "marketId": market_idx,
        }
        if deadline_us is not None:
            body["time"] = int(deadline_us)

        msg = f"{ts}scheduleCancel{canonical(body)}"
        return self._envelope(
            "scheduleCancel",
            body,
            msg,
            ts,
            tx_type=self.TX_TYPE_CANCEL_ALL_ORDERS,
            tx_info=tx_info_str,
            tx_hash=tx_hash_str,
            signer_error=signer_error,
        )

    def cancel_all_immediate(self, m: Optional[Market] = None, nonce: Optional[int] = None) -> dict:
        """Immediate exchange-side cancel-all via Lighter `SignCancelAllOrders` (TxType 16, TIF=0, Time=0)."""
        ts = self.next_ts()
        tx_nonce = int(nonce) if nonce is not None else self.next_nonce()
        expired_at_ms = int(time.time() * 1000) + self.TX_EXPIRY_OFFSET_MS
        market_idx = int(m.market_id) if m is not None else self.NIL_MARKET_INDEX

        tx_info_str: Optional[str] = None
        tx_hash_str: Optional[str] = None
        signer_error: Optional[str] = None

        if self._c_lib is not None:
            if self.c_client_error:
                signer_error = f"Lighter C Signer CreateClient failed: {self.c_client_error}"
            else:
                try:
                    res = self._c_lib.SignCancelAllOrders(
                        int(self.CANCEL_ALL_TIF_IMMEDIATE),
                        0,
                        int(market_idx),
                        int(self.skip_nonce),
                        int(tx_nonce),
                        int(self.api_key_index),
                        int(self.ai),
                    )
                    _, tx_info_str, tx_hash_str, c_err = self._decode_signed_tx(res)
                    if c_err:
                        signer_error = f"SignCancelAllOrders error: {c_err}"
                        log.error("Lighter C SignCancelAllOrders failed: %s", c_err)
                except Exception as e:
                    signer_error = f"SignCancelAllOrders exception: {e}"
                    log.error("Lighter C SignCancelAllOrders exception: %s", e)

        if tx_info_str is None:
            tx_info_obj: dict[str, Any] = {
                "AccountIndex": self.ai,
                "ApiKeyIndex": self.api_key_index,
                "TimeInForce": int(self.CANCEL_ALL_TIF_IMMEDIATE),
                "Time": 0,
                "MarketIndex": int(market_idx),
                "ExpiredAt": int(expired_at_ms),
                "Nonce": int(tx_nonce),
            }
            if self.skip_nonce:
                tx_info_obj["L2TxAttributes"] = {"SkipNonce": 1}
            sig_b64, tx_hash_str = self._sign_l2_tx_info(self.TX_TYPE_CANCEL_ALL_ORDERS, tx_info_obj)
            tx_info_obj["Sig"] = sig_b64
            tx_info_str = json.dumps(tx_info_obj, separators=(",", ":"))

        body = {
            "address": self.address,
            "accountIndex": self.ai,
            "apiKeyIndex": self.api_key_index,
            "marketId": market_idx,
            "timeInForce": self.CANCEL_ALL_TIF_IMMEDIATE,
            "time": 0,
            "nonce": tx_nonce,
        }
        msg = f"{ts}cancelAllOrders{canonical(body)}"
        return self._envelope(
            "cancelAllOrders",
            body,
            msg,
            ts,
            tx_type=self.TX_TYPE_CANCEL_ALL_ORDERS,
            tx_info=tx_info_str,
            tx_hash=tx_hash_str,
            signer_error=signer_error,
        )

    def legacy(self, action: str, body: dict) -> dict:
        ts = self.next_ts()
        return self._envelope(action, body, f"{ts}{action}{canonical(body)}", ts)
