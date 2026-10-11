"""Complete institutional configuration for Bulk Trade Perp DEX Market Maker."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple, Dict, Any

from utils import Fatal

BULK_ENVS = {
    "mainnet": {
        "rest": "https://exchange-api.bulk.trade/api/v1",
        "ws": "wss://exchange-wss.bulk.trade",
        "domain": 1,
    },
    "testnet": {
        "rest": "https://testnet-api.bulk.trade/api/v1",
        "ws": "wss://testnet-wss.bulk.trade",
        "domain": 2,
    },
    "devnet": {
        "rest": "https://devnet-api.bulk.trade/api/v1",
        "ws": "wss://devnet-wss.bulk.trade",
        "domain": 3,
    },
}


def _e(name: str, default: Any = "") -> str:
    v = os.getenv(name)
    return str(default) if v is None or v.strip() == "" else v.strip()


def _d(name: str, default: str) -> Decimal:
    return Decimal(str(_e(name, default)))


def _b(name: str, default: str) -> bool:
    return str(_e(name, default)).lower() in ("1", "true", "yes", "y", "on")


def _parse_weights(raw: str) -> dict:
    out = {}
    for part in raw.split(","):
        if ":" in part:
            k, v = part.split(":", 1)
            try:
                out[k.strip().lower()] = float(v)
            except ValueError:
                pass
    return out


def _parse_guarantee_spread_capture(name: str, default: str) -> tuple[bool, Decimal]:
    v = _e(name, default)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False, Decimal("0")
    try:
        d = Decimal(str(v).strip())
        if d > 0:
            return True, d
    except Exception:
        pass
    if s in ("1", "true", "yes", "on"):
        return True, Decimal("0.5")
    return False, Decimal("0")


@dataclass
class Config:
    # --- Bulk Trade Connection -------------------------------------------- #
    env_name: str
    rest_url: str
    ws_url: str
    domain: int
    private_key: str
    account: str
    agent_key: Optional[str]
    builder_to: Optional[str]
    builder_fee_bps: int
    market: str
    dry_run: bool

    # --- Sizing & Inventory ----------------------------------------------- #
    order_usd: Decimal
    max_position_usd: Decimal
    skew_bps: Decimal

    # --- Ladder Quoting Levels -------------------------------------------- #
    extra_levels: int
    level_spacing_bps: Decimal
    level_size_mult: Decimal

    # --- Edge & Spread ---------------------------------------------------- #
    min_edge_bps: Decimal
    max_edge_bps: Decimal
    maker_fee_bps: Decimal
    vol_k: Decimal
    tox_mult: Decimal
    use_micro: bool
    penny: bool

    # --- Exits & Stress --------------------------------------------------- #
    exit_min_profit_bps: Decimal
    stress_loss_bps: Decimal
    max_hold_s: float

    # --- Microstructure & Risk Guards ------------------------------------- #
    trend_window_s: float
    trend_pull_bps: Decimal
    trend_widen: Decimal
    trend_hold_s: float
    vol_window_s: float
    vol_pause_bps: Decimal
    jump_bps: Decimal
    jump_cooldown_s: float
    burst_fills: int
    burst_window_s: float
    burst_cooldown_s: float
    sweep_guard_fills: int
    sweep_guard_window_s: float
    markout_horizon_s: float
    markout_window: int

    # --- Quantitative Models & Fill Dynamics ------------------------------ #
    min_ev_bps: Decimal
    enable_adaptive_ev: bool
    enable_orderbook_intel: bool
    obi_alpha: Decimal
    tfi_beta: Decimal
    fill_prob_kappa: Decimal
    gamma_risk_aversion: Decimal
    enable_online_learning: bool
    regime_vol_threshold_bps: Decimal
    regime_flow_threshold: Decimal
    regime_toxic_threshold_bps: Decimal
    regime_toxic_spread_mult: Decimal
    ev_hysteresis_bps: Decimal
    learning_state_path: str

    # --- Cross-Exchange & Lead/Lag Intelligence --------------------------- #
    enable_cross_exchange: bool
    cross_lead_lag_weight: Decimal
    cross_dispersion_widen_mult: Decimal
    cross_velocity_threshold_bps: Decimal
    guarantee_spread_capture: bool
    guarantee_spread_capture_bps: Decimal
    aggressive_touch: bool
    touch_min_requote_s: float
    use_depth_imbalance: bool
    imbalance_levels: int
    imbalance_widen_bps: Decimal
    imbalance_size_cut: Decimal
    continue_add_after_reduce: bool
    run_tag: str
    markout_horizons_s: str

    # --- Inventory Risk Management & Exits -------------------------------- #
    enable_smart_inventory_mgmt: bool
    taker_fee_bps: Decimal
    taker_slip_bps: Decimal
    adv_obi_exit: bool
    exclude_own_orders: bool
    market_refresh_s: float
    enable_taker_exits: bool
    maker_exit_first: bool
    maker_exit_slack_bps: Decimal
    maker_exit_min_prob: float
    taker_hard_stop_bps: Decimal
    oracle_guard: bool
    oracle_guard_bps: Decimal
    enable_dynamic_sizing: bool
    dyn_size_min: Decimal
    dyn_inv_cap_frac: Decimal
    dyn_inv_min: Decimal
    dyn_edge_ref_bps: Decimal
    dyn_edge_min_n: int
    dyn_warmup_mult: Decimal
    dyn_vol_ref_bps: Decimal
    dyn_dd_weight: Decimal
    own_order_min_age_s: float
    session_loss_action: str
    quote_dataset_min_s: float
    quote_dataset_max_mb: float
    adv_obi_thresh: Decimal
    adv_obi_secs: float
    adv_obi_loss_bps: Decimal
    taker_fill_price_mode: str
    emergency_taker_loss_bps: Decimal
    emergency_taker_score_threshold: Decimal

    # --- Bulk Trade Specific: Speed Bump & Selective Touch ---------------- #
    taker_speed_bump_enabled: bool
    speed_bump_advantage_bps: Decimal
    batch_replace_enabled: bool
    enable_selective_touch: bool
    enable_queue_model: bool
    queue_horizon_s: float
    enable_funding_carry: bool
    funding_weight: Decimal
    enable_fragility_guard: bool
    fragility_threshold: Decimal
    enable_exhaustion_detection: bool
    queue_reset_cost_bps: Decimal
    enable_absorption_mode: bool
    enable_onesided_touch: bool
    et_pause_windows: str
    enable_quote_dataset: bool
    quote_dataset_path: str
    enable_empirical_learner: bool
    empirical_prior_weight: int

    # --- Risk & Execution Tunables ---------------------------------------- #
    session_max_loss_usd: Decimal
    halt_exit: bool
    requote_bps: Decimal
    retreat_bps: Decimal
    min_requote_s: float
    max_actions_per_min: int
    loop_s: float
    heartbeat_s: float
    dms_enabled: bool
    dms_ttl_s: float
    dms_required: bool
    reconcile_s: float
    status_s: float
    stale_s: float
    max_market_spread_bps: Decimal
    max_oracle_dev_bps: Decimal
    quote_outside_rth: bool
    journal_path: str

    # --- External CEX Feeds (Binance / Bybit) ----------------------------- #
    cross_feed: bool = True
    cross_venues: str = "binance,bybit"
    binance_symbol: str = ""
    bybit_symbol: str = ""
    binance_ws_url: str = "wss://fstream.binance.com"
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    cross_stale_s: float = 2.0
    cross_basis_tau_s: float = 45.0
    cross_warmup_s: float = 10.0
    cross_max_shift_bps: float = 4.0
    cross_flow_k_usd: float = 20000.0
    cross_weights: dict = None
    cross_pull_bps: float = 2.5
    cross_vel_pull_bps: float = 3.0
    cross_pull_hold_s: float = 1.5
    cross_liq_usd: float = 50000.0
    cross_div_adverse_mult: float = 1.0
    cross_flow_weight: float = 0.3

    @classmethod
    def from_env(cls) -> "Config":
        env_name = str(_e("BULK_ENV", _e("ARCUS_ENV", "mainnet"))).lower()
        if env_name not in BULK_ENVS:
            env_name = "mainnet"
        env_info = BULK_ENVS[env_name]

        rest_url = str(_e("BULK_REST_URL", env_info["rest"]))
        ws_url = str(_e("BULK_WS_URL", env_info["ws"]))
        domain = int(_e("BULK_DOMAIN", str(env_info["domain"])))

        priv_key = str(_e("BULK_PRIVATE_KEY", _e("ARCUS_API_SIGNING_KEY", ""))).removeprefix("0x")
        dry = _b("DRY_RUN", "1")
        if not dry and not priv_key:
            raise Fatal("BULK_PRIVATE_KEY must be provided for live trading (DRY_RUN=0)")

        account = str(_e("BULK_ACCOUNT", _e("ARCUS_WALLET_ADDRESS", "")))
        agent_key = _e("BULK_AGENT_KEY", "") or None
        builder_to = _e("BULK_BUILDER_TO", "") or None
        builder_fee_bps = int(_e("BULK_BUILDER_FEE", "0"))

        market = str(_e("MARKET", "SOL-USD")).upper()
        guar_sc, guar_sc_bps = _parse_guarantee_spread_capture("GUARANTEE_SPREAD_CAPTURE", "1")

        weights = _parse_weights(_e("CROSS_WEIGHTS", "binance:0.6,bybit:0.4"))

        return cls(
            env_name=env_name,
            rest_url=rest_url,
            ws_url=ws_url,
            domain=domain,
            private_key=priv_key,
            account=account,
            agent_key=agent_key,
            builder_to=builder_to,
            builder_fee_bps=builder_fee_bps,
            market=market,
            dry_run=dry,
            order_usd=_d("ORDER_USD", "30"),
            max_position_usd=_d("MAX_POSITION_USD", "60"),
            skew_bps=_d("SKEW_BPS", "3"),
            extra_levels=int(_e("EXTRA_LEVELS", "1")),
            level_spacing_bps=_d("LEVEL_SPACING_BPS", "4"),
            level_size_mult=_d("LEVEL_SIZE_MULT", "0.6"),
            min_edge_bps=_d("MIN_EDGE_BPS", "2"),
            max_edge_bps=_d("MAX_EDGE_BPS", "12"),
            maker_fee_bps=_d("MAKER_FEE_BPS", "0"),
            vol_k=_d("VOL_K", "0.5"),
            tox_mult=_d("TOX_MULT", "1"),
            use_micro=_b("USE_MICRO", "1"),
            penny=_b("PENNY", "1"),
            exit_min_profit_bps=_d("EXIT_MIN_PROFIT_BPS", "1.5"),
            stress_loss_bps=_d("STRESS_LOSS_BPS", "20"),
            max_hold_s=float(_e("MAX_HOLD_S", "120")),
            trend_window_s=float(_e("TREND_WINDOW_S", "5")),
            trend_pull_bps=_d("TREND_PULL_BPS", "2.5"),
            trend_widen=_d("TREND_WIDEN", "1"),
            trend_hold_s=float(_e("TREND_HOLD_S", "3")),
            vol_window_s=float(_e("VOL_WINDOW_S", "5")),
            vol_pause_bps=_d("VOL_PAUSE_BPS", "8"),
            jump_bps=_d("JUMP_BPS", "6"),
            jump_cooldown_s=float(_e("JUMP_COOLDOWN_S", "2")),
            burst_fills=int(_e("BURST_FILLS", "3")),
            burst_window_s=float(_e("BURST_WINDOW_S", "15")),
            burst_cooldown_s=float(_e("BURST_COOLDOWN_S", "20")),
            sweep_guard_fills=int(_e("SWEEP_GUARD_FILLS", "2")),
            sweep_guard_window_s=float(_e("SWEEP_GUARD_WINDOW_S", "1.0")),
            markout_horizon_s=float(_e("MARKOUT_HORIZON_S", "5")),
            markout_window=int(_e("MARKOUT_WINDOW", "10")),
            min_ev_bps=_d("MIN_EV_BPS", "0.2"),
            enable_adaptive_ev=_b("ENABLE_ADAPTIVE_EV", "1"),
            enable_orderbook_intel=_b("ENABLE_ORDERBOOK_INTEL", "1"),
            obi_alpha=_d("OBI_ALPHA", "1.0"),
            tfi_beta=_d("TFI_BETA", "1.5"),
            fill_prob_kappa=_d("FILL_PROB_KAPPA", "0.25"),
            gamma_risk_aversion=_d("GAMMA_RISK_AVERSION", "0.1"),
            enable_online_learning=_b("ENABLE_ONLINE_LEARNING", "1"),
            regime_vol_threshold_bps=_d("REGIME_VOL_THRESHOLD_BPS", "5.0"),
            regime_flow_threshold=_d("REGIME_FLOW_THRESHOLD", "0.35"),
            regime_toxic_threshold_bps=_d("REGIME_TOXIC_THRESHOLD_BPS", "1.5"),
            regime_toxic_spread_mult=_d("REGIME_TOXIC_SPREAD_MULT", "1.5"),
            ev_hysteresis_bps=_d("EV_HYSTERESIS_BPS", "0.1"),
            learning_state_path=str(_e("LEARNING_STATE_PATH", "learning_state.json")),
            enable_cross_exchange=_b("ENABLE_CROSS_EXCHANGE", "1"),
            cross_lead_lag_weight=_d("CROSS_LEAD_LAG_WEIGHT", "0.5"),
            cross_dispersion_widen_mult=_d("CROSS_DISPERSION_WIDEN_MULT", "1.5"),
            cross_velocity_threshold_bps=_d("CROSS_VELOCITY_THRESHOLD_BPS", "3.0"),
            guarantee_spread_capture=guar_sc,
            guarantee_spread_capture_bps=guar_sc_bps,
            aggressive_touch=_b("AGGRESSIVE_TOUCH", "1"),
            touch_min_requote_s=float(_e("TOUCH_MIN_REQUOTE_S", "0.3")),
            use_depth_imbalance=_b("USE_DEPTH_IMBALANCE", "1"),
            imbalance_levels=int(_e("IMBALANCE_LEVELS", "5")),
            imbalance_widen_bps=_d("IMBALANCE_WIDEN_BPS", "2.0"),
            imbalance_size_cut=_d("IMBALANCE_SIZE_CUT", "0.5"),
            continue_add_after_reduce=_b("CONTINUE_ADD_AFTER_REDUCE", "0"),
            run_tag=str(_e("RUN_TAG", "bulk_alpha")),
            markout_horizons_s=str(_e("MARKOUT_HORIZONS_S", "0.5,1,2,5")),
            enable_smart_inventory_mgmt=_b("ENABLE_SMART_INVENTORY_MGMT", "1"),
            taker_fee_bps=_d("TAKER_FEE_BPS", "4.0"),
            taker_slip_bps=_d("TAKER_SLIP_BPS", "2.0"),
            adv_obi_exit=_b("ADV_OBI_EXIT", "1"),
            exclude_own_orders=_b("EXCLUDE_OWN_ORDERS", "1"),
            market_refresh_s=float(_e("MARKET_REFRESH_S", "30.0")),
            enable_taker_exits=_b("ENABLE_TAKER_EXITS", "1"),
            maker_exit_first=_b("MAKER_EXIT_FIRST", "1"),
            maker_exit_slack_bps=_d("MAKER_EXIT_SLACK_BPS", "0.5"),
            maker_exit_min_prob=float(_e("MAKER_EXIT_MIN_PROB", "0.35")),
            taker_hard_stop_bps=_d("TAKER_HARD_STOP_BPS", "25.0"),
            oracle_guard=_b("ORACLE_GUARD", "1"),
            oracle_guard_bps=_d("ORACLE_GUARD_BPS", "10.0"),
            enable_dynamic_sizing=_b("ENABLE_DYNAMIC_SIZING", "1"),
            dyn_size_min=_d("DYN_SIZE_MIN", "0.5"),
            dyn_inv_cap_frac=_d("DYN_INV_CAP_FRAC", "0.8"),
            dyn_inv_min=_d("DYN_INV_MIN", "0.2"),
            dyn_edge_ref_bps=_d("DYN_EDGE_REF_BPS", "3.0"),
            dyn_edge_min_n=int(_e("DYN_EDGE_MIN_N", "10")),
            dyn_warmup_mult=_d("DYN_WARMUP_MULT", "0.5"),
            dyn_vol_ref_bps=_d("DYN_VOL_REF_BPS", "5.0"),
            dyn_dd_weight=_d("DYN_DD_WEIGHT", "0.5"),
            own_order_min_age_s=float(_e("OWN_ORDER_MIN_AGE_S", "0.05")),
            session_loss_action=str(_e("SESSION_LOSS_ACTION", "pause_day")),
            quote_dataset_min_s=float(_e("QUOTE_DATASET_MIN_S", "1.0")),
            quote_dataset_max_mb=float(_e("QUOTE_DATASET_MAX_MB", "50.0")),
            adv_obi_thresh=_d("ADV_OBI_THRESH", "0.4"),
            adv_obi_secs=float(_e("ADV_OBI_SECS", "5.0")),
            adv_obi_loss_bps=_d("ADV_OBI_LOSS_BPS", "5.0"),
            taker_fill_price_mode=str(_e("TAKER_FILL_PRICE_MODE", "est")),
            emergency_taker_loss_bps=_d("EMERGENCY_TAKER_LOSS_BPS", "20.0"),
            emergency_taker_score_threshold=_d("EMERGENCY_TAKER_SCORE_THRESHOLD", "3.0"),
            taker_speed_bump_enabled=_b("TAKER_SPEED_BUMP_ENABLED", "1"),
            speed_bump_advantage_bps=_d("SPEED_BUMP_ADVANTAGE_BPS", "1.5"),
            batch_replace_enabled=_b("BATCH_REPLACE_ENABLED", "1"),
            enable_selective_touch=_b("ENABLE_SELECTIVE_TOUCH", "1"),
            enable_queue_model=_b("ENABLE_QUEUE_MODEL", "1"),
            queue_horizon_s=float(_e("QUEUE_HORIZON_S", "2.0")),
            enable_funding_carry=_b("ENABLE_FUNDING_CARRY", "1"),
            funding_weight=_d("FUNDING_WEIGHT", "0.5"),
            enable_fragility_guard=_b("ENABLE_FRAGILITY_GUARD", "1"),
            fragility_threshold=_d("FRAGILITY_THRESHOLD", "0.6"),
            enable_exhaustion_detection=_b("ENABLE_EXHAUSTION_DETECTION", "1"),
            queue_reset_cost_bps=_d("QUEUE_RESET_COST_BPS", "0.20"),
            enable_absorption_mode=_b("ENABLE_ABSORPTION_MODE", "1"),
            enable_onesided_touch=_b("ENABLE_ONESIDED_TOUCH", "1"),
            et_pause_windows=str(_e("ET_PAUSE_WINDOWS", "")),
            enable_quote_dataset=_b("ENABLE_QUOTE_DATASET", "1"),
            quote_dataset_path=str(_e("QUOTE_DATASET_PATH", "quote_opps.jsonl")),
            enable_empirical_learner=_b("ENABLE_EMPIRICAL_LEARNER", "1"),
            empirical_prior_weight=int(_e("EMPIRICAL_PRIOR_WEIGHT", "5")),
            session_max_loss_usd=_d("SESSION_MAX_LOSS_USD", "100.0"),
            halt_exit=_b("HALT_EXIT", "1"),
            requote_bps=_d("REQUOTE_BPS", "1.5"),
            retreat_bps=_d("RETREAT_BPS", "1.0"),
            min_requote_s=float(_e("MIN_REQUOTE_S", "0.5")),
            max_actions_per_min=int(_e("MAX_ACTIONS_PER_MIN", "240")),
            loop_s=float(_e("LOOP_S", "0.05")),
            heartbeat_s=float(_e("HEARTBEAT_S", "3.0")),
            dms_enabled=_b("DMS_ENABLED", "1"),
            dms_ttl_s=float(_e("DMS_TTL_S", "10.0")),
            dms_required=_b("DMS_REQUIRED", "0"),
            reconcile_s=float(_e("RECONCILE_S", "15.0")),
            status_s=float(_e("STATUS_S", "10.0")),
            stale_s=float(_e("STALE_S", "3.0")),
            max_market_spread_bps=_d("MAX_MARKET_SPREAD_BPS", "30.0"),
            max_oracle_dev_bps=_d("MAX_ORACLE_DEV_BPS", "15.0"),
            quote_outside_rth=_b("QUOTE_OUTSIDE_RTH", "1"),
            journal_path=str(_e("JOURNAL_PATH", "journal.jsonl")),
            cross_feed=_b("CROSS_FEED", "1"),
            cross_venues=str(_e("CROSS_VENUES", "binance,bybit")),
            binance_symbol=str(_e("BINANCE_SYMBOL", "")),
            bybit_symbol=str(_e("BYBIT_SYMBOL", "")),
            binance_ws_url=str(_e("BINANCE_WS_URL", "wss://fstream.binance.com")),
            bybit_ws_url=str(_e("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")),
            cross_stale_s=float(_e("CROSS_STALE_S", "2.0")),
            cross_basis_tau_s=float(_e("CROSS_BASIS_TAU_S", "45.0")),
            cross_warmup_s=float(_e("CROSS_WARMUP_S", "10.0")),
            cross_max_shift_bps=float(_e("CROSS_MAX_SHIFT_BPS", "4.0")),
            cross_flow_k_usd=float(_e("CROSS_FLOW_K_USD", "20000.0")),
            cross_weights=weights,
            cross_pull_bps=float(_e("CROSS_PULL_BPS", "2.5")),
            cross_vel_pull_bps=float(_e("CROSS_VEL_PULL_BPS", "3.0")),
            cross_pull_hold_s=float(_e("CROSS_PULL_HOLD_S", "1.5")),
            cross_liq_usd=float(_e("CROSS_LIQ_USD", "50000.0")),
            cross_div_adverse_mult=float(_e("CROSS_DIV_ADVERSE_MULT", "1.0")),
            cross_flow_weight=float(_e("CROSS_FLOW_WEIGHT", "0.3")),
        )
