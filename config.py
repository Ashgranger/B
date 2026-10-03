"""Bulk Trade Perpetual DEX Level 8+ Institutional Market Maker Configuration.

Loads configuration from environment variables and optional .env file.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, Tuple

from utils import Fatal

# Network endpoints for Bulk Trade Perpetual DEX on Solana
ENVS: Dict[str, Dict[str, any]] = {
    "mainnet": {
        "rest": "https://exchange-api.bulk.trade/api/v1",
        "rest_fallback": "https://api.bulk.exchange/api/v1",
        "ws": "wss://exchange-wss.bulk.trade",
        "ws_fallback": "wss://api.bulk.exchange/ws",
        "domain": 1,
    },
    "testnet": {
        "rest": "https://testnet-api.bulk.trade/api/v1",
        "rest_fallback": "https://api.testnet.bulk.exchange/api/v1",
        "ws": "wss://testnet-wss.bulk.trade",
        "ws_fallback": "wss://api.testnet.bulk.exchange/ws",
        "domain": 2,
    },
    "devnet": {
        "rest": "https://devnet-api.bulk.trade/api/v1",
        "rest_fallback": "https://api.devnet.bulk.exchange/api/v1",
        "ws": "wss://devnet-wss.bulk.trade",
        "ws_fallback": "wss://api.devnet.bulk.exchange/ws",
        "domain": 3,
    },
}


def load_env_file(path: str = ".env") -> None:
    """Load key-value pairs from a .env file if it exists."""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    if " #" in val:
                        val = val.split(" #", 1)[0].strip()
                    if key and key not in os.environ:
                        os.environ[key] = val
    except Exception:
        pass


def _e(name: str, default: any) -> str:
    v = os.getenv(name)
    return str(default) if v is None or str(v).strip() == "" else str(v).strip()


def _d(name: str, default: str) -> Decimal:
    return Decimal(_e(name, default))


def _b(name: str, default: str) -> bool:
    return _e(name, default).lower() in ("1", "true", "yes", "y", "on")


def _f(name: str, default: str) -> float:
    return float(_e(name, default))


def _i(name: str, default: str) -> int:
    return int(_e(name, default))


def _parse_guarantee_spread_capture(name: str, default: str) -> Tuple[bool, Decimal]:
    v = _e(name, default).lower()
    if v in ("0", "false", "no", "off"):
        return False, Decimal("0")
    try:
        d = Decimal(v)
        if d > 0:
            return True, d
    except Exception:
        pass
    if v in ("1", "true", "yes", "on"):
        return True, Decimal("0.5")
    return False, Decimal("0")


@dataclass
class Config:
    # --- Connection & Credentials ------------------------------------------ #
    env_name: str
    rest_url: str
    ws_url: str
    signing_key: str
    wallet_address: str
    agent_key: Optional[str]
    market: str
    market_id: int
    dry_run: bool

    # --- Fee Schedule ------------------------------------------------------ #
    maker_fee_bps: Decimal
    taker_fee_bps: Decimal
    guarantee_spread_capture: bool
    guarantee_spread_capture_bps: Decimal

    # --- Sizing & Inventory Control ---------------------------------------- #
    order_usd: Decimal
    max_position_usd: Decimal
    skew_bps: Decimal

    # --- Multi-Ladder Quotes ----------------------------------------------- #
    extra_levels: int
    level_spacing_bps: Decimal
    level_size_mult: Decimal

    # --- Quoting Edge & Valuation ------------------------------------------ #
    min_edge_bps: Decimal
    max_edge_bps: Decimal
    vol_k: Decimal
    tox_mult: Decimal
    use_micro: bool
    penny: bool

    # --- Exits, Trailing & Smart Inventory Management ---------------------- #
    enable_smart_inventory_mgmt: bool
    pos_profit_trail_enabled: bool
    pos_profit_min_bps: Decimal
    pos_profit_trail_bonus_max_bps: Decimal
    pos_profit_flow_tighten_bps: Decimal
    pos_profit_taker_lock_bps: Decimal
    emergency_taker_loss_bps: Decimal
    emergency_taker_score_threshold: Decimal
    exit_min_profit_bps: Decimal
    stress_loss_bps: Decimal
    max_hold_s: float

    # --- Adverse-Selection & Microstructure Guards ------------------------- #
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
    markout_horizons_s: str

    # --- Level 4: Adaptive Valuation & EV Quoting -------------------------- #
    enable_adaptive_ev: bool
    min_ev_bps: Decimal
    ev_hysteresis_bps: Decimal

    # --- Level 5: Order-Book & Flow Intelligence --------------------------- #
    enable_orderbook_intel: bool
    obi_alpha: Decimal
    tfi_beta: Decimal
    fill_prob_kappa: Decimal
    gamma_risk_aversion: Decimal

    # --- Level 6: Online Learning & Regimes -------------------------------- #
    enable_online_learning: bool
    regime_vol_threshold_bps: Decimal
    regime_flow_threshold: Decimal
    regime_toxic_threshold_bps: Decimal
    regime_toxic_spread_mult: Decimal
    learning_state_path: str

    # --- Level 7 & 8: High Frequency, Toxic Protection & Queue Priority ---- #
    enable_onesided_touch: bool
    onesided_tfi_threshold: Decimal
    onesided_flow_threshold: Decimal
    enable_absorption_mode: bool
    fragility_threshold: Decimal
    queue_reset_cost_bps: Decimal
    enable_alpha_target_inv: bool
    funding_carry_weight: Decimal
    alpha_weight: Decimal
    enable_selective_touch: bool
    enable_queue_model: bool
    queue_horizon_s: float
    enable_funding_carry: bool
    funding_weight: Decimal
    enable_fragility_guard: bool
    enable_exhaustion_detection: bool
    enable_empirical_learner: bool
    empirical_prior_weight: int

    # --- Cross-Exchange Arbitrage & Lead-Lag Tracking ---------------------- #
    enable_cross_exchange: bool
    cross_lead_lag_weight: Decimal
    cross_dispersion_widen_mult: Decimal
    cross_velocity_threshold_bps: Decimal
    aggressive_touch: bool
    touch_min_requote_s: float
    use_depth_imbalance: bool
    imbalance_levels: int
    imbalance_widen_bps: Decimal
    imbalance_size_cut: Decimal
    continue_add_after_reduce: bool
    run_tag: str

    # --- Quote Opportunity Dataset Logger ---------------------------------- #
    enable_quote_dataset: bool
    quote_dataset_path: str

    # --- Rate Limiting & Requoting Rules ----------------------------------- #
    session_max_loss_usd: Decimal
    halt_exit: bool
    max_actions_per_min: int
    min_requote_s: float
    retreat_bps: Decimal
    requote_bps: Decimal
    loop_s: float
    heartbeat_s: float
    reconcile_s: float
    status_s: float
    stale_s: float
    max_market_spread_bps: Decimal
    max_oracle_dev_bps: Decimal
    quote_outside_rth: bool
    journal_path: str

    @classmethod
    def load(cls, env_path: str = ".env") -> "Config":
        return cls.from_env(env_path)

    @classmethod
    def from_env(cls, env_path: str = ".env") -> "Config":
        load_env_file(env_path)

        env_name = _e("BULK_ENV", _e("ENV", "mainnet")).lower()
        if env_name not in ENVS:
            raise Fatal(f"Unknown BULK_ENV '{env_name}'. Supported: {list(ENVS.keys())}")

        default_urls = ENVS[env_name]
        rest_url = _e("BULK_REST_URL", default_urls["rest"])
        ws_url = _e("BULK_WS_URL", default_urls["ws"])

        signing_key = _e("BULK_PRIVATE_KEY", _e("BULK_SIGNING_KEY", _e("PRIVATE_KEY", "")))
        wallet_address = _e("BULK_WALLET_ADDRESS", _e("BULK_ACCOUNT_ADDRESS", _e("WALLET_ADDRESS", "")))
        agent_key = _e("BULK_AGENT_KEY", "") or None
        market = _e("MARKET", "BTC-USD").upper()
        market_id = _i("MARKET_ID", "1")
        dry_run = _b("DRY_RUN", "1")

        maker_fee_bps = _d("MAKER_FEE_BPS", "1.0")
        taker_fee_bps = _d("TAKER_FEE_BPS", "3.5")
        g_spread, g_margin = _parse_guarantee_spread_capture("GUARANTEE_SPREAD_CAPTURE", "1")

        min_edge_default = str(maker_fee_bps * Decimal("2") + g_margin) if g_spread else "2.5"
        min_edge_bps = _d("MIN_EDGE_BPS", min_edge_default)
        max_edge_bps = _d("MAX_EDGE_BPS", "30.0")

        return cls(
            env_name=env_name,
            rest_url=rest_url,
            ws_url=ws_url,
            signing_key=signing_key,
            wallet_address=wallet_address,
            agent_key=agent_key,
            market=market,
            market_id=market_id,
            dry_run=dry_run,

            maker_fee_bps=maker_fee_bps,
            taker_fee_bps=taker_fee_bps,
            guarantee_spread_capture=g_spread,
            guarantee_spread_capture_bps=g_margin,

            order_usd=_d("ORDER_USD", "25.0"),
            max_position_usd=_d("MAX_POSITION_USD", "100.0"),
            skew_bps=_d("SKEW_BPS", "3.0"),

            extra_levels=_i("EXTRA_LEVELS", "1"),
            level_spacing_bps=_d("LEVEL_SPACING_BPS", "4.0"),
            level_size_mult=_d("LEVEL_SIZE_MULT", "0.6"),

            min_edge_bps=min_edge_bps,
            max_edge_bps=max_edge_bps,
            vol_k=_d("VOL_K", "0.5"),
            tox_mult=_d("TOX_MULT", "2.0"),
            use_micro=_b("USE_MICRO", "1"),
            penny=_b("PENNY", "1"),

            enable_smart_inventory_mgmt=_b("ENABLE_SMART_INVENTORY_MGMT", "1"),
            pos_profit_trail_enabled=_b("POS_PROFIT_TRAIL_ENABLED", "1"),
            pos_profit_min_bps=_d("POS_PROFIT_MIN_BPS", "2.9"),
            pos_profit_trail_bonus_max_bps=_d("POS_PROFIT_TRAIL_BONUS_MAX_BPS", "3.0"),
            pos_profit_flow_tighten_bps=_d("POS_PROFIT_FLOW_TIGHTEN_BPS", "1.0"),
            pos_profit_taker_lock_bps=_d("POS_PROFIT_TAKER_LOCK_BPS", "5.0"),
            emergency_taker_loss_bps=_d("EMERGENCY_TAKER_LOSS_BPS", "6.0"),
            emergency_taker_score_threshold=_d("EMERGENCY_TAKER_SCORE_THRESHOLD", "2.5"),
            exit_min_profit_bps=_d("EXIT_MIN_PROFIT_BPS", "2.0"),
            stress_loss_bps=_d("STRESS_LOSS_BPS", "15.0"),
            max_hold_s=_f("MAX_HOLD_S", "120.0"),

            trend_window_s=_f("TREND_WINDOW_S", "5.0"),
            trend_pull_bps=_d("TREND_PULL_BPS", "8.0"),
            trend_widen=_d("TREND_WIDEN", "1.2"),
            trend_hold_s=_f("TREND_HOLD_S", "3.0"),
            vol_window_s=_f("VOL_WINDOW_S", "10.0"),
            vol_pause_bps=_d("VOL_PAUSE_BPS", "25.0"),
            jump_bps=_d("JUMP_BPS", "15.0"),
            jump_cooldown_s=_f("JUMP_COOLDOWN_S", "2.0"),
            burst_fills=_i("BURST_FILLS", "3"),
            burst_window_s=_f("BURST_WINDOW_S", "3.0"),
            burst_cooldown_s=_f("BURST_COOLDOWN_S", "5.0"),
            sweep_guard_fills=_i("SWEEP_GUARD_FILLS", "2"),
            sweep_guard_window_s=_f("SWEEP_GUARD_WINDOW_S", "1.5"),
            markout_horizon_s=_f("MARKOUT_HORIZON_S", "2.0"),
            markout_window=_i("MARKOUT_WINDOW", "30"),
            markout_horizons_s=_e("MARKOUT_HORIZONS_S", "0.25,0.5,1,2,5,10,30"),

            enable_adaptive_ev=_b("ENABLE_ADAPTIVE_EV", "1"),
            min_ev_bps=_d("MIN_EV_BPS", "0.15"),
            ev_hysteresis_bps=_d("EV_HYSTERESIS_BPS", "0.10"),

            enable_orderbook_intel=_b("ENABLE_ORDERBOOK_INTEL", "1"),
            obi_alpha=_d("OBI_ALPHA", "1.0"),
            tfi_beta=_d("TFI_BETA", "1.5"),
            fill_prob_kappa=_d("FILL_PROB_KAPPA", "0.35"),
            gamma_risk_aversion=_d("GAMMA_RISK_AVERSION", "0.1"),

            enable_online_learning=_b("ENABLE_ONLINE_LEARNING", "1"),
            regime_vol_threshold_bps=_d("REGIME_VOL_THRESHOLD_BPS", "5.0"),
            regime_flow_threshold=_d("REGIME_FLOW_THRESHOLD", "0.35"),
            regime_toxic_threshold_bps=_d("REGIME_TOXIC_THRESHOLD_BPS", "1.5"),
            regime_toxic_spread_mult=_d("REGIME_TOXIC_SPREAD_MULT", "1.5"),
            learning_state_path=_e("LEARNING_STATE_PATH", "learning_state.json"),

            enable_onesided_touch=_b("ENABLE_ONESIDED_TOUCH", "1"),
            onesided_tfi_threshold=_d("ONESIDED_TFI_THRESHOLD", "0.45"),
            onesided_flow_threshold=_d("ONESIDED_FLOW_THRESHOLD", "0.40"),
            enable_absorption_mode=_b("ENABLE_ABSORPTION_MODE", "1"),
            fragility_threshold=_d("FRAGILITY_THRESHOLD", "0.60"),
            queue_reset_cost_bps=_d("QUEUE_RESET_COST_BPS", "0.20"),
            enable_alpha_target_inv=_b("ENABLE_ALPHA_TARGET_INV", "1"),
            funding_carry_weight=_d("FUNDING_CARRY_WEIGHT", "0.5"),
            alpha_weight=_d("ALPHA_WEIGHT", "1.0"),
            enable_selective_touch=_b("ENABLE_SELECTIVE_TOUCH", "1"),
            enable_queue_model=_b("ENABLE_QUEUE_MODEL", "1"),
            queue_horizon_s=_f("QUEUE_HORIZON_S", "2.0"),
            enable_funding_carry=_b("ENABLE_FUNDING_CARRY", "1"),
            funding_weight=_d("FUNDING_WEIGHT", "0.5"),
            enable_fragility_guard=_b("ENABLE_FRAGILITY_GUARD", "1"),
            enable_exhaustion_detection=_b("ENABLE_EXHAUSTION_DETECTION", "1"),
            enable_empirical_learner=_b("ENABLE_EMPIRICAL_LEARNER", "1"),
            empirical_prior_weight=_i("EMPIRICAL_PRIOR_WEIGHT", "5"),

            enable_cross_exchange=_b("ENABLE_CROSS_EXCHANGE", "0"),
            cross_lead_lag_weight=_d("CROSS_LEAD_LAG_WEIGHT", "0.5"),
            cross_dispersion_widen_mult=_d("CROSS_DISPERSION_WIDEN_MULT", "1.5"),
            cross_velocity_threshold_bps=_d("CROSS_VELOCITY_THRESHOLD_BPS", "1.5"),
            aggressive_touch=_b("AGGRESSIVE_TOUCH", "1"),
            touch_min_requote_s=_f("TOUCH_MIN_REQUOTE_S", "0.25"),
            use_depth_imbalance=_b("USE_DEPTH_IMBALANCE", "1"),
            imbalance_levels=_i("IMBALANCE_LEVELS", "5"),
            imbalance_widen_bps=_d("IMBALANCE_WIDEN_BPS", "3.0"),
            imbalance_size_cut=_d("IMBALANCE_SIZE_CUT", "0.3"),
            continue_add_after_reduce=_b("CONTINUE_ADD_AFTER_REDUCE", "1"),
            run_tag=_e("RUN_TAG", "bulk_default"),

            enable_quote_dataset=_b("ENABLE_QUOTE_DATASET", "1"),
            quote_dataset_path=_e("QUOTE_DATASET_FILE", "bulk_quote_opportunities.jsonl"),

            session_max_loss_usd=_d("SESSION_MAX_LOSS_USD", "100.0"),
            halt_exit=_b("HALT_EXIT", "1"),
            max_actions_per_min=_i("MAX_ACTIONS_PER_MIN", "120"),
            min_requote_s=_f("MIN_REQUOTE_S", "0.5"),
            retreat_bps=_d("RETREAT_BPS", "1.5"),
            requote_bps=_d("REQUOTE_BPS", "3.0"),
            loop_s=_f("LOOP_S", "0.05"),
            heartbeat_s=_f("HEARTBEAT_S", "5.0"),
            reconcile_s=_f("RECONCILE_S", "5.0"),
            status_s=_f("STATUS_S", "10.0"),
            stale_s=_f("STALE_S", "15.0"),
            max_market_spread_bps=_d("MAX_MARKET_SPREAD_BPS", "50.0"),
            max_oracle_dev_bps=_d("MAX_ORACLE_DEV_BPS", "150.0"),
            quote_outside_rth=_b("QUOTE_OUTSIDE_RTH", "0"),
            journal_path=_e("JOURNAL_PATH", "bulk_fills.jsonl"),
        )
