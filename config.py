"""All tunables and configuration for Robinhood Lighter Perp DEX Level 8+ Market Maker.

Official Docs: https://apidocs.rh.lighter.xyz/docs/get-started
Base REST: https://api.rh.lighter.xyz
WebSocket: wss://api.rh.lighter.xyz/stream
L2 Chain ID (Signing Domain): 466324 (mainnet), 300 (testnet)
API Key Index: 4+ (indices 0-3 reserved for Robinhood apps)
Fees: Maker 0.012% (1.2 bps), Taker 0.035% (3.5 bps)
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Set

from utils import Fatal

ENVS = {
    "mainnet": {
        "rest": "https://api.rh.lighter.xyz",
        "ws": "wss://api.rh.lighter.xyz/stream",
        "chain_id": 466324,
    },
    "testnet": {
        "rest": "https://api.rh-testnet.lighter.xyz",
        "ws": "wss://api.rh-testnet.lighter.xyz/stream",
        "chain_id": 300,
    },
    "zklighter_mainnet": {
        "rest": "https://mainnet.zklighter.elliot.ai/api/v1",
        "ws": "wss://mainnet.zklighter.elliot.ai/stream",
        "chain_id": 466324,
    },
}


def _e(name: str, default: any = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() != "" else str(default)


def _d(name: str, default: str) -> Decimal:
    return Decimal(_e(name, default))


def _b(name: str, default: str = "0") -> bool:
    return _e(name, default).lower() in ("1", "true", "yes", "on")


def _s(name: str, default: str = "") -> Set[str]:
    raw = _e(name, default)
    if not raw:
        return set()
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


@dataclass
class Config:
    # --- connection -------------------------------------------------------- #
    env_name: str = "mainnet"
    address: str = "0x0000000000000000000000000000000000000000"
    signing_key: str = "0" * 64
    account_index: int = 0
    api_key_index: int = 4
    chain_id: int = 466324
    market: str = "BTC-USD"
    market_id: int = 1
    dry_run: bool = True

    # --- sizing / inventory ------------------------------------------------ #
    order_usd: Decimal = Decimal("25")
    max_position_usd: Decimal = Decimal("100")
    skew_bps: Decimal = Decimal("3.0")

    # --- ladder: extra quote levels beyond the touch ------------------------ #
    extra_levels: int = 1
    level_spacing_bps: Decimal = Decimal("4.0")
    level_size_mult: Decimal = Decimal("0.6")

    # --- fees (Maker 0.012% = 1.2 bps, Taker 0.035% = 3.5 bps) ------------ #
    maker_fee_bps: Decimal = Decimal("1.2")
    taker_fee_bps: Decimal = Decimal("3.5")

    # --- edge (how far from fair value we quote) --------------------------- #
    min_edge_bps: Decimal = Decimal("2.9")
    max_edge_bps: Decimal = Decimal("12.0")
    vol_k: Decimal = Decimal("0.5")
    tox_mult: Decimal = Decimal("1.0")
    use_micro: bool = True
    penny: bool = True

    # --- exits / stress / positive unrealized pnl -------------------------- #
    exit_min_profit_bps: Decimal = Decimal("2.5")
    stress_loss_bps: Decimal = Decimal("20.0")
    max_hold_s: float = 120.0

    # Positive Unrealized PnL Inventory Management based on all bot data
    enable_smart_inventory_mgmt: bool = True
    pos_profit_trail_enabled: bool = True
    pos_profit_min_bps: Decimal = Decimal("2.9")
    pos_profit_trail_bonus_max_bps: Decimal = Decimal("3.0")
    pos_profit_flow_tighten_bps: Decimal = Decimal("1.0")
    pos_profit_taker_lock_bps: Decimal = Decimal("5.0")
    emergency_taker_loss_bps: Decimal = Decimal("6.0")
    emergency_taker_score_threshold: Decimal = Decimal("2.5")

    # Target Inventory Alpha & Carry Weighting
    target_inventory_flow_alpha_weight: Decimal = Decimal("0.35")
    target_inventory_carry_weight: Decimal = Decimal("0.20")

    # --- adverse-selection guards ------------------------------------------ #
    trend_window_s: float = 5.0
    trend_pull_bps: Decimal = Decimal("6.0")
    trend_widen: Decimal = Decimal("1.0")
    trend_hold_s: float = 2.0
    vol_window_s: float = 5.0
    vol_pause_bps: Decimal = Decimal("25.0")
    jump_bps: Decimal = Decimal("8.0")
    jump_cooldown_s: float = 2.0
    burst_fills: int = 3
    burst_window_s: float = 15.0
    burst_cooldown_s: float = 12.0
    sweep_guard_fills: int = 2
    sweep_guard_window_s: float = 1.0
    markout_horizon_s: float = 5.0
    markout_window: int = 10
    chase_cooldown_s: float = 3.0

    # --- Level 4 - 8 Quantitative Models ----------------------------------- #
    min_ev_bps: Decimal = Decimal("0.15")
    enable_adaptive_ev: bool = True
    enable_orderbook_intel: bool = True
    obi_alpha: Decimal = Decimal("1.0")
    tfi_beta: Decimal = Decimal("1.5")
    fill_prob_kappa: Decimal = Decimal("0.25")
    gamma_risk_aversion: Decimal = Decimal("0.1")
    enable_online_learning: bool = True
    regime_vol_threshold_bps: Decimal = Decimal("6.0")
    regime_flow_threshold: Decimal = Decimal("0.40")
    regime_toxic_threshold_bps: Decimal = Decimal("2.5")
    regime_toxic_spread_mult: Decimal = Decimal("1.3")
    ev_hysteresis_bps: Decimal = Decimal("0.1")
    learning_state_path: str = "learning_state.json"

    # --- Cross-Exchange & Lead/Lag Intelligence ---------------------------- #
    enable_cross_exchange: bool = True
    cross_lead_lag_weight: Decimal = Decimal("0.5")
    cross_dispersion_widen_mult: Decimal = Decimal("1.5")
    cross_velocity_threshold_bps: Decimal = Decimal("1.5")
    guarantee_spread_capture: bool = True
    guarantee_spread_capture_bps: Decimal = Decimal("0.5")
    aggressive_touch: bool = True
    touch_min_requote_s: float = 0.2
    use_depth_imbalance: bool = True
    imbalance_levels: int = 7
    imbalance_widen_bps: Decimal = Decimal("4.0")
    imbalance_size_cut: Decimal = Decimal("0.3")
    continue_add_after_reduce: bool = True
    run_tag: str = "default"
    markout_horizons_s: str = "1,5,30"

    # --- Level 8 Tight-Spread & Queue-Aware Models ------------------------- #
    enable_selective_touch: bool = True
    enable_queue_model: bool = True
    queue_horizon_s: float = 2.0
    enable_funding_carry: bool = True
    funding_weight: Decimal = Decimal("0.5")
    enable_fragility_guard: bool = True
    fragility_threshold: Decimal = Decimal("0.60")
    enable_exhaustion_detection: bool = True
    queue_reset_cost_bps: Decimal = Decimal("0.20")
    enable_absorption_mode: bool = True
    enable_onesided_touch: bool = True
    enable_quote_dataset: bool = False
    quote_dataset_path: str = "quotes.jsonl"
    enable_empirical_learner: bool = True
    empirical_prior_weight: int = 5

    # --- risk -------------------------------------------------------------- #
    session_max_loss_usd: Decimal = Decimal("5.0")
    halt_exit: bool = True

    # --- execution (Hard Rate Limit Protection: max_actions_per_min NOT accessible to learner) --- #
    requote_bps: Decimal = Decimal("1.0")
    retreat_bps: Decimal = Decimal("0.4")
    min_requote_s: float = 1.5
    max_actions_per_min: int = 60  # PROTECTED: Learner never modifies this
    loop_s: float = 0.25
    heartbeat_s: float = 5.0
    reconcile_s: float = 5.0
    status_s: float = 15.0
    stale_s: float = 15.0
    max_market_spread_bps: Decimal = Decimal("40.0")
    max_oracle_dev_bps: Decimal = Decimal("150.0")
    quote_outside_rth: bool = False
    journal_path: str = "fills.jsonl"

    # Learner Fine-Grained Module & Parameter Access Controls
    learner_disabled_modules: Set[str] = field(default_factory=set)
    learner_disabled_params: Set[str] = field(default_factory=set)

    @classmethod
    def from_env(cls) -> "Config":
        env_name = str(_e("LIGHTER_ENV", "mainnet")).lower()
        if env_name not in ENVS:
            raise Fatal(f"LIGHTER_ENV must be one of {list(ENVS)}")
        address = str(_e("LIGHTER_WALLET_ADDRESS", "0x0000000000000000000000000000000000000000"))
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
            raise Fatal("LIGHTER_WALLET_ADDRESS must be your 0x master wallet address")
        key = str(_e("LIGHTER_API_SIGNING_KEY", "0" * 64)).removeprefix("0x")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", key):
            raise Fatal("LIGHTER_API_SIGNING_KEY must be the 64-hex Ed25519 private key")
        dry = _b("DRY_RUN", "1")
        market = str(_e("MARKET", "BTC-USD"))
        market_id = int(_e("MARKET_ID", 1))

        # Fee schedule on Robinhood Lighter: Maker 0.012% (1.2 bps), Taker 0.035% (3.5 bps)
        maker_fee = _d("MAKER_FEE_BPS", "1.2")
        taker_fee = _d("TAKER_FEE_BPS", "3.5")

        default_chain_id = ENVS[env_name]["chain_id"]
        chain_id = int(_e("LIGHTER_CHAIN_ID", default_chain_id))
        api_key_index = int(_e("LIGHTER_API_KEY_INDEX", 4))

        cfg = cls(
            env_name=env_name,
            address=address,
            signing_key=key,
            account_index=int(_e("LIGHTER_ACCOUNT_INDEX", 0)),
            api_key_index=api_key_index,
            chain_id=chain_id,
            market=market,
            market_id=market_id,
            dry_run=dry,
            order_usd=_d("ORDER_USD", "25"),
            max_position_usd=_d("MAX_POSITION_USD", "100"),
            skew_bps=_d("SKEW_BPS", "3.0"),
            extra_levels=int(_e("EXTRA_LEVELS", 1)),
            level_spacing_bps=_d("LEVEL_SPACING_BPS", "4.0"),
            level_size_mult=_d("LEVEL_SIZE_MULT", "0.6"),
            maker_fee_bps=maker_fee,
            taker_fee_bps=taker_fee,
            min_edge_bps=_d("MIN_EDGE_BPS", "2.9"),  # Spread capture baseline: 2*1.2 + 0.5 = 2.9 bps
            max_edge_bps=_d("MAX_EDGE_BPS", "12.0"),
            vol_k=_d("VOL_K", "0.5"),
            tox_mult=_d("TOX_MULT", "1.0"),
            use_micro=_b("USE_MICRO", "1"),
            penny=_b("PENNY", "1"),
            exit_min_profit_bps=_d("EXIT_MIN_PROFIT_BPS", "2.5"),
            stress_loss_bps=_d("STRESS_LOSS_BPS", "20.0"),
            max_hold_s=float(_e("MAX_HOLD_S", 120)),
            enable_smart_inventory_mgmt=_b("ENABLE_SMART_INVENTORY_MGMT", "1"),
            pos_profit_trail_enabled=_b("POS_PROFIT_TRAIL_ENABLED", "1"),
            pos_profit_min_bps=_d("POS_PROFIT_MIN_BPS", "2.9"),
            pos_profit_trail_bonus_max_bps=_d("POS_PROFIT_TRAIL_BONUS_MAX_BPS", "3.0"),
            pos_profit_flow_tighten_bps=_d("POS_PROFIT_FLOW_TIGHTEN_BPS", "1.0"),
            pos_profit_taker_lock_bps=_d("POS_PROFIT_TAKER_LOCK_BPS", "5.0"),
            emergency_taker_loss_bps=_d("EMERGENCY_TAKER_LOSS_BPS", "6.0"),
            emergency_taker_score_threshold=_d("EMERGENCY_TAKER_SCORE_THRESHOLD", "2.5"),
            target_inventory_flow_alpha_weight=_d("TARGET_INVENTORY_FLOW_ALPHA_WEIGHT", "0.35"),
            target_inventory_carry_weight=_d("TARGET_INVENTORY_CARRY_WEIGHT", "0.20"),
            trend_window_s=float(_e("TREND_WINDOW_S", 5.0)),
            trend_pull_bps=_d("TREND_PULL_BPS", "6.0"),
            trend_widen=_d("TREND_WIDEN", "1.0"),
            trend_hold_s=float(_e("TREND_HOLD_S", 2.0)),
            vol_window_s=float(_e("VOL_WINDOW_S", 5.0)),
            vol_pause_bps=_d("VOL_PAUSE_BPS", "25.0"),
            jump_bps=_d("JUMP_BPS", "8.0"),
            jump_cooldown_s=float(_e("JUMP_COOLDOWN_S", 2.0)),
            burst_fills=int(_e("BURST_FILLS", 3)),
            burst_window_s=float(_e("BURST_WINDOW_S", 15.0)),
            burst_cooldown_s=float(_e("BURST_COOLDOWN_S", 12.0)),
            sweep_guard_fills=int(_e("SWEEP_GUARD_FILLS", 2)),
            sweep_guard_window_s=float(_e("SWEEP_GUARD_WINDOW_S", 1.0)),
            markout_horizon_s=float(_e("MARKOUT_HORIZON_S", 5.0)),
            markout_window=int(_e("MARKOUT_WINDOW", 10)),
            chase_cooldown_s=float(_e("CHASE_COOLDOWN_S", 3.0)),
            min_ev_bps=_d("MIN_EV_BPS", "0.15"),
            enable_adaptive_ev=_b("ENABLE_ADAPTIVE_EV", "1"),
            enable_orderbook_intel=_b("ENABLE_ORDERBOOK_INTEL", "1"),
            obi_alpha=_d("OBI_ALPHA", "1.0"),
            tfi_beta=_d("TFI_BETA", "1.5"),
            fill_prob_kappa=_d("FILL_PROB_KAPPA", "0.25"),
            gamma_risk_aversion=_d("GAMMA_RISK_AVERSION", "0.1"),
            enable_online_learning=_b("ENABLE_ONLINE_LEARNING", "1"),
            regime_vol_threshold_bps=_d("REGIME_VOL_THRESHOLD_BPS", "6.0"),
            regime_flow_threshold=_d("REGIME_FLOW_THRESHOLD", "0.40"),
            regime_toxic_threshold_bps=_d("REGIME_TOXIC_THRESHOLD_BPS", "2.5"),
            regime_toxic_spread_mult=_d("REGIME_TOXIC_SPREAD_MULT", "1.3"),
            ev_hysteresis_bps=_d("EV_HYSTERESIS_BPS", "0.1"),
            learning_state_path=str(_e("LEARNING_STATE_PATH", "learning_state.json")),
            enable_cross_exchange=_b("ENABLE_CROSS_EXCHANGE", "1"),
            cross_lead_lag_weight=_d("CROSS_LEAD_LAG_WEIGHT", "0.5"),
            cross_dispersion_widen_mult=_d("CROSS_DISPERSION_WIDEN_MULT", "1.5"),
            cross_velocity_threshold_bps=_d("CROSS_VELOCITY_THRESHOLD_BPS", "1.5"),
            guarantee_spread_capture=_b("GUARANTEE_SPREAD_CAPTURE", "1"),
            guarantee_spread_capture_bps=_d("GUARANTEE_SPREAD_CAPTURE_BPS", "0.5"),
            aggressive_touch=_b("AGGRESSIVE_TOUCH", "1"),
            touch_min_requote_s=float(_e("TOUCH_MIN_REQUOTE_S", "0.2")),
            use_depth_imbalance=_b("USE_DEPTH_IMBALANCE", "1"),
            imbalance_levels=int(_e("IMBALANCE_LEVELS", "7")),
            imbalance_widen_bps=_d("IMBALANCE_WIDEN_BPS", "4.0"),
            imbalance_size_cut=_d("IMBALANCE_SIZE_CUT", "0.3"),
            continue_add_after_reduce=_b("CONTINUE_ADD_AFTER_REDUCE", "1"),
            run_tag=str(_e("RUN_TAG", "default")),
            markout_horizons_s=str(_e("MARKOUT_HORIZONS_S", "1,5,30")),
            enable_selective_touch=_b("ENABLE_SELECTIVE_TOUCH", "1"),
            enable_queue_model=_b("ENABLE_QUEUE_MODEL", "1"),
            queue_horizon_s=float(_e("QUEUE_HORIZON_S", 2.0)),
            enable_funding_carry=_b("ENABLE_FUNDING_CARRY", "1"),
            funding_weight=_d("FUNDING_WEIGHT", "0.5"),
            enable_fragility_guard=_b("ENABLE_FRAGILITY_GUARD", "1"),
            fragility_threshold=_d("FRAGILITY_THRESHOLD", "0.60"),
            enable_exhaustion_detection=_b("ENABLE_EXHAUSTION_DETECTION", "1"),
            queue_reset_cost_bps=_d("QUEUE_RESET_COST_BPS", "0.20"),
            enable_absorption_mode=_b("ENABLE_ABSORPTION_MODE", "1"),
            enable_onesided_touch=_b("ENABLE_ONESIDED_TOUCH", "1"),
            enable_quote_dataset=_b("ENABLE_QUOTE_DATASET", "1"),
            quote_dataset_path=str(_e("QUOTE_DATASET_PATH", f"quotes_{'paper' if dry else 'live'}_{market}.jsonl")),
            enable_empirical_learner=_b("ENABLE_EMPIRICAL_LEARNER", "1"),
            empirical_prior_weight=int(_e("EMPIRICAL_PRIOR_WEIGHT", 5)),
            session_max_loss_usd=_d("SESSION_MAX_LOSS_USD", "5.0"),
            halt_exit=_b("HALT_EXIT", "1"),
            requote_bps=_d("REQUOTE_BPS", "1.0"),
            retreat_bps=_d("RETREAT_BPS", "0.4"),
            min_requote_s=float(_e("MIN_REQUOTE_S", 1.5)),
            max_actions_per_min=int(_e("MAX_ACTIONS_PER_MIN", 60)),
            loop_s=float(_e("LOOP_S", 0.25)),
            heartbeat_s=float(_e("HEARTBEAT_S", 5.0)),
            reconcile_s=float(_e("RECONCILE_S", 5.0)),
            status_s=float(_e("STATUS_S", 15.0)),
            stale_s=float(_e("STALE_S", 15.0)),
            max_market_spread_bps=_d("MAX_MARKET_SPREAD_BPS", "40.0"),
            max_oracle_dev_bps=_d("MAX_ORACLE_DEV_BPS", "150.0"),
            quote_outside_rth=_b("QUOTE_OUTSIDE_RTH", "0"),
            journal_path=str(_e("JOURNAL_PATH", f"fills_{'paper' if dry else 'live'}_{market}.jsonl")),
            learner_disabled_modules=_s("LEARNER_DISABLED_MODULES", ""),
            learner_disabled_params=_s("LEARNER_DISABLED_PARAMS", ""),
        )
        if cfg.max_position_usd < cfg.order_usd:
            raise Fatal("MAX_POSITION_USD must be >= ORDER_USD")
        return cfg
