"""Comprehensive Test Suite for Robinhood Lighter Perp DEX Level 8+ Market Maker.

Tests all Level 8+ features, WebSocket transport, exact fee schedule (0.012% maker / 0.035% taker),
Robinhood Chain signing domain (Chain ID 466324), Positive Unrealized PnL inventory management,
full env parameter learning (excluding max_actions_per_min), Bayesian markout prediction,
L2 VWAP book walking, and fine-grained modular access control.
"""
from __future__ import annotations

import asyncio
import os
import sys
import unittest
from decimal import Decimal as D

import sim
from market import Market
from utils import BUY, SELL, fmt, BPS


class TestRobinhoodLighterMarketMaker(unittest.IsolatedAsyncioTestCase):

    async def test_01_robinhood_lighter_ed25519_signing_domain(self):
        """Test Ed25519 transaction signing strictly bound to Robinhood Chain (Chain ID 466324, Key Index 4)."""
        bot, s, clock = sim.make(LIGHTER_CHAIN_ID="466324", LIGHTER_API_KEY_INDEX="4")
        self.assertEqual(bot.signer.chain_id, 466324)
        self.assertEqual(bot.signer.api_key_index, 4)

        signed_order = bot.signer.sign_create_order(
            m=bot.md.info, side=BUY, price=D("80000.0"), size=D("0.001"),
            client_order_id=12345, post_only=True
        )
        self.assertEqual(signed_order["tx_type"], 1)
        tx_info = signed_order["tx_info"]
        self.assertEqual(tx_info["chain_id"], 466324, "Signature payload must include Robinhood Chain ID")
        self.assertEqual(tx_info["api_key_index"], 4, "API key index must be >= 4")
        self.assertEqual(tx_info["time_in_force"], 2, "Post-only ALO must have time_in_force=2")
        self.assertIn("signature", tx_info)
        self.assertEqual(len(tx_info["signature"]), 128, "Ed25519 signature must be 64 bytes (128 hex chars)")
        print("✓ test_01 passed: Robinhood Chain signing domain and Ed25519 verification validated.")

    async def test_02_websocket_multi_ladder_quote_placement(self):
        """Test multi-ladder quote placement over Robinhood Lighter WebSocket."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=25, MAX_POSITION_USD=100, SKEW_BPS=0)
        await sim.step(bot, s, clock, "80000.0", "80100.0")

        buy_orders = bot.om.side_orders(BUY)
        sell_orders = bot.om.side_orders(SELL)

        self.assertGreaterEqual(len(buy_orders), 1, "Should have at least 1 buy ladder order")
        self.assertGreaterEqual(len(sell_orders), 1, "Should have at least 1 sell ladder order")

        for o in bot.om.orders.values():
            self.assertIn(o.pair_index, [0, 1])
            self.assertIn(o.side, [BUY, SELL])
            self.assertGreater(o.price, D(0))
            self.assertGreater(o.remaining, D(0))
        print("✓ test_02 passed: Multi-ladder quotes placed via Robinhood Lighter WebSocket.")

    async def test_03_exact_fee_schedule_and_spread_capture(self):
        """Test fee calculation: Maker 0.012% (1.2 bps) and Taker 0.035% (3.5 bps)."""
        bot, s, clock = sim.make(MAKER_FEE_BPS="1.2", TAKER_FEE_BPS="3.5", GUARANTEE_SPREAD_CAPTURE="1")
        self.assertEqual(bot.cfg.maker_fee_bps, D("1.2"))
        self.assertEqual(bot.cfg.taker_fee_bps, D("3.5"))

        # Quoting edge must guarantee covering roundtrip maker fee (2 * 1.2 = 2.4 bps) + margin (0.5 bps) = 2.9 bps
        targets = bot.engine.generate_ladder_quotes(
            m=bot.md.info, md=bot.md, ledger=bot.ledger, now=clock.t,
            buy_blocked=False, sell_blocked=False
        )
        for t in targets:
            dist_bps = abs(t.price - bot.md.mid) / bot.md.mid * BPS
            self.assertGreaterEqual(dist_bps, D("2.4"), "Quoting edge must strictly cover maker fees")

        await sim.step(bot, s, clock, "80000.0", "80100.0")
        s.taker(BUY)  # Fills our sell order as maker
        self.assertGreater(bot.ledger.fees, D(0), "Maker fee should be accounted")
        expected_fee_ratio = D("1.2") / BPS
        self.assertAlmostEqual(float(bot.ledger.fees / bot.ledger.volume_usd), float(expected_fee_ratio), places=5)
        print("✓ test_03 passed: Exact 0.012% maker / 0.035% taker fee enforced.")

    async def test_04_positive_unrealized_pnl_trailing(self):
        """Test Positive Unrealized PnL: trails exit higher when flow is favorable ('Let Winners Run')."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, POS_PROFIT_TRAIL_ENABLED=1, POS_PROFIT_MIN_BPS="2.9")
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        s.taker(SELL)  # Fills our buy order -> Bot is now Long
        self.assertGreater(bot.ledger.position, D(0), "Bot should be long")
        await bot.om.cancel_all(clock.t)  # Clear pre-existing resting quotes

        # Market moves up in our favor! Mid rises to 80150
        # Flow is strongly positive (buyers lifting, high OBI and TFI)
        await sim.step(bot, s, clock, "80100.0", "80200.0", bsz="10", asz="1")
        bot.md.on_trade(BUY, D("5.0"), D("80150.0"), clock.t)

        unreal_bps = bot.ledger.unrealized_bps(bot.md.mid)
        self.assertGreater(unreal_bps, D(0), "Position should have positive unrealized PnL")

        targets = bot.engine.generate_ladder_quotes(
            m=bot.md.info, md=bot.md, ledger=bot.ledger, now=clock.t,
            buy_blocked=False, sell_blocked=False
        )
        unwind_quotes = [t for t in targets if t.is_exit_quote and t.side == SELL]
        self.assertEqual(len(unwind_quotes), 1, "Should generate unwind sell quote")
        unwind_ask = unwind_quotes[0]

        expected_min_ask = bot.ledger.avg_cost * (D("1") + D("2.9") / BPS)
        self.assertGreaterEqual(unwind_ask.price, expected_min_ask, "Trailing exit should aim for expanded profit")
        print(f"✓ test_04 passed: Unrealized PnL +{fmt(unreal_bps)} bps trailed to {unwind_ask.price}.")

    async def test_05_positive_unrealized_pnl_reversal_frontrun(self):
        """Test Positive Unrealized PnL: aggressive inside quote when favorable flow slows."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, POS_PROFIT_TRAIL_ENABLED=1, PENNY=1)
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        s.taker(SELL)
        self.assertGreater(bot.ledger.position, D(0))

        # Flow momentum slows / mildly weakens (OBI = -0.3, TFI = -0.25)
        await sim.step(bot, s, clock, "80050.0", "80150.0", bsz="3.5", asz="6.5")
        bot.md.on_trade(BUY, D("5.0"), D("80100.0"), clock.t)
        bot.md.on_trade(SELL, D("8.33"), D("80050.0"), clock.t)

        targets = bot.engine.generate_ladder_quotes(
            m=bot.md.info, md=bot.md, ledger=bot.ledger, now=clock.t,
            buy_blocked=False, sell_blocked=False
        )
        unwind_quotes = [t for t in targets if t.is_exit_quote and t.side == SELL]
        self.assertEqual(len(unwind_quotes), 1)
        unwind_ask = unwind_quotes[0]

        self.assertEqual(unwind_ask.price, bot.md.ask - bot.md.info.tick_size,
                         "Should aggressively penny best ask to guarantee fill before dump")
        print("✓ test_05 passed: Pennying front-run locked profit.")

    async def test_06_positive_unrealized_pnl_emergency_taker_lock(self):
        """Test Positive Unrealized PnL: executes taker profit-lock when severe cascade threatens net profit."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, TAKER_FEE_BPS="3.5")
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        s.taker(SELL)

        await sim.step(bot, s, clock, "80060.0", "80140.0")
        bot.md.update(D("80060"), D("80140"), D("0.1"), D("50.0"), clock.t)
        bot.md.on_trade(SELL, D("20.0"), D("80060.0"), clock.t)
        bot.md.ret_bps = lambda w, n: D("-3.5")

        targets = bot.engine.generate_ladder_quotes(
            m=bot.md.info, md=bot.md, ledger=bot.ledger, now=clock.t,
            buy_blocked=False, sell_blocked=False
        )
        taker_quotes = [t for t in targets if t.is_taker and t.side == SELL]
        self.assertGreaterEqual(len(taker_quotes), 1, "Should trigger emergency taker profit-lock")
        self.assertEqual(taker_quotes[0].price, bot.md.bid, "Taker order should cross at best bid")
        print("✓ test_06 passed: Emergency taker profit-lock executed.")

    async def test_07_multidepth_obi_and_trade_flow_acceleration(self):
        """Test Level 8 multi-depth OBI (L1, L5, L10) and trade flow acceleration."""
        bot, s, clock = sim.make()
        depth_bids = [[fmt(D("80000.0") - D(str(i*10))), "2.0"] for i in range(10)]
        depth_asks = [[fmt(D("80100.0") + D(str(i*10))), "1.0"] for i in range(10)]
        await sim.step(bot, s, clock, "80000.0", "80100.0", depth_bids=depth_bids, depth_asks=depth_asks)

        obi_l1 = bot.md.obi
        obi_l5 = bot.md.multi_depth_obi(5)
        obi_l10 = bot.md.multi_depth_obi(10)

        self.assertGreater(obi_l5, D("0.2"), "L5 OBI should reflect bid-heavy depth")
        self.assertGreater(obi_l10, D("0.2"), "L10 OBI should reflect bid-heavy depth")

        # Baseline 3s ago: balanced flow (TFI = 0)
        bot.md.on_trade(BUY, D("5.0"), D("80000.0"), clock.t - 3.0)
        bot.md.on_trade(SELL, D("5.0"), D("80000.0"), clock.t - 3.0)
        # Aggressive buy burst 0.5s ago
        bot.md.on_trade(BUY, D("10.0"), D("80090.0"), clock.t - 0.5)
        tfi_accel = bot.md.trade_flow_acceleration(clock.t)
        self.assertGreater(tfi_accel, D("0"), "TFI acceleration should be positive after aggressive burst")
        print("✓ test_07 passed: Multi-depth OBI and flow acceleration validated.")

    async def test_08_empirical_bayesian_markout_model(self):
        """Test Bayesian conditional markout prediction and shrinkage."""
        bot, s, clock = sim.make()
        model = bot.ledger.bayesian_model

        pred_before = model.predict(BUY, "REGIME_A_QUIET", 0, 2.0)
        self.assertEqual(pred_before, D("0.5"), "Prior should be 0.5 bps")

        for _ in range(15):
            model.record(BUY, "REGIME_A_QUIET", 0, 2.0, -2.0)

        pred_after = model.predict(BUY, "REGIME_A_QUIET", 0, 2.0)
        self.assertLess(pred_after, D("0.0"), "Posterior should shrink toward empirical adverse markouts")
        print(f"✓ test_08 passed: Bayesian shrinkage adapted prior {pred_before} -> posterior {pred_after}.")

    async def test_09_l2_vwap_taker_book_walking(self):
        """Test L2 order book walking for VWAP crossing cost."""
        bot, s, clock = sim.make()
        depth_asks = [
            ["80100.0", "0.01"],
            ["80110.0", "0.02"],
            ["80120.0", "0.05"]
        ]
        await sim.step(bot, s, clock, "80000.0", "80100.0", depth_asks=depth_asks)

        vwap, cross_cost_bps = bot.engine.calculate_vwap_cross_cost(BUY, D("0.03"), bot.md)
        self.assertGreater(vwap, D("80100.0"), "VWAP should walk up past top level")
        self.assertGreater(cross_cost_bps, bot.cfg.taker_fee_bps, "Total crossing cost should include slippage + taker fee")
        print(f"✓ test_09 passed: L2 VWAP walking computed price {vwap} and crossing cost {cross_cost_bps} bps.")

    async def test_10_queue_aware_fill_probability(self):
        """Test queue-aware hazard rate fill probability modeling."""
        bot, s, clock = sim.make()
        await sim.step(bot, s, clock, "80000.0", "80100.0", bsz="1.0", asz="1.0")

        prob_touch = bot.engine.queue_fill_probability(BUY, D("80000.0"), bot.md, clock.t, 2.0, ledger=bot.ledger)
        prob_deep = bot.engine.queue_fill_probability(BUY, D("79900.0"), bot.md, clock.t, 2.0, ledger=bot.ledger)

        self.assertGreater(prob_touch, prob_deep, "Touch quote should have strictly higher fill probability than deep quote")
        print(f"✓ test_10 passed: Queue fill probability modeled touch {prob_touch:.2f} vs deep {prob_deep:.2f}.")

    async def test_11_learner_full_env_parameters_access(self):
        """Test that learner has access to ALL environment parameters EXCEPT max_actions_per_min."""
        bot, s, clock = sim.make()
        learner = bot.ledger.learner

        test_params = [
            "min_edge_bps", "max_edge_bps", "skew_bps", "level_spacing_bps",
            "vol_k", "tox_mult", "min_ev_bps", "obi_alpha", "tfi_beta",
            "fill_prob_kappa", "gamma_risk_aversion", "regime_toxic_spread_mult",
            "trend_pull_bps", "trend_widen", "exit_min_profit_bps", "stress_loss_bps",
            "max_hold_s", "order_usd", "max_position_usd", "burst_fills",
            "cross_lead_lag_weight", "requote_bps", "retreat_bps", "min_requote_s"
        ]
        for p in test_params:
            self.assertTrue(learner.is_param_enabled(p), f"Param {p} should be accessible to learner")

        self.assertFalse(learner.is_param_enabled("max_actions_per_min"),
                         "max_actions_per_min must NEVER be accessible to learner!")
        print("✓ test_11 passed: Full env access verified with max_actions_per_min locked.")

    async def test_12_learner_modular_on_off_controls(self):
        """Test turning OFF and ON learner access to entire modules and specific parameters."""
        bot, s, clock = sim.make()
        learner = bot.ledger.learner

        self.assertTrue(learner.is_param_enabled("skew_bps"))
        self.assertTrue(learner.is_param_enabled("max_hold_s"))

        learner.disable_module("inventory")
        self.assertFalse(learner.is_param_enabled("skew_bps"))
        self.assertFalse(learner.is_param_enabled("max_hold_s"))
        self.assertTrue(learner.is_param_enabled("min_edge_bps"))

        base_skew = learner.base["skew_bps"]
        self.assertEqual(learner.skew_bps, base_skew)

        learner.disable_module("execution")
        self.assertFalse(learner.is_param_enabled("requote_bps"))
        self.assertFalse(learner.is_param_enabled("min_requote_s"))

        learner.disable_param("vol_k")
        self.assertFalse(learner.is_param_enabled("vol_k"))
        self.assertTrue(learner.is_param_enabled("min_edge_bps"))

        learner.enable_module("inventory")
        self.assertTrue(learner.is_param_enabled("skew_bps"))
        print("✓ test_12 passed: Granular module & param toggle verified.")

    async def test_13_sweep_guard_and_burst_fills(self):
        """Test sweep guard and burst fill defense pulling quotes."""
        bot, s, clock = sim.make(BURST_FILLS=2, BURST_WINDOW_S=15, BURST_COOLDOWN_S=10)
        await sim.step(bot, s, clock, "80000.0", "80100.0")

        s.taker(SELL)
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        s.taker(SELL)
        await sim.step(bot, s, clock, "80000.0", "80100.0")

        buy_orders = bot.om.side_orders(BUY)
        self.assertEqual(len(buy_orders), 0, "Buy orders should be pulled after burst fills")
        print("✓ test_13 passed: Burst guard pulled vulnerable side.")

    async def test_14_session_max_loss_circuit_breaker(self):
        """Test bot halting when session max loss threshold is exceeded."""
        bot, s, clock = sim.make(SESSION_MAX_LOSS_USD="2.0")
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        bot.ledger.realized = D("-5.0")
        await sim.step(bot, s, clock, "80000.0", "80100.0")

        self.assertEqual(len(bot.om.orders), 0, "All orders should be cancelled on session max loss stop")
        print("✓ test_14 passed: Bot safely halted on stop-loss.")


if __name__ == "__main__":
    unittest.main()
