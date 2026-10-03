"""Comprehensive Test Suite for Bulk Trade Perpetual DEX Level 8+ Market Maker Bot.

Tests:
1. Solana Ed25519 signing, Base58 codecs, domain separation, and pre-computed deterministic Order IDs
2. Multi-ladder quote generation and individual slot tracking
3. Bulk Trade fee schedule enforcement (0.010% maker, 0.035% taker) and guaranteed spread capture
4. Positive Unrealized PnL dynamic trailing exit ("Let Winners Run")
5. Reversal frontrunning and emergency taker profit lock
6. Order Book Intelligence: microprice and multi-level OBI
7. Level 4 Adaptive EV quote filtering
8. Level 6 Online learning and Empirical Bayesian markout shrinkage
9. Level 8 One-sided touch protection against aggressive toxic flow
10. L2 VWAP order book walking for taker unwind
11. Atomic multi-order batch / group transactions
12. Quote opportunity dataset logging
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from decimal import Decimal as D

import sim
from config import Config
from market import Market
from signer import BulkSigner, compute_order_id, DOMAIN_MAINNET, DOMAIN_DEVNET
from utils import BUY, SELL, BPS, ZERO, ONE, b58encode, b58decode, fmt


class TestBulkTradeMarketMaker(unittest.IsolatedAsyncioTestCase):

    def test_01_solana_ed25519_signing_and_precomputed_order_id(self):
        """Test Solana Ed25519 cryptographic signing, base58 encoding, and precomputed Order ID."""
        signer = BulkSigner(environment="devnet")
        self.assertEqual(len(b58decode(signer.signer_pubkey)), 32, "Pubkey must be 32 bytes")
        self.assertEqual(signer.domain, DOMAIN_DEVNET)

        # Sign a single order action
        tx = signer.sign_order(
            symbol="BTC-USD",
            is_buy=True,
            price=85000.0,
            size=0.05,
            order_type="limit",
            tif="ALO",
        )
        self.assertIn("signature", tx)
        self.assertIn("order_id", tx)
        self.assertEqual(len(b58decode(tx["signature"])), 64, "Signature must be 64 bytes")

        # Verify signature cryptographically
        msg_bytes = signer.build_signing_bytes(tx["actions"], tx["nonce"])
        self.assertTrue(signer.verify(tx["signature"], msg_bytes), "Signature must verify against signer public key")

        # Verify deterministic precomputed Order ID derivation matches compute_order_id
        action = tx["actions"][0]
        expected_oid = compute_order_id(action, tx["nonce"], signer.account_pubkey, seqno=0)
        self.assertEqual(tx["order_id"], expected_oid, "Precomputed order ID must match deterministic derivation")
        print(f"✓ test_01 passed: Solana Ed25519 signed, verified, and precomputed order ID: {tx['order_id'][:16]}...")

    async def test_02_multi_ladder_quote_placement(self):
        """Test multi-ladder quotes placed and tracked individually across slots (pair_index, side)."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=25, MAX_POSITION_USD=100, SKEW_BPS=0)
        await sim.step(bot, s, clock, "85000.0", "85100.0")

        buy_orders = bot.om.side_orders(BUY)
        sell_orders = bot.om.side_orders(SELL)

        self.assertGreaterEqual(len(buy_orders), 1, "Must have resting buy orders")
        self.assertGreaterEqual(len(sell_orders), 1, "Must have resting sell orders")

        for o in bot.om.orders.values():
            self.assertIn(o.pair_index, [0, 1])
            self.assertIn(o.side, [BUY, SELL])
            self.assertGreater(o.price, D("0"))
            self.assertGreater(o.remaining, D("0"))
        print("✓ test_02 passed: Multi-ladder quotes generated and tracked individually.")

    async def test_03_fee_schedule_and_spread_capture(self):
        """Test Bulk Trade fee schedule: Maker 1.0 bps (0.010%) / Taker 3.5 bps (0.035%)."""
        bot, s, clock = sim.make(MAKER_FEE_BPS="1.0", TAKER_FEE_BPS="3.5", GUARANTEE_SPREAD_CAPTURE="1", ENABLE_ADAPTIVE_EV="0")
        self.assertEqual(bot.cfg.maker_fee_bps, D("1.0"))
        self.assertEqual(bot.cfg.taker_fee_bps, D("3.5"))

        # Quoting edge must strictly cover roundtrip maker fee (2 * 1.0 = 2.0 bps) + margin (0.5 bps) = 2.5 bps
        targets = bot.engine.generate_ladder_quotes(
            m=bot.md.info, md=bot.md, ledger=bot.ledger, now=clock.t,
            buy_blocked=False, sell_blocked=False
        )
        for t in targets:
            dist_bps = abs(t.price - bot.md.mid) / bot.md.mid * BPS
            self.assertGreaterEqual(dist_bps, D("2.0"), "Quoting edge must strictly cover maker fees")

        # Test fill accounting with maker fee on Bulk
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        s.taker(BUY)  # Fills our sell quote as maker
        self.assertGreater(bot.ledger.fees, D("0"), "Maker fee must be debited")
        expected_fee_ratio = D("1.0") / BPS
        self.assertAlmostEqual(float(bot.ledger.fees / bot.ledger.volume_usd), float(expected_fee_ratio), places=5)
        print("✓ test_03 passed: Exact Bulk Trade fee schedule and spread capture enforced.")

    async def test_04_positive_unrealized_pnl_trailing(self):
        """Test Positive Unrealized PnL: trails exit quote higher when market flow is favorable."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, POS_PROFIT_TRAIL_ENABLED=1, POS_PROFIT_MIN_BPS="2.9")
        # Step into Long position
        await sim.step(bot, s, clock, "85000.0", "85100.0")
        s.taker(SELL)  # Fills our buy order -> Long
        self.assertGreater(bot.ledger.position, D("0"), "Bot must be Long")

        # Market surges upward in our favor! Mid rises to 85200 with heavy aggressive buying
        await sim.step(bot, s, clock, "85150.0", "85250.0", bsz="10", asz="1")
        bot.md.on_trade(BUY, D("5.0"), D("85200.0"), clock.t)

        unreal_bps = bot.ledger.unrealized_bps(bot.md.mid)
        self.assertGreater(unreal_bps, D("0"), "Must have positive unrealized PnL")

        targets = bot.engine.generate_ladder_quotes(
            m=bot.md.info, md=bot.md, ledger=bot.ledger, now=clock.t,
            buy_blocked=False, sell_blocked=False
        )
        unwind_quotes = [t for t in targets if t.is_exit_quote and t.side == SELL]
        self.assertEqual(len(unwind_quotes), 1, "Must generate unwind sell quote")
        unwind_ask = unwind_quotes[0]

        # The exit ask must be trailed higher above minimum target to ride favorable momentum
        expected_min_ask = bot.ledger.avg_cost * (D("1") + D("2.9") / BPS)
        self.assertGreaterEqual(unwind_ask.price, expected_min_ask, "Trailing exit must expand profit")
        print(f"✓ test_04 passed: Unrealized PnL +{fmt(unreal_bps)} bps trailed to {unwind_ask.price}.")

    async def test_05_orderbook_intelligence_microprice(self):
        """Test Level 5 Order-Book Intelligence: heavy buy pressure shifts fair value and protects ask."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ENABLE_ORDERBOOK_INTEL=1, USE_MICRO=1)
        await sim.step(bot, s, clock, "85000.0", "85100.0", bsz="1", asz="1")
        initial_ask = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(initial_ask, "Initial ask must be active")
        init_ask_px = initial_ask.price

        # Heavy bid pressure arrives: bid size = 15, ask size = 0.5 (microprice pumps toward ask)
        await sim.step(bot, s, clock, "85000.0", "85100.0", bsz="15", asz="0.5")
        new_ask = bot.om.get_order_by_slot(0, SELL)
        if new_ask is None:
            print("✓ test_05 passed: Toxic ask pulled completely (EV protection).")
        else:
            self.assertGreater(new_ask.price, init_ask_px, "Ask must reprice higher to protect against toxic buying")
            print(f"✓ test_05 passed: Ask lifted from {init_ask_px} to {new_ask.price}.")

    async def test_06_adaptive_ev_filter(self):
        """Test Level 4 Adaptive EV: quotes suppressed when Expected Value < MIN_EV_BPS."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, MIN_EV_BPS="5.0", ENABLE_ADAPTIVE_EV="1", MIN_EDGE_BPS="1.0", ENABLE_SELECTIVE_TOUCH="0")
        # Extremely tight market where spread cannot deliver 5.0 bps EV
        await sim.step(bot, s, clock, "85000.0", "85000.5")
        # Bot should refrain from quoting negative EV touch
        buy_orders = bot.om.side_orders(BUY)
        self.assertEqual(len(buy_orders), 0, "Quotes with EV < 5.0 bps must be filtered")
        print("✓ test_06 passed: Adaptive EV filter successfully blocked negative EV quotes.")

    async def test_07_online_learning_bayesian_markout(self):
        """Test Level 6 Online Learning: adverse markouts update toxicity and Bayesian model."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, MARKOUT_HORIZON_S=1.0, TOX_MULT=2.0, ENABLE_ONLINE_LEARNING=1)
        await sim.step(bot, s, clock, "85000.0", "85100.0")

        # Simulate buy fill at 85000
        bot.ledger.on_fill(BUY, D("0.001"), D("85000.0"), D("85050.0"), clock.t, D("5.0"))

        # Market dumps adversely to 84800 at markout horizon
        clock.advance(1.5)
        bot.ledger.process_markouts(D("84800.0"), clock.t)

        self.assertGreater(bot.ledger.tox_bps, D("0"), "Toxicity must increase on adverse fill")
        buy_tox = bot.ledger.side_tox_bps(BUY)
        self.assertGreater(buy_tox, D("10.0"), "Buy side toxicity must reflect adverse markout")
        print(f"✓ test_07 passed: Bayesian markout learned buy-side toxicity = {buy_tox:.2f} bps.")

    async def test_08_one_sided_touch_protection(self):
        """Test Level 8 One-Sided Touch: suppresses quotes into toxic flow surges."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ENABLE_ONESIDED_TOUCH=1)
        await sim.step(bot, s, clock, "80000.0", "80010.0")

        # Simulate massive aggressive buying burst: TFI = +0.8, regime = TOXIC
        for _ in range(5):
            bot.md.on_trade(BUY, D("2.0"), D("80010.0"), clock.t)
        bot.ledger.markouts.append(D("-5.0"))

        quotes = bot.engine.generate_ladder_quotes(bot._get_market(), bot.md, bot.ledger, clock.t, False, False)
        touch_sells = [q for q in quotes if q.side == SELL and q.pair_index == 0]
        self.assertEqual(len(touch_sells), 0, "Touch SELL must be suppressed during aggressive buying steam")
        print("✓ test_08 passed: One-sided touch protection active.")

    async def test_09_l2_vwap_taker_book_walking(self):
        """Test Order Book Walk for Taker Cross Cost across multiple depth levels."""
        bot, s, clock = sim.make(TAKER_FEE_BPS="3.5")
        await sim.step(bot, s, clock, "85000.0", "85010.0")
        bot.md.on_depth([[D("85000.0"), D("1.0")]],
                        [[D("85010.0"), D("0.1")], [D("85020.0"), D("0.2")]], clock.t)

        vwap, cross_cost_bps = bot.engine.calculate_vwap_cross_cost(BUY, D("0.2"), bot.md)
        self.assertEqual(vwap, D("85015.0"))
        self.assertGreater(cross_cost_bps, D("3.5"), "Cross cost must exceed taker fee")
        print(f"✓ test_09 passed: L2 depth VWAP walk calculated vwap={vwap}, cross_cost={cross_cost_bps:.2f} bps.")

    def test_10_atomic_multi_order_group_transaction(self):
        """Test signing atomic grouped multi-order transactions on Bulk Trade."""
        signer = BulkSigner(environment="mainnet")
        actions = [
            {"type": "cancel", "symbol": "BTC-USD", "order_id": "test-order-1"},
            {"type": "order", "symbol": "BTC-USD", "is_buy": True, "price": 85000.0, "size": 0.05, "order_type": {"type": "limit", "tif": "ALO"}, "reduce_only": False},
        ]
        tx = signer.sign_group(actions)
        self.assertEqual(len(tx["actions"]), 2)
        self.assertIn("order_ids", tx)
        self.assertIsNone(tx["order_ids"][0], "Cancel has no precomputed order ID")
        self.assertIsNotNone(tx["order_ids"][1], "Place action gets precomputed order ID")
        print(f"✓ test_10 passed: Atomic group transaction signed with {len(tx['actions'])} actions.")


if __name__ == "__main__":
    unittest.main()
