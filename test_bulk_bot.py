"""Comprehensive test suite for Bulk Trade Perp DEX Market Maker Bot."""
from __future__ import annotations

import asyncio
import json
import unittest
from decimal import Decimal

from cryptography.hazmat.primitives.asymmetric import ed25519

from config import Config
from exchange import Exchange
from market import Market, MarketData
from orders import Order, OrderManager
from signer import Signer, SignatureDomain
from ledger import Ledger
from engine import MarketMakingEngine
from utils import (
    b58decode,
    b58encode,
    canonical,
    to_fixed_u64,
    from_fixed_u64,
    BUY,
    SELL,
    ZERO,
    ONE,
    bps_diff,
)


class TestBulkTradeBot(unittest.IsolatedAsyncioTestCase):
    """Unit and Integration Tests for Bulk Trade Bot components."""

    def setUp(self):
        # Deterministic 32-byte Ed25519 test seed
        self.test_seed = bytes([i % 256 for i in range(32)])
        self.signer = Signer(self.test_seed, domain=SignatureDomain.MAINNET)

        # Mock configuration
        import os
        os.environ["BULK_ENV"] = "mainnet"
        os.environ["BULK_PRIVATE_KEY"] = b58encode(self.test_seed)
        os.environ["BULK_ACCOUNT"] = self.signer.account_b58
        os.environ["MARKET"] = "SOL-USD"
        os.environ["DRY_RUN"] = "1"
        self.cfg = Config.from_env()

        self.mock_market = Market(
            market_id=1,
            name="SOL-USD",
            status="ONLINE",
            tick=Decimal("0.01"),
            step=Decimal("0.01"),
            tiers=[],
            min_notional=Decimal("5.0"),
            min_size=Decimal("0.01"),
            max_size=Decimal("10000"),
            mark=Decimal("150.00"),
            is_outside_rth=False,
            funding_rate=Decimal("0.0001"),
            next_funding_time=0.0,
        )

    # 1. Base58 Tests
    def test_01_base58_encoding_decoding(self):
        test_cases = [
            b"",
            b"\x00",
            b"\x00\x00\x01\x02",
            b"Hello Solana Bulk Trade!",
            self.test_seed,
        ]
        for data in test_cases:
            enc = b58encode(data)
            dec = b58decode(enc)
            self.assertEqual(dec, data, f"Mismatch for {data}: {enc} -> {dec}")

    # 2. Ed25519 Signing & Verification
    def test_02_ed25519_signing_and_verification(self):
        action = self.signer.build_limit_action("SOL-USD", BUY, Decimal("149.50"), Decimal("0.5"), "ALO")
        envelope, oids = self.signer.sign_transaction([action], nonce=1700000000000)

        self.assertEqual(envelope["account"], self.signer.account_b58)
        self.assertEqual(envelope["signer"], self.signer.pubkey_b58)
        self.assertTrue(len(envelope["signature"]) > 40)
        self.assertTrue(len(oids) == 1 and len(oids[0]) > 20)

        # Cryptographically verify the signature
        msg_payload = {
            "domain": self.signer.domain,
            "account": self.signer.account_b58,
            "signer": self.signer.pubkey_b58,
            "nonce": envelope["nonce"],
            "actions": envelope["actions"],
        }
        canonical_bytes = canonical(msg_payload).encode("utf-8")
        pubkey = ed25519.Ed25519PublicKey.from_public_bytes(self.signer.pubkey_bytes)
        sig_bytes = b58decode(envelope["signature"])
        # Should not raise cryptography.exceptions.InvalidSignature
        pubkey.verify(sig_bytes, canonical_bytes)

    # 3. Builder Code Attachment
    def test_03_builder_code_attachment(self):
        signer_with_builder = Signer(
            self.test_seed,
            builder_to="9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
            builder_fee_bps=5,
        )
        act = signer_with_builder.build_limit_action("BTC-USD", BUY, Decimal("65000"), Decimal("0.1"))
        self.assertIn("builderCode", act["l"])
        self.assertEqual(act["l"]["builderCode"]["to"], "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin")
        self.assertEqual(act["l"]["builderCode"]["fee"], 5)

    # 4. Atomic Batch Transaction
    def test_04_atomic_batch_transaction(self):
        c_act = self.signer.build_cancel_action("old_order_123", "SOL-USD")
        l_act1 = self.signer.build_limit_action("SOL-USD", BUY, Decimal("148.00"), Decimal("0.2"), "ALO")
        l_act2 = self.signer.build_limit_action("SOL-USD", SELL, Decimal("152.00"), Decimal("0.2"), "ALO")

        batch = [c_act, l_act1, l_act2]
        envelope, oids = self.signer.sign_transaction(batch)

        self.assertEqual(len(envelope["actions"]), 3)
        self.assertEqual(len(oids), 3)
        self.assertEqual(oids[0], "")  # Cancel action has no order ID
        self.assertTrue(len(oids[1]) > 0)
        self.assertTrue(len(oids[2]) > 0)
        self.assertNotEqual(oids[1], oids[2])  # Unique precomputed IDs

    # 5. Market Specification Parsing
    def test_05_market_spec_parsing(self):
        api_data = {
            "symbol": "BTC-USD",
            "tickSize": "0.10",
            "stepSize": "0.001",
            "minOrderSize": "0.001",
            "maxOrderSize": "500",
            "minOrderNotional": "10.0",
            "markPrice": "82500.50",
            "indexPrice": "82490.00",
            "fundingRate": "0.00015",
            "status": "ONLINE",
        }
        m = Market.from_api(api_data)
        self.assertEqual(m.name, "BTC-USD")
        self.assertEqual(m.tick, Decimal("0.10"))
        self.assertEqual(m.step, Decimal("0.001"))
        self.assertEqual(m.mark, Decimal("82500.50"))
        self.assertEqual(m.funding_rate, Decimal("0.00015"))
        self.assertEqual(m.status, "ONLINE")

    # 6. Orderbook & Microstructure (OBI & TFI)
    def test_06_orderbook_and_microstructure(self):
        md = MarketData(self.cfg)
        md.update(Decimal("149.90"), Decimal("150.10"), Decimal("100"), Decimal("50"), now=1.0)
        
        self.assertEqual(md.mid, Decimal("150.00"))
        # OBI = (100 - 50) / 150 = +0.3333...
        self.assertAlmostEqual(float(md.obi), 0.3333, places=3)
        # Microprice should be pulled toward ask because bid size is larger
        self.assertTrue(md.micro > md.mid)

        # Ingest aggressive trades
        md.on_trade(BUY, Decimal("20"), Decimal("150.10"), now=1.5)
        md.on_trade(BUY, Decimal("30"), Decimal("150.10"), now=2.0)
        md.on_trade(SELL, Decimal("10"), Decimal("149.90"), now=2.5)

        # TFI in 5s window: (50 buy - 10 sell) / 60 = +0.6666
        tfi = md.trade_flow_imbalance(5.0, now=3.0)
        self.assertAlmostEqual(float(tfi), 0.6666, places=3)

    # 7. OrderManager Single Place & Atomic Batch Replace
    async def test_07_ordermanager_batch_replace(self):
        ex = Exchange(self.cfg, lambda *a: None)
        fills = []
        om = OrderManager(self.cfg, ex, self.signer, lambda: self.mock_market, lambda s, p, q, o: fills.append((s, p, q)))

        # Place initial quote
        o1 = await om.place(0, BUY, Decimal("149.00"), Decimal("0.1"), now=1.0)
        self.assertIsNotNone(o1)
        self.assertEqual(len(om.orders), 1)
        self.assertIn((0, BUY), om.pair_slots)

        # Perform atomic batch replacement: cancel o1 and place new BUY and SELL quotes
        places = [
            (0, BUY, Decimal("149.50"), Decimal("0.15"), False),
            (0, SELL, Decimal("150.50"), Decimal("0.15"), False),
        ]
        replaced = await om.batch_replace([o1], places, now=2.0)

        self.assertEqual(len(replaced), 2)
        self.assertEqual(len(om.orders), 2)
        self.assertNotIn(o1.order_id, om.orders)
        self.assertEqual(om.pair_slots[(0, BUY)], replaced[0].order_id)
        self.assertEqual(om.pair_slots[(0, SELL)], replaced[1].order_id)

    # 8. Sweep Guard Fill Protection
    async def test_08_sweep_guard_cooldown(self):
        ex = Exchange(self.cfg, lambda *a: None)
        ledger = Ledger(self.cfg)
        from bot import MarketMaker

        mm = MarketMaker(self.cfg)
        mm.md.info = self.mock_market
        mm.md.update(Decimal("149.90"), Decimal("150.10"), Decimal("50"), Decimal("50"), now=1.0)

        o_buy = await mm.om.place(0, BUY, Decimal("149.50"), Decimal("0.1"), now=1.0)
        
        # Simulate 2 rapid fills within 1.0 second on BUY side
        mm._on_fill(BUY, Decimal("0.1"), Decimal("149.50"), o_buy)
        mm._on_fill(BUY, Decimal("0.1"), Decimal("149.50"), o_buy)

        self.assertTrue(mm._burst_blocked_until[BUY] > mm.now())

    # 9. Taker Speed Bump Advantage in Quoting
    def test_09_speed_bump_advantage(self):
        engine = MarketMakingEngine(self.cfg)
        self.assertTrue(self.cfg.taker_speed_bump_enabled)
        self.assertEqual(self.cfg.speed_bump_advantage_bps, Decimal("1.5"))

    # 10. Ledger PnL & Markout Tracking
    def test_10_ledger_accounting_and_pnl(self):
        ledger = Ledger(self.cfg)
        # Buy 1.0 SOL @ 150.00
        f1 = ledger.record_fill(BUY, Decimal("1.0"), Decimal("150.00"), Decimal("150.00"), now=1.0)
        self.assertEqual(ledger.position, Decimal("1.0"))
        self.assertEqual(ledger.avg_cost, Decimal("150.00"))

        # Mark price moves to 155.00
        unreal = ledger.unrealized(Decimal("155.00"))
        self.assertEqual(unreal, Decimal("5.00"))

        # Sell 1.0 SOL @ 156.00
        f2 = ledger.record_fill(SELL, Decimal("1.0"), Decimal("156.00"), Decimal("155.00"), now=5.0)
        self.assertEqual(ledger.position, Decimal("0.0"))
        self.assertEqual(ledger.realized, Decimal("6.00"))
        self.assertEqual(ledger.total_pnl(Decimal("156.00")), Decimal("6.00"))



    # 11. WebSocket-First State Queries & Low-Latency Socket Tuning
    async def test_11_websocket_first_and_socket_tuning(self):
        ex = Exchange(self.cfg, lambda *a: None)
        
        # Test socket optimization helper (TCP_NODELAY)
        class MockSocket:
            def __init__(self):
                self.opts = {}
            def setsockopt(self, level, optname, value):
                self.opts[optname] = value

        mock_sock = MockSocket()
        ex.optimize_socket(mock_sock)
        import socket
        self.assertEqual(mock_sock.opts.get(socket.TCP_NODELAY), 1)

        # Verify markets fetch works over WebSocket / cached fallback
        markets = await ex.fetch_markets("SOL-USD")
        self.assertTrue(len(markets) >= 1)
        self.assertEqual(markets[0]["symbol"], "SOL-USD")

    # 12. Ultra-Low Latency Signing Microbenchmark
    def test_12_sub_millisecond_signing_latency(self):
        import time
        action = self.signer.build_limit_action("SOL-USD", BUY, Decimal("150.00"), Decimal("0.1"), "ALO")
        
        # Warmup
        for _ in range(50):
            self.signer.sign_transaction([action])

        # Benchmark 500 signatures
        t0 = time.perf_counter()
        N = 500
        for _ in range(N):
            env, oids = self.signer.sign_transaction([action])
        t1 = time.perf_counter()
        avg_us = ((t1 - t0) / N) * 1_000_000
        print(f"\nAverage Ed25519 signing latency: {avg_us:.2f} microseconds per transaction")
        self.assertLess(avg_us, 500.0, "Signing latency must be sub-millisecond (< 500 us)")


if __name__ == "__main__":
    unittest.main()
