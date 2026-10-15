"""Comprehensive Unit & Integration Test Suite for Robinhood Lighter Perp DEX Port."""
from __future__ import annotations

import asyncio
import json
import os
import time
import unittest
from decimal import Decimal

from bot import MarketMaker
from config import Config, ENVS, RH_RESERVED_API_KEY_INDICES
from exchange import Exchange
from market import Market, MarketData
from signer import Signer
from utils import BUY, SELL, Fatal


class _RealLikeFakeWS:
    def __init__(self):
        self.sent: list[dict] = []
        self.closed = False
        self.transport = object()

    async def send(self, data: str) -> None:
        self.sent.append(json.loads(data))


class TestRobinhoodLighterPerpDex(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._env_backup = dict(os.environ)
        os.environ["LIGHTER_ENV"] = "mainnet"
        os.environ["LIGHTER_WALLET_ADDRESS"] = "0x1111111111111111111111111111111111111111"
        os.environ["LIGHTER_API_PRIVATE_KEY"] = "a" * 80
        os.environ["LIGHTER_ACCOUNT_INDEX"] = "42"
        os.environ["LIGHTER_API_KEY_INDEX"] = "4"
        os.environ["MARKET"] = "BTC"
        os.environ["DRY_RUN"] = "1"
        os.environ["JOURNAL_PATH"] = os.devnull
        os.environ["QUOTE_DATASET_PATH"] = os.devnull

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)

    def test_01_rh_lighter_endpoints_and_chain_ids(self):
        cfg_main = Config.from_env()
        self.assertEqual(cfg_main.rest_url, "https://api.rh.lighter.xyz")
        self.assertEqual(cfg_main.ws_url, "wss://api.rh.lighter.xyz/stream")
        self.assertEqual(cfg_main.chain_id, 466324)
        self.assertEqual(cfg_main.settlement_asset, "USDG")
        self.assertEqual(cfg_main.account_index, 42)
        self.assertEqual(cfg_main.api_key_index, 4)
        self.assertEqual(len(cfg_main.signing_key), 80)

        os.environ["LIGHTER_ENV"] = "rh_testnet"
        cfg_test = Config.from_env()
        self.assertEqual(cfg_test.rest_url, "https://api.rh-testnet.lighter.xyz")
        self.assertEqual(cfg_test.ws_url, "wss://api.rh-testnet.lighter.xyz/stream")
        self.assertEqual(cfg_test.chain_id, 300)
        self.assertEqual(cfg_test.settlement_asset, "USDG")
        print("✓ test_01_rh_lighter_endpoints_and_chain_ids passed.")

    def test_02_rh_reserved_api_key_indices_rejected(self):
        for reserved_idx in (0, 1, 2, 3, 157, 255):
            os.environ["LIGHTER_API_KEY_INDEX"] = str(reserved_idx)
            with self.assertRaises(Fatal):
                Config.from_env()

        for valid_idx in (4, 5, 156, 158, 254):
            os.environ["LIGHTER_API_KEY_INDEX"] = str(valid_idx)
            cfg = Config.from_env()
            self.assertEqual(cfg.api_key_index, valid_idx)
        print("✓ test_02_rh_reserved_api_key_indices_rejected passed.")

    def test_03_lighter_orderbook_details_parsing_and_scaling(self):
        lighter_perp_detail = {
            "market_id": 0,
            "symbol": "ETH",
            "market_type": "perp",
            "status": "active",
            "price_decimals": 2,
            "size_decimals": 4,
            "quote_decimals": 6,
            "min_base_amount": "0.0050",
            "min_quote_amount": "10.000000",
            "maker_fee": "0.0000",
            "taker_fee": "0.00035",
            "mark_price": "2540.25",
            "index_price": "2540.10",
            "current_funding_rate": "0.000125",
            "funding_timestamp": 1760000000.0,
        }
        m = Market.from_api(lighter_perp_detail)
        self.assertEqual(m.market_id, 0)
        self.assertEqual(m.symbol, "ETH")
        self.assertEqual(m.status, "ONLINE")
        self.assertEqual(m.tick, Decimal("0.01"))
        self.assertEqual(m.step, Decimal("0.0001"))
        self.assertEqual(m.min_size, Decimal("0.0050"))
        self.assertEqual(m.min_notional, Decimal("10.000000"))
        self.assertEqual(m.mark, Decimal("2540.25"))
        self.assertEqual(m.funding_rate, Decimal("0.000125"))
        print("✓ test_03_lighter_orderbook_details_parsing_and_scaling passed.")

    def test_04_lighter_l2_tx_signing_and_tif_expiry_rules(self):
        cfg = Config.from_env()
        signer = Signer(
            cfg.signing_key,
            cfg.address,
            cfg.account_index,
            api_key_index=cfg.api_key_index,
            chain_id=cfg.chain_id,
            rest_url=cfg.rest_url,
        )
        signer.set_nonce(100)
        m = Market.from_api({
            "market_id": 1,
            "symbol": "BTC",
            "status": "active",
            "price_decimals": 1,
            "size_decimals": 5,
            "min_base_amount": "0.0002",
            "min_quote_amount": "10",
            "mark_price": "80000.0",
        })

        good_til_us = int((time.time() + 3600) * 1_000_000)
        place_alo = signer.place(m, BUY, Decimal("79995.5"), Decimal("0.01234"), good_til_us, time_in_force="ALO", reduce_only=False)
        self.assertEqual(place_alo["tx_type"], Signer.TX_TYPE_CREATE_ORDER)
        tx_info_alo = json.loads(place_alo["tx_info"])
        self.assertEqual(tx_info_alo["AccountIndex"], 42)
        self.assertEqual(tx_info_alo["ApiKeyIndex"], 4)
        self.assertEqual(tx_info_alo["MarketIndex"], 1)
        self.assertEqual(tx_info_alo["Price"], 799955)
        self.assertEqual(tx_info_alo["BaseAmount"], 1234)
        self.assertEqual(tx_info_alo["IsAsk"], 0)
        self.assertEqual(tx_info_alo["Type"], Signer.ORDER_TYPE_LIMIT)
        self.assertEqual(tx_info_alo["TimeInForce"], Signer.LIGHTER_TIF_POST_ONLY)
        self.assertEqual(tx_info_alo["ReduceOnly"], 0)
        self.assertEqual(tx_info_alo["TriggerPrice"], 0)
        self.assertGreater(tx_info_alo["OrderExpiry"], int(time.time() * 1000))
        self.assertEqual(tx_info_alo["Nonce"], 100)
        self.assertTrue(tx_info_alo["Sig"])

        place_ioc = signer.place(m, SELL, Decimal("79980.0"), Decimal("0.00500"), good_til_us, time_in_force="IOC", reduce_only=True)
        tx_info_ioc = json.loads(place_ioc["tx_info"])
        self.assertEqual(tx_info_ioc["IsAsk"], 1)
        self.assertEqual(tx_info_ioc["TimeInForce"], Signer.LIGHTER_TIF_IOC)
        self.assertEqual(tx_info_ioc["OrderExpiry"], 0)
        self.assertEqual(tx_info_ioc["ReduceOnly"], 1)
        self.assertEqual(tx_info_ioc["Nonce"], 101)

        coi = str(place_alo["client_order_index"])
        signer.register_order_mapping(coi, 987654321)
        mod_req = signer.modify(m, coi, BUY, Decimal("79996.0"), Decimal("0.01234"), good_til_us, reduce_only=False)
        self.assertEqual(mod_req["tx_type"], Signer.TX_TYPE_MODIFY_ORDER)
        tx_info_mod = json.loads(mod_req["tx_info"])
        self.assertEqual(tx_info_mod["Index"], 987654321)
        self.assertEqual(tx_info_mod["Price"], 799960)
        self.assertEqual(tx_info_mod["BaseAmount"], 1234)
        self.assertEqual(tx_info_mod["Nonce"], 102)

        can_req = signer.cancel(m, coi)
        self.assertEqual(can_req["tx_type"], Signer.TX_TYPE_CANCEL_ORDER)
        tx_info_can = json.loads(can_req["tx_info"])
        self.assertEqual(tx_info_can["Index"], 987654321)
        self.assertEqual(tx_info_can["MarketIndex"], 1)
        self.assertEqual(tx_info_can["Nonce"], 103)

        deadline_us = int((time.time() + 30) * 1_000_000)
        dms_arm = signer.schedule_cancel(m, deadline_us)
        self.assertEqual(dms_arm["tx_type"], Signer.TX_TYPE_CANCEL_ALL_ORDERS)
        tx_info_dms = json.loads(dms_arm["tx_info"])
        self.assertEqual(tx_info_dms["TimeInForce"], Signer.CANCEL_ALL_TIF_SCHEDULED)
        self.assertEqual(tx_info_dms["Time"], deadline_us // 1000)
        self.assertEqual(tx_info_dms["MarketIndex"], 1)

        dms_disarm = signer.schedule_cancel(m, None)
        tx_info_disarm = json.loads(dms_disarm["tx_info"])
        self.assertEqual(tx_info_disarm["TimeInForce"], Signer.CANCEL_ALL_TIF_ABORT)
        self.assertEqual(tx_info_disarm["Time"], 0)

        token = signer.create_auth_token(expiry_seconds=3600, now_unix=1760000000)
        parts = token.split(":")
        self.assertEqual(len(parts), 4)
        self.assertEqual(parts[0], str(1760000000 + 3600))
        self.assertEqual(parts[1], "42")
        self.assertEqual(parts[2], "4")
        self.assertEqual(len(parts[3]), 160)
        print("✓ test_04_lighter_l2_tx_signing_and_tif_expiry_rules passed.")

    async def test_05_incremental_l2_orderbook_and_nonce_gap_resync(self):
        cfg = Config.from_env()
        bot = MarketMaker(cfg)
        bot.md.info = Market.from_api({
            "market_id": 2, "symbol": "CRCL", "status": "active",
            "price_decimals": 2, "size_decimals": 2,
            "min_base_amount": "0.1", "min_quote_amount": "5", "mark_price": "100.00",
        })
        fake_ws = _RealLikeFakeWS()
        bot.ex.ws = fake_ws

        bot.ex.handle_message(json.dumps({
            "type": "subscribed/order_book",
            "channel": "order_book:2",
            "order_book": {
                "code": 0,
                "nonce": 500,
                "begin_nonce": 490,
                "bids": [{"price": "99.90", "size": "10.0"}, {"price": "99.80", "size": "25.0"}],
                "asks": [{"price": "100.10", "size": "12.0"}, {"price": "100.20", "size": "30.0"}],
            },
        }))
        self.assertEqual(bot.md.bid, Decimal("99.90"))
        self.assertEqual(bot.md.ask, Decimal("100.10"))
        self.assertEqual(bot.md.bid_sz, Decimal("10.0"))
        self.assertEqual(bot.md.ask_sz, Decimal("12.0"))

        bot.ex.handle_message(json.dumps({
            "type": "update/order_book",
            "channel": "order_book:2",
            "order_book": {
                "code": 0,
                "begin_nonce": 500,
                "nonce": 501,
                "bids": [{"price": "99.90", "size": "0"}, {"price": "99.95", "size": "8.5"}],
                "asks": [{"price": "100.10", "size": "15.0"}],
            },
        }))
        self.assertEqual(bot.md.bid, Decimal("99.95"))
        self.assertEqual(bot.md.bid_sz, Decimal("8.5"))
        self.assertEqual(bot.md.ask_sz, Decimal("15.0"))
        self.assertEqual(bot.md.depth_levels("BUY")[0], (Decimal("99.95"), Decimal("8.5")))
        self.assertEqual(bot.md.depth_levels("BUY")[1], (Decimal("99.80"), Decimal("25.0")))

        fake_ws.sent.clear()
        bot.ex.handle_message(json.dumps({
            "type": "update/order_book",
            "channel": "order_book:2",
            "order_book": {
                "code": 0,
                "begin_nonce": 505,
                "nonce": 506,
                "bids": [{"price": "99.50", "size": "5.0"}],
                "asks": [],
            },
        }))
        await asyncio.sleep(0.08)
        sent_channels = [(m.get("type"), m.get("channel")) for m in fake_ws.sent]
        self.assertIn(("unsubscribe", "order_book/2"), sent_channels)
        self.assertIn(("subscribe", "order_book/2"), sent_channels)
        print("✓ test_05_incremental_l2_orderbook_and_nonce_gap_resync passed.")

    async def test_06_lighter_ws_channels_and_live_order_lifecycle(self):
        os.environ["DRY_RUN"] = "0"
        cfg = Config.from_env()
        bot = MarketMaker(cfg)
        bot.md.info = Market.from_api({
            "market_id": 0, "symbol": "BTC", "status": "active",
            "price_decimals": 1, "size_decimals": 4,
            "min_base_amount": "0.0001", "min_quote_amount": "5", "mark_price": "80000.0",
        })
        fake_ws = _RealLikeFakeWS()
        bot.ex.ws = fake_ws
        bot.signer.set_nonce(10)

        await bot.ex.subscribe_lighter_feeds(0, cfg.account_index)
        sub_channels = [m.get("channel") for m in fake_ws.sent if m.get("type") == "subscribe"]
        self.assertIn("ticker/0", sub_channels)
        self.assertIn("order_book/0", sub_channels)
        self.assertIn("trade/0", sub_channels)
        self.assertIn("market_stats/0", sub_channels)
        self.assertIn("account_orders/0/42", sub_channels)
        self.assertIn("account_all_positions/42", sub_channels)
        self.assertIn("account_all_trades/42", sub_channels)

        bot.ex.handle_message(json.dumps({
            "type": "update/ticker",
            "channel": "ticker:0",
            "ticker": {
                "s": "BTC",
                "b": {"price": "79990.0", "size": "1.2500"},
                "a": {"price": "80010.0", "size": "1.5000"},
            },
        }))
        self.assertEqual(bot.md.bid, Decimal("79990.0"))
        self.assertEqual(bot.md.ask, Decimal("80010.0"))

        bot.ex.handle_message(json.dumps({
            "type": "update/trade",
            "channel": "trade:0",
            "trades": [{"trade_id": 1, "price": "80010.0", "size": "0.5000", "is_maker_ask": True}],
        }))
        self.assertGreater(bot.md.trade_flow_imbalance(5.0, bot.now()), Decimal("0"))

        fake_ws.sent.clear()
        place_task = asyncio.create_task(
            bot.om.place(0, BUY, Decimal("79990.0"), Decimal("0.0100"), bot.now(), time_in_force="ALO")
        )
        await asyncio.sleep(0.02)
        self.assertEqual(len(fake_ws.sent), 1)
        sent_tx = fake_ws.sent[0]
        self.assertEqual(sent_tx["type"], "jsonapi/sendtx")
        self.assertEqual(sent_tx["data"]["tx_type"], Signer.TX_TYPE_CREATE_ORDER)
        req_id = sent_tx["data"]["id"]
        coi = sent_tx["data"]["tx_info"]["ClientOrderIndex"]

        bot.ex.handle_message(json.dumps({
            "type": "jsonapi/sendtx",
            "id": req_id,
            "code": 200,
            "tx_hash": "0xabc123",
            "volume_quota_remaining": 1450,
        }))
        order = await place_task
        self.assertIsNotNone(order)
        self.assertEqual(order.order_id, str(coi))
        self.assertEqual(bot.ex.volume_quota_remaining, 1450)

        bot.ex.handle_message(json.dumps({
            "type": "update/account_orders",
            "channel": "account_orders:0:42",
            "orders": {
                "0": [{
                    "order_index": 777888,
                    "client_order_index": coi,
                    "market_index": 0,
                    "is_ask": False,
                    "price": "79990.0",
                    "initial_base_amount": "0.0100",
                    "filled_base_amount": "0.0100",
                    "remaining_base_amount": "0.0000",
                    "filled_quote_amount": "799.90",
                    "status": "filled",
                }]
            },
        }))
        self.assertEqual(bot.signer.resolve_order_index(str(coi)), 777888)
        self.assertEqual(bot.ledger.position, Decimal("0.0100"))

        bot.now = lambda: time.monotonic() + 15.0
        pos_msg = json.dumps({
            "type": "update/account_all_positions",
            "channel": "account_all_positions:42",
            "positions": {
                "0": {
                    "market_id": 0,
                    "symbol": "BTC",
                    "sign": -1,
                    "position": "0.0250",
                    "avg_entry_price": "80010.0",
                }
            },
        })
        for _ in range(3):
            bot.ex.handle_message(pos_msg)
        self.assertEqual(bot.ledger.position, Decimal("-0.0250"))
        print("✓ test_06_lighter_ws_channels_and_live_order_lifecycle passed.")



    async def test_07_c_shared_library_19_arg_abi_and_credential_sync(self):
        """Compile a real C shared library matching lighter-signer-linux-amd64.so ABI (19-arg SignCreateOrder, 15-arg SignModifyOrder) and verify exact stack/register arguments."""
        import subprocess, tempfile
        c_src = r"""
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    uint8_t txType;
    void* txInfo;
    void* txHash;
    void* messageToSign;
    void* err;
} SignedTxResponse;

typedef struct {
    void* str;
    void* err;
} StrOrErr;

static int g_chain_id = 0;
static int g_api_key_index = -1;
static long long g_account_index = -1;

void Free(void* ptr) {
    if (ptr) free(ptr);
}

void* CreateClient(const char* url, const char* priv_key, int chain_id, int api_key_index, long long account_index) {
    g_chain_id = chain_id;
    g_api_key_index = api_key_index;
    g_account_index = account_index;
    return NULL;
}

void* CheckClient(int api_key_index, long long account_index) {
    if (api_key_index != g_api_key_index || account_index != g_account_index) {
        return strdup("client mismatch");
    }
    return NULL;
}

SignedTxResponse SignCreateOrder(
    int market_index,
    long long client_order_index,
    long long base_amount,
    int price,
    int is_ask,
    int order_type,
    int time_in_force,
    int reduce_only,
    int trigger_price,
    long long order_expiry,
    long long integrator_account_index,
    int integrator_taker_fee,
    int integrator_maker_fee,
    uint8_t self_trade_behavior_mode,
    uint8_t self_trade_equality_mode,
    uint8_t skip_nonce,
    long long nonce,
    int api_key_index,
    long long account_index
) {
    SignedTxResponse r = {14, NULL, NULL, NULL, NULL};
    if (api_key_index != g_api_key_index || account_index != g_account_index) {
        r.err = strdup("client is not created, call CreateClient first");
        return r;
    }
    char buf[512];
    long long eff_nonce = (nonce == -1) ? 777 : nonce;
    snprintf(buf, sizeof(buf),
        "{\"AccountIndex\":%lld,\"ApiKeyIndex\":%d,\"MarketIndex\":%d,\"ClientOrderIndex\":%lld,"
        "\"BaseAmount\":%lld,\"Price\":%d,\"IsAsk\":%d,\"Type\":%d,\"TimeInForce\":%d,"
        "\"ReduceOnly\":%d,\"TriggerPrice\":%d,\"OrderExpiry\":%lld,\"Nonce\":%lld,\"Sig\":\"REAL_C_SCHNORR_SIG\"}",
        account_index, api_key_index, market_index, client_order_index,
        base_amount, price, is_ask, order_type, time_in_force,
        reduce_only, trigger_price, order_expiry, eff_nonce);
    r.txInfo = strdup(buf);
    r.txHash = strdup("0xc_tx_hash_create");
    return r;
}

SignedTxResponse SignModifyOrder(
    int market_index,
    long long order_index,
    long long base_amount,
    long long price,
    long long trigger_price,
    long long integrator_account_index,
    int integrator_taker_fee,
    int integrator_maker_fee,
    uint8_t self_trade_behavior_mode,
    uint8_t self_trade_equality_mode,
    uint8_t skip_nonce,
    long long nonce,
    long long order_version,
    int api_key_index,
    long long account_index
) {
    SignedTxResponse r = {17, NULL, NULL, NULL, NULL};
    if (api_key_index != g_api_key_index || account_index != g_account_index) {
        r.err = strdup("client is not created, call CreateClient first");
        return r;
    }
    char buf[512];
    snprintf(buf, sizeof(buf),
        "{\"AccountIndex\":%lld,\"ApiKeyIndex\":%d,\"MarketIndex\":%d,\"Index\":%lld,"
        "\"BaseAmount\":%lld,\"Price\":%lld,\"Nonce\":%lld,\"Sig\":\"REAL_C_MOD_SIG\"}",
        account_index, api_key_index, market_index, order_index, base_amount, price, nonce);
    r.txInfo = strdup(buf);
    r.txHash = strdup("0xc_tx_hash_mod");
    return r;
}

SignedTxResponse SignCancelOrder(
    int market_index,
    long long order_index,
    uint8_t skip_nonce,
    long long nonce,
    int api_key_index,
    long long account_index
) {
    SignedTxResponse r = {15, NULL, NULL, NULL, NULL};
    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"AccountIndex\":%lld,\"ApiKeyIndex\":%d,\"MarketIndex\":%d,\"Index\":%lld,\"Nonce\":%lld,\"Sig\":\"REAL_C_CAN_SIG\"}",
        account_index, api_key_index, market_index, order_index, nonce);
    r.txInfo = strdup(buf);
    r.txHash = strdup("0xc_tx_hash_can");
    return r;
}

SignedTxResponse SignCancelAllOrders(
    int time_in_force,
    long long timestamp_ms,
    int cancel_all_market_index,
    uint8_t skip_nonce,
    long long nonce,
    int api_key_index,
    long long account_index
) {
    SignedTxResponse r = {16, NULL, NULL, NULL, NULL};
    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"AccountIndex\":%lld,\"ApiKeyIndex\":%d,\"TimeInForce\":%d,\"Time\":%lld,\"MarketIndex\":%d,\"Nonce\":%lld,\"Sig\":\"REAL_C_DMS_SIG\"}",
        account_index, api_key_index, time_in_force, timestamp_ms, cancel_all_market_index, nonce);
    r.txInfo = strdup(buf);
    r.txHash = strdup("0xc_tx_hash_dms");
    return r;
}

StrOrErr CreateAuthToken(long long deadline, int api_key_index, long long account_index) {
    StrOrErr r = {NULL, NULL};
    char buf[128];
    snprintf(buf, sizeof(buf), "%lld:%lld:%d:c_auth_sig", deadline, account_index, api_key_index);
    r.str = strdup(buf);
    return r;
}
"""
        with tempfile.TemporaryDirectory() as tmpd:
            c_path = os.path.join(tmpd, "mock_signer.c")
            so_path = os.path.join(tmpd, "mock_signer.so")
            with open(c_path, "w") as f:
                f.write(c_src)
            subprocess.check_call(["gcc", "-shared", "-fPIC", "-O2", c_path, "-o", so_path])

            signer = Signer(
                "a" * 80,
                "0x1111111111111111111111111111111111111111",
                account_index=42,
                api_key_index=4,
                chain_id=466324,
                rest_url="https://api.rh.lighter.xyz",
                signer_lib_path=so_path,
            )
            self.assertTrue(signer.uses_native_c_signer)
            self.assertIsNone(signer.check_client())

            m_nvda = Market.from_api({
                "market_id": 15, "symbol": "NVDA", "status": "active",
                "price_decimals": 2, "size_decimals": 4,
                "min_base_amount": "0.0001", "min_quote_amount": "10.000000", "mark_price": "217.06",
            })

            # Place order without pre-setting nonce -> C library receives c_nonce=-1, returns Nonce=777, and signer syncs _nonce=778!
            req_place = signer.place(m_nvda, BUY, Decimal("217.05"), Decimal("0.0507"), int((time.time() + 3600) * 1e6), time_in_force="ALO")
            self.assertIsNone(req_place.get("signer_error"))
            tx_info = json.loads(req_place["tx_info"])
            self.assertEqual(tx_info["Sig"], "REAL_C_SCHNORR_SIG")
            self.assertEqual(tx_info["AccountIndex"], 42)
            self.assertEqual(tx_info["ApiKeyIndex"], 4)
            self.assertEqual(tx_info["MarketIndex"], 15)
            self.assertEqual(tx_info["Price"], 21705)
            self.assertEqual(tx_info["BaseAmount"], 507)
            self.assertEqual(tx_info["OrderExpiry"], -1)
            self.assertEqual(tx_info["Nonce"], 777)
            self.assertEqual(signer._nonce, 778)

            # Modify order -> uses 15-arg C SignModifyOrder and synced nonce 778
            req_mod = signer.modify(m_nvda, str(req_place["client_order_index"]), BUY, Decimal("217.04"), Decimal("0.0507"), int((time.time() + 3600) * 1e6))
            self.assertIsNone(req_mod.get("signer_error"))
            tx_mod = json.loads(req_mod["tx_info"])
            self.assertEqual(tx_mod["Sig"], "REAL_C_MOD_SIG")
            self.assertEqual(tx_mod["Nonce"], 778)

            # Cancel order -> uses 6-arg C SignCancelOrder and nonce 779
            req_can = signer.cancel(m_nvda, str(req_place["client_order_index"]))
            self.assertIsNone(req_can.get("signer_error"))
            tx_can = json.loads(req_can["tx_info"])
            self.assertEqual(tx_can["Sig"], "REAL_C_CAN_SIG")
            self.assertEqual(tx_can["Nonce"], 779)

            # Auth token -> uses C CreateAuthToken
            tok = signer.create_auth_token(expiry_seconds=600, now_unix=1760000000)
            self.assertEqual(tok, "1760000600:42:4:c_auth_sig")
        print("✓ test_07_c_shared_library_19_arg_abi_and_credential_sync passed.")


if __name__ == "__main__":
    unittest.main()
