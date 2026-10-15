# Arcus Full Quantitative Market Maker on Robinhood Lighter Perp DEX (Level 7/8+)

A full 1:1 mirror of the Arcus Level 7/8+ institutional quantitative market-making system, engineered specifically for the **Robinhood Lighter Perpetual DEX** (`https://apidocs.rh.lighter.xyz/docs/get-started` & `https://apidocs.lighter.xyz/docs/lighter-rh`).

## Robinhood Lighter Perp DEX Specifications Implemented

| Parameter | Robinhood Lighter Mainnet (`mainnet` / `rh_mainnet`) | Robinhood Lighter Testnet (`testnet` / `rh_testnet`) |
| :--- | :--- | :--- |
| **REST API Base URL** | `https://api.rh.lighter.xyz` (`/api/v1/*`) | `https://api.rh-testnet.lighter.xyz` (`/api/v1/*`) |
| **WebSocket Stream URL** | `wss://api.rh.lighter.xyz/stream` | `wss://api.rh-testnet.lighter.xyz/stream` |
| **L2 Signing Chain ID** | `466324` | `300` |
| **Settlement Collateral** | `USDG` (Asset Index `3`, 6 decimals) | `USDG` (Asset Index `3`, 6 decimals) |
| **Available Bot API Key Indices** | `4–156` and `158–254` (`0–3` and `157` are reserved; `255` is query sentinel) | `4–156` and `158–254` |
| **Account Tiers & Maker Latency** | Standard: `200ms` post-only maker latency; Premium (14d volume): `0ms` | Standard: `200ms`; Premium: `0ms` |

### Core Protocol & Architecture Highlights
1. **Zero-Guesswork Lighter L2 Transaction Signing (`signer.py`)**:
   * Supports both the official `lighter-sdk` native C shared library (`lighter-signer-{os}-{arch}.so/.dylib/.dll`) and a built-in pure-Python deterministic signer & paper simulator.
   * Implements exact Lighter L2 transaction types and JSON schemas:
     * `TxTypeL2CreateOrder = 14` (`L2CreateOrderTxInfo`: `AccountIndex`, `ApiKeyIndex`, `MarketIndex`, `ClientOrderIndex`, `BaseAmount`, `Price`, `IsAsk`, `Type`, `TimeInForce`, `ReduceOnly`, `TriggerPrice`, `OrderExpiry`, `ExpiredAt`, `Nonce`, `Sig`).
     * `TxTypeL2CancelOrder = 15` (`L2CancelOrderTxInfo`: single-order cancellation — **does not consume Lighter volume quota**).
     * `TxTypeL2CancelAllOrders = 16` (`L2CancelAllOrdersTxInfo`: supports `CANCEL_ALL_TIF_IMMEDIATE = 0`, exchange-side Dead Man's Switch `CANCEL_ALL_TIF_SCHEDULED = 1` with millisecond deadline, and `CANCEL_ALL_TIF_ABORT = 2` to disarm).
     * `TxTypeL2ModifyOrder = 17` (`L2ModifyOrderTxInfo`: in-place price/size modification).
   * Enforces Lighter's strict Time-In-Force & Expiry rules:
     * `ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0` (`IOC`) with `OrderExpiry = 0` (`DEFAULT_IOC_EXPIRY = 0` to prevent error `21711 invalid expiry`).
     * `ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1` (`GTT`).
     * `ORDER_TIME_IN_FORCE_POST_ONLY = 2` (`ALO` / `POST_ONLY`) with millisecond Unix timestamp `OrderExpiry`.
   * Generates Lighter `Authorization` tokens (`{deadline_unix}:{account_index}:{api_key_index}:{sig_hex}`, max 8h expiry) for private REST endpoints and authenticated WebSocket feeds.
2. **Incremental L2 Order Book Engine & Sequence Validation (`market.py` & `exchange.py`)**:
   * Parses `/api/v1/orderBookDetails` (`market_id`, `symbol`, `status`, `price_decimals`, `size_decimals`, `min_base_amount`, `min_quote_amount`, `mark_price`, `current_funding_rate`).
   * Scales prices by `10 ** price_decimals` and base sizes by `10 ** size_decimals`.
   * Maintains full local L2 book state from `subscribed/order_book` snapshots and 50ms `update/order_book` deltas, deleting levels with `size == "0"` and verifying `begin_nonce == prev_nonce` continuity (automatically resubscribing on any sequence gap).
3. **WebSocket + REST Dual Transport (`exchange.py`)**:
   * Subscribes to `ticker/{market_id}`, `order_book/{market_id}`, `trade/{market_id}` (translating `is_maker_ask` to aggressor side), `market_stats/{market_id}`, and authenticated `account_orders/{market_id}/{account_index}`, `account_all_positions/{account_index}`, `account_all_trades/{account_index}`.
   * Sends low-latency mutations via WebSocket `jsonapi/sendtx` and `jsonapi/sendtxbatch` with automatic HTTP `POST /api/v1/sendTx` (`application/x-www-form-urlencoded`) fallback.
   * Tracks `volume_quota_remaining` and automatically resynchronizes nonces via `GET /api/v1/nextNonce` on code `21104` (`AppErrInvalidNonce`).
4. **Full Level 2 / 3 / 4 / 7 / 8 Quantitative Intelligence (`engine.py`, `ledger.py`, `latent_state.py`, `level4_control.py`, `markout_model.py`, `meta_learner.py`, `optimizer.py`, `mean_field_competition.py`, `causal_impact.py`, `tail_risk_budget.py`)**:
   * All quantitative alpha, POMDP belief-state quoting, empirical Bayesian markout modeling, selective-touch queue hazard evaluation, cross-exchange Binance/Bybit lead-lag defense, and L2 VWAP taker book-walking modules are preserved 1:1.

## Quick Start

```bash
# 1. Run all 80 unit & integration tests (Quantitative + Robinhood Lighter Perp DEX)
python3 -m unittest test_lighter_rh.py test_level7.py test_level4.py test_level8_level3.py

# 2. Scan active Robinhood Lighter perp markets by spread vs volatility
python3 main.py scan --env mainnet --seconds 30

# 3. Paper-trade (DRY_RUN=1) on live Robinhood Lighter market data
python3 main.py --env mainnet --market BTC

# 4. Run live post-only market making on Robinhood Lighter Perp DEX
python3 main.py --live --env mainnet --market BTC
```
