# Robinhood Lighter Perpetual DEX Quantitative Market Maker (Level 8 Engine)

A high-frequency quantitative market-making engine engineered specifically for **Robinhood Lighter Perpetual DEX** (`api.rh.lighter.xyz`) and the **zkLighter** orderbook matching engine on Robinhood Chain / Ethereum Layer-2.

---

## 1. System Architecture Overview

```
                               ┌───────────────────────────────────────────────────────────┐
                               │   Robinhood Lighter Ingestion & zk Order Book Stream      │
                               │   (wss://api.rh.lighter.xyz/stream - 50ms batching)       │
                               └─────────────────────────────┬─────────────────────────────┘
                                                             │
                              ┌──────────────────────────────┴──────────────────────────────┐
                              ▼                                                             ▼
┌──────────────────────────────────────────────┐                           ┌──────────────────────────────────────────────┐
│   Lighter Microstructure & Orderbook Intel   │                           │   Cross-Venue & Reference Lead-Lag Alpha     │
│ - Multi-horizon Trade Flow Imbalance (TFI)   │                           │ - CEX Lead-Lag (Binance & Bybit Futures)     │
│ - Multi-depth Order Book Imbalance (L1-L10)  │                           │ - Rolling USDT-vs-USD Basis Cointegration    │
│ - Liquidity Fragility & Fast Depletion Check │                           │ - Funding Carry Directional Skew             │
│ - Flow Acceleration & Exhaustion Detection   │                           │ - External Liquidation Pressure Tracking     │
└──────────────────────┬───────────────────────┘                           └──────────────────────┬───────────────────────┘
                       │                                                                          │
                       └──────────────────────────────┬───────────────────────────────────────────┘
                                                      ▼
                               ┌─────────────────────────────────────────────┐
                               │       Alpha-Aware Target Inventory          │
                               │        q_target = f(Alpha, Funding)         │
                               │   ResPrice = Fair - gamma * (q - q_targ)    │
                               └──────────────────────┬──────────────────────┘
                                                      │
                                                      ▼
                               ┌─────────────────────────────────────────────┐
                               │      Selective-Touch Candidate Evaluator    │
                               │  - Touch vs 1-Tick vs Model Spread Pricing  │
                               │  - Queue Hazard Fill Probability Model P(H) │
                               │  - Empirical Bayesian E[Markout|State]      │
                               │  - One-Sided Steam Suppression              │
                               └──────────────────────┬──────────────────────┘
                                                      │
                              ┌───────────────────────┴─────────────────────────┐
                              ▼                                                 ▼
┌──────────────────────────────────────────────┐               ┌──────────────────────────────────────────────┐
│       Smart Inventory & Unwind Exits         │               │         Execution & Protection Pipeline      │
│ - Zero-maker-fee queue priority unwinds      │               │ - Scheduled Cancel Dead Man's Switch (DMS)   │
│ - Time-decay dynamic breakeven scratch       │               │ - Multi-Ladder Quoting (L0 Touch, L1 Depth)  │
│ - Preemptive Sweep Guard & Toxic Halt        │               │ - Strict Reduce-Only Protection on Unwinds   │
│ - Surgical IOC Taker Stop-Loss Cut           │               │ - Regular Trading Hours (RTH) Filter (NVDA)  │
└──────────────────────────────────────────────┘               └──────────────────────────────────────────────┘
```

---

## 2. Deep Research: Robinhood Lighter Perpetual DEX Architecture

### A. Deployment & Infrastructure
- **Robinhood Chain**: An Arbitrum-based Layer-2 rollup on top of Ethereum using Ethereum blobs for data availability.
- **Lighter Integration**: Lighter operates its high-throughput, zero-knowledge orderbook matching engine on Robinhood Chain, powering perpetual futures trading inside the Robinhood Wallet ecosystem.
- **Host Separation**: Robinhood Lighter operates as a dedicated exchange instance with its own independent books and liquidity:
  - **Robinhood Lighter REST**: `https://api.rh.lighter.xyz`
  - **Robinhood Lighter WebSocket**: `wss://api.rh.lighter.xyz/stream`
  - **Robinhood Lighter Documentation**: `https://apidocs.rh.lighter.xyz/docs`
  - **Standard Lighter Mainnet**: `https://mainnet.zklighter.elliot.ai` / `wss://mainnet.zklighter.elliot.ai/stream`
  - **Standard Lighter Testnet**: `https://testnet.zklighter.elliot.ai` / `wss://testnet.zklighter.elliot.ai/stream`
- **Asset Classes**:
  1. **Tokenized US Equities Perps**: NVDA-USD, AAPL-USD, TSLA-USD, etc.
  2. **Crypto Perps**: BTC-USD, ETH-USD, SOL-USD, etc.

### B. Account & Authentication Architecture
1. **L1 Wallet**: Standard 20-byte Ethereum / Robinhood Chain address (`0x...`).
2. **Account Index (`account_index`)**: Unique integer identifier on Lighter assigned to the account/sub-account.
3. **API Key Index (`api_key_index`)**: Range `4` to `254`. Indices `0-3` are reserved for Lighter's web and mobile interfaces; `255` is used for querying all registered keys.
4. **Nonces**: Every signed transaction carries a strictly increasing integer nonce (`nonce`) per API key. Can be fetched via `GET /api/v1/nextNonce`. Alternatively, setting `skip_nonce = 1` allows monotonically increasing nonces without network roundtrips.
5. **Auth Tokens**: Private REST endpoints and authenticated WebSocket channels require an auth token created with an expiry deadline (up to 8 hours):
   `create_auth_token(deadline_s=28800)`

### C. Protocol Transaction Types & Constants
| Constant | Type Value | Description |
| :--- | :--- | :--- |
| `TxTypeL2ChangePubKey` | `8` | Register or update API key |
| `TxTypeL2CreateSubAccount` | `9` | Create new sub-account |
| `TxTypeL2CreateOrder` | `14` | Place limit, market, or stop order |
| `TxTypeL2CancelOrder` | `15` | Cancel specific order |
| `TxTypeL2CancelAllOrders` | `16` | Cancel all orders / Dead Man's Switch |
| `TxTypeL2ModifyOrder` | `17` | Modify existing order price/size |

### D. Order Types & Time-in-Force (TIF)
- **Order Types**:
  - `0`: Limit Order
  - `1`: Market Order
  - `2`: Stop-Loss (Market)
  - `3`: Stop-Loss Limit
  - `4`: Take-Profit (Market)
  - `5`: Take-Profit Limit
  - `6`: TWAP
- **Time In Force (TIF)**:
  - `0`: Immediate-Or-Cancel (`IOC`)
  - `1`: Good-Till-Time (`GTT`)
  - `2`: Post-Only (`ALO` / Add-Liquidity-Only / Maker-Only)
- **Cancel-All Time In Force**:
  - `0`: Immediate Cancel All
  - `1`: Scheduled Cancel All (**Dead Man's Switch**)
  - `2`: Abort Scheduled Cancel All

### E. Integer Precision Scaling
Lighter's zero-knowledge matching engine processes all prices and quantities as integers:
1939\text{BaseAmount} = \lfloor \text{Quantity} \times 10^{\text{size\_decimals}} \rfloor1939
1939\text{Price} = \lfloor \text{Price} \times 10^{\text{price\_decimals}} \rfloor1939
For example, for NVDA-USD (`price_decimals = 2`, `size_decimals = 4`):
- Price $\25.50 \to 12550$
- Quantity .5000 \to 25000$

### F. WebSocket Channel Reference
1. **Order Book (`order_book/{market_id}`)**:
   - Emits snapshots and 50ms batch differential updates.
   - Message type: `update/order_book` with `asks` and `bids` arrays of `{"price": str, "size": str}`.
2. **Ticker (`ticker/{market_id}`)**:
   - High-speed BBO touch updates on every book modification.
   - Message type: `update/ticker` with `b` (best bid) and `a` (best ask).
3. **Trades (`trade/{market_id}`)**:
   - Real-time trade fills.
   - Message type: `update/trade` with `trades` array containing `trade_id`, `price`, `size`, `is_maker_ask`.
4. **Market Stats (`market_stats/{market_id}`)**:
   - Live mark price, index price, funding rate, next funding timestamp.
5. **Account Orders (`account_orders/{market_id}/{account_index}`)**:
   - Authenticated stream emitting order status changes (`open`, `filled`, `canceled`, `rejected`).
6. **Account Market (`account_market/{market_id}/{account_index}`)**:
   - Authenticated stream emitting live position sign, size, entry price, and unrealized PnL.
7. **Keepalive Ping**:
   - Client sends `{"type": "ping"}` every 30s; server responds `{"type": "pong"}` (must be $< 120).

---

## 3. Quantitative Quoting & Risk Management Modules

### 1. Microstructure & Orderbook Intelligence (`market.py`)
- **Micro-price**: {\text{micro}} = \frac{P_{\text{bid}} \cdot Q_{\text{ask}} + P_{\text{ask}} \cdot Q_{\text{bid}}}{Q_{\text{bid}} + Q_{\text{ask}}}$
- **Multi-Depth OBI**: Volume-weighted order book imbalance across L1, L5, and L10 levels.
- **Trade Flow Imbalance (TFI)**: Evaluated over rolling horizons of /usr/bin/bash.25\text{s}$, /usr/bin/bash.5\text{s}$, \text{s}$, \text{s}$, \text{s}$, 0\text{s}$.
- **Liquidity Fragility**: Measures the instantaneous consumption rate of resting liquidity to detect level sweeps before they happen.
- **Own-Order Exclusion**: Dynamically subtracts the bot's own resting quotes from the orderbook depth and micro-price to avoid self-reinforcing quote distortion.

### 2. Alpha-Aware Target Inventory & Reservation Price (`engine.py`)
- Shifts inventory target {\text{target}}$ dynamically based on funding rate carry and cross-exchange momentum.
- Reservation price formula:
  1939r(s, q) = \text{Fair} - \gamma (q - q_{\text{target}}) \sigma^21939
- Guarantees asymmetric quoting that naturally absorbs inventory when advantageous and unloads aggressively when inventory limits are reached.

### 3. Selective Touch & Queue Hazard Model (`engine.py`)
- Evaluates candidate quote placements: direct touch, 1-tick inside/outside, and model spread.
- Computes Expected Value (EV):
  1939\text{EV} = P(\text{Fill}) \cdot (\text{Edge} - \text{Toxicity}) - \text{Adverse Selection Cost}1939
- Only places or advances quotes when EV exceeds `QUEUE_RESET_COST_BPS`.

### 4. Dynamic Maker Scratch Unwind & Emergency Taker Cut (`engine.py`, `orders.py`)
- Positions held past `MAX_HOLD_S` automatically initiate dynamic maker scratch unwinds at breakeven or small scratch profit.
- If adverse market drift exceeds `EMERGENCY_TAKER_LOSS_BPS` or adverse OBI persists for $> 4\text{s}$, executes a surgical reduce-only IOC taker order to immediately eliminate tail risk.

### 5. Dead Man's Switch (`signer.py`, `bot.py`)
- Uses Lighter's native `TxTypeL2CancelAllOrders` with `TimeInForce = CANCEL_ALL_SCHEDULED` (1).
- Automatically refreshed every $\text{DMS\_TTL\_S} / 3$ seconds.
- If the bot halts, crashes, or loses internet connectivity, the exchange sequencer automatically cancels all resting quotes upon deadline expiry.

### 6. Regular Trading Hours (RTH) Filter (`market.py`, `bot.py`)
- Tailored for US tokenized equities on Robinhood Lighter (e.g. NVDA-USD).
- Checks Eastern Time (ET) market hours (/usr/bin/bash9:30 - 16:00$ ET). Outside RTH, pauses new position accumulation while allowing open positions to unwind smoothly.

---

## 4. Repository Structure

```
robinhood_lighter_bot/
├── config.py           # Configuration loader, Robinhood Lighter endpoints, tunables
├── signer.py           # Lighter transaction builder, nonces, client order index, signatures
├── market.py           # Market metadata, orderbook tracking, microstructure metrics
├── exchange.py         # Async WebSocket & REST client for Robinhood Lighter DEX
├── orders.py           # OrderManager: resting maker ladder, taker slot isolation, reconcile
├── engine.py           # Level 8 Quantitative MM Engine: EV filter, queue model, unwinds
├── ledger.py           # Position tracking, multi-horizon markout PnL, online Bayesian learner
├── feeds.py            # External cross-exchange feeds (Binance & Bybit futures)
├── bot.py              # Main orchestrator: event loop, DMS heartbeat, status reporting
├── main.py             # CLI entrypoint (run, scan, --live, --env, --market)
├── scan.py             # Live market scanner for Robinhood Lighter DEX
├── analyze_journal.py  # Post-trade markout and fill analysis tool
├── sim.py              # Exchange WebSocket & matching engine simulator
├── test_bot.py         # 44-test institutional test suite (100% pass)
├── run_forever.sh      # Production watchdog runner with exponential backoff
├── .env.example        # Comprehensive configuration template
├── .env.robinhood_nvda # Optimized profile for NVDA equity perpetuals
└── .env.robinhood_btc  # Optimized profile for BTC-USD crypto perpetuals
```

---

## 5. Verification & Test Suite

The test suite validates 44 institutional quantitative, risk management, and protocol scenarios:
```bash
python3 test_bot.py
```
Test results:
```
Ran 44 tests in 2.733s
OK
✓ Multi-ladder order placement and individual tracking
✓ Micro-price and orderbook intelligence (OBI, TFI)
✓ Adaptive EV filter & zero-crossing protection
✓ Online Bayesian learning of flow toxicity
✓ Spread capture & roundtrip accounting
✓ Toxic regime detection & preemptive Sweep Guard
✓ Smart inventory fast breakeven unwinds & no-loss selling
✓ Emergency taker cut on adverse cascade
✓ Strict reduce-only enforcement on unwinds
✓ Queue-aware fill probability & selective touch
✓ Liquidity fragility & flow exhaustion detection
✓ Alpha & funding carry-aware target inventory
✓ Multi-horizon markout tracking (1s, 5s, 15s, 30s)
✓ Lighter TIF=0 (IOC), TIF=1 (GTT), TIF=2 (Post-Only)
✓ Lighter integer decimal scaling and transaction formats
✓ Scheduled Cancel Dead Man's Switch (DMS)
✓ Cross-exchange lead-lag alpha (Binance & Bybit)
```

---

## 6. Quickstart Guide

### Step 1: Configure Environment
Copy `.env.example` (or `.env.robinhood_nvda`) to `.env`:
```bash
cp .env.example .env
```
Fill in your credentials:
```ini
ROBINHOOD_LIGHTER_ENV=rh_mainnet
ROBINHOOD_WALLET_ADDRESS=0xYourEthereumOrRobinhoodWalletAddress
ROBINHOOD_API_PRIVATE_KEY=your_64_hex_private_key
ROBINHOOD_ACCOUNT_INDEX=0
ROBINHOOD_API_KEY_INDEX=4
MARKET=NVDA-USD
DRY_RUN=1
```

### Step 2: Scan Markets
Run the market scanner to inspect active markets and funding rates:
```bash
python3 main.py scan
```

### Step 3: Run Paper Trading
Run the bot in simulation/dry-run mode to observe live quote calculation and risk management without sending real orders:
```bash
python3 main.py
```

### Step 4: Run Live Trading
Once satisfied with paper trading performance, launch live trading:
```bash
python3 main.py --live
```
Or run under the watchdog script for high-availability production deployment:
```bash
./run_forever.sh .env
```
