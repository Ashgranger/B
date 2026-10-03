# Bulk Trade Perpetual DEX Level 8+ Institutional Market Maker

A high-frequency quantitative market-making system built for **Bulk Trade** (BULK DEX on Solana), ported from the institutional Arcus Level 8+ engine.

---

## Key Capabilities

1. **Native Solana Ed25519 Cryptography & Pre-computed Order IDs**:
   - Canonical little-endian binary serialization (`CommissionSignableActions` fixed-point $10^8$ scale).
   - Domain separation: Mainnet (`1`), Testnet (`2`), Devnet (`3`).
   - Pre-computed deterministic Order IDs derived locally before node response.
   - Atomic multi-action batch transactions (`sign_group`) for atomic modify (cancel + re-quote in a single transaction).

2. **WebSocket-First Transport & REST Fallback**:
   - Streaming L2 orderbook, public trades tape, ticker/BBO, and private account order/fill events.
   - Order mutations sent directly over WebSocket actor for sub-millisecond execution.
   - Built-in sliding-window rate limiter (`MAX_ACTIONS_PER_MIN=120`) and automatic reconnect resilience.

3. **Expanded Microstructure Feature Pipeline**:
   - Size-weighted microprice: $P_{\text{micro}} = \frac{P_b \cdot S_a + P_a \cdot S_b}{S_b + S_a}$
   - Multi-depth Order Book Imbalance: $\text{OBI}_{\text{L1}}$, $\text{OBI}_{\text{L5}}$, $\text{OBI}_{\text{L10}}$
   - Rolling multi-horizon Trade Flow Imbalance ($\text{TFI}$) across 0.25s, 0.5s, 1s, 2s, 5s, 10s
   - Flow Acceleration: $\text{TFI}(1s) - \text{TFI}(5s)$
   - Liquidity Fragility Index: $\frac{\text{Aggressive Counter-Volume}_{1s}}{\text{Resting Depth}_{\text{L1-L5}}}$

4. **Positive Unrealized PnL Profit Trailing ("Let Winners Run")**:
   - Trails profit target higher during favorable momentum rather than closing prematurely.
   - Aggressive inside pennying when flow decelerates.
   - Emergency taker profit lock if severe reversal threatens accumulated profits.

5. **L2 VWAP Order Book Walking**:
   - Walks full multi-tier book depth to compute effective execution VWAP and slippage for emergency taker exits.

6. **Empirical Bayesian Markout Prediction**:
   - Estimates $E[\text{Markout} \mid \text{Side}, \text{Regime}, \text{Level}, \text{Horizon}]$ with Bayesian shrinkage toward theoretical priors.

7. **Cross-Exchange Lead/Lag Discovery**:
   - Tracks external benchmark venues (Binance, Bybit) with venue-isolated velocity tracking, dispersion defense, and lead/lag divergence skewing.

---

## File Structure

- `bot.py`: Main bot coordinator, WebSocket event router, and tick execution cycle.
- `config.py`: Environment configuration loader and parameter validation.
- `engine.py`: Quantitative market making engine, reservation pricing, and multi-ladder EV evaluator.
- `exchange.py`: Bulk Trade WebSocket streaming and REST API client.
- `ledger.py`: Position accounting, PnL tracking, and Bayesian markout learner.
- `market.py`: L2 orderbook management, trade tape, microprice, and cross-venue tracker.
- `orders.py`: Order manager with slot tracking and atomic batch transactions.
- `signer.py`: Solana Ed25519 signer and deterministic order ID calculator.
- `sim.py`: Deterministic simulation test harness with mock matching engine.
- `scan.py`: Market scanner CLI for discovering active perpetual contracts.
- `test_bulk_bot.py`: Complete test suite verifying all 10 core subsystems.
- `.env.example`: Configuration template.
