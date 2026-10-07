# Robinhood Lighter Perpetual DEX Documentation

## 1. Overview
Lighter operates a high-frequency, zero-knowledge orderbook perpetual DEX on Robinhood Chain / Ethereum Layer-2.
- Robinhood Lighter REST: https://api.rh.lighter.xyz
- Robinhood Lighter WebSocket: wss://api.rh.lighter.xyz/stream
- Standard Lighter Mainnet: https://mainnet.zklighter.elliot.ai
- Standard Lighter Testnet: https://testnet.zklighter.elliot.ai

## 2. Protocol Constants
- TxTypeCreateOrder = 14
- TxTypeCancelOrder = 15
- TxTypeCancelAllOrders = 16
- TxTypeModifyOrder = 17

## 3. Order Types & TIF
- Order Types: 0=Limit, 1=Market, 2=Stop-loss, 3=Stop-loss limit, 4=Take-profit, 5=Take-profit limit, 6=TWAP
- Time In Force: 0=IOC, 1=GTT, 2=Post-Only (ALO)
- Cancel All TIF: 0=Immediate, 1=Scheduled (Dead Man's Switch), 2=Abort

## 4. Integer Precision
- BaseAmount = int(size * 10^size_decimals)
- Price = int(price * 10^price_decimals)
