# Hyperliquid Data Collector

Collects and aggregates liquidation data from Hyperliquid DEX. All positions on Hyperliquid are on-chain and public, allowing us to query exact liquidation prices for any wallet.

## Features

- **Wallet Discovery**: Discovers active wallets from recent trades
- **Position Scanner**: Scans wallets for open positions with liquidation prices
- **Liquidation Aggregator**: Clusters liquidations into price bands
- **Market Data**: Fetches OI, volume, funding rates, order book depth
- **Historical Storage**: SQLite database for time-series analysis
- **REST API**: Serves data to trading bots and dashboards

## Architecture

```
Hyperliquid API
      │
      ▼
┌─────────────────┐
│ Wallet Discovery│ ──▶ Finds active traders from recent trades
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│Position Scanner │ ──▶ Gets positions + liquidation prices per wallet
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Aggregator     │ ──▶ Clusters liquidations into price bands
└────────┬────────┘
         │
         ├──▶ data/liquidation_map.json
         ├──▶ data/historical.db
         └──▶ REST API (port 8001)
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/liquidations` | All liquidation maps by asset |
| `GET /api/market-data` | OI, volume, funding, liquidity for all assets |
| `GET /api/asset/{coin}` | Complete data for single asset |
| `GET /api/prices` | Current mark prices |
| `GET /api/health` | Health check |

## Installation

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

**Run data collector (one-time):**
```bash
python collector.py
```

**Run collector continuously (every 5 min):**
```bash
python collector.py --continuous --interval 300
```

**Start API server:**
```bash
python dashboard.py
# Dashboard at http://localhost:8001
```

## Configuration

Edit `src/config.py` or set environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `API_HOST` | `0.0.0.0` | API bind address |
| `API_PORT` | `8001` | API port |
| `SCAN_INTERVAL` | `300` | Seconds between scans |
| `TOP_ASSETS` | `['BTC', 'ETH', 'SOL']` | Priority assets to scan |

## Data Format

**Liquidation Map** (`/api/liquidations`):
```json
{
  "BTC": {
    "long_liquidations": [
      {
        "price_center": 85000,
        "price_low": 84500,
        "price_high": 85500,
        "total_size_usd": 5000000,
        "position_count": 12
      }
    ],
    "short_liquidations": [...],
    "total_long_liq_usd": 150000000,
    "total_short_liq_usd": 120000000
  }
}
```

## Related Projects

- [hl-trading-bot](https://github.com/iamnaok/hl-trading-bot) - Trading signals using this data

## License

MIT
