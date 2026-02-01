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

**Hyperliquid (primary):**

| Endpoint | Description |
|----------|-------------|
| `GET /api/liquidations` | All liquidation maps by asset |
| `GET /api/market-data` | OI, volume, funding, liquidity for all assets |
| `GET /api/asset/{coin}` | Complete data for single asset |
| `GET /api/prices` | Current mark prices |
| `GET /api/health` | Health check |

**Apex Exchange:**

| Endpoint | Description |
|----------|-------------|
| `GET /api/apex/market-data` | Apex tickers for top 50 symbols |
| `GET /api/apex/ticker/{symbol}` | Single symbol ticker + orderbook |
| `GET /api/apex/symbols` | List of 130 Apex perpetual symbols |

**Cross-Exchange:**

| Endpoint | Description |
|----------|-------------|
| `GET /api/combined/market-data` | Merged HL + Apex data by coin |
| `GET /api/combined/funding` | Funding rate comparison (arbitrage finder) |

See [API.md](API.md) for full documentation.

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

**Run collector continuously (every 15 min):**
```bash
python collector.py --continuous --interval 900
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
| `SCAN_INTERVAL` | `900` | Seconds between scans (15 min) |
| `TOP_ASSETS` | `['BTC', 'ETH', 'SOL']` | Priority assets to scan |

## Data Storage

Historical data is stored in SQLite with zlib compression:

| File | Description |
|------|-------------|
| `data/historical.db` | SQLite database with snapshots and price history |
| `data/liquidation_map.json` | Current liquidation clusters (updated every 15 min) |
| `data/wallets.json` | Discovered whale wallets |

**Compression:** The `clusters_json` column is compressed with zlib (~64% reduction), reducing storage from ~50 MB/day to ~10 MB/day.

**Maintenance scripts:**
```bash
# Analyze database size and composition
python scripts/db_maintenance.py --analyze --db data/historical.db

# Compress existing uncompressed data (one-time migration)
python scripts/migrate_compress.py --db data/historical.db
```

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
