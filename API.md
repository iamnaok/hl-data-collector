# HL Data Collector API

REST API for accessing Hyperliquid liquidation data, market metrics, and order book liquidity.

## Base URL

```
http://206.189.155.3:8001
# or via nginx proxy:
http://206.189.155.3/hl-data
```

## Authentication

None required. API is open.

---

## Endpoints

### GET /api/health

Check if the collector is running and data is fresh.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2026-02-01T04:12:34.345706+00:00",
  "assets_tracked": 187,
  "data_freshness": {
    "last_update": "2026-02-01T04:11:57.916095+00:00",
    "age_seconds": 36
  }
}
```

**Status values:**
| Status | Meaning |
|--------|---------|
| `healthy` | Data updated < 10 min ago |
| `degraded` | Data updated 10-30 min ago |
| `unhealthy` | Data is > 30 min stale |

---

### GET /api/prices

Get current prices for all tracked assets.

**Response:**
```json
{
  "BTC": 78997.50,
  "ETH": 2451.85,
  "SOL": 105.25,
  ...
}
```

---

### GET /api/liquidations

Get liquidation clusters for all tracked assets. This is the core data showing where leveraged positions will be liquidated.

**Response:**
```json
{
  "BTC": {
    "coin": "BTC",
    "current_price": 78874.5,
    "long_liquidations": [...],
    "short_liquidations": [...],
    "total_long_at_risk_usd": 504469.08,
    "total_short_at_risk_usd": 18186742.53,
    "nearest_long_cluster": {...},
    "nearest_short_cluster": {...}
  },
  "ETH": {...},
  ...
}
```

**Liquidation cluster object:**
```json
{
  "coin": "BTC",
  "side": "long",
  "price_low": 75956.14,
  "price_high": 76035.02,
  "price_center": 75995.58,
  "total_size_usd": 17930.63,
  "position_count": 1,
  "avg_leverage": 30.0
}
```

| Field | Description |
|-------|-------------|
| `side` | `"long"` or `"short"` - position type that gets liquidated |
| `price_low` / `price_high` | Price range where liquidations trigger |
| `price_center` | Middle of the liquidation range |
| `total_size_usd` | Total USD value at risk |
| `position_count` | Number of positions in this cluster |
| `avg_leverage` | Average leverage of positions |

**How to interpret:**
- `long_liquidations` = Long positions liquidated if price drops TO these levels
- `short_liquidations` = Short positions liquidated if price rises TO these levels

---

### GET /api/market-data

Get comprehensive market data: open interest, volume, funding, and order book depth.

**Response:**
```json
{
  "BTC": {
    "coin": "BTC",
    "timestamp": "2026-02-01T04:12:18.071126",
    "mark_price": 78968.0,
    "oracle_price": 78990.0,
    "mid_price": 78967.5,
    "open_interest": 25559.05,
    "open_interest_usd": 2018347649.50,
    "volume_24h_usd": 6069352038.50,
    "volume_24h_base": 76110.69,
    "funding_rate": 0.0000125,
    "funding_rate_annualized": 10.95,
    "premium": -0.00028,
    "prev_day_price": 84059.0,
    "price_change_24h_pct": -6.06,
    "liquidity": {
      "coin": "BTC",
      "timestamp": "2026-02-01T04:12:18.485405",
      "best_bid": 78967.0,
      "best_ask": 78968.0,
      "spread_percent": 0.0013,
      "bid_depth_0_5_pct": 3748599.67,
      "ask_depth_0_5_pct": 1582846.77,
      "bid_depth_1_pct": 3748599.67,
      "ask_depth_1_pct": 1582846.77,
      "bid_depth_2_pct": 3748599.67,
      "ask_depth_2_pct": 1582846.77,
      "imbalance_0_5_pct": 0.406,
      "imbalance_1_pct": 0.406
    }
  },
  ...
}
```

| Field | Description |
|-------|-------------|
| `mark_price` | Current mark price used for P&L |
| `oracle_price` | Oracle price (spot reference) |
| `open_interest` | Total OI in base units (e.g., BTC) |
| `open_interest_usd` | Total OI in USD |
| `volume_24h_usd` | 24h trading volume in USD |
| `funding_rate` | Current 8-hour funding rate (0.0001 = 0.01%) |
| `funding_rate_annualized` | Annualized funding % |
| `premium` | Futures premium over spot |
| `price_change_24h_pct` | 24h price change % |

**Liquidity fields:**
| Field | Description |
|-------|-------------|
| `bid_depth_X_pct` | USD liquidity within X% below mid price |
| `ask_depth_X_pct` | USD liquidity within X% above mid price |
| `imbalance_X_pct` | Book imbalance: `(bids - asks) / (bids + asks)`. Positive = bid-heavy (bullish), negative = ask-heavy (bearish) |

---

### GET /api/asset/{coin}

Get complete data for a single asset (liquidations + market data combined).

**Example:** `GET /api/asset/ETH`

**Response:**
```json
{
  "coin": "ETH",
  "liquidations": {
    "coin": "ETH",
    "current_price": 2451.85,
    "long_liquidations": [...],
    "short_liquidations": [...],
    ...
  },
  "market": {
    "coin": "ETH",
    "mark_price": 2451.85,
    "funding_rate": 0.0000125,
    ...
  },
  "timestamp": "2026-02-01T04:12:34.123456+00:00"
}
```

---

## Example Usage

### Python

```python
import httpx
import asyncio

async def get_liquidation_data():
    async with httpx.AsyncClient() as client:
        # Check health
        health = await client.get("http://206.189.155.3:8001/api/health")
        print(f"Status: {health.json()['status']}")
        
        # Get BTC liquidations
        resp = await client.get("http://206.189.155.3:8001/api/liquidations")
        data = resp.json()
        
        btc = data.get("BTC", {})
        price = btc.get("current_price", 0)
        
        # Find nearest long liquidation cluster (below price)
        long_liqs = btc.get("long_liquidations", [])
        nearest_long = None
        for cluster in long_liqs:
            if cluster["price_center"] < price:
                if nearest_long is None or cluster["price_center"] > nearest_long["price_center"]:
                    nearest_long = cluster
        
        if nearest_long:
            distance = (price - nearest_long["price_center"]) / price * 100
            print(f"BTC: ${price:,.0f}")
            print(f"Nearest long liquidations: ${nearest_long['price_center']:,.0f} ({distance:.1f}% below)")
            print(f"Size at risk: ${nearest_long['total_size_usd']:,.0f}")

asyncio.run(get_liquidation_data())
```

### JavaScript / Node.js

```javascript
const axios = require('axios');

async function getLiquidations() {
  const { data } = await axios.get('http://206.189.155.3:8001/api/liquidations');
  
  const btc = data.BTC;
  console.log(`BTC Price: $${btc.current_price.toLocaleString()}`);
  console.log(`Long clusters: ${btc.long_liquidations.length}`);
  console.log(`Short clusters: ${btc.short_liquidations.length}`);
  console.log(`Total long at risk: $${btc.total_long_at_risk_usd.toLocaleString()}`);
  console.log(`Total short at risk: $${btc.total_short_at_risk_usd.toLocaleString()}`);
}

getLiquidations();
```

### cURL

```bash
# Health check
curl -s "http://206.189.155.3:8001/api/health" | jq

# Get BTC liquidations
curl -s "http://206.189.155.3:8001/api/liquidations" | jq '.BTC'

# Get ETH market data
curl -s "http://206.189.155.3:8001/api/market-data" | jq '.ETH'

# Get single asset
curl -s "http://206.189.155.3:8001/api/asset/SOL" | jq
```

### AI Agent Prompt Example

```
You have access to a Hyperliquid liquidation data API at http://206.189.155.3:8001

Available endpoints:
- GET /api/health - Check if data is fresh
- GET /api/prices - Current prices for all coins
- GET /api/liquidations - Liquidation clusters for all coins
- GET /api/market-data - OI, volume, funding, order book depth
- GET /api/asset/{coin} - Complete data for one coin

Use this data to:
1. Identify coins with large liquidation clusters near current price
2. Find funding rate extremes (crowded longs/shorts)
3. Detect order book imbalances
4. Calculate liquidation-to-liquidity ratios (LLR)

Example: To find a potential short squeeze, look for:
- Large short_liquidations cluster 1-5% above current price
- Weak ask_depth (thin sells)
- Negative funding (shorts paying longs)
```

---

## Data Update Frequency

- **Liquidation data**: Updated every 5 minutes
- **Market data**: Cached for 60 seconds, then refreshed
- **Prices**: Real-time from Hyperliquid API

---

## Rate Limits

No hard rate limits, but please be reasonable:
- Don't poll more than once per second
- Cache responses when possible
- Use `/api/health` to check freshness before fetching full data

---

## Tracked Assets

The collector tracks ~190 assets including:
- Major: BTC, ETH, SOL, AVAX, DOGE, LINK, ARB, OP
- Mid-cap: APE, AAVE, UNI, CRV, SNX, GMX, etc.
- Memes: PEPE, WIF, BONK, FARTCOIN, etc.

Full list available via `/api/prices` response keys.

---

## Source Code

GitHub: https://github.com/iamnaok/hl-data-collector
