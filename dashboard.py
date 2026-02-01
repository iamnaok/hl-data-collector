#!/usr/bin/env python3
"""
Hyperliquid Data Collector - Dashboard & API

Provides:
- Real-time liquidation map visualization
- Market data (OI, volume, funding, liquidity)
- REST API for data access by trading bots

Usage:
    python dashboard.py
    # API available at http://localhost:8001
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import config
from src.hyperliquid_api import get_current_prices
from src.market_data import get_market_data
from src.apex_client import ApexClient, collect_apex_data

app = FastAPI(
    title="Hyperliquid Data Collector",
    description="Collects and serves liquidation & market data from Hyperliquid"
)

# Enable CORS for trading bot access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache
_market_data_cache = {}
_cache_lock = asyncio.Lock()
_market_data_timestamp = None


def load_json_file(filepath: str) -> dict:
    """Load JSON file safely"""
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


async def get_cached_market_data() -> Dict:
    """Get market data with caching (refresh every 30s) - with lock to prevent race conditions"""
    global _market_data_cache, _market_data_timestamp

    async with _cache_lock:
        now = datetime.now(timezone.utc)
        if _market_data_timestamp is None or (now - _market_data_timestamp).seconds > 30:
            try:
                data = await get_market_data(include_liquidity=True)
                _market_data_cache = {coin: d.to_dict() for coin, d in data.items()}
                _market_data_timestamp = now
            except Exception as e:
                print(f"Error fetching market data: {e}")

    return _market_data_cache


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Data Collector Dashboard"""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hyperliquid Data Collector</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
    body { background: #0f172a; }
    .long-bar { background: linear-gradient(90deg, #22c55e 0%, #16a34a 100%); }
    .short-bar { background: linear-gradient(90deg, #ef4444 0%, #dc2626 100%); }
    .card { background: #1e293b; border-radius: 12px; }
    .scrollbar-thin::-webkit-scrollbar { width: 6px; }
    .scrollbar-thin::-webkit-scrollbar-track { background: #1e293b; }
    .scrollbar-thin::-webkit-scrollbar-thumb { background: #475569; border-radius: 3px; }
</style>
</head>
<body class="text-gray-100 min-h-screen p-4">
<div class="max-w-7xl mx-auto">
    <header class="flex items-center justify-between mb-6">
        <div>
            <h1 class="text-2xl font-bold text-white">📊 Hyperliquid Data Collector</h1>
            <p class="text-gray-400 text-sm">Raw liquidation & market data for trading bots</p>
        </div>
        <div class="text-right">
            <div class="text-sm text-gray-400">Last Update</div>
            <div id="last-update" class="text-white font-mono">--:--:--</div>
        </div>
    </header>
    
    <!-- Stats Row -->
    <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
        <div class="card p-4">
            <div class="text-xs text-gray-400 uppercase">Total Long Liq</div>
            <div id="total-long" class="text-xl font-bold text-green-400">$0</div>
        </div>
        <div class="card p-4">
            <div class="text-xs text-gray-400 uppercase">Total Short Liq</div>
            <div id="total-short" class="text-xl font-bold text-red-400">$0</div>
        </div>
        <div class="card p-4">
            <div class="text-xs text-gray-400 uppercase">Total Open Interest</div>
            <div id="total-oi" class="text-xl font-bold text-blue-400">$0</div>
        </div>
        <div class="card p-4">
            <div class="text-xs text-gray-400 uppercase">24h Volume</div>
            <div id="total-volume" class="text-xl font-bold text-purple-400">$0</div>
        </div>
        <div class="card p-4">
            <div class="text-xs text-gray-400 uppercase">Assets Tracked</div>
            <div id="total-assets" class="text-xl font-bold text-white">0</div>
        </div>
    </div>
    
    <!-- API Info -->
    <div class="card p-4 mb-6">
        <h2 class="text-sm font-semibold mb-2 text-gray-300">API Endpoints</h2>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs font-mono">
            <div>
                <span class="text-green-400">GET</span>
                <span class="text-gray-400">/api/liquidations</span>
                <span class="text-gray-500 block">All liquidation maps</span>
            </div>
            <div>
                <span class="text-green-400">GET</span>
                <span class="text-gray-400">/api/market-data</span>
                <span class="text-gray-500 block">OI, volume, funding, liquidity</span>
            </div>
            <div>
                <span class="text-green-400">GET</span>
                <span class="text-gray-400">/api/asset/{coin}</span>
                <span class="text-gray-500 block">Full data for one asset</span>
            </div>
        </div>
    </div>
    
    <!-- Main Content -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <!-- Left: Liquidation Map -->
        <div class="lg:col-span-2 card p-6">
            <div class="flex items-center justify-between mb-4">
                <h2 class="text-lg font-semibold">Liquidation Map</h2>
                <select id="coin-select" class="bg-gray-700 text-white rounded px-3 py-1 text-sm">
                    <option value="">Select Asset</option>
                </select>
            </div>
            <div id="liq-map" class="min-h-[400px]">
                <p class="text-gray-500">Select an asset to view liquidation map</p>
            </div>
        </div>
        
        <!-- Right: Market Data -->
        <div class="space-y-6">
            <div class="card p-6">
                <h2 class="text-lg font-semibold mb-4">Market Data</h2>
                <div id="market-data" class="space-y-3 text-sm">
                    <p class="text-gray-500">Select an asset above</p>
                </div>
            </div>
            
            <div class="card p-6">
                <h2 class="text-lg font-semibold mb-4">Top by Open Interest</h2>
                <div id="top-oi" class="space-y-2 text-sm max-h-64 overflow-y-auto scrollbar-thin">
                    <p class="text-gray-500">Loading...</p>
                </div>
            </div>
        </div>
    </div>
</div>

<script>
    let data = { maps: {}, market: {} };
    
    async function fetchData() {
        try {
            const [liqResponse, marketResponse] = await Promise.all([
                fetch('api/liquidations'),
                fetch('api/market-data')
            ]);
            
            data.maps = await liqResponse.json();
            data.market = await marketResponse.json();
            
            updateUI();
        } catch (e) {
            console.error('Error fetching data:', e);
        }
    }
    
    function formatUSD(value) {
        if (value >= 1e12) return '$' + (value / 1e12).toFixed(2) + 'T';
        if (value >= 1e9) return '$' + (value / 1e9).toFixed(2) + 'B';
        if (value >= 1e6) return '$' + (value / 1e6).toFixed(2) + 'M';
        if (value >= 1e3) return '$' + (value / 1e3).toFixed(2) + 'K';
        return '$' + value.toFixed(2);
    }
    
    function formatPct(value) {
        const sign = value >= 0 ? '+' : '';
        return sign + value.toFixed(2) + '%';
    }
    
    function updateUI() {
        const maps = data.maps || {};
        const market = data.market || {};
        
        let totalLong = 0, totalShort = 0, totalOI = 0, totalVolume = 0;
        
        Object.values(maps).forEach(m => {
            totalLong += m.total_long_at_risk_usd || 0;
            totalShort += m.total_short_at_risk_usd || 0;
        });
        
        Object.values(market).forEach(m => {
            totalOI += m.open_interest_usd || 0;
            totalVolume += m.volume_24h_usd || 0;
        });
        
        document.getElementById('total-long').textContent = formatUSD(totalLong);
        document.getElementById('total-short').textContent = formatUSD(totalShort);
        document.getElementById('total-oi').textContent = formatUSD(totalOI);
        document.getElementById('total-volume').textContent = formatUSD(totalVolume);
        document.getElementById('total-assets').textContent = Object.keys(maps).length;
        document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
        
        // Update coin selector
        const select = document.getElementById('coin-select');
        const currentValue = select.value;
        select.innerHTML = '<option value="">Select Asset</option>';
        
        const sortedCoins = Object.keys(maps).sort((a, b) => {
            const totalA = (maps[a].total_long_at_risk_usd || 0) + (maps[a].total_short_at_risk_usd || 0);
            const totalB = (maps[b].total_long_at_risk_usd || 0) + (maps[b].total_short_at_risk_usd || 0);
            return totalB - totalA;
        });
        
        sortedCoins.forEach(coin => {
            const m = maps[coin];
            const total = (m.total_long_at_risk_usd || 0) + (m.total_short_at_risk_usd || 0);
            if (total > 50000) {
                const opt = document.createElement('option');
                opt.value = coin;
                opt.textContent = `${coin} (${formatUSD(total)})`;
                select.appendChild(opt);
            }
        });
        
        if (currentValue) select.value = currentValue;
        
        // Update top OI
        const topOIDiv = document.getElementById('top-oi');
        const topOIAssets = Object.entries(market)
            .sort((a, b) => (b[1].open_interest_usd || 0) - (a[1].open_interest_usd || 0))
            .slice(0, 15);
        
        topOIDiv.innerHTML = topOIAssets.map(([coin, m]) => `
            <div class="flex items-center justify-between py-1 border-b border-gray-700/50">
                <span class="font-medium">${coin}</span>
                <div class="text-right">
                    <span class="text-blue-400">${formatUSD(m.open_interest_usd || 0)}</span>
                    <span class="text-xs ml-2 ${m.price_change_24h_pct >= 0 ? 'text-green-400' : 'text-red-400'}">
                        ${formatPct(m.price_change_24h_pct || 0)}
                    </span>
                </div>
            </div>
        `).join('');
        
        if (currentValue && maps[currentValue]) {
            renderLiqMap(maps[currentValue], market[currentValue]);
        }
    }
    
    function renderLiqMap(map, marketInfo) {
        const container = document.getElementById('liq-map');
        const price = map.current_price;
        
        const longClusters = (map.long_liquidations || []).map(c => ({...c, type: 'long'}));
        const shortClusters = (map.short_liquidations || []).map(c => ({...c, type: 'short'}));
        
        const allClusters = [...longClusters, ...shortClusters]
            .filter(c => c.total_size_usd > 50000)
            .sort((a, b) => b.price_center - a.price_center);
        
        let marketHTML = '';
        if (marketInfo) {
            marketHTML = `
                <div class="grid grid-cols-4 gap-4 mb-6 p-4 bg-gray-800/50 rounded-lg">
                    <div>
                        <div class="text-xs text-gray-400">Open Interest</div>
                        <div class="font-bold text-blue-400">${formatUSD(marketInfo.open_interest_usd || 0)}</div>
                    </div>
                    <div>
                        <div class="text-xs text-gray-400">24h Volume</div>
                        <div class="font-bold text-purple-400">${formatUSD(marketInfo.volume_24h_usd || 0)}</div>
                    </div>
                    <div>
                        <div class="text-xs text-gray-400">Funding Rate</div>
                        <div class="font-bold ${marketInfo.funding_rate >= 0 ? 'text-green-400' : 'text-red-400'}">
                            ${(marketInfo.funding_rate * 100).toFixed(4)}%/hr
                        </div>
                    </div>
                    <div>
                        <div class="text-xs text-gray-400">24h Change</div>
                        <div class="font-bold ${marketInfo.price_change_24h_pct >= 0 ? 'text-green-400' : 'text-red-400'}">
                            ${formatPct(marketInfo.price_change_24h_pct || 0)}
                        </div>
                    </div>
                </div>
            `;
            
            if (marketInfo.liquidity) {
                const liq = marketInfo.liquidity;
                marketHTML += `
                    <div class="grid grid-cols-4 gap-4 mb-6 p-4 bg-gray-800/50 rounded-lg">
                        <div>
                            <div class="text-xs text-gray-400">Spread</div>
                            <div class="font-bold text-white">${liq.spread_percent.toFixed(4)}%</div>
                        </div>
                        <div>
                            <div class="text-xs text-gray-400">Bid Depth (1%)</div>
                            <div class="font-bold text-green-400">${formatUSD(liq.bid_depth_1_pct || 0)}</div>
                        </div>
                        <div>
                            <div class="text-xs text-gray-400">Ask Depth (1%)</div>
                            <div class="font-bold text-red-400">${formatUSD(liq.ask_depth_1_pct || 0)}</div>
                        </div>
                        <div>
                            <div class="text-xs text-gray-400">Imbalance</div>
                            <div class="font-bold ${liq.imbalance_1_pct >= 0 ? 'text-green-400' : 'text-red-400'}">
                                ${(liq.imbalance_1_pct * 100).toFixed(1)}%
                            </div>
                        </div>
                    </div>
                `;
            }
            
            document.getElementById('market-data').innerHTML = `
                <div class="space-y-2">
                    <div class="flex justify-between">
                        <span class="text-gray-400">Mark Price</span>
                        <span class="font-mono">$${marketInfo.mark_price?.toLocaleString() || '--'}</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-400">Open Interest</span>
                        <span class="font-mono text-blue-400">${formatUSD(marketInfo.open_interest_usd || 0)}</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-400">24h Volume</span>
                        <span class="font-mono text-purple-400">${formatUSD(marketInfo.volume_24h_usd || 0)}</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-400">Funding (1h)</span>
                        <span class="font-mono ${marketInfo.funding_rate >= 0 ? 'text-green-400' : 'text-red-400'}">
                            ${(marketInfo.funding_rate * 100).toFixed(4)}%
                        </span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-400">Funding (Ann.)</span>
                        <span class="font-mono ${marketInfo.funding_rate_annualized >= 0 ? 'text-green-400' : 'text-red-400'}">
                            ${marketInfo.funding_rate_annualized?.toFixed(1) || '--'}%
                        </span>
                    </div>
                </div>
            `;
        }
        
        if (allClusters.length === 0) {
            container.innerHTML = marketHTML + '<p class="text-gray-500">No significant liquidation clusters</p>';
            return;
        }
        
        const maxSize = Math.max(...allClusters.map(c => c.total_size_usd));
        
        container.innerHTML = marketHTML + `
            <div class="text-center mb-4">
                <span class="text-xl font-bold text-white">${map.coin}</span>
                <span class="text-gray-400 ml-2">Current: $${price.toLocaleString()}</span>
            </div>
            <div class="space-y-1">
                ${allClusters.map(c => {
                    const width = (c.total_size_usd / maxSize) * 100;
                    const isLong = c.type === 'long';
                    const distPct = ((c.price_center - price) / price * 100).toFixed(1);
                    const sign = distPct >= 0 ? '+' : '';
                    
                    return `
                        <div class="flex items-center gap-2 text-sm">
                            <div class="w-24 text-right text-gray-400">$${c.price_center.toLocaleString()}</div>
                            <div class="flex-1 h-6 bg-gray-800 rounded overflow-hidden relative">
                                <div class="${isLong ? 'long-bar' : 'short-bar'} h-full" style="width: ${width}%"></div>
                                <div class="absolute inset-0 flex items-center px-2">
                                    <span class="text-xs text-white font-medium">${formatUSD(c.total_size_usd)}</span>
                                </div>
                            </div>
                            <div class="w-16 text-xs ${distPct >= 0 ? 'text-green-400' : 'text-red-400'}">${sign}${distPct}%</div>
                        </div>
                    `;
                }).join('')}
            </div>
            <div class="flex justify-center gap-8 mt-4 text-sm">
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 long-bar rounded"></div>
                    <span>Long Liquidations</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 short-bar rounded"></div>
                    <span>Short Liquidations</span>
                </div>
            </div>
        `;
    }
    
    document.getElementById('coin-select').addEventListener('change', (e) => {
        if (e.target.value) {
            renderLiqMap(data.maps[e.target.value], data.market[e.target.value]);
        }
    });
    
    fetchData();
    setInterval(fetchData, 30000);
</script>
</body>
</html>
    """


# ============== API ENDPOINTS ==============

@app.get("/api/liquidations")
async def get_liquidations():
    """Get all liquidation maps"""
    return load_json_file(config.LIQUIDATION_MAP_FILE)


@app.get("/api/market-data")
async def get_market_data_endpoint():
    """Get market data for all assets (OI, volume, funding, liquidity)"""
    return await get_cached_market_data()


@app.get("/api/asset/{coin}")
async def get_asset_data(coin: str):
    """Get complete data for a single asset"""
    coin = coin.upper()
    
    liq_data = load_json_file(config.LIQUIDATION_MAP_FILE)
    market_data = await get_cached_market_data()
    
    if coin not in liq_data and coin not in market_data:
        raise HTTPException(status_code=404, detail=f"No data for {coin}")
    
    return {
        "coin": coin,
        "liquidations": liq_data.get(coin, {}),
        "market": market_data.get(coin, {}),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/prices")
async def get_prices():
    """Get current prices for all assets"""
    return await get_current_prices()


@app.get("/api/health")
async def health_check():
    """
    Detailed health check with data freshness monitoring
    """
    import os
    
    try:
        liq_file = config.LIQUIDATION_MAP_FILE
        liq_data = load_json_file(liq_file)
        
        liq_file_age = float('inf')
        liq_last_update = None
        
        if os.path.exists(liq_file):
            liq_last_update = datetime.fromtimestamp(
                os.path.getmtime(liq_file),
                tz=timezone.utc
            )
            liq_file_age = (datetime.now(timezone.utc) - liq_last_update).total_seconds()
        
        # Health status based on data freshness
        if liq_file_age < 600:  # <10 min = healthy
            status = 'healthy'
        elif liq_file_age < 1800:  # <30 min = degraded
            status = 'degraded'
        else:
            status = 'unhealthy'
        
        return {
            'status': status,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'assets_tracked': len(liq_data),
            'data_freshness': {
                'last_update': liq_last_update.isoformat() if liq_last_update else None,
                'age_seconds': int(liq_file_age) if liq_file_age != float('inf') else None,
            }
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


# ============================================================================
# APEX EXCHANGE ENDPOINTS
# ============================================================================

# Cache for Apex data
_apex_cache: Dict = {}
_apex_cache_time: Optional[datetime] = None
_apex_cache_lock = asyncio.Lock()
APEX_CACHE_TTL = 60  # seconds


async def get_cached_apex_data() -> Dict:
    """Get Apex market data with caching"""
    global _apex_cache, _apex_cache_time
    
    async with _apex_cache_lock:
        now = datetime.now(timezone.utc)
        
        if _apex_cache_time and (now - _apex_cache_time).total_seconds() < APEX_CACHE_TTL:
            return _apex_cache
        
        try:
            _apex_cache = await collect_apex_data()
            _apex_cache_time = now
            return _apex_cache
        except Exception as e:
            print(f"Error fetching Apex data: {e}")
            return _apex_cache or {}


@app.get("/api/apex/market-data")
async def get_apex_market_data():
    """
    Get market data from Apex Exchange.
    
    Returns ticker and orderbook data for top symbols.
    Note: Apex doesn't expose individual positions, so no liquidation data.
    """
    return await get_cached_apex_data()


@app.get("/api/apex/ticker/{symbol}")
async def get_apex_ticker(symbol: str):
    """Get ticker data for a single Apex symbol"""
    symbol = symbol.upper()
    if not symbol.endswith("USDT"):
        symbol = f"{symbol}USDT"
    
    async with ApexClient() as client:
        ticker = await client.get_ticker(symbol)
        if not ticker:
            raise HTTPException(status_code=404, detail=f"No data for {symbol}")
        
        orderbook = await client.get_orderbook(symbol)
        
        return {
            "ticker": ticker.to_dict(),
            "orderbook": orderbook.to_dict() if orderbook else None,
            "source": "apex"
        }


@app.get("/api/apex/symbols")
async def get_apex_symbols():
    """Get list of all Apex perpetual symbols"""
    async with ApexClient() as client:
        symbols = await client.get_symbols()
        return {"symbols": symbols, "count": len(symbols)}


@app.get("/api/apex/funding/{symbol}")
async def get_apex_funding_history(symbol: str, limit: int = 100):
    """Get historical funding rates for a symbol"""
    symbol = symbol.upper()
    if not symbol.endswith("USDT"):
        symbol = f"{symbol}USDT"
    
    async with ApexClient() as client:
        history = await client.get_funding_history(symbol, limit)
        return {"symbol": symbol, "history": history}


# ============================================================================
# COMBINED MULTI-EXCHANGE ENDPOINTS  
# ============================================================================

@app.get("/api/combined/market-data")
async def get_combined_market_data():
    """
    Get market data from both Hyperliquid and Apex.
    
    Useful for comparing funding rates, OI, and liquidity across exchanges.
    """
    hl_data = await get_cached_market_data()
    apex_data = await get_cached_apex_data()
    
    # Merge by base symbol
    combined = {}
    
    # Add Hyperliquid data
    for coin, data in hl_data.items():
        combined[coin] = {
            "hyperliquid": data,
            "apex": None
        }
    
    # Add Apex data (convert BTCUSDT -> BTC)
    for symbol, data in apex_data.items():
        coin = symbol.replace("USDT", "")
        if coin in combined:
            combined[coin]["apex"] = data
        else:
            combined[coin] = {
                "hyperliquid": None,
                "apex": data
            }
    
    return combined


@app.get("/api/combined/funding")
async def get_combined_funding():
    """
    Compare funding rates between Hyperliquid and Apex.
    
    Returns funding rate comparison for symbols available on both exchanges.
    """
    hl_data = await get_cached_market_data()
    apex_data = await get_cached_apex_data()
    
    comparison = []
    
    for coin, hl in hl_data.items():
        apex_symbol = f"{coin}USDT"
        apex = apex_data.get(apex_symbol, {})
        
        hl_funding = hl.get("funding_rate", 0)
        apex_ticker = apex.get("ticker", {})
        apex_funding = apex_ticker.get("funding_rate", 0) if apex_ticker else None
        
        if apex_funding is not None:
            comparison.append({
                "coin": coin,
                "hyperliquid_funding": hl_funding,
                "hyperliquid_annualized": hl_funding * 3 * 365 * 100,
                "apex_funding": apex_funding,
                "apex_annualized": apex_funding * 3 * 365 * 100,
                "spread": (hl_funding - apex_funding) * 100 if apex_funding else None
            })
    
    # Sort by spread (arbitrage opportunity)
    comparison.sort(key=lambda x: abs(x.get("spread", 0) or 0), reverse=True)
    
    return {"comparisons": comparison, "count": len(comparison)}


if __name__ == "__main__":
    print("Starting Hyperliquid Data Collector API...")
    print(f"Dashboard: http://localhost:{config.API_PORT}")
    print(f"API: http://localhost:{config.API_PORT}/api/")
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
