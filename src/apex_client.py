"""
Apex Exchange Data Client

Collects market data from omni.apex.exchange API.
Note: Apex doesn't expose individual positions publicly, so we can't calculate
liquidation clusters like we do for Hyperliquid. We collect market data only.
"""
import asyncio
import httpx
from typing import Dict, List, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://omni.apex.exchange/api/v3"


@dataclass
class ApexTicker:
    """Ticker data for a single symbol"""
    symbol: str
    last_price: float
    mark_price: float
    index_price: float
    open_interest: float  # In base currency (e.g., BTC)
    open_interest_usd: float
    volume_24h: float  # In base currency
    volume_24h_usd: float
    funding_rate: float
    predicted_funding_rate: float
    next_funding_time: str
    price_change_24h_pct: float
    high_24h: float
    low_24h: float
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ApexOrderBook:
    """Order book snapshot"""
    symbol: str
    timestamp: str
    best_bid: float
    best_ask: float
    spread_pct: float
    bid_depth_1_pct: float  # USD depth within 1% of mid
    ask_depth_1_pct: float
    bid_depth_2_pct: float  # USD depth within 2% of mid
    ask_depth_2_pct: float
    imbalance_1_pct: float  # (bids - asks) / (bids + asks)
    
    def to_dict(self) -> dict:
        return asdict(self)


class ApexClient:
    """
    Client for fetching market data from Apex Exchange.
    
    Usage:
        async with ApexClient() as client:
            symbols = await client.get_symbols()
            ticker = await client.get_ticker("BTCUSDT")
            orderbook = await client.get_orderbook("BTCUSDT")
    """
    
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    async def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make GET request"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching {endpoint}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {endpoint}: {e}")
            return None
    
    async def get_symbols(self) -> List[str]:
        """Get list of all perpetual symbols"""
        data = await self._get("/symbols")
        if not data:
            return []
        
        perp_contracts = data.get("data", {}).get("contractConfig", {}).get("perpetualContract", [])
        
        # Convert BTC-USDT to BTCUSDT format
        symbols = []
        for contract in perp_contracts:
            symbol = contract.get("symbol", "")
            # API uses BTCUSDT format, config uses BTC-USDT
            api_symbol = symbol.replace("-", "")
            symbols.append(api_symbol)
        
        return symbols
    
    async def get_ticker(self, symbol: str) -> Optional[ApexTicker]:
        """Get ticker data for a single symbol"""
        data = await self._get("/ticker", params={"symbol": symbol})
        if not data or not data.get("data"):
            return None
        
        ticker_data = data["data"][0] if isinstance(data["data"], list) else data["data"]
        
        try:
            last_price = float(ticker_data.get("lastPrice", 0))
            oi_base = float(ticker_data.get("openInterest", 0))
            volume_base = float(ticker_data.get("volume24h", 0))
            
            return ApexTicker(
                symbol=ticker_data.get("symbol", symbol),
                last_price=last_price,
                mark_price=float(ticker_data.get("markPrice", last_price)),
                index_price=float(ticker_data.get("indexPrice", last_price)),
                open_interest=oi_base,
                open_interest_usd=oi_base * last_price,
                volume_24h=volume_base,
                volume_24h_usd=float(ticker_data.get("turnover24h", volume_base * last_price)),
                funding_rate=float(ticker_data.get("fundingRate", 0)),
                predicted_funding_rate=float(ticker_data.get("predictedFundingRate", 0)),
                next_funding_time=ticker_data.get("nextFundingTime", ""),
                price_change_24h_pct=float(ticker_data.get("price24hPcnt", 0)) * 100,
                high_24h=float(ticker_data.get("highPrice24h", 0)),
                low_24h=float(ticker_data.get("lowPrice24h", 0))
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing ticker for {symbol}: {e}")
            return None
    
    async def get_orderbook(self, symbol: str, limit: int = 50) -> Optional[ApexOrderBook]:
        """Get order book depth for a symbol"""
        data = await self._get("/depth", params={"symbol": symbol, "limit": limit})
        if not data or not data.get("data"):
            return None
        
        book = data["data"]
        bids = book.get("b", [])  # [[price, size], ...]
        asks = book.get("a", [])
        
        if not bids or not asks:
            return None
        
        try:
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid_price = (best_bid + best_ask) / 2
            spread_pct = (best_ask - best_bid) / mid_price * 100
            
            # Calculate depth within 1% and 2% of mid price
            def calc_depth(orders: list, is_bid: bool, pct: float) -> float:
                depth = 0
                threshold = mid_price * (1 - pct / 100) if is_bid else mid_price * (1 + pct / 100)
                for order in orders:
                    price = float(order[0])
                    size = float(order[1])
                    if is_bid and price >= threshold:
                        depth += price * size
                    elif not is_bid and price <= threshold:
                        depth += price * size
                return depth
            
            bid_depth_1 = calc_depth(bids, True, 1.0)
            ask_depth_1 = calc_depth(asks, False, 1.0)
            bid_depth_2 = calc_depth(bids, True, 2.0)
            ask_depth_2 = calc_depth(asks, False, 2.0)
            
            total_1 = bid_depth_1 + ask_depth_1
            imbalance_1 = (bid_depth_1 - ask_depth_1) / total_1 if total_1 > 0 else 0
            
            return ApexOrderBook(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc).isoformat(),
                best_bid=best_bid,
                best_ask=best_ask,
                spread_pct=spread_pct,
                bid_depth_1_pct=bid_depth_1,
                ask_depth_1_pct=ask_depth_1,
                bid_depth_2_pct=bid_depth_2,
                ask_depth_2_pct=ask_depth_2,
                imbalance_1_pct=imbalance_1
            )
        except (ValueError, TypeError, IndexError) as e:
            logger.error(f"Error parsing orderbook for {symbol}: {e}")
            return None
    
    async def get_funding_history(self, symbol: str, limit: int = 100) -> List[dict]:
        """Get historical funding rates"""
        data = await self._get("/history-funding", params={"symbol": symbol, "limit": limit})
        if not data or not data.get("data"):
            return []
        
        return data.get("data", [])
    
    async def get_all_market_data(self, symbols: List[str] = None) -> Dict[str, dict]:
        """
        Get market data for multiple symbols.
        
        Returns dict of {symbol: {ticker: ..., orderbook: ...}}
        """
        if symbols is None:
            symbols = await self.get_symbols()
        
        # Prioritize major assets
        priority = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "DOGEUSDT", 
                    "LINKUSDT", "ARBUSDT", "OPUSDT", "APTUSDT", "SUIUSDT"]
        
        # Reorder: priority first, then rest
        ordered = [s for s in priority if s in symbols]
        ordered += [s for s in symbols if s not in priority]
        
        results = {}
        
        # Fetch in batches to avoid rate limits
        batch_size = 10
        for i in range(0, min(len(ordered), 50), batch_size):  # Limit to top 50
            batch = ordered[i:i + batch_size]
            
            tasks = []
            for symbol in batch:
                tasks.append(self._fetch_symbol_data(symbol))
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for symbol, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching {symbol}: {result}")
                elif result:
                    results[symbol] = result
            
            # Small delay between batches
            if i + batch_size < len(ordered):
                await asyncio.sleep(0.5)
        
        return results
    
    async def _fetch_symbol_data(self, symbol: str) -> Optional[dict]:
        """Fetch ticker and orderbook for a single symbol"""
        ticker = await self.get_ticker(symbol)
        orderbook = await self.get_orderbook(symbol)
        
        if not ticker:
            return None
        
        return {
            "ticker": ticker.to_dict(),
            "orderbook": orderbook.to_dict() if orderbook else None,
            "source": "apex"
        }


async def collect_apex_data() -> Dict[str, dict]:
    """
    Main function to collect all Apex market data.
    
    Returns dict ready to be merged with Hyperliquid data.
    """
    async with ApexClient() as client:
        logger.info("[Apex] Starting data collection...")
        
        data = await client.get_all_market_data()
        
        logger.info(f"[Apex] Collected data for {len(data)} symbols")
        
        return data


# For testing
if __name__ == "__main__":
    async def main():
        async with ApexClient() as client:
            symbols = await client.get_symbols()
            print(f"Found {len(symbols)} symbols")
            
            ticker = await client.get_ticker("BTCUSDT")
            if ticker:
                print(f"\nBTC Ticker:")
                print(f"  Price: ${ticker.last_price:,.2f}")
                print(f"  OI: {ticker.open_interest:,.2f} BTC (${ticker.open_interest_usd:,.0f})")
                print(f"  Funding: {ticker.funding_rate * 100:.4f}%")
                print(f"  24h Change: {ticker.price_change_24h_pct:+.2f}%")
            
            orderbook = await client.get_orderbook("BTCUSDT")
            if orderbook:
                print(f"\nBTC Order Book:")
                print(f"  Spread: {orderbook.spread_pct:.4f}%")
                print(f"  Bid depth (1%): ${orderbook.bid_depth_1_pct:,.0f}")
                print(f"  Ask depth (1%): ${orderbook.ask_depth_1_pct:,.0f}")
                print(f"  Imbalance: {orderbook.imbalance_1_pct:+.2%}")
    
    asyncio.run(main())
