"""
Market Data Module
Fetches open interest, volume, funding rates, and order book liquidity from Hyperliquid
"""
import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from .config import config


@dataclass
class OrderBookLevel:
    price: float
    size: float
    num_orders: int


@dataclass
class OrderBookLiquidity:
    """Liquidity metrics from order book"""
    coin: str
    timestamp: datetime
    
    # Spread
    best_bid: float
    best_ask: float
    spread_percent: float
    
    # Depth at various levels (cumulative size in USD)
    bid_depth_0_5_pct: float   # Liquidity within 0.5% of mid
    ask_depth_0_5_pct: float
    bid_depth_1_pct: float     # Liquidity within 1% of mid
    ask_depth_1_pct: float
    bid_depth_2_pct: float     # Liquidity within 2% of mid
    ask_depth_2_pct: float
    
    # Imbalance
    imbalance_0_5_pct: float   # Positive = more bids, negative = more asks
    imbalance_1_pct: float
    
    def to_dict(self) -> Dict:
        return {
            'coin': self.coin,
            'timestamp': self.timestamp.isoformat(),
            'best_bid': self.best_bid,
            'best_ask': self.best_ask,
            'spread_percent': self.spread_percent,
            'bid_depth_0_5_pct': self.bid_depth_0_5_pct,
            'ask_depth_0_5_pct': self.ask_depth_0_5_pct,
            'bid_depth_1_pct': self.bid_depth_1_pct,
            'ask_depth_1_pct': self.ask_depth_1_pct,
            'bid_depth_2_pct': self.bid_depth_2_pct,
            'ask_depth_2_pct': self.ask_depth_2_pct,
            'imbalance_0_5_pct': self.imbalance_0_5_pct,
            'imbalance_1_pct': self.imbalance_1_pct,
        }


@dataclass
class AssetMarketData:
    """Market data for a single asset"""
    coin: str
    timestamp: datetime
    
    # Price data
    mark_price: float
    oracle_price: float
    mid_price: float
    
    # Open Interest
    open_interest: float         # In base asset (e.g., BTC)
    open_interest_usd: float     # In USD
    
    # Volume
    volume_24h_usd: float        # 24h notional volume
    volume_24h_base: float       # 24h base volume
    
    # Funding
    funding_rate: float          # Current funding rate (hourly)
    funding_rate_annualized: float
    premium: float               # Premium over oracle
    
    # Price change
    prev_day_price: float
    price_change_24h_pct: float
    
    # Liquidity (optional, filled if order book fetched)
    liquidity: Optional[OrderBookLiquidity] = None
    
    def to_dict(self) -> Dict:
        result = {
            'coin': self.coin,
            'timestamp': self.timestamp.isoformat(),
            'mark_price': self.mark_price,
            'oracle_price': self.oracle_price,
            'mid_price': self.mid_price,
            'open_interest': self.open_interest,
            'open_interest_usd': self.open_interest_usd,
            'volume_24h_usd': self.volume_24h_usd,
            'volume_24h_base': self.volume_24h_base,
            'funding_rate': self.funding_rate,
            'funding_rate_annualized': self.funding_rate_annualized,
            'premium': self.premium,
            'prev_day_price': self.prev_day_price,
            'price_change_24h_pct': self.price_change_24h_pct,
        }
        if self.liquidity:
            result['liquidity'] = self.liquidity.to_dict()
        return result


class MarketDataFetcher:
    """Fetches market data from Hyperliquid"""
    
    def __init__(self):
        self.base_url = config.API_URL
        self._session: Optional[aiohttp.ClientSession] = None
        self._asset_index_map: Dict[str, int] = {}
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
    
    async def _post(self, payload: Dict) -> Dict:
        """Make POST request to Hyperliquid API"""
        async with self._session.post(
            f"{self.base_url}/info",
            json=payload,
            headers={"Content-Type": "application/json"}
        ) as response:
            return await response.json()
    
    async def fetch_all_market_data(self, include_liquidity: bool = False) -> Dict[str, AssetMarketData]:
        """Fetch market data for all assets"""
        timestamp = datetime.utcnow()
        
        # Fetch meta and asset contexts
        data = await self._post({"type": "metaAndAssetCtxs"})
        
        if not isinstance(data, list) or len(data) < 2:
            return {}
        
        meta = data[0]
        asset_ctxs = data[1]
        universe = meta.get('universe', [])
        
        # Build index map
        self._asset_index_map = {
            asset['name']: idx for idx, asset in enumerate(universe)
        }
        
        results = {}
        
        for idx, ctx in enumerate(asset_ctxs):
            if idx >= len(universe):
                break
            
            coin = universe[idx]['name']
            
            try:
                mark_price = float(ctx.get('markPx', 0))
                oracle_price = float(ctx.get('oraclePx', 0))
                mid_price = float(ctx.get('midPx', 0))
                open_interest = float(ctx.get('openInterest', 0))
                volume_24h_usd = float(ctx.get('dayNtlVlm', 0))
                volume_24h_base = float(ctx.get('dayBaseVlm', 0))
                funding_rate = float(ctx.get('funding', 0))
                premium = float(ctx.get('premium', 0))
                prev_day_price = float(ctx.get('prevDayPx', 0))
                
                # Calculate derived metrics
                open_interest_usd = open_interest * mark_price
                funding_rate_annualized = funding_rate * 24 * 365 * 100  # Convert to annual %
                
                price_change_24h_pct = 0
                if prev_day_price > 0:
                    price_change_24h_pct = ((mark_price - prev_day_price) / prev_day_price) * 100
                
                results[coin] = AssetMarketData(
                    coin=coin,
                    timestamp=timestamp,
                    mark_price=mark_price,
                    oracle_price=oracle_price,
                    mid_price=mid_price,
                    open_interest=open_interest,
                    open_interest_usd=open_interest_usd,
                    volume_24h_usd=volume_24h_usd,
                    volume_24h_base=volume_24h_base,
                    funding_rate=funding_rate,
                    funding_rate_annualized=funding_rate_annualized,
                    premium=premium,
                    prev_day_price=prev_day_price,
                    price_change_24h_pct=price_change_24h_pct,
                )
            except (ValueError, TypeError) as e:
                continue
        
        # Optionally fetch liquidity for top assets
        if include_liquidity:
            top_coins = sorted(
                results.keys(),
                key=lambda c: results[c].open_interest_usd,
                reverse=True
            )[:20]  # Top 20 by OI
            
            liquidity_data = await self.fetch_liquidity_batch(top_coins)
            for coin, liquidity in liquidity_data.items():
                if coin in results:
                    results[coin].liquidity = liquidity
        
        return results
    
    async def fetch_order_book(self, coin: str) -> Optional[Tuple[List[OrderBookLevel], List[OrderBookLevel]]]:
        """Fetch order book for a single coin"""
        try:
            data = await self._post({"type": "l2Book", "coin": coin})
            levels = data.get('levels', [[], []])
            
            bids = []
            for level in levels[0]:
                bids.append(OrderBookLevel(
                    price=float(level['px']),
                    size=float(level['sz']),
                    num_orders=int(level['n'])
                ))
            
            asks = []
            for level in levels[1]:
                asks.append(OrderBookLevel(
                    price=float(level['px']),
                    size=float(level['sz']),
                    num_orders=int(level['n'])
                ))
            
            return bids, asks
        except Exception as e:
            return None
    
    async def fetch_liquidity(self, coin: str, mark_price: float = None) -> Optional[OrderBookLiquidity]:
        """Calculate liquidity metrics for a coin"""
        book = await self.fetch_order_book(coin)
        if not book:
            return None
        
        bids, asks = book
        if not bids or not asks:
            return None
        
        timestamp = datetime.utcnow()
        best_bid = bids[0].price
        best_ask = asks[0].price
        mid_price = (best_bid + best_ask) / 2
        
        if mark_price:
            mid_price = mark_price
        
        spread_percent = ((best_ask - best_bid) / mid_price) * 100
        
        # Calculate depth at various levels
        def calc_depth(levels: List[OrderBookLevel], mid: float, pct: float, is_bid: bool) -> float:
            """Calculate cumulative depth within pct% of mid price"""
            total = 0
            threshold = mid * (1 - pct/100) if is_bid else mid * (1 + pct/100)
            
            for level in levels:
                if is_bid and level.price >= threshold:
                    total += level.size * level.price
                elif not is_bid and level.price <= threshold:
                    total += level.size * level.price
            
            return total
        
        bid_0_5 = calc_depth(bids, mid_price, 0.5, True)
        ask_0_5 = calc_depth(asks, mid_price, 0.5, False)
        bid_1 = calc_depth(bids, mid_price, 1.0, True)
        ask_1 = calc_depth(asks, mid_price, 1.0, False)
        bid_2 = calc_depth(bids, mid_price, 2.0, True)
        ask_2 = calc_depth(asks, mid_price, 2.0, False)
        
        # Imbalance: positive = more bids (bullish), negative = more asks (bearish)
        imbalance_0_5 = 0
        if bid_0_5 + ask_0_5 > 0:
            imbalance_0_5 = (bid_0_5 - ask_0_5) / (bid_0_5 + ask_0_5)
        
        imbalance_1 = 0
        if bid_1 + ask_1 > 0:
            imbalance_1 = (bid_1 - ask_1) / (bid_1 + ask_1)
        
        return OrderBookLiquidity(
            coin=coin,
            timestamp=timestamp,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_percent=spread_percent,
            bid_depth_0_5_pct=bid_0_5,
            ask_depth_0_5_pct=ask_0_5,
            bid_depth_1_pct=bid_1,
            ask_depth_1_pct=ask_1,
            bid_depth_2_pct=bid_2,
            ask_depth_2_pct=ask_2,
            imbalance_0_5_pct=imbalance_0_5,
            imbalance_1_pct=imbalance_1,
        )
    
    async def fetch_liquidity_batch(self, coins: List[str]) -> Dict[str, OrderBookLiquidity]:
        """Fetch liquidity for multiple coins"""
        results = {}
        
        for coin in coins:
            liquidity = await self.fetch_liquidity(coin)
            if liquidity:
                results[coin] = liquidity
            await asyncio.sleep(0.1)  # Rate limiting
        
        return results


async def get_market_data(include_liquidity: bool = False) -> Dict[str, AssetMarketData]:
    """Convenience function to fetch all market data"""
    async with MarketDataFetcher() as fetcher:
        return await fetcher.fetch_all_market_data(include_liquidity)


async def get_top_oi_assets(limit: int = 20) -> List[Tuple[str, float]]:
    """Get top assets by open interest"""
    data = await get_market_data()
    sorted_assets = sorted(
        data.items(),
        key=lambda x: x[1].open_interest_usd,
        reverse=True
    )
    return [(coin, d.open_interest_usd) for coin, d in sorted_assets[:limit]]
