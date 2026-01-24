"""
Hyperliquid API wrapper for liquidation tracking
"""
import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import time

from .config import config


@dataclass
class Position:
    """Represents a trading position"""
    wallet: str
    coin: str
    size: float  # Positive = long, negative = short
    entry_price: float
    liquidation_price: Optional[float]
    leverage: float
    notional_value: float
    unrealized_pnl: float
    margin_used: float
    
    @property
    def is_long(self) -> bool:
        return self.size > 0
    
    @property
    def side(self) -> str:
        return "long" if self.is_long else "short"


@dataclass 
class AssetInfo:
    """Asset metadata"""
    name: str
    max_leverage: int
    sz_decimals: int
    mark_price: float
    open_interest: float
    funding_rate: float


class HyperliquidAPI:
    """Async API client for Hyperliquid"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter = asyncio.Semaphore(config.API_REQUESTS_PER_SECOND)
        self._last_request_time = 0
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
            
    async def _request(self, data: Dict) -> Any:
        """Make rate-limited API request"""
        async with self._rate_limiter:
            # Ensure minimum delay between requests
            elapsed = time.time() - self._last_request_time
            if elapsed < 0.1:  # 100ms minimum between requests
                await asyncio.sleep(0.1 - elapsed)
            
            async with self.session.post(
                f"{config.API_URL}/info",
                json=data,
                headers={"Content-Type": "application/json"}
            ) as response:
                self._last_request_time = time.time()
                if response.status == 200:
                    return await response.json()
                else:
                    text = await response.text()
                    raise Exception(f"API error {response.status}: {text}")
    
    async def get_meta(self) -> Dict:
        """Get exchange metadata (assets, leverage limits, etc.)"""
        return await self._request({"type": "meta"})
    
    async def get_all_mids(self) -> Dict[str, str]:
        """Get all mid prices"""
        return await self._request({"type": "allMids"})
    
    async def get_meta_and_asset_ctxs(self) -> tuple:
        """Get metadata and asset contexts (OI, funding, etc.)"""
        data = await self._request({"type": "metaAndAssetCtxs"})
        return data[0], data[1]  # meta, asset_contexts
    
    async def get_clearinghouse_state(self, wallet: str) -> Dict:
        """Get a wallet's positions and margin state"""
        return await self._request({
            "type": "clearinghouseState",
            "user": wallet
        })
    
    async def get_user_positions(self, wallet: str) -> List[Position]:
        """Get parsed positions for a wallet"""
        try:
            state = await self.get_clearinghouse_state(wallet)
            positions = []
            
            for asset_pos in state.get("assetPositions", []):
                pos = asset_pos.get("position", {})
                if not pos:
                    continue
                    
                size = float(pos.get("szi", 0))
                if abs(size) < 0.0001:  # Skip dust positions
                    continue
                
                # Parse leverage info
                leverage_info = pos.get("leverage", {})
                leverage = float(leverage_info.get("value", 1))
                
                # Parse liquidation price (can be null for well-margined positions)
                liq_px = pos.get("liquidationPx")
                if liq_px and liq_px != "null":
                    liq_px = float(liq_px)
                else:
                    liq_px = None
                
                positions.append(Position(
                    wallet=wallet,
                    coin=pos.get("coin", ""),
                    size=size,
                    entry_price=float(pos.get("entryPx", 0)),
                    liquidation_price=liq_px,
                    leverage=leverage,
                    notional_value=abs(float(pos.get("positionValue", 0))),
                    unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
                    margin_used=float(pos.get("marginUsed", 0))
                ))
            
            return positions
            
        except Exception as e:
            # Wallet might not exist or have no positions
            return []
    
    async def get_recent_trades(self, coin: str, limit: int = 100) -> List[Dict]:
        """Get recent trades for an asset"""
        return await self._request({
            "type": "recentTrades",
            "coin": coin
        })
    
    async def get_asset_info(self) -> Dict[str, AssetInfo]:
        """Get current state of all assets"""
        meta, contexts = await self.get_meta_and_asset_ctxs()
        universe = meta.get("universe", [])
        
        assets = {}
        for i, asset_meta in enumerate(universe):
            if i >= len(contexts):
                break
                
            ctx = contexts[i]
            name = asset_meta.get("name", "")
            
            # Skip delisted assets
            if asset_meta.get("isDelisted"):
                continue
            
            assets[name] = AssetInfo(
                name=name,
                max_leverage=asset_meta.get("maxLeverage", 1),
                sz_decimals=asset_meta.get("szDecimals", 0),
                mark_price=float(ctx.get("markPx", 0)),
                open_interest=float(ctx.get("openInterest", 0)),
                funding_rate=float(ctx.get("funding", 0))
            )
        
        return assets


# Convenience function for one-off requests
async def get_wallet_positions(wallet: str) -> List[Position]:
    """Get positions for a single wallet"""
    async with HyperliquidAPI() as api:
        return await api.get_user_positions(wallet)


async def get_current_prices() -> Dict[str, float]:
    """Get current mid prices for all assets"""
    async with HyperliquidAPI() as api:
        mids = await api.get_all_mids()
        return {k: float(v) for k, v in mids.items() if not k.startswith("@")}
