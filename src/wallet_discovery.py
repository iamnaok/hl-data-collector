"""
Wallet Discovery Module
Discovers active trading wallets from trade stream and API queries
"""
import asyncio
import json
import websockets
from typing import Set, Dict, Callable, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import aiohttp

from .config import config


class WalletDiscovery:
    """
    Discovers and tracks active wallets on Hyperliquid.
    Uses multiple methods:
    1. WebSocket trade stream - captures wallets from real-time trades
    2. Historical trade queries - backfills from recent trades
    """
    
    def __init__(self):
        self.active_wallets: Set[str] = set()
        self.wallet_last_seen: Dict[str, datetime] = {}
        self.wallet_trade_count: Dict[str, int] = defaultdict(int)
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._callbacks: list[Callable] = []
        
    def add_callback(self, callback: Callable[[str], None]):
        """Add callback for new wallet discovery"""
        self._callbacks.append(callback)
        
    async def _notify_new_wallet(self, wallet: str):
        """Notify callbacks of new wallet"""
        for callback in self._callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(wallet)
                else:
                    callback(wallet)
            except Exception as e:
                print(f"Callback error: {e}")
    
    def add_wallet(self, wallet: str) -> bool:
        """Add a wallet to tracking, returns True if new"""
        wallet = wallet.lower()
        is_new = wallet not in self.active_wallets
        
        self.active_wallets.add(wallet)
        self.wallet_last_seen[wallet] = datetime.now()
        self.wallet_trade_count[wallet] += 1
        
        return is_new
    
    async def discover_from_trades(self, trades: list) -> int:
        """Extract wallets from trade data"""
        new_count = 0
        for trade in trades:
            users = trade.get("users", [])
            for user in users:
                if self.add_wallet(user):
                    new_count += 1
                    await self._notify_new_wallet(user)
        return new_count
    
    async def backfill_from_recent_trades(self, coins: list[str] = None):
        """Backfill wallets from recent trades for each coin"""
        if coins is None:
            coins = config.ASSETS
            
        print(f"[WalletDiscovery] Backfilling from {len(coins)} assets...")
        
        async with aiohttp.ClientSession() as session:
            for coin in coins:
                try:
                    async with session.post(
                        f"{config.API_URL}/info",
                        json={"type": "recentTrades", "coin": coin},
                        headers={"Content-Type": "application/json"}
                    ) as response:
                        if response.status == 200:
                            trades = await response.json()
                            new_count = await self.discover_from_trades(trades)
                            if new_count > 0:
                                print(f"  {coin}: +{new_count} wallets")
                        
                        # Rate limiting
                        await asyncio.sleep(0.1)
                        
                except Exception as e:
                    print(f"  {coin}: Error - {e}")
        
        print(f"[WalletDiscovery] Total wallets: {len(self.active_wallets)}")
    
    async def start_websocket_discovery(self):
        """Start WebSocket listener for real-time trade discovery"""
        self._running = True
        
        while self._running:
            try:
                async with websockets.connect(config.WS_URL) as ws:
                    self._ws = ws
                    
                    # Subscribe to trades for tracked assets
                    for coin in config.ASSETS:
                        subscribe_msg = {
                            "method": "subscribe",
                            "subscription": {"type": "trades", "coin": coin}
                        }
                        await ws.send(json.dumps(subscribe_msg))
                    
                    print(f"[WalletDiscovery] WebSocket connected, subscribed to {len(config.ASSETS)} assets")
                    
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            
                            if data.get("channel") == "trades":
                                trades = data.get("data", [])
                                new_count = await self.discover_from_trades(trades)
                                
                                if new_count > 0 and len(self.active_wallets) % 100 == 0:
                                    print(f"[WalletDiscovery] {len(self.active_wallets)} wallets tracked")
                                    
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                print(f"[WalletDiscovery] WebSocket error: {e}")
                if self._running:
                    print(f"[WalletDiscovery] Reconnecting in {config.WS_RECONNECT_DELAY_SECONDS}s...")
                    await asyncio.sleep(config.WS_RECONNECT_DELAY_SECONDS)
    
    def stop(self):
        """Stop WebSocket discovery"""
        self._running = False
        if self._ws:
            asyncio.create_task(self._ws.close())
    
    def get_wallets(self, min_trades: int = 1, max_age_hours: int = 24) -> Set[str]:
        """Get wallets filtered by activity"""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        return {
            wallet for wallet in self.active_wallets
            if self.wallet_trade_count[wallet] >= min_trades
            and self.wallet_last_seen.get(wallet, datetime.min) > cutoff
        }
    
    def get_stats(self) -> Dict:
        """Get discovery statistics"""
        return {
            "total_wallets": len(self.active_wallets),
            "wallets_last_hour": len(self.get_wallets(max_age_hours=1)),
            "wallets_last_24h": len(self.get_wallets(max_age_hours=24)),
            "top_traders": sorted(
                self.wallet_trade_count.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
        }
    
    def save_to_file(self, filepath: str = None):
        """Save wallet list to file"""
        filepath = filepath or config.WALLET_CACHE_FILE
        data = {
            "wallets": list(self.active_wallets),
            "last_seen": {k: v.isoformat() for k, v in self.wallet_last_seen.items()},
            "trade_counts": dict(self.wallet_trade_count),
            "saved_at": datetime.now().isoformat()
        }
        with open(filepath, "w") as f:
            json.dump(data, f)
        print(f"[WalletDiscovery] Saved {len(self.active_wallets)} wallets to {filepath}")
    
    def load_from_file(self, filepath: str = None):
        """Load wallet list from file"""
        filepath = filepath or config.WALLET_CACHE_FILE
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            
            self.active_wallets = set(data.get("wallets", []))
            self.wallet_last_seen = {
                k: datetime.fromisoformat(v) 
                for k, v in data.get("last_seen", {}).items()
            }
            self.wallet_trade_count = defaultdict(int, data.get("trade_counts", {}))
            
            print(f"[WalletDiscovery] Loaded {len(self.active_wallets)} wallets from {filepath}")
            
        except FileNotFoundError:
            print(f"[WalletDiscovery] No cache file found at {filepath}")
        except Exception as e:
            print(f"[WalletDiscovery] Error loading cache: {e}")
