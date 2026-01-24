"""
Position Scanner Module
Scans wallets to collect position data and liquidation levels
"""
import asyncio
import json
from typing import Dict, List, Set, Optional
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, asdict

from .config import config
from .hyperliquid_api import HyperliquidAPI, Position


@dataclass
class LiquidationLevel:
    """A single liquidation level"""
    price: float
    size_usd: float  # Notional value at risk
    side: str  # "long" or "short"
    wallet: str
    coin: str
    leverage: float


@dataclass
class ScanResult:
    """Result of a position scan"""
    timestamp: datetime
    total_wallets_scanned: int
    total_positions_found: int
    total_long_exposure_usd: float
    total_short_exposure_usd: float
    liquidation_levels: List[LiquidationLevel]
    positions_by_coin: Dict[str, List[Position]]
    errors: int


class PositionScanner:
    """
    Scans wallets to collect all position data and liquidation levels.
    This is the core data collection component.
    """
    
    def __init__(self):
        self.api: Optional[HyperliquidAPI] = None
        self.last_scan_result: Optional[ScanResult] = None
        self._scan_lock = asyncio.Lock()
        
        # Cache for positions
        self.positions_cache: Dict[str, List[Position]] = {}
        self.liquidation_levels: List[LiquidationLevel] = []
        
    async def __aenter__(self):
        self.api = HyperliquidAPI()
        await self.api.__aenter__()
        return self
    
    async def __aexit__(self, *args):
        if self.api:
            await self.api.__aexit__(*args)
    
    async def scan_wallet(self, wallet: str) -> List[Position]:
        """Scan a single wallet for positions"""
        try:
            positions = await self.api.get_user_positions(wallet)
            return positions
        except Exception as e:
            return []
    
    async def scan_wallets(
        self, 
        wallets: Set[str], 
        progress_callback: callable = None
    ) -> ScanResult:
        """
        Scan multiple wallets for positions.
        Returns aggregated position data and liquidation levels.
        """
        async with self._scan_lock:
            start_time = datetime.now()
            
            all_positions: List[Position] = []
            positions_by_coin: Dict[str, List[Position]] = defaultdict(list)
            liquidation_levels: List[LiquidationLevel] = []
            errors = 0
            
            # Convert to list for progress tracking
            wallet_list = list(wallets)[:config.MAX_WALLETS_TO_TRACK]
            total = len(wallet_list)
            
            print(f"[PositionScanner] Scanning {total} wallets...")
            
            # Scan in batches to respect rate limits
            batch_size = config.API_REQUESTS_PER_SECOND
            for i in range(0, total, batch_size):
                batch = wallet_list[i:i + batch_size]
                
                # Create tasks for batch
                tasks = [self.scan_wallet(w) for w in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for wallet, result in zip(batch, results):
                    if isinstance(result, Exception):
                        errors += 1
                        continue
                    
                    for position in result:
                        # Filter small positions
                        if position.notional_value < config.MIN_POSITION_VALUE_USD:
                            continue
                        
                        all_positions.append(position)
                        positions_by_coin[position.coin].append(position)
                        
                        # Extract liquidation level if available
                        if position.liquidation_price is not None:
                            liquidation_levels.append(LiquidationLevel(
                                price=position.liquidation_price,
                                size_usd=position.notional_value,
                                side=position.side,
                                wallet=position.wallet,
                                coin=position.coin,
                                leverage=position.leverage
                            ))
                
                # Progress update
                scanned = min(i + batch_size, total)
                if progress_callback:
                    progress_callback(scanned, total)
                elif scanned % 100 == 0 or scanned == total:
                    print(f"  Progress: {scanned}/{total} wallets ({len(all_positions)} positions)")
                
                # Small delay between batches
                await asyncio.sleep(0.1)
            
            # Calculate totals
            total_long = sum(p.notional_value for p in all_positions if p.is_long)
            total_short = sum(p.notional_value for p in all_positions if not p.is_long)
            
            # Create result
            result = ScanResult(
                timestamp=datetime.now(),
                total_wallets_scanned=len(wallet_list),
                total_positions_found=len(all_positions),
                total_long_exposure_usd=total_long,
                total_short_exposure_usd=total_short,
                liquidation_levels=liquidation_levels,
                positions_by_coin=dict(positions_by_coin),
                errors=errors
            )
            
            # Cache results
            self.last_scan_result = result
            self.positions_cache = dict(positions_by_coin)
            self.liquidation_levels = liquidation_levels
            
            duration = (datetime.now() - start_time).total_seconds()
            print(f"[PositionScanner] Scan complete in {duration:.1f}s")
            print(f"  Positions: {len(all_positions)}")
            print(f"  Long exposure: ${total_long:,.0f}")
            print(f"  Short exposure: ${total_short:,.0f}")
            print(f"  Liquidation levels: {len(liquidation_levels)}")
            print(f"  Errors: {errors}")
            
            return result
    
    def get_liquidation_levels_for_coin(self, coin: str) -> List[LiquidationLevel]:
        """Get liquidation levels for a specific coin"""
        return [l for l in self.liquidation_levels if l.coin == coin]
    
    def get_positions_for_coin(self, coin: str) -> List[Position]:
        """Get all positions for a specific coin"""
        return self.positions_cache.get(coin, [])
    
    def save_to_file(self, filepath: str = None):
        """Save scan results to file"""
        filepath = filepath or config.POSITIONS_CACHE_FILE
        
        if not self.last_scan_result:
            print("[PositionScanner] No scan results to save")
            return
        
        data = {
            "timestamp": self.last_scan_result.timestamp.isoformat(),
            "stats": {
                "wallets_scanned": self.last_scan_result.total_wallets_scanned,
                "positions_found": self.last_scan_result.total_positions_found,
                "long_exposure_usd": self.last_scan_result.total_long_exposure_usd,
                "short_exposure_usd": self.last_scan_result.total_short_exposure_usd,
            },
            "liquidation_levels": [asdict(l) for l in self.liquidation_levels],
        }
        
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        
        print(f"[PositionScanner] Saved to {filepath}")
    
    def load_from_file(self, filepath: str = None):
        """Load cached scan results"""
        filepath = filepath or config.POSITIONS_CACHE_FILE
        
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            
            self.liquidation_levels = [
                LiquidationLevel(**l) for l in data.get("liquidation_levels", [])
            ]
            
            print(f"[PositionScanner] Loaded {len(self.liquidation_levels)} liquidation levels from {filepath}")
            
        except FileNotFoundError:
            print(f"[PositionScanner] No cache file found at {filepath}")
        except Exception as e:
            print(f"[PositionScanner] Error loading cache: {e}")


async def quick_scan_sample_wallets(sample_size: int = 50) -> ScanResult:
    """Quick scan of a sample of active wallets for testing"""
    from .wallet_discovery import WalletDiscovery
    
    # Discover some wallets
    discovery = WalletDiscovery()
    await discovery.backfill_from_recent_trades(config.ASSETS[:5])  # Top 5 assets
    
    wallets = list(discovery.active_wallets)[:sample_size]
    
    async with PositionScanner() as scanner:
        return await scanner.scan_wallets(set(wallets))
