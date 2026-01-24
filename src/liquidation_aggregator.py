"""
Liquidation Aggregator Module
Aggregates liquidation levels into clusters and builds the liquidation map
"""
import json
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict
import math

from .config import config
from .position_scanner import LiquidationLevel


@dataclass
class LiquidationCluster:
    """A cluster of liquidation levels at similar prices"""
    coin: str
    side: str  # "long" or "short"
    price_low: float
    price_high: float
    price_center: float
    total_size_usd: float
    position_count: int
    avg_leverage: float
    
    @property
    def price_range_percent(self) -> float:
        return ((self.price_high - self.price_low) / self.price_center) * 100


@dataclass
class LiquidationMap:
    """Complete liquidation map for an asset"""
    coin: str
    current_price: float
    long_liquidations: List[LiquidationCluster]  # Below current price
    short_liquidations: List[LiquidationCluster]  # Above current price
    total_long_at_risk_usd: float
    total_short_at_risk_usd: float
    nearest_long_cluster: Optional[LiquidationCluster]
    nearest_short_cluster: Optional[LiquidationCluster]
    
    def to_dict(self) -> Dict:
        return {
            "coin": self.coin,
            "current_price": self.current_price,
            "long_liquidations": [asdict(c) for c in self.long_liquidations],
            "short_liquidations": [asdict(c) for c in self.short_liquidations],
            "total_long_at_risk_usd": self.total_long_at_risk_usd,
            "total_short_at_risk_usd": self.total_short_at_risk_usd,
            "nearest_long_cluster": asdict(self.nearest_long_cluster) if self.nearest_long_cluster else None,
            "nearest_short_cluster": asdict(self.nearest_short_cluster) if self.nearest_short_cluster else None,
        }


class LiquidationAggregator:
    """
    Aggregates raw liquidation levels into tradeable clusters.
    This is where the magic happens - turning raw data into actionable intelligence.
    """
    
    def __init__(self, bucket_percent: float = None, min_cluster_size: float = None):
        self.bucket_percent = bucket_percent or config.PRICE_BUCKET_PERCENT
        self.min_cluster_size = min_cluster_size or config.MIN_CLUSTER_SIZE_USD
        self.liquidation_maps: Dict[str, LiquidationMap] = {}
        
    def _price_to_bucket(self, price: float, reference_price: float) -> int:
        """Convert price to bucket index"""
        if reference_price <= 0:
            return 0
        # Calculate percentage difference from reference
        pct_diff = ((price - reference_price) / reference_price) * 100
        # Convert to bucket index
        return int(pct_diff / self.bucket_percent)
    
    def _bucket_to_price_range(self, bucket: int, reference_price: float) -> Tuple[float, float]:
        """Convert bucket index back to price range"""
        pct_low = bucket * self.bucket_percent
        pct_high = (bucket + 1) * self.bucket_percent
        price_low = reference_price * (1 + pct_low / 100)
        price_high = reference_price * (1 + pct_high / 100)
        return price_low, price_high
    
    def aggregate_levels(
        self, 
        levels: List[LiquidationLevel], 
        current_price: float,
        coin: str
    ) -> LiquidationMap:
        """
        Aggregate liquidation levels into clusters for a single coin.
        """
        if not levels or current_price <= 0:
            return LiquidationMap(
                coin=coin,
                current_price=current_price,
                long_liquidations=[],
                short_liquidations=[],
                total_long_at_risk_usd=0,
                total_short_at_risk_usd=0,
                nearest_long_cluster=None,
                nearest_short_cluster=None,
            )
        
        # Separate long and short liquidations
        # Long positions get liquidated when price drops (their liq price is BELOW entry)
        # Short positions get liquidated when price rises (their liq price is ABOVE entry)
        long_levels = [l for l in levels if l.side == "long"]
        short_levels = [l for l in levels if l.side == "short"]
        
        # Aggregate into buckets
        long_clusters = self._aggregate_to_clusters(long_levels, current_price, "long")
        short_clusters = self._aggregate_to_clusters(short_levels, current_price, "short")
        
        # Sort by distance from current price
        long_clusters.sort(key=lambda c: current_price - c.price_center)  # Closest below first
        short_clusters.sort(key=lambda c: c.price_center - current_price)  # Closest above first
        
        # Calculate totals
        total_long = sum(c.total_size_usd for c in long_clusters)
        total_short = sum(c.total_size_usd for c in short_clusters)
        
        # Find nearest significant clusters
        nearest_long = None
        nearest_short = None
        
        for cluster in long_clusters:
            if cluster.total_size_usd >= self.min_cluster_size:
                nearest_long = cluster
                break
                
        for cluster in short_clusters:
            if cluster.total_size_usd >= self.min_cluster_size:
                nearest_short = cluster
                break
        
        return LiquidationMap(
            coin=coin,
            current_price=current_price,
            long_liquidations=long_clusters,
            short_liquidations=short_clusters,
            total_long_at_risk_usd=total_long,
            total_short_at_risk_usd=total_short,
            nearest_long_cluster=nearest_long,
            nearest_short_cluster=nearest_short,
        )
    
    def _aggregate_to_clusters(
        self, 
        levels: List[LiquidationLevel], 
        reference_price: float,
        side: str
    ) -> List[LiquidationCluster]:
        """Aggregate levels into price bucket clusters"""
        if not levels:
            return []
        
        # Group by bucket
        buckets: Dict[int, List[LiquidationLevel]] = defaultdict(list)
        for level in levels:
            bucket = self._price_to_bucket(level.price, reference_price)
            buckets[bucket].append(level)
        
        # Convert buckets to clusters
        clusters = []
        for bucket, bucket_levels in buckets.items():
            total_size = sum(l.size_usd for l in bucket_levels)
            
            # Skip tiny clusters
            if total_size < 10000:  # $10k minimum
                continue
            
            price_low, price_high = self._bucket_to_price_range(bucket, reference_price)
            avg_leverage = sum(l.leverage * l.size_usd for l in bucket_levels) / total_size
            
            clusters.append(LiquidationCluster(
                coin=bucket_levels[0].coin,
                side=side,
                price_low=price_low,
                price_high=price_high,
                price_center=(price_low + price_high) / 2,
                total_size_usd=total_size,
                position_count=len(bucket_levels),
                avg_leverage=avg_leverage,
            ))
        
        # Merge adjacent clusters if they're small
        clusters = self._merge_adjacent_clusters(clusters)
        
        return clusters
    
    def _merge_adjacent_clusters(
        self, 
        clusters: List[LiquidationCluster]
    ) -> List[LiquidationCluster]:
        """Merge adjacent small clusters"""
        if len(clusters) < 2:
            return clusters
        
        # Sort by price
        clusters.sort(key=lambda c: c.price_center)
        
        merged = []
        current = clusters[0]
        
        for next_cluster in clusters[1:]:
            # Check if clusters should be merged
            gap_percent = ((next_cluster.price_low - current.price_high) / current.price_center) * 100
            
            should_merge = (
                gap_percent < config.CLUSTER_MERGE_PERCENT and
                current.total_size_usd < self.min_cluster_size and
                next_cluster.total_size_usd < self.min_cluster_size
            )
            
            if should_merge:
                # Merge clusters
                total_size = current.total_size_usd + next_cluster.total_size_usd
                current = LiquidationCluster(
                    coin=current.coin,
                    side=current.side,
                    price_low=current.price_low,
                    price_high=next_cluster.price_high,
                    price_center=(current.price_low + next_cluster.price_high) / 2,
                    total_size_usd=total_size,
                    position_count=current.position_count + next_cluster.position_count,
                    avg_leverage=(
                        current.avg_leverage * current.total_size_usd +
                        next_cluster.avg_leverage * next_cluster.total_size_usd
                    ) / total_size,
                )
            else:
                merged.append(current)
                current = next_cluster
        
        merged.append(current)
        return merged
    
    def build_maps_from_levels(
        self, 
        all_levels: List[LiquidationLevel],
        prices: Dict[str, float]
    ) -> Dict[str, LiquidationMap]:
        """Build liquidation maps for all coins"""
        # Group levels by coin
        levels_by_coin: Dict[str, List[LiquidationLevel]] = defaultdict(list)
        for level in all_levels:
            levels_by_coin[level.coin].append(level)
        
        # Build map for each coin
        self.liquidation_maps = {}
        for coin, levels in levels_by_coin.items():
            price = prices.get(coin, 0)
            if price > 0:
                self.liquidation_maps[coin] = self.aggregate_levels(levels, price, coin)
        
        return self.liquidation_maps
    
    def get_trading_signals(self, prices: Dict[str, float]) -> List[Dict]:
        """
        Generate trading signals based on liquidation map analysis.
        Returns potential trade setups.
        """
        signals = []
        
        for coin, liq_map in self.liquidation_maps.items():
            current_price = prices.get(coin, liq_map.current_price)
            
            # Check for nearby short liquidation cluster (potential long entry)
            if liq_map.nearest_short_cluster:
                cluster = liq_map.nearest_short_cluster
                distance_pct = ((cluster.price_center - current_price) / current_price) * 100
                
                if 0.5 <= distance_pct <= 3.0 and cluster.total_size_usd >= config.ALERT_CLUSTER_SIZE_USD:
                    signals.append({
                        "coin": coin,
                        "signal": "LONG",
                        "reason": f"Short liquidation cluster ${cluster.total_size_usd/1e6:.1f}M at {cluster.price_center:.2f}",
                        "target": cluster.price_center,
                        "distance_percent": distance_pct,
                        "cluster_size_usd": cluster.total_size_usd,
                        "current_price": current_price,
                    })
            
            # Check for nearby long liquidation cluster (potential short entry)
            if liq_map.nearest_long_cluster:
                cluster = liq_map.nearest_long_cluster
                distance_pct = ((current_price - cluster.price_center) / current_price) * 100
                
                if 0.5 <= distance_pct <= 3.0 and cluster.total_size_usd >= config.ALERT_CLUSTER_SIZE_USD:
                    signals.append({
                        "coin": coin,
                        "signal": "SHORT",
                        "reason": f"Long liquidation cluster ${cluster.total_size_usd/1e6:.1f}M at {cluster.price_center:.2f}",
                        "target": cluster.price_center,
                        "distance_percent": distance_pct,
                        "cluster_size_usd": cluster.total_size_usd,
                        "current_price": current_price,
                    })
        
        # Sort by cluster size (bigger = more significant)
        signals.sort(key=lambda s: s["cluster_size_usd"], reverse=True)
        
        return signals
    
    def save_maps(self, filepath: str = None):
        """Save liquidation maps to file"""
        filepath = filepath or config.LIQUIDATION_MAP_FILE
        
        data = {
            coin: liq_map.to_dict() 
            for coin, liq_map in self.liquidation_maps.items()
        }
        
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        
        print(f"[LiquidationAggregator] Saved maps for {len(data)} coins to {filepath}")
    
    def print_summary(self):
        """Print a summary of liquidation maps"""
        print("\n" + "="*60)
        print("LIQUIDATION MAP SUMMARY")
        print("="*60)
        
        for coin, liq_map in sorted(self.liquidation_maps.items()):
            if liq_map.total_long_at_risk_usd < 100000 and liq_map.total_short_at_risk_usd < 100000:
                continue  # Skip coins with minimal liquidity
                
            print(f"\n{coin} @ ${liq_map.current_price:,.2f}")
            print(f"  Long liq (below): ${liq_map.total_long_at_risk_usd/1e6:.2f}M across {len(liq_map.long_liquidations)} clusters")
            print(f"  Short liq (above): ${liq_map.total_short_at_risk_usd/1e6:.2f}M across {len(liq_map.short_liquidations)} clusters")
            
            if liq_map.nearest_long_cluster:
                c = liq_map.nearest_long_cluster
                dist = ((liq_map.current_price - c.price_center) / liq_map.current_price) * 100
                print(f"  ⬇ Nearest LONG liq: ${c.total_size_usd/1e6:.2f}M @ ${c.price_center:,.2f} ({dist:.1f}% away)")
            
            if liq_map.nearest_short_cluster:
                c = liq_map.nearest_short_cluster
                dist = ((c.price_center - liq_map.current_price) / liq_map.current_price) * 100
                print(f"  ⬆ Nearest SHORT liq: ${c.total_size_usd/1e6:.2f}M @ ${c.price_center:,.2f} ({dist:.1f}% away)")
