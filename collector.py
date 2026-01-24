#!/usr/bin/env python3
"""
Data Collector - Continuously collects liquidation data for backtesting

This script runs in the background and:
1. Scans wallets periodically
2. Stores snapshots to the historical database
3. Tracks price movements
4. Records when liquidation clusters are hit

Usage:
    python collector.py                    # Run once and exit
    python collector.py --continuous       # Run continuously (every 5 min)
    python collector.py --interval 60      # Custom interval in seconds
"""
import asyncio
import argparse
import json
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import config
from src.hyperliquid_api import get_current_prices
from src.wallet_discovery import WalletDiscovery
from src.position_scanner import PositionScanner
from src.liquidation_aggregator import LiquidationAggregator
from src.historical_storage import HistoricalStorage


async def run_collection_cycle(storage: HistoricalStorage, verbose: bool = True):
    """Run a single data collection cycle"""
    timestamp = datetime.now(timezone.utc)
    
    if verbose:
        print(f"\n[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] Starting collection cycle...")
    
    # Load wallets
    discovery = WalletDiscovery()
    discovery.load_from_file()
    
    if len(discovery.active_wallets) < 50:
        if verbose:
            print("  Discovering wallets...")
        await discovery.backfill_from_recent_trades(config.ASSETS[:10])
        discovery.save_to_file()
    
    wallets = discovery.get_wallets(max_age_hours=24)
    
    if verbose:
        print(f"  Scanning {len(wallets)} wallets...")
    
    # Scan positions
    async with PositionScanner() as scanner:
        scan_result = await scanner.scan_wallets(wallets)
    
    # Get prices
    prices = await get_current_prices()
    
    # Build liquidation maps
    aggregator = LiquidationAggregator()
    liq_maps = aggregator.build_maps_from_levels(scan_result.liquidation_levels, prices)
    
    # Convert to dict format for storage
    liq_maps_dict = {}
    for coin, liq_map in liq_maps.items():
        liq_maps_dict[coin] = liq_map.to_dict()
    
    # Store snapshot
    storage.store_snapshot(liq_maps_dict, timestamp)
    storage.store_prices(prices, timestamp)
    
    # Save current state to JSON files too
    aggregator.save_maps()
    
    if verbose:
        stats = storage.get_stats()
        print(f"  Stored snapshot: {len(liq_maps)} coins")
        print(f"  Total snapshots in DB: {stats['snapshot_count']}")
        print(f"  DB size: {stats['db_size_mb']:.2f} MB")
    
    return len(liq_maps)


async def run_continuous(interval_seconds: int = 300, verbose: bool = True):
    """Run continuous data collection"""
    storage = HistoricalStorage()
    
    print(f"Starting continuous data collection (interval: {interval_seconds}s)")
    print("Press Ctrl+C to stop\n")
    
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            coins_collected = await run_collection_cycle(storage, verbose)
            
            if verbose:
                print(f"  Cycle #{cycle_count} complete. Next in {interval_seconds}s...")
            
            await asyncio.sleep(interval_seconds)
            
        except KeyboardInterrupt:
            print("\nStopping collector...")
            break
        except Exception as e:
            print(f"  Error: {e}")
            await asyncio.sleep(30)  # Wait 30s on error


async def run_once(verbose: bool = True):
    """Run a single collection and store to database"""
    storage = HistoricalStorage()
    await run_collection_cycle(storage, verbose)
    
    stats = storage.get_stats()
    print(f"\nDatabase stats:")
    print(f"  Snapshots: {stats['snapshot_count']}")
    print(f"  Price records: {stats['price_count']}")
    print(f"  Coins tracked: {stats['coins_tracked']}")
    print(f"  DB size: {stats['db_size_mb']:.2f} MB")


def main():
    parser = argparse.ArgumentParser(description="Hyperliquid Liquidation Data Collector")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=300, help="Collection interval in seconds (default: 300)")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    
    args = parser.parse_args()
    verbose = not args.quiet
    
    if args.continuous:
        asyncio.run(run_continuous(args.interval, verbose))
    else:
        asyncio.run(run_once(verbose))


if __name__ == "__main__":
    main()
