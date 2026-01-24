"""
Configuration for Hyperliquid Data Collector
"""
from dataclasses import dataclass, field
from typing import List

@dataclass
class Config:
    # Hyperliquid API endpoints
    API_URL: str = "https://api.hyperliquid.xyz"
    WS_URL: str = "wss://api.hyperliquid.xyz/ws"
    
    # Scanning settings
    SCAN_INTERVAL_SECONDS: int = 300  # 5 minutes
    MAX_WALLETS_TO_TRACK: int = 5000
    MIN_POSITION_VALUE_USD: float = 1000
    
    # Liquidation map settings
    PRICE_BUCKET_PERCENT: float = 0.1
    MIN_CLUSTER_SIZE_USD: float = 100_000  # Lower threshold for data collection
    CLUSTER_MERGE_PERCENT: float = 0.5
    
    # Assets to track
    ASSETS: List[str] = field(default_factory=lambda: [
        "BTC", "ETH", "SOL", "ARB", "DOGE", "SUI", "AVAX", 
        "LINK", "OP", "APT", "INJ", "TIA", "SEI", "WLD",
        "HYPE", "XRP", "FARTCOIN", "PEPE", "WIF", "BONK"
    ])
    
    # Rate limiting
    API_REQUESTS_PER_SECOND: int = 10
    
    # Data storage
    DATA_DIR: str = "data"
    WALLET_CACHE_FILE: str = "data/wallets.json"
    POSITIONS_CACHE_FILE: str = "data/positions.json"
    LIQUIDATION_MAP_FILE: str = "data/liquidation_map.json"
    MARKET_DATA_FILE: str = "data/market_data.json"
    DATABASE_FILE: str = "data/historical.db"
    
    # API server
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8001


config = Config()
