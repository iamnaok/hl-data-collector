"""
Historical Data Storage Module
Stores liquidation snapshots over time for backtesting and analysis

Compression: clusters_json is compressed with zlib to reduce storage by ~70%
"""
import json
import os
import zlib
import base64
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import sqlite3
from pathlib import Path

from .config import config


# Compression helpers
COMPRESSION_MARKER = "ZLIB:"  # Prefix to identify compressed data


def compress_json(data: dict) -> str:
    """Compress JSON data with zlib, return base64-encoded string"""
    json_bytes = json.dumps(data, separators=(',', ':')).encode('utf-8')
    compressed = zlib.compress(json_bytes, level=6)
    encoded = base64.b64encode(compressed).decode('ascii')
    return COMPRESSION_MARKER + encoded


def decompress_json(data: str) -> dict:
    """Decompress zlib-compressed JSON, handles both compressed and uncompressed"""
    if data is None:
        return {}
    
    # Check if compressed
    if data.startswith(COMPRESSION_MARKER):
        try:
            encoded = data[len(COMPRESSION_MARKER):]
            compressed = base64.b64decode(encoded)
            json_bytes = zlib.decompress(compressed)
            return json.loads(json_bytes.decode('utf-8'))
        except Exception as e:
            print(f"Decompression error: {e}")
            return {}
    
    # Not compressed, parse as regular JSON
    try:
        return json.loads(data)
    except:
        return {}


@dataclass
class LiquidationSnapshot:
    """A point-in-time snapshot of liquidation data"""
    timestamp: datetime
    coin: str
    current_price: float
    total_long_at_risk: float
    total_short_at_risk: float
    nearest_long_price: Optional[float]
    nearest_long_size: Optional[float]
    nearest_short_price: Optional[float]
    nearest_short_size: Optional[float]
    long_clusters: List[Dict]
    short_clusters: List[Dict]


class HistoricalStorage:
    """
    SQLite-based storage for historical liquidation data.
    Enables backtesting and trend analysis.
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.path.join(config.DATA_DIR, "historical.db")
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    current_price REAL NOT NULL,
                    total_long_at_risk REAL,
                    total_short_at_risk REAL,
                    nearest_long_price REAL,
                    nearest_long_size REAL,
                    nearest_short_price REAL,
                    nearest_short_size REAL,
                    clusters_json TEXT,
                    UNIQUE(timestamp, coin)
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_coin_time 
                ON snapshots(coin, timestamp)
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    price REAL NOT NULL,
                    UNIQUE(timestamp, coin)
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_coin_time 
                ON price_history(coin, timestamp)
            """)
            
            # Table for tracking liquidation events
            conn.execute("""
                CREATE TABLE IF NOT EXISTS liquidation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    price REAL NOT NULL,
                    side TEXT NOT NULL,
                    cluster_size REAL,
                    price_move_percent REAL,
                    time_to_hit_minutes REAL
                )
            """)
            
            conn.commit()
    
    def store_snapshot(self, liq_map: Dict, timestamp: datetime = None):
        """Store a liquidation map snapshot"""
        timestamp = timestamp or datetime.utcnow()
        ts_str = timestamp.isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            for coin, data in liq_map.items():
                nearest_long = data.get('nearest_long_cluster')
                nearest_short = data.get('nearest_short_cluster')
                
                clusters = {
                    'long': data.get('long_liquidations', []),
                    'short': data.get('short_liquidations', [])
                }
                
                conn.execute("""
                    INSERT OR REPLACE INTO snapshots 
                    (timestamp, coin, current_price, total_long_at_risk, total_short_at_risk,
                     nearest_long_price, nearest_long_size, nearest_short_price, nearest_short_size,
                     clusters_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ts_str,
                    coin,
                    data.get('current_price', 0),
                    data.get('total_long_at_risk_usd', 0),
                    data.get('total_short_at_risk_usd', 0),
                    nearest_long.get('price_center') if nearest_long else None,
                    nearest_long.get('total_size_usd') if nearest_long else None,
                    nearest_short.get('price_center') if nearest_short else None,
                    nearest_short.get('total_size_usd') if nearest_short else None,
                    compress_json(clusters)  # Compressed with zlib
                ))
            
            conn.commit()
        
        print(f"[HistoricalStorage] Stored snapshot for {len(liq_map)} coins at {ts_str}")
    
    def store_prices(self, prices: Dict[str, float], timestamp: datetime = None):
        """Store price snapshot"""
        timestamp = timestamp or datetime.utcnow()
        ts_str = timestamp.isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            for coin, price in prices.items():
                if price and price > 0:
                    conn.execute("""
                        INSERT OR REPLACE INTO price_history (timestamp, coin, price)
                        VALUES (?, ?, ?)
                    """, (ts_str, coin, price))
            conn.commit()
    
    def record_liquidation_event(
        self, 
        coin: str, 
        price: float, 
        side: str,
        cluster_size: float,
        price_move_percent: float,
        time_to_hit_minutes: float,
        timestamp: datetime = None
    ):
        """Record when price hit a liquidation cluster"""
        timestamp = timestamp or datetime.utcnow()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO liquidation_events 
                (timestamp, coin, price, side, cluster_size, price_move_percent, time_to_hit_minutes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp.isoformat(),
                coin,
                price,
                side,
                cluster_size,
                price_move_percent,
                time_to_hit_minutes
            ))
            conn.commit()
    
    def get_snapshots(
        self, 
        coin: str, 
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 1000
    ) -> List[Dict]:
        """Get historical snapshots for a coin"""
        start_time = start_time or (datetime.utcnow() - timedelta(days=7))
        end_time = end_time or datetime.utcnow()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM snapshots
                WHERE coin = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (coin, start_time.isoformat(), end_time.isoformat(), limit))
            
            results = []
            for row in cursor.fetchall():
                snap = dict(row)
                # Decompress clusters_json if present
                if 'clusters_json' in snap and snap['clusters_json']:
                    snap['clusters_json'] = decompress_json(snap['clusters_json'])
                results.append(snap)
            return results
    
    def get_price_history(
        self, 
        coin: str,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> List[Tuple[datetime, float]]:
        """Get price history for a coin"""
        start_time = start_time or (datetime.utcnow() - timedelta(days=7))
        end_time = end_time or datetime.utcnow()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT timestamp, price FROM price_history
                WHERE coin = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp
            """, (coin, start_time.isoformat(), end_time.isoformat()))
            
            return [(datetime.fromisoformat(row[0]), row[1]) for row in cursor.fetchall()]
    
    def get_liquidation_events(
        self,
        coin: str = None,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> List[Dict]:
        """Get recorded liquidation events"""
        start_time = start_time or (datetime.utcnow() - timedelta(days=30))
        end_time = end_time or datetime.utcnow()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            if coin:
                cursor = conn.execute("""
                    SELECT * FROM liquidation_events
                    WHERE coin = ? AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                """, (coin, start_time.isoformat(), end_time.isoformat()))
            else:
                cursor = conn.execute("""
                    SELECT * FROM liquidation_events
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                """, (start_time.isoformat(), end_time.isoformat()))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_stats(self) -> Dict:
        """Get storage statistics"""
        with sqlite3.connect(self.db_path) as conn:
            snapshot_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            price_count = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
            event_count = conn.execute("SELECT COUNT(*) FROM liquidation_events").fetchone()[0]
            
            coins = conn.execute("SELECT DISTINCT coin FROM snapshots").fetchall()
            
            oldest = conn.execute("SELECT MIN(timestamp) FROM snapshots").fetchone()[0]
            newest = conn.execute("SELECT MAX(timestamp) FROM snapshots").fetchone()[0]
            
            return {
                "snapshot_count": snapshot_count,
                "price_count": price_count,
                "event_count": event_count,
                "coins_tracked": len(coins),
                "oldest_snapshot": oldest,
                "newest_snapshot": newest,
                "db_size_mb": os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            }
    
    def export_to_csv(self, coin: str, output_path: str):
        """Export historical data to CSV for external analysis"""
        import csv
        
        snapshots = self.get_snapshots(coin, limit=10000)
        
        if not snapshots:
            print(f"No data for {coin}")
            return
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp', 'coin', 'current_price', 
                'total_long_at_risk', 'total_short_at_risk',
                'nearest_long_price', 'nearest_long_size',
                'nearest_short_price', 'nearest_short_size'
            ])
            writer.writeheader()
            
            for snap in snapshots:
                writer.writerow({
                    'timestamp': snap['timestamp'],
                    'coin': snap['coin'],
                    'current_price': snap['current_price'],
                    'total_long_at_risk': snap['total_long_at_risk'],
                    'total_short_at_risk': snap['total_short_at_risk'],
                    'nearest_long_price': snap['nearest_long_price'],
                    'nearest_long_size': snap['nearest_long_size'],
                    'nearest_short_price': snap['nearest_short_price'],
                    'nearest_short_size': snap['nearest_short_size']
                })
        
        print(f"Exported {len(snapshots)} snapshots to {output_path}")


# Convenience function
def store_current_snapshot():
    """Store current liquidation map snapshot"""
    try:
        with open(config.LIQUIDATION_MAP_FILE, 'r') as f:
            liq_map = json.load(f)
        
        storage = HistoricalStorage()
        storage.store_snapshot(liq_map)
        
        return True
    except Exception as e:
        print(f"Error storing snapshot: {e}")
        return False
