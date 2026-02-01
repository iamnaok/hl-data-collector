#!/usr/bin/env python3
"""
Migration Script: Compress existing clusters_json data

This script compresses all uncompressed clusters_json data in the database.
Run once after updating historical_storage.py to use compression.

Usage:
    python scripts/migrate_compress.py --db data/historical.db
    python scripts/migrate_compress.py --db data/historical.db --dry-run
"""
import sqlite3
import json
import zlib
import base64
import os
import sys
from datetime import datetime

# Compression marker (must match historical_storage.py)
COMPRESSION_MARKER = "ZLIB:"


def compress_json(data: dict) -> str:
    """Compress JSON data with zlib, return base64-encoded string"""
    json_bytes = json.dumps(data, separators=(',', ':')).encode('utf-8')
    compressed = zlib.compress(json_bytes, level=6)
    encoded = base64.b64encode(compressed).decode('ascii')
    return COMPRESSION_MARKER + encoded


def is_compressed(data: str) -> bool:
    """Check if data is already compressed"""
    return data is not None and data.startswith(COMPRESSION_MARKER)


def get_db_size(path: str) -> float:
    """Get database size in MB"""
    return os.path.getsize(path) / (1024 * 1024)


def migrate_compress(db_path: str, dry_run: bool = False, batch_size: int = 1000):
    """
    Compress all uncompressed clusters_json data.
    """
    print(f"=== Compression Migration ===")
    print(f"Database: {db_path}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print()
    
    size_before = get_db_size(db_path)
    print(f"Size before: {size_before:.1f} MB")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Count total and uncompressed records
    cur.execute("SELECT COUNT(*) FROM snapshots")
    total_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM snapshots WHERE clusters_json IS NOT NULL AND clusters_json NOT LIKE 'ZLIB:%'")
    uncompressed_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM snapshots WHERE clusters_json LIKE 'ZLIB:%'")
    already_compressed = cur.fetchone()[0]
    
    print(f"Total records: {total_count:,}")
    print(f"Already compressed: {already_compressed:,}")
    print(f"Need compression: {uncompressed_count:,}")
    print()
    
    if uncompressed_count == 0:
        print("Nothing to compress!")
        conn.close()
        return
    
    if dry_run:
        # Show sample compression ratio
        cur.execute("""
            SELECT id, clusters_json FROM snapshots 
            WHERE clusters_json IS NOT NULL AND clusters_json NOT LIKE 'ZLIB:%'
            LIMIT 10
        """)
        samples = cur.fetchall()
        
        total_original = 0
        total_compressed = 0
        
        print("Sample compression ratios:")
        for row_id, clusters_json in samples:
            original_size = len(clusters_json)
            try:
                data = json.loads(clusters_json)
                compressed = compress_json(data)
                compressed_size = len(compressed)
                ratio = (1 - compressed_size / original_size) * 100
                total_original += original_size
                total_compressed += compressed_size
                print(f"  ID {row_id}: {original_size:,} → {compressed_size:,} bytes ({ratio:.0f}% reduction)")
            except Exception as e:
                print(f"  ID {row_id}: Error - {e}")
        
        if total_original > 0:
            avg_ratio = (1 - total_compressed / total_original) * 100
            print(f"\nAverage compression: {avg_ratio:.0f}%")
            
            # Estimate total savings
            cur.execute("SELECT SUM(LENGTH(clusters_json)) FROM snapshots WHERE clusters_json IS NOT NULL AND clusters_json NOT LIKE 'ZLIB:%'")
            total_uncompressed_size = cur.fetchone()[0] or 0
            estimated_savings = total_uncompressed_size * (avg_ratio / 100) / (1024 * 1024)
            print(f"Estimated savings: ~{estimated_savings:.0f} MB")
        
        print("\n[DRY RUN] No changes made. Run without --dry-run to compress.")
        conn.close()
        return
    
    # Process in batches
    print(f"Compressing in batches of {batch_size}...")
    processed = 0
    errors = 0
    
    while True:
        cur.execute("""
            SELECT id, clusters_json FROM snapshots 
            WHERE clusters_json IS NOT NULL AND clusters_json NOT LIKE 'ZLIB:%'
            LIMIT ?
        """, (batch_size,))
        
        rows = cur.fetchall()
        if not rows:
            break
        
        for row_id, clusters_json in rows:
            try:
                data = json.loads(clusters_json)
                compressed = compress_json(data)
                cur.execute("UPDATE snapshots SET clusters_json = ? WHERE id = ?", (compressed, row_id))
                processed += 1
            except Exception as e:
                print(f"  Error compressing ID {row_id}: {e}")
                errors += 1
        
        conn.commit()
        print(f"  Processed {processed:,} / {uncompressed_count:,} ({processed * 100 // uncompressed_count}%)")
    
    print(f"\nCompression complete: {processed:,} records, {errors} errors")
    
    # Vacuum to reclaim space
    print("\nRunning VACUUM to reclaim space...")
    conn.execute("VACUUM")
    conn.close()
    
    size_after = get_db_size(db_path)
    print(f"\nSize after: {size_after:.1f} MB")
    print(f"Saved: {size_before - size_after:.1f} MB ({(1 - size_after/size_before) * 100:.0f}% reduction)")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Compress clusters_json data")
    parser.add_argument("--db", default="data/historical.db", help="Path to database")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--batch-size", type=int, default=1000, help="Records per batch")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        sys.exit(1)
    
    migrate_compress(args.db, dry_run=args.dry_run, batch_size=args.batch_size)
