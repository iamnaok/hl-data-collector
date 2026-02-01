#!/usr/bin/env python3
"""
Database Maintenance Script

Implements tiered data retention:
- Last 24h: Keep all (5-min granularity)
- 1-7 days: Keep hourly snapshots
- 7-30 days: Keep daily snapshots (noon UTC)
- 30+ days: Delete

Run daily via cron:
0 3 * * * /opt/hl-data-collector/venv/bin/python /opt/hl-data-collector/scripts/db_maintenance.py
"""
import sqlite3
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = os.environ.get('DB_PATH', 'data/historical.db')

def get_db_size(path: str) -> float:
    """Get database size in MB"""
    return os.path.getsize(path) / (1024 * 1024)

def run_maintenance(db_path: str = DB_PATH, dry_run: bool = False):
    """
    Run database maintenance with tiered retention.
    """
    print(f"=== Database Maintenance ===")
    print(f"DB: {db_path}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print()
    
    size_before = get_db_size(db_path)
    print(f"Size before: {size_before:.1f} MB")
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Get current counts
    cur.execute("SELECT COUNT(*) FROM snapshots")
    count_before = cur.fetchone()[0]
    print(f"Snapshots before: {count_before:,}")
    
    # === Step 1: Delete data older than 30 days ===
    print("\n[1] Deleting data > 30 days old...")
    cur.execute("""
        SELECT COUNT(*) FROM snapshots 
        WHERE timestamp < datetime('now', '-30 days')
    """)
    old_count = cur.fetchone()[0]
    print(f"  Found {old_count:,} records to delete")
    
    if not dry_run and old_count > 0:
        cur.execute("""
            DELETE FROM snapshots 
            WHERE timestamp < datetime('now', '-30 days')
        """)
        conn.commit()
        print(f"  Deleted {cur.rowcount:,} records")
    
    # === Step 2: Downsample 7-30 day old data to daily ===
    print("\n[2] Downsampling 7-30 day data to daily (keep noon UTC)...")
    cur.execute("""
        SELECT COUNT(*) FROM snapshots 
        WHERE timestamp < datetime('now', '-7 days')
          AND timestamp >= datetime('now', '-30 days')
          AND strftime('%H', timestamp) != '12'
    """)
    downsample_count = cur.fetchone()[0]
    print(f"  Found {downsample_count:,} non-noon records to delete")
    
    if not dry_run and downsample_count > 0:
        cur.execute("""
            DELETE FROM snapshots 
            WHERE timestamp < datetime('now', '-7 days')
              AND timestamp >= datetime('now', '-30 days')
              AND strftime('%H', timestamp) != '12'
        """)
        conn.commit()
        print(f"  Deleted {cur.rowcount:,} records")
    
    # === Step 3: Downsample 1-7 day old data to hourly ===
    print("\n[3] Downsampling 1-7 day data to hourly (keep :00 minute)...")
    cur.execute("""
        SELECT COUNT(*) FROM snapshots 
        WHERE timestamp < datetime('now', '-1 days')
          AND timestamp >= datetime('now', '-7 days')
          AND strftime('%M', timestamp) != '00'
    """)
    hourly_count = cur.fetchone()[0]
    print(f"  Found {hourly_count:,} non-hourly records to delete")
    
    if not dry_run and hourly_count > 0:
        cur.execute("""
            DELETE FROM snapshots 
            WHERE timestamp < datetime('now', '-1 days')
              AND timestamp >= datetime('now', '-7 days')
              AND strftime('%M', timestamp) != '00'
        """)
        conn.commit()
        print(f"  Deleted {cur.rowcount:,} records")
    
    # === Step 4: Same for price_history ===
    print("\n[4] Applying same retention to price_history...")
    
    if not dry_run:
        # Delete > 30 days
        cur.execute("""
            DELETE FROM price_history 
            WHERE timestamp < datetime('now', '-30 days')
        """)
        deleted_30 = cur.rowcount
        
        # Downsample 7-30 days to daily
        cur.execute("""
            DELETE FROM price_history 
            WHERE timestamp < datetime('now', '-7 days')
              AND timestamp >= datetime('now', '-30 days')
              AND strftime('%H', timestamp) != '12'
        """)
        deleted_daily = cur.rowcount
        
        # Downsample 1-7 days to hourly
        cur.execute("""
            DELETE FROM price_history 
            WHERE timestamp < datetime('now', '-1 days')
              AND timestamp >= datetime('now', '-7 days')
              AND strftime('%M', timestamp) != '00'
        """)
        deleted_hourly = cur.rowcount
        
        conn.commit()
        print(f"  Deleted {deleted_30 + deleted_daily + deleted_hourly:,} price records")
    
    # === Step 5: VACUUM to reclaim space ===
    print("\n[5] Running VACUUM to reclaim space...")
    if not dry_run:
        conn.execute("VACUUM")
        print("  VACUUM complete")
    
    conn.close()
    
    # Final stats
    if not dry_run:
        size_after = get_db_size(db_path)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM snapshots")
        count_after = cur.fetchone()[0]
        conn.close()
        
        print(f"\n=== Results ===")
        print(f"Size: {size_before:.1f} MB → {size_after:.1f} MB ({size_before - size_after:.1f} MB saved)")
        print(f"Snapshots: {count_before:,} → {count_after:,} ({count_before - count_after:,} deleted)")
    else:
        print("\n[DRY RUN] No changes made. Run without --dry-run to apply.")


def analyze_db(db_path: str = DB_PATH):
    """Show database statistics"""
    print(f"=== Database Analysis ===")
    print(f"DB: {db_path}")
    print(f"Size: {get_db_size(db_path):.1f} MB")
    print()
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Table sizes
    print("Table record counts:")
    for table in ['snapshots', 'price_history', 'liquidation_events']:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {table}: {count:,}")
        except:
            pass
    
    # Date range
    cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM snapshots")
    min_ts, max_ts = cur.fetchone()
    print(f"\nDate range: {min_ts[:10] if min_ts else 'N/A'} to {max_ts[:10] if max_ts else 'N/A'}")
    
    # Data by age
    print("\nSnapshots by age:")
    cur.execute("""
        SELECT 
            CASE 
                WHEN timestamp >= datetime('now', '-1 days') THEN 'Last 24h'
                WHEN timestamp >= datetime('now', '-7 days') THEN '1-7 days'
                WHEN timestamp >= datetime('now', '-30 days') THEN '7-30 days'
                ELSE '30+ days'
            END as age,
            COUNT(*) as cnt
        FROM snapshots
        GROUP BY age
        ORDER BY 
            CASE age
                WHEN 'Last 24h' THEN 1
                WHEN '1-7 days' THEN 2
                WHEN '7-30 days' THEN 3
                ELSE 4
            END
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")
    
    # Estimated size after maintenance
    cur.execute("""
        SELECT COUNT(*) FROM snapshots 
        WHERE timestamp >= datetime('now', '-1 days')
    """)
    last_24h = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(DISTINCT strftime('%Y-%m-%d %H', timestamp)) FROM snapshots 
        WHERE timestamp < datetime('now', '-1 days')
          AND timestamp >= datetime('now', '-7 days')
    """)
    hourly_slots = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(DISTINCT strftime('%Y-%m-%d', timestamp)) FROM snapshots 
        WHERE timestamp < datetime('now', '-7 days')
          AND timestamp >= datetime('now', '-30 days')
    """)
    daily_slots = cur.fetchone()[0]
    
    # Estimate: ~190 coins per slot
    estimated_after = last_24h + (hourly_slots * 190) + (daily_slots * 190)
    cur.execute("SELECT COUNT(*) FROM snapshots")
    current = cur.fetchone()[0]
    
    reduction_pct = (1 - estimated_after / current) * 100 if current > 0 else 0
    print(f"\nEstimated after maintenance: ~{estimated_after:,} records ({reduction_pct:.0f}% reduction)")
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Database maintenance")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--analyze", action="store_true", help="Just show database statistics")
    parser.add_argument("--db", default=DB_PATH, help="Path to database")
    
    args = parser.parse_args()
    
    if args.analyze:
        analyze_db(args.db)
    else:
        run_maintenance(args.db, dry_run=args.dry_run)
