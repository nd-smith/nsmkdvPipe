#!/usr/bin/env python3
"""
Quick pipeline verification script.
Run: python run_verify.py

Set XACT_EVENTS_TABLE_PATH env var or edit the path below.
"""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from datetime import datetime, timedelta, timezone
import polars as pl
from core.auth.credentials import get_storage_options

# CONFIG - edit if needed
TABLE_PATH = os.getenv("XACT_EVENTS_TABLE_PATH", "")
SINCE = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
UNTIL = datetime.now(timezone.utc)

if not TABLE_PATH:
    print("ERROR: Set XACT_EVENTS_TABLE_PATH environment variable")
    print("Example: export XACT_EVENTS_TABLE_PATH='abfss://...'")
    sys.exit(1)

print(f"Analyzing: {TABLE_PATH}")
print(f"Time range: {SINCE} to {UNTIL}")
print("-" * 60)

try:
    storage_opts = get_storage_options()

    df = (
        pl.scan_delta(TABLE_PATH, storage_options=storage_opts)
        .filter(pl.col("event_date") >= SINCE.date())
        .filter(pl.col("event_date") <= UNTIL.date())
        .filter(pl.col("ingested_at") >= SINCE)
        .filter(pl.col("ingested_at") <= UNTIL)
        .collect()
    )

    total_rows = len(df)
    unique_trace_ids = df["trace_id"].n_unique()
    duplicates = total_rows - unique_trace_ids
    dup_pct = (duplicates / total_rows * 100) if total_rows > 0 else 0

    print(f"\nRESULTS:")
    print(f"  Total rows in Delta:    {total_rows:,}")
    print(f"  Unique trace_ids:       {unique_trace_ids:,}")
    print(f"  Duplicate rows:         {duplicates:,}")
    print(f"  Duplicate percentage:   {dup_pct:.1f}%")

    # Compare to expected
    print(f"\nCOMPARISON TO KQL:")
    print(f"  KQL events (expected):  189,000")
    print(f"  Delta unique trace_ids: {unique_trace_ids:,}")
    print(f"  Difference:             {unique_trace_ids - 189000:+,}")

    if duplicates > 0:
        print(f"\nDUPLICATE ANALYSIS:")
        dup_df = (
            df.group_by("trace_id")
            .agg(pl.count().alias("count"))
            .filter(pl.col("count") > 1)
            .sort("count", descending=True)
        )
        print(f"  Trace IDs with duplicates: {len(dup_df):,}")
        print(f"  Max duplicates per ID:     {dup_df['count'].max()}")
        print(f"\n  Top 5 duplicated trace_ids:")
        for row in dup_df.head(5).iter_rows(named=True):
            print(f"    {row['trace_id']}: {row['count']}x")

    print("\n" + "-" * 60)
    if dup_pct > 10:
        print("⚠️  HIGH DUPLICATE RATE - Deduplication is NOT working!")
    elif unique_trace_ids > 200000:
        print("⚠️  More unique IDs than expected - events being re-polled")
    else:
        print("✓  Numbers look reasonable")

except FileNotFoundError:
    print(f"ERROR: Delta table not found at {TABLE_PATH}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
