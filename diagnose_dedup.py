#!/usr/bin/env python3
"""
Simplified diagnostic - tests Delta table directly without pipeline imports.
Run from repo root: python diagnose_dedup.py
"""

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "src"))

if not os.getenv("AZURE_TOKEN_FILE"):
    os.environ["AZURE_TOKEN_FILE"] = os.path.join(SCRIPT_DIR, "src", "tokens.json")

from datetime import datetime, timedelta, timezone
import time

print("=" * 70)
print("DEDUPLICATION DIAGNOSTIC (Simplified)")
print("=" * 70)

TABLE_PATH = os.getenv("XACT_EVENTS_TABLE_PATH", "")
if not TABLE_PATH:
    print("\nERROR: Set XACT_EVENTS_TABLE_PATH environment variable")
    sys.exit(1)

print(f"\nDelta Table: {TABLE_PATH}")
print(f"Time: {datetime.now(timezone.utc).isoformat()}")

# Test 1: Load auth
print("\n" + "-" * 70)
print("TEST 1: Load authentication")
print("-" * 70)
try:
    from core.auth.credentials import get_storage_options
    storage_opts = get_storage_options()
    print(f"✓ Auth loaded successfully")
    print(f"  Keys: {list(storage_opts.keys())}")
except Exception as e:
    print(f"✗ Failed: {e}")
    sys.exit(1)

# Test 2: Query Delta table
print("\n" + "-" * 70)
print("TEST 2: Query Delta table for trace_ids")
print("-" * 70)
try:
    import polars as pl
    from deltalake import DeltaTable

    since = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    start = time.perf_counter()

    # Use deltalake directly to avoid polars version mismatch
    dt = DeltaTable(TABLE_PATH, storage_options=storage_opts)
    df = pl.from_arrow(dt.to_pyarrow_table(columns=["trace_id", "ingested_at", "event_date"]))

    # Filter in polars
    df = df.filter(
        (pl.col("event_date") >= since.date()) &
        (pl.col("ingested_at") >= since)
    ).select("trace_id", "ingested_at")

    duration = (time.perf_counter() - start) * 1000

    total = len(df)
    unique = df["trace_id"].n_unique()
    dups = total - unique

    print(f"✓ Query completed in {duration:.0f}ms")
    print(f"  Total rows: {total:,}")
    print(f"  Unique trace_ids: {unique:,}")
    print(f"  Duplicate rows: {dups:,} ({dups/total*100:.1f}%)")

except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Analyze duplicate pattern
print("\n" + "-" * 70)
print("TEST 3: Duplicate pattern analysis")
print("-" * 70)
try:
    if dups > 0:
        dup_counts = (
            df.group_by("trace_id")
            .agg(pl.count().alias("cnt"))
            .filter(pl.col("cnt") > 1)
            .sort("cnt", descending=True)
        )

        max_dup = dup_counts["cnt"].max()
        trace_ids_with_dups = len(dup_counts)

        print(f"  Trace IDs with duplicates: {trace_ids_with_dups:,}")
        print(f"  Max copies of single ID: {max_dup}")

        # Distribution of duplicate counts
        print(f"\n  Duplicate count distribution:")
        dist = dup_counts.group_by("cnt").agg(pl.count().alias("num_ids")).sort("cnt")
        for row in dist.iter_rows(named=True):
            print(f"    {row['cnt']}x duplicated: {row['num_ids']:,} trace_ids")
    else:
        print("  No duplicates found")

except Exception as e:
    print(f"✗ Failed: {e}")

# Test 4: Temporal analysis of duplicates
print("\n" + "-" * 70)
print("TEST 4: Temporal analysis (when did duplicates occur?)")
print("-" * 70)
try:
    if dups > 0:
        # Get most duplicated trace_id
        worst_id = dup_counts.head(1)["trace_id"][0]

        dup_times = (
            df.filter(pl.col("trace_id") == worst_id)
            .sort("ingested_at")
        )

        print(f"  Most duplicated trace_id: {worst_id}")
        print(f"  Ingestion times:")

        times = dup_times["ingested_at"].to_list()
        for i, t in enumerate(times[:15]):  # Show first 15
            if i > 0:
                delta = (t - times[i-1]).total_seconds()
                print(f"    {i+1}. {t}  (+{delta:.0f}s)")
            else:
                print(f"    {i+1}. {t}")

        if len(times) > 15:
            print(f"    ... and {len(times) - 15} more")

        # Calculate avg interval
        if len(times) > 1:
            intervals = [(times[i] - times[i-1]).total_seconds() for i in range(1, len(times))]
            avg_interval = sum(intervals) / len(intervals)
            print(f"\n  Average interval between duplicates: {avg_interval:.1f}s")

            if 25 <= avg_interval <= 35:
                print("  ⚠️  ~30s interval matches poll_interval_seconds!")
                print("     This confirms dedup cache is not working.")
    else:
        print("  No duplicates to analyze")

except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Check hourly pattern
print("\n" + "-" * 70)
print("TEST 5: Hourly ingestion pattern")
print("-" * 70)
try:
    hourly = (
        df.with_columns(pl.col("ingested_at").dt.truncate("1h").alias("hour"))
        .group_by("hour")
        .agg([
            pl.count().alias("total"),
            pl.col("trace_id").n_unique().alias("unique"),
        ])
        .with_columns((pl.col("total") - pl.col("unique")).alias("dups"))
        .sort("hour")
    )

    print(f"  {'Hour':<20} {'Total':>10} {'Unique':>10} {'Dups':>10} {'Dup%':>8}")
    print(f"  {'-'*58}")

    for row in hourly.iter_rows(named=True):
        hour_str = row["hour"].strftime("%Y-%m-%d %H:00")
        dup_pct = row["dups"] / row["total"] * 100 if row["total"] > 0 else 0
        flag = "⚠️" if dup_pct > 20 else ""
        print(f"  {hour_str:<20} {row['total']:>10,} {row['unique']:>10,} {row['dups']:>10,} {dup_pct:>7.1f}% {flag}")

except Exception as e:
    print(f"✗ Failed: {e}")

# Summary
print("\n" + "=" * 70)
print("DIAGNOSIS SUMMARY")
print("=" * 70)

if dups > 0:
    dup_pct = dups / total * 100
    print(f"\n⚠️  DUPLICATE RATE: {dup_pct:.1f}%")
    print(f"\n📋 FINDINGS:")
    print(f"   - {dups:,} duplicate rows out of {total:,} total")
    print(f"   - Each event processed ~{total/unique:.1f}x on average")
    if 'avg_interval' in dir() and 25 <= avg_interval <= 35:
        print(f"   - Duplicates occur every ~{avg_interval:.0f}s (poll interval)")
        print(f"\n🔍 ROOT CAUSE: Dedup cache not preventing re-polling")
        print(f"   Events are being fetched from Eventhouse repeatedly")
        print(f"   because the KQL anti-join filter has no trace_ids to exclude.")
else:
    print("\n✓ No duplicates found in Delta table")

print("\n" + "=" * 70)
