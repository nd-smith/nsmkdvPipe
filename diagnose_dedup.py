#!/usr/bin/env python3
"""
Diagnostic script to identify deduplication issues in the Eventhouse poller.

Tests:
1. Can we load trace_ids from Delta table?
2. Is the dedup cache being populated?
3. Is the KQL anti-join filter being generated?
4. What's the timing of cache operations?

Run: python diagnose_dedup.py
"""

import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "src"))

if not os.getenv("AZURE_TOKEN_FILE"):
    os.environ["AZURE_TOKEN_FILE"] = os.path.join(SCRIPT_DIR, "src", "tokens.json")

from datetime import datetime, timedelta, timezone

print("=" * 70)
print("DEDUPLICATION DIAGNOSTIC")
print("=" * 70)

# Get table path
TABLE_PATH = os.getenv("XACT_EVENTS_TABLE_PATH", "")
if not TABLE_PATH:
    print("\nERROR: Set XACT_EVENTS_TABLE_PATH environment variable")
    sys.exit(1)

print(f"\nDelta Table: {TABLE_PATH}")
print(f"Time: {datetime.now(timezone.utc).isoformat()}")

# Test 1: Can we import the dedup module?
print("\n" + "-" * 70)
print("TEST 1: Import dedup module")
print("-" * 70)
try:
    from kafka_pipeline.common.eventhouse.dedup import (
        DedupConfig,
        EventhouseDeduplicator,
        get_recent_trace_ids_sync,
    )
    print("✓ Successfully imported dedup module")
except Exception as e:
    print(f"✗ Failed to import: {e}")
    sys.exit(1)

# Test 2: Can we load trace_ids from Delta directly?
print("\n" + "-" * 70)
print("TEST 2: Load trace_ids from Delta table (standalone function)")
print("-" * 70)
try:
    start = time.perf_counter()
    trace_ids = get_recent_trace_ids_sync(TABLE_PATH, hours=24)
    duration = (time.perf_counter() - start) * 1000
    print(f"✓ Loaded {len(trace_ids):,} trace_ids in {duration:.0f}ms")
    if trace_ids:
        print(f"  Sample: {trace_ids[0]}")
except FileNotFoundError:
    print(f"✗ Delta table NOT FOUND at {TABLE_PATH}")
    print("  This means dedup cache will always be empty!")
except Exception as e:
    print(f"✗ Failed to load: {e}")

# Test 3: Initialize deduplicator and check cache
print("\n" + "-" * 70)
print("TEST 3: Initialize EventhouseDeduplicator")
print("-" * 70)
try:
    config = DedupConfig(
        xact_events_table_path=TABLE_PATH,
        xact_events_window_hours=24,
        eventhouse_query_window_hours=1,
        overlap_minutes=5,
    )
    dedup = EventhouseDeduplicator(config)
    print(f"✓ Created deduplicator")
    print(f"  Cache initialized: {dedup.is_cache_initialized()}")
    print(f"  Cache size: {dedup.get_cache_size()}")
except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Get recent trace_ids (triggers cache load)
print("\n" + "-" * 70)
print("TEST 4: Load cache via get_recent_trace_ids()")
print("-" * 70)
try:
    start = time.perf_counter()
    cached_ids = dedup.get_recent_trace_ids()
    duration = (time.perf_counter() - start) * 1000
    print(f"✓ Cache loaded in {duration:.0f}ms")
    print(f"  Cache initialized: {dedup.is_cache_initialized()}")
    print(f"  Cache size: {dedup.get_cache_size()}")
    if cached_ids:
        print(f"  Sample ID: {cached_ids[0]}")
except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Build KQL anti-join filter
print("\n" + "-" * 70)
print("TEST 5: Build KQL anti-join filter")
print("-" * 70)
try:
    if dedup.get_cache_size() > 0:
        # Get sample of trace_ids for filter
        sample_ids = cached_ids[:5]
        filter_clause = dedup.build_kql_anti_join_filter(sample_ids)
        print(f"✓ Filter generated ({len(filter_clause)} chars)")
        print(f"  Preview: {filter_clause[:100]}...")
    else:
        print("⚠ Cache is empty - no filter would be generated!")
        print("  This means ALL events will be re-polled every cycle!")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 6: Build complete deduped query
print("\n" + "-" * 70)
print("TEST 6: Build complete deduped KQL query")
print("-" * 70)
try:
    poll_from = datetime.now(timezone.utc) - timedelta(hours=1)
    poll_to = datetime.now(timezone.utc)

    query = dedup.build_deduped_query(
        base_table="Events",
        poll_from=poll_from,
        poll_to=poll_to,
        limit=100,
    )

    # Check if anti-join is present
    has_anti_join = "!in" in query or "not in" in query.lower()
    print(f"✓ Query generated ({len(query)} chars)")
    print(f"  Has anti-join filter: {has_anti_join}")
    print(f"\n  Query preview:")
    for line in query.split("\n")[:6]:
        print(f"    {line}")
    if len(query.split("\n")) > 6:
        print("    ...")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 7: Simulate cache update timing
print("\n" + "-" * 70)
print("TEST 7: Cache update timing simulation")
print("-" * 70)
try:
    # Add some trace_ids to cache
    test_ids = ["test-trace-1", "test-trace-2", "test-trace-3"]
    before_size = dedup.get_cache_size()

    start = time.perf_counter()
    added = dedup.add_to_cache(test_ids)
    duration = (time.perf_counter() - start) * 1000

    after_size = dedup.get_cache_size()
    print(f"✓ Added {added} trace_ids to cache in {duration:.3f}ms")
    print(f"  Cache size: {before_size} → {after_size}")

    # Check if they're in cache
    in_cache = all(tid in dedup._trace_id_cache for tid in test_ids)
    print(f"  IDs in cache: {in_cache}")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 8: Check for duplicate trace_ids in Delta
print("\n" + "-" * 70)
print("TEST 8: Check for duplicates in Delta table")
print("-" * 70)
try:
    import polars as pl
    from core.auth.credentials import get_storage_options

    storage_opts = get_storage_options()
    since = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    df = (
        pl.scan_delta(TABLE_PATH, storage_options=storage_opts)
        .filter(pl.col("event_date") >= since.date())
        .filter(pl.col("ingested_at") >= since)
        .collect()
    )

    total = len(df)
    unique = df["trace_id"].n_unique()
    dups = total - unique

    print(f"✓ Delta table analysis (since 2026-01-02):")
    print(f"  Total rows: {total:,}")
    print(f"  Unique trace_ids: {unique:,}")
    print(f"  Duplicates: {dups:,} ({dups/total*100:.1f}%)")

    if dups > 0:
        # Find max duplication
        dup_counts = (
            df.group_by("trace_id")
            .agg(pl.count().alias("cnt"))
            .filter(pl.col("cnt") > 1)
            .sort("cnt", descending=True)
        )
        max_dup = dup_counts["cnt"].max()
        print(f"  Max copies of single trace_id: {max_dup}")

except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    traceback.print_exc()

# Summary
print("\n" + "=" * 70)
print("DIAGNOSIS SUMMARY")
print("=" * 70)

issues = []

if dedup.get_cache_size() == 0:
    issues.append("Cache is EMPTY - deduplication will not work")

if not dedup.is_cache_initialized():
    issues.append("Cache never initialized - Delta table may be inaccessible")

try:
    if dups > 0 and dups / total > 0.1:
        issues.append(f"HIGH duplicate rate ({dups/total*100:.0f}%) confirms dedup failure")
except:
    pass

if issues:
    print("\n⚠️  ISSUES FOUND:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")

    print("\n📋 LIKELY ROOT CAUSE:")
    print("   The dedup cache is not preventing re-polling because:")
    print("   - Cache updates happen in async background tasks")
    print("   - Next poll cycle starts before cache is updated")
    print("   - Result: same events fetched every 30 seconds")
else:
    print("\n✓ No obvious issues found in dedup configuration")
    print("  If duplicates exist, the issue may be in async timing")

print("\n" + "=" * 70)
