# In-Memory Delta Testing TLDR

Test the full pipeline end-to-end without Azure, OneLake, or Delta Lake dependencies.

## Quick Start

```python
# In your test file
import polars as pl
from kafka_pipeline.common.storage.inmemory_delta import InMemoryDeltaTable

def test_my_feature(inmemory_xact_events):
    """Use the fixture directly."""
    # Write data (same API as production DeltaTableWriter)
    df = pl.DataFrame({"trace_id": ["t1", "t2"], "event_type": ["A", "B"]})
    inmemory_xact_events.append(df)

    # Read back for assertions
    result = inmemory_xact_events.read()
    assert len(result) == 2
```

## What It Does

```
┌─────────────────────────────────────────────────────────────────┐
│                     Test Environment                             │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │ Dummy Source │───>│    Kafka     │───>│     Workers      │  │
│  │ (test data)  │    │  (Docker)    │    │ (event_ingester) │  │
│  └──────────────┘    └──────────────┘    └────────┬─────────┘  │
│                                                    │             │
│                                          ┌────────v─────────┐   │
│                                          │ InMemoryDelta    │   │
│   Instead of Azure/OneLake ───────────>  │ (Polars DataFrames)│ │
│                                          │ - Same API        │   │
│                                          │ - Queryable       │   │
│                                          │ - No cloud deps   │   │
│                                          └──────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Available Fixtures

Add these to your test function signature:

| Fixture | Description |
|---------|-------------|
| `inmemory_xact_events` | XACT events table |
| `inmemory_xact_attachments` | XACT attachments/inventory table |
| `inmemory_claimx_events` | ClaimX events table |
| `inmemory_delta_registry` | Registry for managing multiple tables |
| `inmemory_delta_storage` | Dict with all tables + mock OneLake |

## Three Ways to Use

### 1. Direct Fixture (Simplest)

```python
def test_events_written(inmemory_xact_events):
    df = pl.DataFrame({"id": [1, 2, 3]})
    inmemory_xact_events.append(df)

    assert len(inmemory_xact_events) == 3
    assert inmemory_xact_events.read()["id"].to_list() == [1, 2, 3]
```

### 2. Patch Production Writers (E2E Tests)

```python
from kafka_pipeline.common.storage.testing import patch_delta_writers

def test_full_pipeline():
    with patch_delta_writers() as storage:
        # All DeltaTableWriter instances now use in-memory storage
        # Run your pipeline code here...

        # Verify what was written
        events = storage["xact_events"].read()
        assert len(events) > 0
```

### 3. Monkeypatch in Fixtures

```python
from kafka_pipeline.common.storage.testing import setup_inmemory_writers

@pytest.fixture
def patched_storage(inmemory_delta_registry, monkeypatch):
    return setup_inmemory_writers(inmemory_delta_registry, monkeypatch)

def test_with_patched_writers(patched_storage):
    # Run pipeline - writes go to in-memory tables
    result = patched_storage["xact_events"].read()
```

## API Reference

### Writing Data

```python
# Append (no deduplication)
table.append(df)
table.append(df, batch_id="batch-001")  # With tracking

# Merge (upsert)
table.merge(df, merge_keys=["id"])
table.merge(df, merge_keys=["id"], preserve_columns=["created_at"])

# Write dicts
table.write_rows([{"id": 1, "name": "test"}])
```

### Reading Data

```python
# Read all
df = table.read()
df = table.read(columns=["id", "name"])  # Select columns

# Read filtered
df = table.read_filtered(pl.col("status") == "completed")
df = table.read_filtered(pl.col("value") > 100, limit=10)

# Lazy scan
lf = table.scan()
result = lf.filter(pl.col("x") > 0).collect()
```

### Inspection (for Assertions)

```python
len(table)                      # Row count
table.exists()                  # Has data?
table.get_columns()             # Column names
table.get_schema()              # {col: dtype} dict
table.get_unique_values("col")  # Distinct values
table.get_partition_values()    # Partition column values
table.to_dicts()                # List of row dicts
table.get_write_history()       # All write operations
```

### Cleanup

```python
table.clear()           # Reset single table
registry.clear_all()    # Reset all tables in registry
registry.reset()        # Remove all tables entirely
```

## Example: Full E2E Test

```python
import pytest
import polars as pl
from kafka_pipeline.common.storage.testing import patch_delta_writers
from kafka_pipeline.common.dummy import DummyDataSource, DummySourceConfig

@pytest.mark.asyncio
async def test_dummy_source_to_delta(kafka_config):
    """Generate dummy events and verify they reach Delta tables."""

    with patch_delta_writers() as storage:
        # Configure dummy source for quick test
        config = DummySourceConfig(
            kafka=kafka_config,
            domains=["xact"],
            max_events=10,
        )

        # Generate events
        async with DummyDataSource(config) as source:
            events = await source.generate_batch("xact", count=10)

        # Process through pipeline...
        # (your worker code here)

        # Verify results
        xact_events = storage["xact_events"]
        assert len(xact_events) == 10

        # Check specific fields
        df = xact_events.read()
        assert "trace_id" in df.columns
        assert df["trace_id"].null_count() == 0
```

## Schema Evolution

The in-memory table handles schema changes automatically:

```python
# First write establishes schema
table.append(pl.DataFrame({"a": [1], "b": [2]}))

# New columns are added (existing rows get nulls)
table.append(pl.DataFrame({"a": [3], "b": [4], "c": [5]}))

result = table.read()
# Row 1: a=1, b=2, c=null
# Row 2: a=3, b=4, c=5
```

## Debugging

### Check Write History

```python
for record in table.get_write_history():
    print(f"{record.operation}: {record.rows_affected} rows")
    print(f"  Columns: {record.columns}")
    print(f"  Schema changes: {record.schema_changes}")
```

### Registry Stats

```python
stats = registry.get_stats()
# {'events': {'row_count': 100, 'write_count': 5, 'columns': [...]}}
```

## Common Patterns

### Verify Specific Records

```python
def test_event_processed(inmemory_xact_events):
    # ... run pipeline ...

    # Find specific record
    result = inmemory_xact_events.read_filtered(
        pl.col("trace_id") == "expected-trace-id"
    )
    assert len(result) == 1
    assert result["status"][0] == "completed"
```

### Count by Category

```python
def test_event_distribution(inmemory_claimx_events):
    # ... run pipeline ...

    df = inmemory_claimx_events.read()
    counts = df.group_by("event_type").len()

    assert counts.filter(pl.col("event_type") == "PROJECT_CREATED")["len"][0] > 0
```

### Verify No Duplicates

```python
def test_no_duplicate_events(inmemory_xact_events):
    # ... run pipeline ...

    df = inmemory_xact_events.read()
    unique_count = df["event_id"].n_unique()
    assert unique_count == len(df), "Found duplicate events!"
```

## Files

| File | Purpose |
|------|---------|
| `src/kafka_pipeline/common/storage/inmemory_delta.py` | Core classes |
| `src/kafka_pipeline/common/storage/testing.py` | Patching helpers |
| `tests/kafka_pipeline/conftest.py` | Pytest fixtures |
