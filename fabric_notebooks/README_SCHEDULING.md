# Fabric Notebook Scheduling Guide

## Daily Deduplication Maintenance Setup

This guide explains how to schedule the `daily_dedup_maintenance.py` notebook in Microsoft Fabric.

---

## Overview

The daily deduplication notebook:
- Removes duplicate events from `xact_events` and `claimx_events` tables
- Keeps the most recent record for each `trace_id` or `event_id`
- Optimizes tables with file compaction and Z-ordering
- Vacuums old file versions

**Recommended Schedule**: Daily at 2:00 AM UTC (low-traffic period)

---

## Setup Instructions

### 1. Upload Notebook to Fabric

1. Navigate to your Fabric workspace
2. Go to **Data Engineering** or **Data Science** experience
3. Click **Import** → **Notebook**
4. Upload `daily_dedup_maintenance.py`
5. The notebook will be converted to `.ipynb` format automatically

### 2. Update Configuration

Open the notebook and update the table paths in the Configuration cell:

```python
# Update these paths for your environment
XACT_EVENTS_TABLE = "abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Tables/xact_events"
CLAIMX_EVENTS_TABLE = "abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Tables/claimx_events"
```

**To find your paths**:
1. Open your Lakehouse in Fabric
2. Navigate to Tables → xact_events
3. Click "Copy ABFSS path" from the context menu

### 3. Test Run

Before scheduling, run the notebook manually:

1. Click **Run all** in the notebook
2. Verify it completes successfully (~15-30 minutes)
3. Check the Summary Report for statistics
4. Verify no errors in the execution log

**Expected output**:
```
DAILY DEDUPLICATION MAINTENANCE - SUMMARY
================================================================================
Execution Time: 2026-01-05 02:00:15
Lookback Period: 3 days

xact_events:
  - Rows before: 1,234,567
  - Rows after: 1,233,890
  - Duplicates removed: 677

claimx_events:
  - Rows before: 567,890
  - Rows after: 567,850
  - Duplicates removed: 40

Total duplicates removed: 717
================================================================================
```

### 4. Create Schedule

#### Option A: Using Fabric Data Pipelines (Recommended)

1. In your workspace, create a new **Data Pipeline**
2. Name it: `Daily Dedup Maintenance Pipeline`
3. Add a **Notebook** activity
   - Select your `daily_dedup_maintenance` notebook
   - Configure to use Spark pool (default settings are fine)
4. Add a **Schedule** trigger
   - **Frequency**: Daily
   - **Time**: 02:00 AM
   - **Timezone**: UTC
   - **Start date**: Tomorrow's date
5. Click **Publish**

#### Option B: Using Notebook Scheduling (Alternative)

1. Open the `daily_dedup_maintenance` notebook
2. Click the **Schedule** button in the toolbar
3. Configure:
   - **Name**: Daily Dedup Maintenance
   - **Frequency**: Daily
   - **Time**: 02:00 AM UTC
   - **Retry policy**: 2 retries with 5-minute delay
4. Click **Save**

---

## Monitoring

### Check Execution History

1. Navigate to your workspace
2. Go to **Monitor** → **Pipelines** (or **Notebooks**)
3. Find `Daily Dedup Maintenance`
4. Click to view run history and logs

### Set Up Alerts

Configure email alerts for failures:

1. In the pipeline/notebook settings
2. Go to **Alerts**
3. Add rule:
   - **Condition**: On failure
   - **Action**: Send email to `your-team@company.com`

---

## Maintenance & Tuning

### Adjust Lookback Window

If you see consistent duplicates older than 3 days, increase `LOOKBACK_DAYS`:

```python
LOOKBACK_DAYS = 7  # Process last 7 days instead of 3
```

**Trade-offs**:
- Longer lookback = more data scanned = longer runtime
- Recommended: Start with 3 days, increase only if needed

### Optimize Runtime

If runtime exceeds 45 minutes:

1. **Reduce lookback window** (if possible)
2. **Increase Spark cluster size**:
   - Edit notebook settings
   - Select larger cluster (e.g., Medium or Large)
3. **Run less frequently** (e.g., every 2-3 days)

### Vacuum Retention

Default: 7 days (168 hours). Adjust based on your needs:

```python
VACUUM_RETENTION_HOURS = 336  # 14 days for more safety
```

**Note**: Time travel queries older than retention period will fail.

---

## Troubleshooting

### "Table not found" error

**Cause**: Incorrect table path

**Fix**:
1. Verify table paths in Lakehouse
2. Copy exact ABFSS paths from Fabric UI
3. Update notebook configuration

### "Concurrent modification" error

**Cause**: Another process is writing to the table during maintenance

**Fix**:
1. Schedule during low-traffic period (2-4 AM recommended)
2. Pause ingestion pipeline during maintenance if necessary
3. Add retry logic (automatic in Data Pipeline)

### High duplicate count

**Cause**: Multiple workers downloading same files, or Eventhouse sending duplicates

**Expected**: 0.1-0.5% duplicate rate (normal)
**Action needed if**: >1% duplicate rate

**Fix**:
1. Check DownloadWorker dedup cache is working
2. Verify Eventhouse is 99.5%+ unique
3. Investigate recent deployments/config changes

### Slow execution (>60 minutes)

**Causes**:
- Too much data in lookback window
- Small Spark cluster
- Many small files (needs compaction)

**Fixes**:
1. Reduce `LOOKBACK_DAYS` to 2
2. Increase cluster size
3. Run OPTIMIZE more frequently (separate job)

---

## Cost Optimization

### Compute Costs

- **Spark cluster runtime**: ~$2-5 per run (varies by cluster size)
- **Monthly cost**: ~$60-150 for daily runs

**To reduce**:
1. Use smallest cluster that completes in <30 minutes
2. Schedule less frequently if acceptable (every 2-3 days)
3. Only process partitions with changes (advanced)

### Storage Costs

- **VACUUM** deletes old file versions → reduces storage
- **OPTIMIZE** compacts small files → reduces file count
- Net effect: Usually cost-neutral or slight reduction

---

## Advanced: Conditional Execution

Only run if duplicates are detected:

```python
# Add before main processing
from pyspark.sql import functions as F

# Quick duplicate check
df_sample = spark.read.format("delta").load(XACT_EVENTS_TABLE) \
    .filter(f"event_date >= current_date() - 3")

dup_count = df_sample.groupBy("trace_id").count() \
    .filter("count > 1").count()

if dup_count == 0:
    print("No duplicates detected - skipping maintenance")
    dbutils.notebook.exit("skipped")

# ... continue with normal dedup process
```

---

## Support

For issues:
1. Check Fabric execution logs
2. Review Summary Report for statistics
3. Contact data engineering team

**Common Questions**:
- Q: Can I run this manually?
  - A: Yes, click "Run all" anytime
- Q: What if I miss a scheduled run?
  - A: Next run will process 3 days, catching up
- Q: Can I run on-demand after a big ingestion?
  - A: Yes, safe to run multiple times per day

---

## Change Log

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-05 | 1.0 | Initial version - replaced real-time dedup with daily maintenance |
