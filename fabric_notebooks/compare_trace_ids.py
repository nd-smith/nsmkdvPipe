"""
Compare trace_ids between Eventhouse source and Delta xact_events.

Identifies records that exist in Eventhouse but are missing from Delta.

Usage in Fabric notebook:
    1. Update configuration in Cell 1
    2. Run Cell 2 (KQL magic) to get Eventhouse data
    3. Run remaining cells to compare
"""

# Cell 1: Configuration
# ====================

from datetime import datetime, timezone
from pyspark.sql import functions as F
import pandas as pd

# Time range to compare (adjust as needed)
START_TIME = "2026-01-06 16:15:00"
END_TIME = None  # None = now

# Eventhouse settings (for KQL magic cell)
EVENTHOUSE_DATABASE = "your-database-name"  # Used in %%kql connection
EVENTHOUSE_TABLE = "tbl_VERISK_XACTANALYSIS_STATUS_EXPORT"

# Delta table path
XACT_EVENTS_PATH = "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/xact_events"

print(f"Comparing trace_ids from {START_TIME} to {END_TIME or 'now'}")
print(f"\nNext: Run the KQL magic cell below to query Eventhouse")


# Cell 2: Query Eventhouse using KQL magic
# ========================================
# NOTE: This cell must be a separate cell with %%kql magic
# Copy this to a new cell and run it:

"""
%%kql
// Connect to your Eventhouse database first via the notebook UI
// Or use: %%kql your-eventhouse-uri://databases/your-database

tbl_VERISK_XACTANALYSIS_STATUS_EXPORT
| where ingestion_time() > datetime('2026-01-06 16:15:00')
| project traceId, ingestion_time = ingestion_time(), utcDateTime
| summarize
    eh_ingestion_time = min(ingestion_time),
    eh_utcDateTime = min(utcDateTime)
    by traceId
"""

# Cell 3: Convert KQL result to Spark DataFrame
# =============================================
# After running the %%kql cell above, the result is in _kql_raw_result_

# Convert KQL result to pandas then Spark
try:
    eventhouse_pdf = _kql_raw_result_.to_dataframe()
    eventhouse_df = spark.createDataFrame(eventhouse_pdf)
    eventhouse_count = eventhouse_df.count()
    print(f"Eventhouse source records: {eventhouse_count:,}")
except NameError:
    print("ERROR: Run the %%kql cell first to populate _kql_raw_result_")
    print("Create a new cell with the KQL magic command from Cell 2")


# Cell 4: Query Delta for destination trace_ids
# =============================================

# Read Delta table
delta_df = spark.read.format("delta").load(XACT_EVENTS_PATH)

# Filter by time range (using ingested_at which maps to utcDateTime)
delta_filtered = delta_df \
    .filter(F.col("ingested_at") > START_TIME)

if END_TIME:
    delta_filtered = delta_filtered.filter(F.col("ingested_at") < END_TIME)

delta_traces = delta_filtered.select(
    F.col("trace_id"),
    F.col("ingested_at"),
    F.col("created_at")
).distinct()

delta_count = delta_traces.count()
print(f"Delta xact_events records: {delta_count:,}")
print(f"Gap: {eventhouse_count - delta_count:,} records")


# Cell 5: Find missing trace_ids (in Eventhouse but not in Delta)
# ===============================================================

# Rename Eventhouse columns for join (KQL result has traceId)
eventhouse_traces = eventhouse_df.select(
    F.col("traceId").alias("trace_id"),
    F.col("eh_ingestion_time"),
    F.col("eh_utcDateTime")
)

delta_traces_renamed = delta_traces.select(
    F.col("trace_id"),
    F.col("ingested_at").alias("delta_ingested_at"),
    F.col("created_at").alias("delta_created_at")
)

# Left anti join: records in Eventhouse but NOT in Delta
missing_traces = eventhouse_traces.join(
    delta_traces_renamed,
    on="trace_id",
    how="left_anti"
)

missing_count = missing_traces.count()
print(f"\nMissing from Delta: {missing_count:,} trace_ids")


# Cell 6: Analyze missing records
# ==============================

if missing_count > 0:
    print("\n" + "=" * 60)
    print("MISSING RECORDS ANALYSIS")
    print("=" * 60)

    # Show sample of missing trace_ids
    print("\nSample missing trace_ids (first 20):")
    missing_traces.show(20, truncate=False)

    # Time distribution of missing records
    print("\nTime distribution of missing records (by hour):")
    missing_traces \
        .withColumn("hour", F.date_trunc("hour", F.col("eh_ingestion_time"))) \
        .groupBy("hour") \
        .count() \
        .orderBy("hour") \
        .show(50)

    # Save missing trace_ids for further investigation
    missing_traces.createOrReplaceTempView("missing_traces")
    print("\nMissing traces available as temp view: missing_traces")
    print("Query with: spark.sql('SELECT * FROM missing_traces')")
else:
    print("\nNo missing records found!")


# Cell 7: Export missing trace_ids (optional)
# ==========================================

# Uncomment to save missing trace_ids to a file
# missing_traces.write.mode("overwrite").parquet(
#     "abfss://.../missing_trace_ids"
# )

# Or to CSV for easy sharing
# missing_traces.toPandas().to_csv("/tmp/missing_traces.csv", index=False)


# Cell 8: Check for duplicates in Delta (bonus)
# ============================================

print("\n" + "=" * 60)
print("DUPLICATE CHECK IN DELTA")
print("=" * 60)

dup_check = delta_filtered \
    .groupBy("trace_id") \
    .count() \
    .filter(F.col("count") > 1)

dup_count = dup_check.count()
print(f"Trace_ids with duplicates: {dup_count:,}")

if dup_count > 0:
    print("\nSample duplicates:")
    dup_check.orderBy(F.desc("count")).show(10)
