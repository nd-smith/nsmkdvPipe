"""
Compare trace_ids between Eventhouse source and Delta xact_events.

Identifies records that exist in Eventhouse but are missing from Delta.

Usage in Fabric notebook:
    1. Run all cells
    2. Check missing_traces DataFrame for gaps
"""

# Cell 1: Configuration
# ====================

from datetime import datetime, timezone
from pyspark.sql import functions as F

# Time range to compare (adjust as needed)
START_TIME = "2026-01-06 16:15:00"
END_TIME = None  # None = now

# Eventhouse connection
EVENTHOUSE_CLUSTER = "https://<your-cluster>.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = "<your-database>"
EVENTHOUSE_TABLE = "tbl_VERISK_XACTANALYSIS_STATUS_EXPORT"

# Delta table path
XACT_EVENTS_PATH = "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/xact_events"

print(f"Comparing trace_ids from {START_TIME} to {END_TIME or 'now'}")


# Cell 2: Query Eventhouse for source trace_ids
# =============================================

# Build KQL query
end_filter = f"and ingestion_time() < datetime('{END_TIME}')" if END_TIME else ""
kql_query = f"""
{EVENTHOUSE_TABLE}
| where ingestion_time() > datetime('{START_TIME}')
{end_filter}
| project traceId, ingestion_time = ingestion_time(), utcDateTime
| distinct traceId, ingestion_time, utcDateTime
"""

print("KQL Query:")
print(kql_query)

# Execute via Kusto connector
eventhouse_df = spark.read \
    .format("com.microsoft.kusto.spark.synapse.datasource") \
    .option("kustoCluster", EVENTHOUSE_CLUSTER) \
    .option("kustoDatabase", EVENTHOUSE_DATABASE) \
    .option("kustoQuery", kql_query) \
    .load()

eventhouse_count = eventhouse_df.count()
print(f"\nEventhouse source records: {eventhouse_count:,}")


# Cell 3: Query Delta for destination trace_ids
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


# Cell 4: Find missing trace_ids (in Eventhouse but not in Delta)
# ===============================================================

# Rename columns for join
eventhouse_traces = eventhouse_df.select(
    F.col("traceId").alias("trace_id"),
    F.col("ingestion_time").alias("eh_ingestion_time"),
    F.col("utcDateTime").alias("eh_utcDateTime")
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


# Cell 5: Analyze missing records
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


# Cell 6: Export missing trace_ids (optional)
# ==========================================

# Uncomment to save missing trace_ids to a file
# missing_traces.write.mode("overwrite").parquet(
#     "abfss://.../missing_trace_ids"
# )

# Or to CSV for easy sharing
# missing_traces.toPandas().to_csv("/tmp/missing_traces.csv", index=False)


# Cell 7: Check for duplicates in Delta (bonus)
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
