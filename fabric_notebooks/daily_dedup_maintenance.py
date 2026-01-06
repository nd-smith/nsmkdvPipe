# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Deduplication Maintenance
# MAGIC
# MAGIC This notebook removes duplicate events from xact_events and claimx_events tables.
# MAGIC
# MAGIC **Schedule**: Daily at 2:00 AM UTC
# MAGIC
# MAGIC **What it does**:
# MAGIC 1. Scans last 3 days of data for duplicates
# MAGIC 2. Keeps most recent record for each trace_id/event_id
# MAGIC 3. Overwrites partitions with deduped data
# MAGIC 4. Optimizes tables (compaction + Z-ordering)
# MAGIC 5. Vacuums old file versions (7-day retention)
# MAGIC
# MAGIC **Runtime**: ~15-30 minutes for 1M events/day

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dedup_maintenance")

# Table paths (update these for your environment)
XACT_EVENTS_TABLE = "abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Tables/xact_events"
CLAIMX_EVENTS_TABLE = "abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse/Tables/claimx_events"

# Dedup configuration
LOOKBACK_DAYS = 3  # Process last N days
VACUUM_RETENTION_HOURS = 168  # 7 days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def deduplicate_table(
    spark: SparkSession,
    table_path: str,
    dedupe_column: str,
    timestamp_column: str = "ingested_at",
    partition_column: str = "event_date",
    lookback_days: int = 3
) -> dict:
    """
    Remove duplicates from Delta table, keeping most recent record.

    Args:
        spark: SparkSession
        table_path: Path to Delta table
        dedupe_column: Column to deduplicate on (trace_id or event_id)
        timestamp_column: Column to determine "most recent"
        partition_column: Date partition column
        lookback_days: Number of days to process

    Returns:
        Dict with stats (rows_before, rows_after, duplicates_removed)
    """
    logger.info(f"Starting deduplication for {table_path}")

    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)

    logger.info(f"Processing partitions from {start_date} to {end_date}")

    # Read table with partition filter
    df = spark.read.format("delta").load(table_path) \
        .filter(f"{partition_column} >= '{start_date}'") \
        .filter(f"{partition_column} <= '{end_date}'")

    rows_before = df.count()
    logger.info(f"Rows before dedup: {rows_before:,}")

    if rows_before == 0:
        logger.info("No data to process")
        return {"rows_before": 0, "rows_after": 0, "duplicates_removed": 0}

    # Deduplicate - keep row with latest timestamp for each dedupe_column
    # Using window function to rank by timestamp DESC, then filter rank=1
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    window_spec = Window.partitionBy(dedupe_column).orderBy(F.col(timestamp_column).desc())

    deduped_df = df.withColumn("_row_num", F.row_number().over(window_spec)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")

    rows_after = deduped_df.count()
    duplicates_removed = rows_before - rows_after

    logger.info(f"Rows after dedup: {rows_after:,}")
    logger.info(f"Duplicates removed: {duplicates_removed:,}")

    # Overwrite affected partitions
    logger.info("Writing deduped data back to table...")

    deduped_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy(partition_column) \
        .option("replaceWhere", f"{partition_column} >= '{start_date}' AND {partition_column} <= '{end_date}'") \
        .save(table_path)

    logger.info("Deduplication complete")

    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": duplicates_removed,
        "start_date": str(start_date),
        "end_date": str(end_date)
    }

# COMMAND ----------

def optimize_table(spark: SparkSession, table_path: str, z_order_columns: list) -> None:
    """
    Optimize Delta table with compaction and Z-ordering.

    Args:
        spark: SparkSession
        table_path: Path to Delta table
        z_order_columns: Columns for Z-ordering
    """
    logger.info(f"Optimizing table {table_path}")

    delta_table = DeltaTable.forPath(spark, table_path)

    # Compact small files
    logger.info("Running OPTIMIZE (file compaction)...")
    delta_table.optimize().executeCompaction()

    # Z-order for query performance
    if z_order_columns:
        logger.info(f"Running Z-ORDER on columns: {z_order_columns}")
        delta_table.optimize().executeZOrderBy(z_order_columns)

    logger.info("Optimization complete")

# COMMAND ----------

def vacuum_table(spark: SparkSession, table_path: str, retention_hours: int = 168) -> None:
    """
    Vacuum old file versions from Delta table.

    Args:
        spark: SparkSession
        table_path: Path to Delta table
        retention_hours: Retention period in hours (default: 7 days)
    """
    logger.info(f"Vacuuming table {table_path} (retention: {retention_hours}h)")

    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.vacuum(retention_hours / 24)  # Convert to days

    logger.info("Vacuum complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process xact_events Table

# COMMAND ----------

logger.info("=" * 80)
logger.info("Processing xact_events table")
logger.info("=" * 80)

xact_stats = deduplicate_table(
    spark=spark,
    table_path=XACT_EVENTS_TABLE,
    dedupe_column="trace_id",
    timestamp_column="ingested_at",
    partition_column="event_date",
    lookback_days=LOOKBACK_DAYS
)

optimize_table(
    spark=spark,
    table_path=XACT_EVENTS_TABLE,
    z_order_columns=["event_date", "trace_id", "event_type"]
)

vacuum_table(
    spark=spark,
    table_path=XACT_EVENTS_TABLE,
    retention_hours=VACUUM_RETENTION_HOURS
)

logger.info(f"xact_events stats: {xact_stats}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process claimx_events Table

# COMMAND ----------

logger.info("=" * 80)
logger.info("Processing claimx_events table")
logger.info("=" * 80)

claimx_stats = deduplicate_table(
    spark=spark,
    table_path=CLAIMX_EVENTS_TABLE,
    dedupe_column="event_id",
    timestamp_column="ingested_at",
    partition_column="event_date",
    lookback_days=LOOKBACK_DAYS
)

optimize_table(
    spark=spark,
    table_path=CLAIMX_EVENTS_TABLE,
    z_order_columns=["event_date", "event_id", "event_type"]
)

vacuum_table(
    spark=spark,
    table_path=CLAIMX_EVENTS_TABLE,
    retention_hours=VACUUM_RETENTION_HOURS
)

logger.info(f"claimx_events stats: {claimx_stats}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print("\n" + "=" * 80)
print("DAILY DEDUPLICATION MAINTENANCE - SUMMARY")
print("=" * 80)
print(f"\nExecution Time: {datetime.now()}")
print(f"Lookback Period: {LOOKBACK_DAYS} days")
print("\nxact_events:")
print(f"  - Rows before: {xact_stats['rows_before']:,}")
print(f"  - Rows after: {xact_stats['rows_after']:,}")
print(f"  - Duplicates removed: {xact_stats['duplicates_removed']:,}")
print(f"  - Date range: {xact_stats.get('start_date')} to {xact_stats.get('end_date')}")
print("\nclaimx_events:")
print(f"  - Rows before: {claimx_stats['rows_before']:,}")
print(f"  - Rows after: {claimx_stats['rows_after']:,}")
print(f"  - Duplicates removed: {claimx_stats['duplicates_removed']:,}")
print(f"  - Date range: {claimx_stats.get('start_date')} to {claimx_stats.get('end_date')}")
print("\nTotal duplicates removed: {:,}".format(
    xact_stats['duplicates_removed'] + claimx_stats['duplicates_removed']
))
print("=" * 80)
