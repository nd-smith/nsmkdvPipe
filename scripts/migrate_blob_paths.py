"""
Migrate Blob Paths: Add assignment_id to Directory Structure

This script migrates existing files and table records from:
    {status_subtype}/{trace_id}/{filename}
to:
    {status_subtype}/{assignment_id}/{trace_id}/{filename}

Usage in Microsoft Fabric:
    1. Create a new notebook in Fabric attached to your lakehouse
    2. Copy each section below into separate cells
    3. Update the configuration values
    4. Run with DRY_RUN=True first to preview changes
    5. Set DRY_RUN=False to execute the migration
"""

# =============================================================================
# CELL 1: Configuration
# =============================================================================

# Update these values for your environment
LAKEHOUSE_NAME = "your_lakehouse_name"  # Update this
ATTACHMENTS_TABLE = "xact_attachments"
RETRY_TABLE = "xact_retry"

# Dry run mode - set to False to actually move files and update tables
DRY_RUN = True

# Batch size for processing
BATCH_SIZE = 1000

# =============================================================================
# CELL 2: Setup and Imports
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, concat, lit, when
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from notebookutils import mssparkutils

spark = SparkSession.builder.getOrCreate()

# Get the attached lakehouse path
# In Fabric, tables are at: Tables/{table_name}, files at: Files/
attachments_table_path = f"Tables/{ATTACHMENTS_TABLE}"
retry_table_path = f"Tables/{RETRY_TABLE}"
files_base_path = "Files"

print(f"Attachments table: {attachments_table_path}")
print(f"Retry table: {retry_table_path}")
print(f"Files base path: {files_base_path}")
print(f"Dry run mode: {DRY_RUN}")


# =============================================================================
# CELL 3: Path Transformation Logic
# =============================================================================

def transform_blob_path(old_path: str, assignment_id: str) -> str:
    """
    Transform blob_path from old format to new format.

    Old: {status_subtype}/{trace_id}/{filename}
    New: {status_subtype}/{assignment_id}/{trace_id}/{filename}

    Returns None if path is already in new format or invalid.
    """
    if not old_path or not assignment_id:
        return None

    parts = old_path.split('/')

    # Expected old format: status_subtype/trace_id/filename (3 parts)
    # New format: status_subtype/assignment_id/trace_id/filename (4 parts)

    if len(parts) == 4:
        # Already in new format - check if assignment_id matches
        if parts[1] == assignment_id:
            return None  # Already migrated
        # Might be old format with nested trace_id, continue

    if len(parts) == 3:
        # Old format: status_subtype/trace_id/filename
        status_subtype, trace_id, filename = parts
        return f"{status_subtype}/{assignment_id}/{trace_id}/{filename}"

    # Unexpected format
    return None


# Register as UDF for Spark
transform_blob_path_udf = spark.udf.register(
    "transform_blob_path", transform_blob_path, StringType()
)


# =============================================================================
# CELL 4: Read and Analyze Tables
# =============================================================================

# Read attachments table
try:
    attachments_df = spark.read.format("delta").load(attachments_table_path)
    print(f"Attachments table: {attachments_df.count()} records")
except Exception as e:
    print(f"Could not read attachments table: {e}")
    attachments_df = None

# Read retry table
try:
    retry_df = spark.read.format("delta").load(retry_table_path)
    print(f"Retry table: {retry_df.count()} records")
except Exception as e:
    print(f"Could not read retry table: {e}")
    retry_df = None

# Analyze blob_path formats
if attachments_df:
    print("\n=== Attachments Table blob_path Analysis ===")
    attachments_df.selectExpr(
        "size(split(blob_path, '/')) as segment_count"
    ).groupBy("segment_count").count().orderBy("segment_count").show()

    print("\nSample blob_paths:")
    attachments_df.select("blob_path", "assignment_id").show(5, truncate=False)


# =============================================================================
# CELL 5: Identify Records Needing Migration
# =============================================================================

def get_records_to_migrate(df, table_name):
    """
    Identify records that need blob_path migration.

    Old format has 3 segments: status_subtype/trace_id/filename
    New format has 4 segments: status_subtype/assignment_id/trace_id/filename
    """
    if df is None:
        return None

    # Add new_blob_path column
    migrated_df = df.withColumn(
        "new_blob_path",
        transform_blob_path_udf(col("blob_path"), col("assignment_id"))
    )

    # Filter to only records needing migration
    to_migrate = migrated_df.filter(col("new_blob_path").isNotNull())

    migrate_count = to_migrate.count()
    total_count = df.count()
    already_migrated = total_count - migrate_count

    print(f"\n{table_name}:")
    print(f"  Total records: {total_count}")
    print(f"  Need migration: {migrate_count}")
    print(f"  Already migrated: {already_migrated}")

    return to_migrate


attachments_to_migrate = get_records_to_migrate(attachments_df, "xact_attachments")
retry_to_migrate = get_records_to_migrate(retry_df, "xact_retry")

# Preview migration changes
if attachments_to_migrate and attachments_to_migrate.count() > 0:
    print("\n=== Sample Attachments Migration ===")
    attachments_to_migrate.select(
        "trace_id", "assignment_id", "blob_path", "new_blob_path"
    ).show(10, truncate=False)


# =============================================================================
# CELL 6: Move Files in OneLake
# =============================================================================

def move_files_batch(records_df, files_base_path, dry_run=True):
    """
    Move files from old paths to new paths.

    Returns tuple of (success_count, error_count, errors)
    """
    if records_df is None or records_df.count() == 0:
        return 0, 0, []

    # Collect to driver for file operations
    records = records_df.select("blob_path", "new_blob_path").collect()

    success_count = 0
    error_count = 0
    errors = []
    skipped_count = 0

    for row in records:
        old_path = f"{files_base_path}/{row.blob_path}"
        new_path = f"{files_base_path}/{row.new_blob_path}"

        try:
            if dry_run:
                # Check if source exists
                try:
                    mssparkutils.fs.head(old_path, 1)
                    print(f"[DRY RUN] Would move: {old_path} -> {new_path}")
                    success_count += 1
                except:
                    # File doesn't exist at old path - might already be moved
                    skipped_count += 1
            else:
                # Check if source exists before moving
                try:
                    mssparkutils.fs.head(old_path, 1)
                except:
                    # Source doesn't exist - skip
                    skipped_count += 1
                    continue

                # Create parent directory for new path
                new_parent = '/'.join(new_path.split('/')[:-1])
                mssparkutils.fs.mkdirs(new_parent)

                # Move file
                mssparkutils.fs.mv(old_path, new_path)
                success_count += 1

        except Exception as e:
            error_count += 1
            errors.append(f"{old_path}: {str(e)}")
            if error_count <= 5:  # Only print first 5 errors
                print(f"Error moving {old_path}: {e}")

    print(f"\nFile operations: {success_count} successful, {skipped_count} skipped, {error_count} errors")
    return success_count, error_count, errors


# Move files for attachments
print("=== Moving Attachment Files ===")
att_success, att_errors, att_error_list = move_files_batch(
    attachments_to_migrate, files_base_path, dry_run=DRY_RUN
)

# Move files for retry queue (if any)
print("\n=== Moving Retry Queue Files ===")
retry_success, retry_errors, retry_error_list = move_files_batch(
    retry_to_migrate, files_base_path, dry_run=DRY_RUN
)


# =============================================================================
# CELL 7: Update Delta Tables with New blob_path
# =============================================================================

def update_table_blob_paths(table_path, records_df, dry_run=True):
    """
    Update blob_path column in Delta table using MERGE.

    Uses trace_id + attachment_url as the composite key for matching.
    """
    if records_df is None or records_df.count() == 0:
        print(f"No records to update in {table_path}")
        return 0

    update_count = records_df.count()

    if dry_run:
        print(f"[DRY RUN] Would update {update_count} records in {table_path}")
        return update_count

    try:
        # Get Delta table
        delta_table = DeltaTable.forPath(spark, table_path)

        # Prepare updates DataFrame with only needed columns
        updates_df = records_df.select(
            col("trace_id"),
            col("attachment_url"),
            col("new_blob_path").alias("updated_blob_path")
        )

        # Perform MERGE
        delta_table.alias("target").merge(
            updates_df.alias("source"),
            "target.trace_id = source.trace_id AND target.attachment_url = source.attachment_url"
        ).whenMatchedUpdate(
            set={"blob_path": col("source.updated_blob_path")}
        ).execute()

        print(f"Updated {update_count} records in {table_path}")
        return update_count

    except Exception as e:
        print(f"Error updating {table_path}: {e}")
        raise


# Update attachments table
print("=== Updating xact_attachments Table ===")
att_updated = update_table_blob_paths(
    attachments_table_path, attachments_to_migrate, dry_run=DRY_RUN
)

# Update retry table
print("\n=== Updating xact_retry Table ===")
retry_updated = update_table_blob_paths(
    retry_table_path, retry_to_migrate, dry_run=DRY_RUN
)


# =============================================================================
# CELL 8: Verification and Summary
# =============================================================================

if not DRY_RUN:
    print("=== Post-Migration Verification ===")

    # Check attachments table
    if attachments_df:
        new_attachments_df = spark.read.format("delta").load(attachments_table_path)
        print("\nAttachments table blob_path segment counts (should all be 4):")
        new_attachments_df.selectExpr(
            "size(split(blob_path, '/')) as segment_count"
        ).groupBy("segment_count").count().orderBy("segment_count").show()

        print("\nSample migrated paths:")
        new_attachments_df.select("blob_path").show(5, truncate=False)

print("=" * 60)
print("MIGRATION SUMMARY")
print("=" * 60)
print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
print()
print("xact_attachments:")
print(f"  Records to migrate: {attachments_to_migrate.count() if attachments_to_migrate else 0}")
print(f"  Files moved: {att_success}")
print(f"  File errors: {att_errors}")
print(f"  Table records updated: {att_updated}")
print()
print("xact_retry:")
print(f"  Records to migrate: {retry_to_migrate.count() if retry_to_migrate else 0}")
print(f"  Files moved: {retry_success}")
print(f"  File errors: {retry_errors}")
print(f"  Table records updated: {retry_updated}")
print()
if DRY_RUN:
    print(">>> Set DRY_RUN = False to execute the migration <<<")
else:
    print("Migration complete!")
