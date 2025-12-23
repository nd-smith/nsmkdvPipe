"""
Delta Lake optimization utilities for file compaction and layout optimization.

Provides scheduled OPTIMIZE operations to maintain query performance.

Background:
    Delta Lake performance degrades over time due to:
    - Small files from incremental writes
    - Poor data layout across files
    - Metadata overhead from many operations

    OPTIMIZE compacts small files and optionally applies Z-ordering
    to co-locate related data for faster filtered queries.

    Expected impact: 2-10x faster queries on optimized tables.
"""

import logging
import os
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass

from deltalake import DeltaTable
import polars as pl

from verisk_pipeline.common.logging.setup import get_logger

logger = get_logger(__name__)


@dataclass
class OptimizationConfig:
    """Configuration for Delta table optimization."""

    # File size targets
    target_file_size_mb: int = 128  # Target size after optimization
    min_file_size_mb: int = 10  # Files below this trigger optimization

    # Scheduling
    optimization_interval_hours: int = 24  # Run daily
    max_concurrent_optimizations: int = 2  # Parallel optimizations

    # Safety limits
    max_files_to_compact: int = 1000  # Prevent overwhelming operations
    dry_run: bool = False  # Test mode


class DeltaTableOptimizer:
    """
    Optimize Delta tables through file compaction.

    Implements bin-packing algorithm to merge small files into larger files,
    improving query performance and reducing metadata overhead.

    Usage:
        config = OptimizationConfig(target_file_size_mb=128)
        optimizer = DeltaTableOptimizer(config)

        # Check if optimization needed
        if optimizer.should_optimize("path/to/table"):
            result = optimizer.optimize("path/to/table")
    """

    def __init__(self, config: OptimizationConfig):
        """
        Initialize optimizer with configuration.

        Args:
            config: Optimization configuration
        """
        self.config = config

    def should_optimize(self, table_path: str) -> bool:
        """
        Check if table needs optimization.

        Analyzes file sizes and counts to determine if optimization
        would provide meaningful benefit.

        Args:
            table_path: Path to Delta table

        Returns:
            True if optimization recommended, False otherwise
        """
        try:
            dt = DeltaTable(table_path)

            # Get file statistics
            files = dt.files()
            if not files:
                logger.info(
                    "Table is empty, no optimization needed",
                    extra={"table": table_path},
                )
                return False

            # Analyze file sizes
            file_stats = self._analyze_file_sizes(table_path, files)

            # Decide if optimization needed
            needs_optimization = (
                file_stats["small_file_count"] > 10  # Many small files
                or file_stats["avg_size_mb"]
                < self.config.min_file_size_mb  # Small average
            )

            logger.info(
                "Optimization analysis complete",
                extra={
                    "table": table_path,
                    "total_files": file_stats["total_files"],
                    "small_files": file_stats["small_file_count"],
                    "avg_size_mb": file_stats["avg_size_mb"],
                    "needs_optimization": needs_optimization,
                },
            )

            return needs_optimization

        except Exception as e:
            logger.error(
                "Failed to analyze table for optimization",
                extra={"table": table_path, "error": str(e)},
                exc_info=True,
            )
            return False

    def _analyze_file_sizes(self, table_path: str, files: List[str]) -> dict:
        """
        Analyze file size distribution.

        Args:
            table_path: Path to Delta table
            files: List of file paths relative to table

        Returns:
            Dictionary with file statistics
        """
        sizes_mb = []
        for file in files:
            full_path = os.path.join(table_path, file)
            try:
                size_mb = os.path.getsize(full_path) / (1024 * 1024)
                sizes_mb.append(size_mb)
            except OSError:
                # File may be remote or inaccessible
                continue

        if not sizes_mb:
            return {
                "total_files": len(files),
                "small_file_count": 0,
                "avg_size_mb": 0.0,
                "total_size_mb": 0.0,
            }

        small_file_count = sum(
            1 for size in sizes_mb if size < self.config.min_file_size_mb
        )
        avg_size = sum(sizes_mb) / len(sizes_mb)
        total_size = sum(sizes_mb)

        return {
            "total_files": len(files),
            "small_file_count": small_file_count,
            "avg_size_mb": round(avg_size, 2),
            "total_size_mb": round(total_size, 2),
            "max_size_mb": round(max(sizes_mb), 2),
            "min_size_mb": round(min(sizes_mb), 2),
        }

    def optimize(
        self, table_path: str, z_order_columns: Optional[List[str]] = None
    ) -> dict:
        """
        Optimize Delta table through file compaction.

        Reads the entire table, optionally applies Z-ordering, and rewrites
        with optimal file sizes. This operation rewrites data files but
        maintains ACID properties.

        IMPORTANT: Run VACUUM after OPTIMIZE to remove old files and reclaim storage.

        Args:
            table_path: Path to Delta table
            z_order_columns: Optional columns for Z-ordering (co-location)

        Returns:
            Optimization metrics dictionary with keys:
                - table: Table path
                - files_before: Number of files before optimization
                - files_after: Number of files after optimization
                - files_removed: Number of files compacted
                - z_ordered: Whether Z-ordering was applied
                - z_order_columns: Columns used for Z-ordering
                - duration_seconds: Time taken
                - success: Operation success status
        """
        if self.config.dry_run:
            logger.info(
                "DRY RUN: Would optimize table",
                extra={"table": table_path, "z_order_columns": z_order_columns},
            )
            return {"dry_run": True, "table": table_path}

        logger.info(
            "Starting OPTIMIZE operation",
            extra={"table": table_path, "z_order_columns": z_order_columns},
        )
        start_time = datetime.now(UTC)

        try:
            dt = DeltaTable(table_path)

            # Get initial metrics
            files_before = dt.files()
            logger.info(
                "Table state before optimization",
                extra={"table": table_path, "files_before": len(files_before)},
            )

            # Run OPTIMIZE
            result = self._run_optimize(dt, z_order_columns)

            # Get final metrics
            dt.update_incremental()  # Refresh metadata
            files_after = dt.files()

            elapsed = (datetime.now(UTC) - start_time).total_seconds()

            metrics = {
                "table": table_path,
                "files_before": len(files_before),
                "files_after": len(files_after),
                "files_removed": len(files_before) - len(files_after),
                "z_ordered": bool(z_order_columns),
                "z_order_columns": z_order_columns or [],
                "duration_seconds": round(elapsed, 2),
                "success": True,
            }

            logger.info("OPTIMIZE completed successfully", extra=metrics)

            return metrics

        except Exception as e:
            logger.error(
                "OPTIMIZE failed",
                extra={"table": table_path, "error": str(e)},
                exc_info=True,
            )
            return {"table": table_path, "success": False, "error": str(e)}

    def _run_optimize(
        self, dt: DeltaTable, z_order_columns: Optional[List[str]] = None
    ) -> dict:
        """
        Run OPTIMIZE operation.

        Currently uses rewrite approach since delta-rs doesn't expose
        native OPTIMIZE yet. When delta-rs adds native OPTIMIZE support,
        update this to use that API for better performance.

        Args:
            dt: DeltaTable instance
            z_order_columns: Optional Z-ordering columns

        Returns:
            Result dictionary with operation status
        """
        # Read entire table
        df = dt.to_pyarrow_table()

        # Convert to Polars for processing
        pl_df = pl.from_arrow(df)

        # Sort by Z-order columns if specified (approximate Z-ordering)
        if z_order_columns:
            logger.info(
                "Applying Z-ordering", extra={"z_order_columns": z_order_columns}
            )
            # Validate columns exist
            missing_cols = [col for col in z_order_columns if col not in pl_df.columns]
            if missing_cols:
                logger.warning(
                    "Z-order columns not found in table, skipping Z-ordering",
                    extra={"missing_columns": missing_cols},
                )
            else:
                pl_df = pl_df.sort(z_order_columns)

        # Repartition to target file size
        # Calculate number of partitions based on target size
        total_size_bytes = pl_df.estimated_size()
        target_size_bytes = self.config.target_file_size_mb * 1024 * 1024
        num_partitions = max(1, int(total_size_bytes / target_size_bytes))

        logger.info(
            "Repartitioning for optimal file sizes",
            extra={
                "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
                "target_size_mb": self.config.target_file_size_mb,
                "num_partitions": num_partitions,
            },
        )

        # Write back (overwrite mode for optimization)
        # Note: In production, use VACUUM to clean up old files
        pl_df.write_delta(
            dt.table_uri,
            mode="overwrite",
            delta_write_options={
                "max_rows_per_file": (
                    len(pl_df) // num_partitions if num_partitions > 0 else len(pl_df)
                ),
                "max_rows_per_group": 1_000_000,
            },
        )

        return {"status": "optimized"}


class OptimizationScheduler:
    """
    Schedule and coordinate Delta table optimizations.

    Manages periodic optimization runs across multiple tables,
    tracking last run time and deciding when to re-optimize.

    Usage:
        config = OptimizationConfig(optimization_interval_hours=24)
        scheduler = OptimizationScheduler(config)

        # Optimize single table if needed
        result = scheduler.optimize_if_needed(
            "path/to/table",
            z_order_columns=["date", "status"]
        )

        # Optimize all tables
        results = scheduler.optimize_all_tables([
            {"path": "table1", "z_order_columns": ["date"]},
            {"path": "table2", "z_order_columns": ["status", "id"]}
        ])
    """

    def __init__(self, config: OptimizationConfig):
        """
        Initialize scheduler with configuration.

        Args:
            config: Optimization configuration
        """
        self.config = config
        self.optimizer = DeltaTableOptimizer(config)
        self._last_run: dict[str, datetime] = {}

    def should_run_optimization(self, table_path: str) -> bool:
        """
        Check if enough time has passed since last optimization.

        Args:
            table_path: Path to Delta table

        Returns:
            True if optimization should run, False otherwise
        """
        last_run = self._last_run.get(table_path)

        if not last_run:
            return True  # Never run before

        elapsed = datetime.now(UTC) - last_run
        interval = timedelta(hours=self.config.optimization_interval_hours)

        return elapsed >= interval

    def optimize_if_needed(
        self, table_path: str, z_order_columns: Optional[List[str]] = None
    ) -> Optional[dict]:
        """
        Optimize table if needed based on schedule and file analysis.

        Checks both timing (interval since last run) and table state
        (file sizes) to determine if optimization is beneficial.

        Args:
            table_path: Path to Delta table
            z_order_columns: Optional Z-ordering columns

        Returns:
            Optimization metrics if run, None if skipped
        """
        # Check schedule
        if not self.should_run_optimization(table_path):
            logger.debug(
                "Skipping optimization - too recent", extra={"table": table_path}
            )
            return None

        # Check if optimization needed
        if not self.optimizer.should_optimize(table_path):
            logger.info("Table does not need optimization", extra={"table": table_path})
            self._last_run[table_path] = datetime.now(UTC)
            return None

        # Run optimization
        logger.info(
            "Running scheduled optimization",
            extra={"table": table_path, "z_order_columns": z_order_columns},
        )
        result = self.optimizer.optimize(table_path, z_order_columns)

        # Record run time
        if result.get("success"):
            self._last_run[table_path] = datetime.now(UTC)

        return result

    def optimize_all_tables(
        self, table_configs: List[dict]  # [{path: str, z_order_columns: List[str]}]
    ) -> List[dict]:
        """
        Optimize all configured tables.

        Processes each table sequentially, checking if optimization
        is needed before running.

        Args:
            table_configs: List of table configurations, each with:
                - path: str - Table path
                - z_order_columns: Optional[List[str]] - Z-ordering columns

        Returns:
            List of optimization results (only for tables that were optimized)

        Example:
            results = scheduler.optimize_all_tables([
                {"path": "inventory", "z_order_columns": ["created_date", "status"]},
                {"path": "retry_queue", "z_order_columns": ["retry_count"]}
            ])
        """
        results = []

        for config in table_configs:
            table_path = config["path"]
            z_order_columns = config.get("z_order_columns")

            logger.info(
                "Processing table for optimization", extra={"table": table_path}
            )

            result = self.optimize_if_needed(table_path, z_order_columns)
            if result:
                results.append(result)

        logger.info(
            "Optimization batch complete",
            extra={
                "tables_processed": len(table_configs),
                "optimizations_run": len(results),
            },
        )

        return results
