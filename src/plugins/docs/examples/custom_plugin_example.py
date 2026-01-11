"""
Custom Plugin Example - Using LoggedClass Infrastructure

This example demonstrates how to create a custom plugin that leverages
the built-in logging infrastructure exposed in commit 6066082.

All plugins now inherit from LoggedClass, which provides:
- self._logger: Pre-configured logger instance
- self._log(level, msg, **extra): Structured logging with context
- self._log_exception(exc, msg, **extra): Exception logging with traceback

For more information, see:
- kafka_pipeline/plugins/base.py - Plugin base class
- kafka_pipeline/common/logging.py - LoggedClass implementation
"""

import logging
from typing import Any, Dict

from kafka_pipeline.plugins.base import (
    Domain,
    Plugin,
    PluginContext,
    PluginResult,
    PipelineStage,
)


class DataQualityPlugin(Plugin):
    """
    Example custom plugin that validates data quality.

    Demonstrates:
    - Using built-in logging (self._log, self._log_exception)
    - Structured context in log messages
    - Error handling with automatic exception logging
    - Configuration-driven behavior
    """

    name = "data_quality_plugin"
    domains = [Domain.CLAIMX]
    stages = [PipelineStage.ENRICHMENT_COMPLETE]
    event_types = []  # All events
    priority = 100  # Run after other plugins

    default_config = {
        "required_fields": [],
        "min_field_count": 5,
        "fail_on_invalid": False,  # Skip vs fail
    }

    def execute(self, context: PluginContext) -> PluginResult:
        """
        Validate data quality and log issues.

        Args:
            context: Plugin execution context with event data

        Returns:
            PluginResult indicating success/failure/skip
        """
        # Log execution start with context
        self._log(
            logging.DEBUG,
            "Data quality check starting",
            event_id=context.event_id,
            event_type=context.event_type,
        )

        try:
            # Extract data from context
            enriched_data = context.enriched_entities.get("primary", {})

            # Run validation checks
            validation_results = self._validate_data(enriched_data, context)

            if validation_results["issues"]:
                # Log issues found
                self._log(
                    logging.WARNING,
                    "Data quality issues detected",
                    event_id=context.event_id,
                    issue_count=len(validation_results["issues"]),
                    issues=validation_results["issues"],
                )

                # Decide action based on config
                if self.config.get("fail_on_invalid"):
                    return PluginResult.failed("Data quality validation failed")
                else:
                    # Log and continue
                    self._log(
                        logging.INFO,
                        "Continuing despite data quality issues",
                        event_id=context.event_id,
                    )

            # Success - log completion
            self._log(
                logging.DEBUG,
                "Data quality check completed",
                event_id=context.event_id,
                fields_validated=validation_results["fields_validated"],
            )

            return PluginResult.ok()

        except Exception as e:
            # Use _log_exception for automatic traceback and context
            self._log_exception(
                e,
                "Data quality check failed with exception",
                event_id=context.event_id,
            )
            return PluginResult.failed(f"Exception during validation: {str(e)}")

    def _validate_data(
        self, data: Dict[str, Any], context: PluginContext
    ) -> Dict[str, Any]:
        """
        Validate data against configured rules.

        Args:
            data: Data to validate
            context: Plugin context

        Returns:
            Dict with validation results
        """
        issues = []
        fields_validated = 0

        # Check required fields
        required_fields = self.config.get("required_fields", [])
        for field in required_fields:
            if field not in data or data[field] is None:
                issues.append(f"Missing required field: {field}")
                # Log individual field issue
                self._log(
                    logging.DEBUG,
                    "Missing required field",
                    field=field,
                    event_id=context.event_id,
                )
            else:
                fields_validated += 1

        # Check minimum field count
        min_fields = self.config.get("min_field_count", 0)
        if len(data) < min_fields:
            issues.append(
                f"Insufficient fields: {len(data)} < {min_fields}"
            )

        return {
            "issues": issues,
            "fields_validated": fields_validated,
        }


class EnrichmentMetricsPlugin(Plugin):
    """
    Example plugin that tracks enrichment metrics.

    Demonstrates:
    - Using @logged_operation decorator (if needed for more complex methods)
    - Logging with structured context
    - Different log levels for different scenarios
    """

    name = "enrichment_metrics_plugin"
    domains = [Domain.CLAIMX]
    stages = [PipelineStage.ENRICHMENT_COMPLETE]
    event_types = []

    default_config = {
        "track_enrichment_time": True,
        "alert_slow_threshold_ms": 5000,
    }

    def execute(self, context: PluginContext) -> PluginResult:
        """Track enrichment metrics."""
        # Calculate enrichment time
        if hasattr(context.metadata, "enrichment_start_time"):
            import time

            enrichment_time_ms = (
                time.time() - context.metadata.enrichment_start_time
            ) * 1000

            # Log with appropriate level based on threshold
            threshold = self.config.get("alert_slow_threshold_ms", 5000)
            if enrichment_time_ms > threshold:
                self._log(
                    logging.WARNING,
                    "Slow enrichment detected",
                    event_id=context.event_id,
                    enrichment_time_ms=enrichment_time_ms,
                    threshold_ms=threshold,
                )
            else:
                self._log(
                    logging.DEBUG,
                    "Enrichment completed",
                    event_id=context.event_id,
                    enrichment_time_ms=enrichment_time_ms,
                )

        # Count enriched entities
        entity_count = len(context.enriched_entities)
        self._log(
            logging.INFO,
            "Enrichment metrics tracked",
            event_id=context.event_id,
            entity_count=entity_count,
        )

        return PluginResult.ok()


class ErrorRecoveryPlugin(Plugin):
    """
    Example plugin showing error handling patterns.

    Demonstrates:
    - Using _log_exception for detailed error logging
    - Different error handling strategies
    - Logging context for debugging
    """

    name = "error_recovery_plugin"
    domains = [Domain.CLAIMX]
    stages = [PipelineStage.ERROR]
    event_types = []

    def execute(self, context: PluginContext) -> PluginResult:
        """Handle error events with logging."""
        error_info = context.metadata.get("error", {})

        # Log error details
        self._log(
            logging.ERROR,
            "Error event received",
            event_id=context.event_id,
            error_type=error_info.get("type"),
            error_message=error_info.get("message"),
            retry_count=context.metadata.get("retry_count", 0),
        )

        # Attempt recovery
        try:
            recovery_result = self._attempt_recovery(context)

            if recovery_result["success"]:
                self._log(
                    logging.INFO,
                    "Error recovery successful",
                    event_id=context.event_id,
                    recovery_action=recovery_result["action"],
                )
                return PluginResult.ok()
            else:
                self._log(
                    logging.WARNING,
                    "Error recovery failed",
                    event_id=context.event_id,
                    reason=recovery_result["reason"],
                )
                return PluginResult.failed("Recovery failed")

        except Exception as e:
            # Log exception with full context
            self._log_exception(
                e,
                "Exception during error recovery",
                event_id=context.event_id,
                error_type=error_info.get("type"),
            )
            return PluginResult.failed(f"Recovery exception: {str(e)}")

    def _attempt_recovery(self, context: PluginContext) -> Dict[str, Any]:
        """Attempt to recover from error."""
        # Implementation would go here
        return {"success": False, "reason": "Not implemented"}


# =============================================================================
# Configuration Example
# =============================================================================
"""
To use these plugins, create a config.yaml file:

# plugins/config/data_quality/config.yaml
name: data_quality_plugin
module: path.to.custom_plugin_example
class: DataQualityPlugin
enabled: true
priority: 100

config:
  required_fields:
    - project_id
    - event_id
    - event_type
  min_field_count: 10
  fail_on_invalid: false

# plugins/config/enrichment_metrics/config.yaml
name: enrichment_metrics_plugin
module: path.to.custom_plugin_example
class: EnrichmentMetricsPlugin
enabled: true
priority: 200

config:
  track_enrichment_time: true
  alert_slow_threshold_ms: 5000
"""


# =============================================================================
# Logging Best Practices
# =============================================================================
"""
1. Use self._log() for all logging in plugins:
   ✅ self._log(logging.INFO, "Message", key=value)
   ❌ logger.info(f"Message key={value}")

2. Always include event_id for correlation:
   self._log(logging.INFO, "Processing", event_id=context.event_id)

3. Use appropriate log levels:
   - DEBUG: Detailed execution flow
   - INFO: Important state changes
   - WARNING: Issues that don't prevent execution
   - ERROR: Failures that prevent execution

4. Use self._log_exception for exceptions:
   ✅ self._log_exception(e, "Failed to process", event_id=context.event_id)
   ❌ logger.exception(f"Failed: {e}")

5. Include relevant context in every log:
   self._log(
       logging.INFO,
       "Task completed",
       event_id=context.event_id,
       task_id=task_id,
       duration_ms=duration,
   )

6. Log results of operations:
   self._log(
       logging.INFO,
       "Validation completed",
       event_id=context.event_id,
       issues_found=len(issues),
       fields_validated=field_count,
   )
"""
