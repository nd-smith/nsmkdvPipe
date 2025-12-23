"""
RBAC validation for Azure permissions (Task H.3).

Validates that the service principal or user has required RBAC permissions
for pipeline operations before execution. Prevents runtime failures due to
insufficient permissions.

Required Permissions:
- Storage Blob Data Contributor: OneLake/ADLS Gen2 write operations
- Storage Blob Data Reader: OneLake/ADLS Gen2 read operations
- Key Vault Secrets User: Read secrets from Key Vault
- Contributor (optional): Kusto cluster operations

Usage:
    validator = RBACValidator()
    result = validator.validate_all_permissions()
    if not result.all_valid:
        for failure in result.failures:
            logger.error(f"Missing permission: {failure}")
"""

import json
import logging
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from verisk_pipeline.common.config.xact import get_config
from verisk_pipeline.common.logging.audit import AuditEventType, get_audit_logger

logger = logging.getLogger(__name__)
audit = get_audit_logger()


class AzureRole(Enum):
    """Azure RBAC roles required for pipeline operations."""

    # Storage roles for OneLake/ADLS Gen2
    STORAGE_BLOB_DATA_CONTRIBUTOR = "Storage Blob Data Contributor"
    STORAGE_BLOB_DATA_READER = "Storage Blob Data Reader"

    # Key Vault roles
    KEY_VAULT_SECRETS_USER = "Key Vault Secrets User"
    KEY_VAULT_SECRETS_OFFICER = "Key Vault Secrets Officer"

    # Optional roles
    CONTRIBUTOR = "Contributor"
    READER = "Reader"


class ResourceType(Enum):
    """Azure resource types for RBAC validation."""

    STORAGE_ACCOUNT = "storage_account"
    KEY_VAULT = "key_vault"
    KUSTO_CLUSTER = "kusto_cluster"
    SUBSCRIPTION = "subscription"
    RESOURCE_GROUP = "resource_group"


@dataclass
class PermissionRequirement:
    """Required RBAC permission for a pipeline operation."""

    role: AzureRole
    resource_type: ResourceType
    resource_id: Optional[str] = None
    required: bool = True
    description: str = ""


@dataclass
class PermissionValidationResult:
    """Result of RBAC permission validation."""

    requirement: PermissionRequirement
    has_permission: bool
    assigned_roles: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class ValidationSummary:
    """Summary of all permission validations."""

    results: List[PermissionValidationResult]
    all_valid: bool
    required_valid: bool
    failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class RBACValidator:
    """
    Validates Azure RBAC permissions for pipeline operations.

    Checks that the authenticated principal (service principal or user)
    has required permissions before pipeline execution to prevent runtime failures.

    Usage:
        validator = RBACValidator()
        if not validator.validate_startup_permissions():
            raise RuntimeError("Insufficient permissions for pipeline execution")
    """

    def __init__(self):
        """Initialize RBAC validator with configuration."""
        config = get_config()
        self.subscription_id = config.security.subscription_id
        self.resource_group = config.security.resource_group
        self.key_vault_url = config.security.key_vault_url

        # Define required permissions for pipeline operations
        self.requirements = self._define_requirements()

    def _define_requirements(self) -> List[PermissionRequirement]:
        """Define required RBAC permissions for pipeline operations."""
        requirements = [
            # Storage permissions for OneLake/ADLS Gen2 operations
            PermissionRequirement(
                role=AzureRole.STORAGE_BLOB_DATA_CONTRIBUTOR,
                resource_type=ResourceType.STORAGE_ACCOUNT,
                required=True,
                description="Required for writing data to OneLake/ADLS Gen2",
            ),
            PermissionRequirement(
                role=AzureRole.STORAGE_BLOB_DATA_READER,
                resource_type=ResourceType.STORAGE_ACCOUNT,
                required=True,
                description="Required for reading data from OneLake/ADLS Gen2",
            ),
            # Key Vault permissions for credential access
            PermissionRequirement(
                role=AzureRole.KEY_VAULT_SECRETS_USER,
                resource_type=ResourceType.KEY_VAULT,
                required=True,
                description="Required for reading secrets from Key Vault",
            ),
            # Optional: Enhanced Key Vault permissions for rotation
            PermissionRequirement(
                role=AzureRole.KEY_VAULT_SECRETS_OFFICER,
                resource_type=ResourceType.KEY_VAULT,
                required=False,
                description="Optional: Required for credential rotation operations",
            ),
            # Optional: Contributor for resource management
            PermissionRequirement(
                role=AzureRole.CONTRIBUTOR,
                resource_type=ResourceType.RESOURCE_GROUP,
                required=False,
                description="Optional: Required for resource management operations",
            ),
        ]
        return requirements

    def _run_az_command(self, cmd: List[str]) -> subprocess.CompletedProcess:
        """Execute Azure CLI command."""
        logger.debug(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=30
        )
        if result.returncode != 0:
            logger.debug(f"Command failed: {result.stderr}")
        return result

    def _get_current_principal(self) -> Optional[Dict]:
        """Get current authenticated principal (user or service principal)."""
        result = self._run_az_command(
            [
                "az",
                "account",
                "show",
                "--query",
                "{id: user.name, type: user.type}",
                "-o",
                "json",
            ]
        )

        if result.returncode != 0:
            logger.error(
                "Failed to get current principal. Ensure 'az login' is completed."
            )
            return None

        try:
            principal = json.loads(result.stdout)
            return principal
        except json.JSONDecodeError:
            logger.error(f"Failed to parse principal info: {result.stdout}")
            return None

    def _get_role_assignments(self, scope: Optional[str] = None) -> List[Dict]:
        """Get RBAC role assignments for current principal."""
        principal = self._get_current_principal()
        if not principal:
            return []

        principal_id = principal.get("id")

        # Build command
        cmd = ["az", "role", "assignment", "list"]
        if scope:
            cmd.extend(["--scope", scope])
        cmd.extend(["--assignee", principal_id, "-o", "json"])

        result = self._run_az_command(cmd)

        if result.returncode != 0:
            logger.warning(f"Failed to get role assignments for scope: {scope}")
            return []

        try:
            assignments = json.loads(result.stdout)
            return assignments
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse role assignments: {result.stdout}")
            return []

    def _validate_permission(
        self, requirement: PermissionRequirement
    ) -> PermissionValidationResult:
        """Validate a single RBAC permission requirement."""
        logger.info(
            f"Validating permission: {requirement.role.value} on {requirement.resource_type.value}"
        )

        # Determine scope based on resource type
        scope = self._get_scope_for_resource_type(requirement.resource_type)

        if not scope:
            return PermissionValidationResult(
                requirement=requirement,
                has_permission=False,
                error_message=f"Could not determine scope for {requirement.resource_type.value}",
            )

        # Get role assignments for scope
        assignments = self._get_role_assignments(scope)

        # Check if required role is assigned
        assigned_roles = [a.get("roleDefinitionName") for a in assignments]
        has_permission = requirement.role.value in assigned_roles

        result = PermissionValidationResult(
            requirement=requirement,
            has_permission=has_permission,
            assigned_roles=assigned_roles,
        )

        # Log audit event
        audit.log_authz_event(
            event_type=AuditEventType.AUTHZ_RBAC_CHECK,
            permission=requirement.role.value,
            success=has_permission,
            resource=scope,
            reason=requirement.description,
        )

        if not has_permission and requirement.required:
            logger.warning(
                f"Missing required permission: {requirement.role.value} on {scope}"
            )

        return result

    def _get_scope_for_resource_type(
        self, resource_type: ResourceType
    ) -> Optional[str]:
        """Get Azure scope string for resource type."""
        if resource_type == ResourceType.SUBSCRIPTION:
            return f"/subscriptions/{self.subscription_id}"

        if resource_type == ResourceType.RESOURCE_GROUP:
            return f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}"

        if resource_type == ResourceType.STORAGE_ACCOUNT:
            # Storage account scope (requires resource group)
            return f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}"

        if resource_type == ResourceType.KEY_VAULT:
            # Extract vault name from URL
            if self.key_vault_url:
                vault_name = self.key_vault_url.split("//")[1].split(".")[0]
                return f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.KeyVault/vaults/{vault_name}"

        if resource_type == ResourceType.KUSTO_CLUSTER:
            # Kusto cluster scope (optional, requires specific resource ID)
            return f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}"

        logger.warning(f"Unknown resource type: {resource_type}")
        return None

    def validate_all_permissions(self) -> ValidationSummary:
        """
        Validate all required RBAC permissions.

        Returns:
            ValidationSummary with results for all permission checks
        """
        logger.info("Starting RBAC permission validation...")

        results = []
        failures = []
        warnings = []

        for requirement in self.requirements:
            result = self._validate_permission(requirement)
            results.append(result)

            if not result.has_permission:
                message = (
                    f"{requirement.role.value} on {requirement.resource_type.value}"
                )
                if requirement.required:
                    failures.append(message)
                else:
                    warnings.append(message)

        # Determine overall validation status
        all_valid = len(failures) == 0 and len(warnings) == 0
        required_valid = len(failures) == 0

        summary = ValidationSummary(
            results=results,
            all_valid=all_valid,
            required_valid=required_valid,
            failures=failures,
            warnings=warnings,
        )

        # Log summary
        if summary.required_valid:
            logger.info("✓ All required RBAC permissions validated successfully")
        else:
            logger.error(
                f"✗ Missing required permissions: {', '.join(summary.failures)}"
            )

        if summary.warnings:
            logger.warning(
                f"⚠ Missing optional permissions: {', '.join(summary.warnings)}"
            )

        return summary

    def validate_startup_permissions(self) -> bool:
        """
        Validate required permissions at pipeline startup.

        Returns:
            True if all required permissions are present, False otherwise
        """
        summary = self.validate_all_permissions()

        if not summary.required_valid:
            logger.error("Pipeline cannot start due to insufficient RBAC permissions")
            audit.log_authz_event(
                event_type=AuditEventType.AUTHZ_RBAC_DENIED,
                permission="pipeline_execution",
                success=False,
                reason=f"Missing required permissions: {', '.join(summary.failures)}",
            )
            return False

        logger.info("RBAC permission validation passed - pipeline can proceed")
        return True

    def get_permission_report(self) -> str:
        """
        Generate human-readable permission validation report.

        Returns:
            Formatted report string
        """
        summary = self.validate_all_permissions()

        report = []
        report.append("=" * 60)
        report.append("RBAC Permission Validation Report")
        report.append("=" * 60)

        for result in summary.results:
            req = result.requirement
            status = "✓" if result.has_permission else "✗"
            required_marker = "[REQUIRED]" if req.required else "[OPTIONAL]"

            report.append(f"\n{status} {req.role.value} {required_marker}")
            report.append(f"   Resource: {req.resource_type.value}")
            report.append(f"   Description: {req.description}")

            if result.has_permission:
                report.append(f"   Status: Permission granted")
            else:
                report.append(f"   Status: Permission MISSING")

        report.append("\n" + "=" * 60)
        report.append("Summary:")
        report.append(f"  All permissions valid: {summary.all_valid}")
        report.append(f"  Required permissions valid: {summary.required_valid}")

        if summary.failures:
            report.append(f"\n  ✗ Missing required permissions:")
            for failure in summary.failures:
                report.append(f"    - {failure}")

        if summary.warnings:
            report.append(f"\n  ⚠ Missing optional permissions:")
            for warning in summary.warnings:
                report.append(f"    - {warning}")

        report.append("=" * 60)

        return "\n".join(report)


def validate_rbac_startup() -> bool:
    """
    Convenience function for startup RBAC validation.

    Returns:
        True if validation passes, False otherwise
    """
    validator = RBACValidator()
    return validator.validate_startup_permissions()


def print_permission_report() -> None:
    """Print RBAC permission validation report to console."""
    validator = RBACValidator()
    print(validator.get_permission_report())
