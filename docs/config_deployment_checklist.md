# Config Deployment Checklist

**Date:** 2026-01-10
**Purpose:** Ensure safe deployment of multi-file config system
**Note:** Single-file config.yaml is no longer supported

---

## Pre-Deployment Validation

### 1. Local Testing
- [ ] All unit tests pass: `pytest tests/kafka_pipeline/test_config.py -v`
- [ ] Integration tests pass: `pytest tests/kafka_pipeline/integration/ -v`
- [ ] Config validation passes: `python -m kafka_pipeline.config --validate`
- [ ] Merged config matches expected structure: `python -m kafka_pipeline.config --show-merged`

### 2. Configuration Files Ready
- [ ] `config/shared.yaml` created with connection and defaults
- [ ] `config/xact_config.yaml` created with XACT domain settings
- [ ] `config/claimx_config.yaml` created with ClaimX domain settings
- [ ] `config/plugins/` directory created (if using plugins)
- [ ] All required topic names configured
- [ ] All worker settings configured
- [ ] Environment-specific values identified (bootstrap_servers, etc.)

### 3. Configuration Verified
- [ ] All config files properly formatted YAML
- [ ] No syntax errors in any config file
- [ ] All required sections present

---

## Deployment Steps

### Step 1: Backup Current Configuration
```bash
# Archive config directory
tar -czf config_backup_$(date +%Y%m%d_%H%M%S).tar.gz config/
```

**Checklist:**
- [ ] Backup created
- [ ] Backup verified (can be extracted)
- [ ] Backup stored in safe location

### Step 2: Create Config Directory Structure
```bash
# Create config directory structure
mkdir -p config/plugins

# Verify directory exists
ls -la config/
```

**Checklist:**
- [ ] `config/` directory created
- [ ] `config/plugins/` directory created
- [ ] Permissions correct (readable by application user)

### Step 3: Copy Split Config Files
```bash
# Copy split config files to production location
# (Adjust paths based on your deployment method)

# Option A: Direct copy (local deployment)
cp config/shared.yaml /path/to/production/config/
cp config/xact_config.yaml /path/to/production/config/
cp config/claimx_config.yaml /path/to/production/config/
cp config/plugins/*.yaml /path/to/production/config/plugins/

# Option B: Deploy via Docker/Kubernetes
# Update your Dockerfile/K8s ConfigMap to include config/ directory

# Option C: Deploy via configuration management (Ansible, Terraform, etc.)
# Update your IaC scripts to include new config files
```

**Checklist:**
- [ ] All config files copied to deployment location
- [ ] File permissions correct
- [ ] Files readable by application
- [ ] Sensitive values (tokens, passwords) set via environment variables

### Step 4: Update Environment Variables
```bash
# Verify required environment variables are set
echo $CLAIMX_API_TOKEN  # Should be set
echo $BOOTSTRAP_SERVERS # Optional override

# Set environment variables in production
# (Method depends on deployment: systemd, Docker, K8s, etc.)
```

**Environment-Specific Variables:**
- [ ] `CLAIMX_API_TOKEN` set correctly
- [ ] `BOOTSTRAP_SERVERS` set if overriding config (optional)
- [ ] Any other environment-specific settings configured

### Step 5: Validate Configuration in Production Environment
```bash
# On production server, validate config before starting services
python -m kafka_pipeline.config --validate

# Expected output:
# ✓ Configuration validation passed
#   - Shared settings: OK
#   - XACT domain: OK
#   - ClaimX domain: OK
#   - Merge integrity: OK
```

**Checklist:**
- [ ] Validation passes in production environment
- [ ] No missing required keys
- [ ] All domains configured correctly
- [ ] Bootstrap servers resolve correctly
- [ ] Topics exist in Kafka cluster

### Step 6: Deploy Application
```bash
# Deploy updated application code
# (Method depends on deployment: systemd restart, Docker redeploy, K8s rollout, etc.)

# Example: systemd
sudo systemctl restart kafka-pipeline-xact
sudo systemctl restart kafka-pipeline-claimx

# Example: Docker Compose
docker-compose down
docker-compose up -d

# Example: Kubernetes
kubectl rollout restart deployment/kafka-pipeline-xact
kubectl rollout restart deployment/kafka-pipeline-claimx
```

**Checklist:**
- [ ] Application deployed successfully
- [ ] Services started without errors
- [ ] Health checks passing
- [ ] Logs show config loaded correctly

### Step 7: Verify Deployment
```bash
# Check service status
systemctl status kafka-pipeline-* || docker ps || kubectl get pods

# Check application logs for config loading
tail -f /var/log/kafka-pipeline/*.log | grep -i config

# Expected log entries:
# DEBUG: Loading configuration from: /path/to/config/shared.yaml
# DEBUG: Successfully loaded config file: /path/to/config/shared.yaml
# DEBUG: Configuration merge complete
# DEBUG: Configuration validation passed
```

**Checklist:**
- [ ] Services running
- [ ] No config errors in logs
- [ ] Workers connecting to Kafka successfully
- [ ] Messages being processed correctly
- [ ] Metrics/monitoring showing normal operation

### Step 8: Monitor for 24-48 Hours
**Monitoring checklist:**
- [ ] Message throughput normal
- [ ] No increase in error rates
- [ ] No configuration-related exceptions
- [ ] Consumer lag within acceptable limits
- [ ] Producer publish rates normal
- [ ] No dead-letter-queue spikes

---

## Rollback Plan

### When to Rollback
Rollback immediately if:
- Configuration validation fails in production
- Services fail to start after deployment
- Critical functionality broken (messages not processing)
- Error rates spike above 5%
- Consumer lag increases uncontrollably

### Rollback Steps

#### Restore from Backup

```bash
# 1. Stop services
sudo systemctl stop kafka-pipeline-* || docker-compose down || kubectl scale deployment --replicas=0

# 2. Find latest backup
LATEST_BACKUP=$(ls -t config_backup_*.tar.gz | head -1)

# 3. Restore config directory from backup
rm -rf config/
tar -xzf $LATEST_BACKUP

# 4. Verify config restored
python -m kafka_pipeline.config --validate

# 5. Restart services
sudo systemctl start kafka-pipeline-* || docker-compose up -d || kubectl scale deployment --replicas=1

# 6. Verify services running
systemctl status kafka-pipeline-* || docker ps || kubectl get pods
```

**Checklist:**
- [ ] Services stopped
- [ ] Config directory restored from backup
- [ ] Config validation passes
- [ ] Services restarted
- [ ] Normal operation resumed

### Post-Rollback Actions
- [ ] Document what went wrong
- [ ] Investigate root cause
- [ ] Fix issues in development
- [ ] Re-test before next deployment attempt
- [ ] Update deployment checklist with lessons learned

---

## Environment-Specific Considerations

### Development Environment
- Use local Kafka: `bootstrap_servers: "localhost:9092"`
- Use PLAINTEXT security: `security_protocol: "PLAINTEXT"`
- Keep `config/local.yaml` in `.gitignore`
- Use smaller batch sizes for easier debugging

### Staging Environment
- Mirror production configuration structure
- Use staging Kafka cluster
- Test full deployment procedure
- Validate with production-like data volumes
- Run integration tests

### Production Environment
- Use production Kafka cluster
- Use SASL_SSL security: `security_protocol: "SASL_SSL"`
- Set secrets via environment variables only
- Monitor closely for 24-48 hours
- Keep old config.yaml for 2-4 weeks

---

## Deployment Scripts

### Automated Deployment Script
Create `scripts/deploy_config.sh`:

```bash
#!/bin/bash
set -e

ENVIRONMENT=${1:-dev}
BACKUP_DIR="./backups"
CONFIG_DIR="./config"
PROD_CONFIG_DIR="/opt/kafka-pipeline/config"

echo "Deploying config for environment: $ENVIRONMENT"

# Step 1: Backup
mkdir -p $BACKUP_DIR
if [ -f "$PROD_CONFIG_DIR/../src/config.yaml" ]; then
    cp "$PROD_CONFIG_DIR/../src/config.yaml" "$BACKUP_DIR/config.yaml.$(date +%Y%m%d_%H%M%S)"
    echo "✓ Backup created"
fi

# Step 2: Validate local config
python -m kafka_pipeline.config --validate
if [ $? -ne 0 ]; then
    echo "✗ Local validation failed"
    exit 1
fi
echo "✓ Local validation passed"

# Step 3: Copy files
mkdir -p "$PROD_CONFIG_DIR/plugins"
cp $CONFIG_DIR/shared.yaml "$PROD_CONFIG_DIR/"
cp $CONFIG_DIR/xact_config.yaml "$PROD_CONFIG_DIR/"
cp $CONFIG_DIR/claimx_config.yaml "$PROD_CONFIG_DIR/"
cp $CONFIG_DIR/plugins/*.yaml "$PROD_CONFIG_DIR/plugins/" 2>/dev/null || true
echo "✓ Config files copied"

# Step 4: Validate production config
cd /opt/kafka-pipeline
python -m kafka_pipeline.config --validate
if [ $? -ne 0 ]; then
    echo "✗ Production validation failed"
    echo "Rolling back..."
    cp "$BACKUP_DIR/config.yaml.latest" "$PROD_CONFIG_DIR/../src/config.yaml"
    exit 1
fi
echo "✓ Production validation passed"

echo "✓ Deployment complete"
echo "  - Monitor logs: tail -f /var/log/kafka-pipeline/*.log"
echo "  - Check status: systemctl status kafka-pipeline-*"
```

Make it executable:
```bash
chmod +x scripts/deploy_config.sh
```

### Rollback Script
Create `scripts/rollback_config.sh`:

```bash
#!/bin/bash
set -e

BACKUP_DIR="./backups"
PROD_CONFIG_DIR="/opt/kafka-pipeline"

echo "Rolling back configuration..."

# Find latest backup
LATEST_BACKUP=$(ls -t $BACKUP_DIR/config_backup_*.tar.gz | head -1)

if [ -z "$LATEST_BACKUP" ]; then
    echo "✗ No backup found"
    exit 1
fi

echo "Using backup: $LATEST_BACKUP"

# Stop services
echo "Stopping services..."
sudo systemctl stop kafka-pipeline-*

# Restore backup
cd "$PROD_CONFIG_DIR"
rm -rf config/
tar -xzf "$BACKUP_DIR/$LATEST_BACKUP"
echo "✓ Config restored"

# Validate restored config
python -m kafka_pipeline.config --validate
if [ $? -ne 0 ]; then
    echo "✗ Restored config validation failed"
    exit 1
fi

# Restart services
echo "Starting services..."
sudo systemctl start kafka-pipeline-*

# Verify
sleep 5
sudo systemctl status kafka-pipeline-* --no-pager

echo "✓ Rollback complete"
```

Make it executable:
```bash
chmod +x scripts/rollback_config.sh
```

---

## Success Criteria

Deployment is considered successful when:
- [ ] All services started without errors
- [ ] Config validation passes in production
- [ ] Message processing continues normally
- [ ] No config-related errors in logs
- [ ] Consumer lag remains stable
- [ ] Producer throughput unchanged
- [ ] No increase in dead-letter queue
- [ ] Monitoring dashboards show green
- [ ] 24 hours of stable operation

---

## Post-Deployment Cleanup

After 2-4 weeks of stable operation:
- [ ] Remove old config backups
- [ ] Verify documentation is up to date
- [ ] Archive old deployment scripts
- [ ] Update CI/CD pipelines if needed

---

## Contact Information

**Deployment Owner:** [Your Name]
**Escalation:** [Team Lead]
**On-Call:** [Pager Duty / Phone]

**Support Resources:**
- Config documentation: `/home/nick/projects/nsmkdvPipe/docs/config_loading.md`
- Validation tool: `python -m kafka_pipeline.config --validate`
- Logs: `/var/log/kafka-pipeline/*.log`
- Monitoring: [Dashboard URL]

---

## Revision History

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2026-01-10 | 1.0 | Initial checklist | Agent 4 |
