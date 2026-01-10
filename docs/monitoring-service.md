# Monitoring Service for External Health Checks

This service provides HTTP endpoints that external monitoring systems (like Iris) can call to check the health of the Kafka pipeline workers.

## Overview

The monitoring service queries Prometheus to determine the health status of all workers and exposes four HTTP endpoints:

- **`GET /health`** - Overall system health (returns 200 if all workers up, 503 otherwise)
- **`GET /health/workers`** - Detailed health status for each worker
- **`GET /health/prometheus`** - Check if Prometheus itself is reachable
- **`GET /status`** - Worker status with consumer lag information

## Quick Start

### Using Docker Compose

1. Start the observability stack:
   ```bash
   docker-compose -f docker-compose.obs.yml up -d
   ```

2. The monitoring service will be available at `http://localhost:9091`

3. Test the endpoints:
   ```bash
   # Check overall health
   curl http://localhost:9091/health

   # Check detailed worker status
   curl http://localhost:9091/health/workers

   # Check Prometheus connectivity
   curl http://localhost:9091/health/prometheus

   # Check worker status with consumer lag
   curl http://localhost:9091/status
   ```

### Running Standalone

```bash
cd src
python -m kafka_pipeline.common.monitoring_service \
  --prometheus-url http://localhost:9090 \
  --port 9091
```

## API Endpoints

### GET /health

Returns overall system health based on all workers.

**Response Codes:**
- `200 OK` - All workers are up and healthy
- `503 Service Unavailable` - One or more workers are down
- `503 Service Unavailable` - No workers found or error querying Prometheus

**Response (all healthy):**
```json
{
  "status": "healthy",
  "workers_total": 12,
  "workers_up": 12,
  "workers_down": 0,
  "timestamp": "2026-01-10T12:00:00.000000+00:00"
}
```

**Response (unhealthy):**
```json
{
  "status": "unhealthy",
  "workers_total": 12,
  "workers_up": 10,
  "workers_down": 2,
  "down_workers": [
    {"job": "claimx-upload-worker", "instance": "host.docker.internal:8083"},
    {"job": "xact-download-worker", "instance": "host.docker.internal:8090"}
  ],
  "timestamp": "2026-01-10T12:00:00.000000+00:00"
}
```

### GET /health/workers

Returns detailed health status for each worker.

**Response Code:** `200 OK` (always, unless error)

**Response:**
```json
{
  "workers": [
    {
      "job": "claimx-enrichment-worker",
      "instance": "host.docker.internal:8081",
      "domain": "claimx",
      "worker_type": "enrichment",
      "status": "up"
    },
    {
      "job": "xact-event-ingester",
      "instance": "host.docker.internal:8080",
      "domain": "xact",
      "worker_type": "event_ingester",
      "status": "down"
    }
  ],
  "total": 12,
  "up": 11,
  "down": 1,
  "timestamp": "2026-01-10T12:00:00.000000+00:00"
}
```

### GET /health/prometheus

Checks if Prometheus itself is reachable and healthy.

**Response Codes:**
- `200 OK` - Prometheus is healthy
- `503 Service Unavailable` - Prometheus is unreachable

**Response:**
```json
{
  "status": "healthy",
  "prometheus_url": "http://prometheus:9090",
  "timestamp": "2026-01-10T12:00:00.000000+00:00"
}
```

### GET /status

Returns worker health status along with Kafka consumer lag metrics for each worker.

**Response Code:** `200 OK` (always, unless error)

**Response:**
```json
{
  "workers": [
    {
      "job": "claimx-enrichment-worker",
      "instance": "host.docker.internal:8081",
      "domain": "claimx",
      "worker_type": "enrichment",
      "status": "up",
      "consumer_lag": {
        "consumer_group": "claimx-enrichment",
        "total_lag": 150,
        "topics": [
          {
            "topic": "claimx.enrichment.tasks",
            "total_lag": 150,
            "partitions": [
              {"partition": 0, "lag": 75},
              {"partition": 1, "lag": 75}
            ]
          }
        ]
      }
    },
    {
      "job": "xact-event-ingester",
      "instance": "host.docker.internal:8080",
      "domain": "xact",
      "worker_type": "event_ingester",
      "status": "up",
      "consumer_lag": null
    }
  ],
  "total_workers": 12,
  "workers_up": 12,
  "workers_down": 0,
  "consumer_groups": [
    {
      "consumer_group": "claimx-enrichment",
      "total_lag": 150,
      "topics": [
        {
          "topic": "claimx.enrichment.tasks",
          "total_lag": 150,
          "partitions": [
            {"partition": 0, "lag": 75},
            {"partition": 1, "lag": 75}
          ]
        }
      ]
    }
  ],
  "timestamp": "2026-01-10T12:00:00.000000+00:00"
}
```

**Field Descriptions:**
- `workers[].consumer_lag`: Consumer lag information for this worker's consumer group (null if not consuming)
- `workers[].consumer_lag.total_lag`: Total number of messages behind across all partitions
- `workers[].consumer_lag.topics`: Breakdown by topic
- `consumer_groups`: All consumer groups in the system with their lag metrics

**Use Cases:**
- Monitor which workers are falling behind in message processing
- Alert when consumer lag exceeds thresholds (e.g., total_lag > 1000)
- Track per-topic and per-partition lag for troubleshooting
- Identify slow consumers that may need scaling

## Integration with Iris

To integrate with Iris monitoring service:

1. **Register the monitoring endpoint** in Iris using the `/health` endpoint:
   - URL: `https://your-domain.com:9091/health` (or whatever your public URL is)
   - Expected status: `200` for healthy, `503` for unhealthy
   - Check interval: Recommended 30-60 seconds

2. **Optional: Use detailed endpoint** for more granular monitoring:
   - URL: `https://your-domain.com:9091/health/workers`
   - Parse the JSON response to track individual worker health

3. **Monitor consumer lag** for performance tracking:
   - URL: `https://your-domain.com:9091/status`
   - Track `workers[].consumer_lag.total_lag` to identify backlog issues
   - Alert when lag exceeds threshold (e.g., > 1000 messages)

4. **Configure alerts** in Iris based on:
   - Overall health status code (503 = trigger alert)
   - `workers_down` count (> 0 = warning, > 2 = critical)
   - Specific worker outages by checking `down_workers` list
   - Consumer lag thresholds (warning if > 1000, critical if > 10000)

## How It Works

1. The monitoring service queries Prometheus using PromQL:
   ```promql
   # Worker health status
   up{job=~".*(worker|ingester).*"}

   # Consumer lag metrics
   kafka_consumer_lag
   ```

2. Prometheus maintains metrics for each worker:
   - `up=1` means the target is reachable and returning metrics
   - `up=0` means the target is down or unreachable
   - `kafka_consumer_lag` shows how many messages behind each consumer is

3. Workers expose these metrics by:
   - Running health check HTTP servers on configured ports
   - Using `prometheus_client` library to expose `/metrics` endpoint
   - Calling `update_consumer_lag()` to report lag values

4. The service aggregates this data and returns it in a format suitable for external monitoring systems

## Troubleshooting

### "No workers found in Prometheus"

- Check that Prometheus is scraping your workers (visit `http://localhost:9090/targets`)
- Ensure workers are running and exposing metrics endpoints
- Verify the Prometheus configuration in `observability/prometheus/prometheus.yml`

### "Cannot connect to Prometheus"

- Ensure Prometheus is running: `docker ps | grep prometheus`
- Check the Prometheus URL is correct
- Verify network connectivity between services

### Monitoring service not starting

- Check Docker logs: `docker logs kafka-pipeline-monitoring`
- Ensure port 9091 is not already in use
- Verify the build succeeded: `docker-compose -f docker-compose.obs.yml build monitoring-service`

## Configuration

### Environment Variables

None currently required. Configuration is via command-line arguments.

### Command-Line Arguments

- `--prometheus-url`: Prometheus base URL (default: `http://localhost:9090`)
- `--port`: HTTP port to listen on (default: `9091`)

### Docker Compose Override

To change the port or Prometheus URL, edit `docker-compose.obs.yml`:

```yaml
monitoring-service:
  command: python -m kafka_pipeline.common.monitoring_service --prometheus-url http://prometheus:9090 --port 9091
  ports:
    - "9091:9091"  # Change external port here
```

## Security Considerations

- **No authentication**: The service currently has no authentication. Use firewall rules or a reverse proxy (nginx, Caddy) to restrict access.
- **Internal use**: Designed for internal monitoring systems within your network.
- **HTTPS**: For production, put behind a reverse proxy with TLS (Let's Encrypt recommended).

Example nginx config for HTTPS:
```nginx
server {
    listen 443 ssl;
    server_name monitoring.your-domain.com;

    ssl_certificate /etc/letsencrypt/live/monitoring.your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/monitoring.your-domain.com/privkey.pem;

    location / {
        proxy_pass http://localhost:9091;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```
