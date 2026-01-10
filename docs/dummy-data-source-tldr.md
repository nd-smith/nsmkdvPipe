# Dummy Data Source TLDR

Test the pipeline locally without access to production data sources.

## Quick Start

```bash
# 1. Start local Kafka
docker-compose -f docker-compose.kafka.yml up -d

# 2. Run the dummy data source
python -m kafka_pipeline --worker dummy-source --dev

# 3. Run workers in other terminals to process generated events
python -m kafka_pipeline --worker xact-download --dev
python -m kafka_pipeline --worker xact-upload --dev
```

## What It Does

```
┌─────────────────────┐
│  Dummy Data Source  │
│  ┌───────────────┐  │     ┌─────────────────┐
│  │ Data Generator│──┼────>│ xact.events.raw │
│  │ (claims, files│  │     │claimx.events.raw│
│  │  addresses...)│  │     └─────────────────┘
│  └───────────────┘  │              │
│  ┌───────────────┐  │              v
│  │  File Server  │<─┼───── Download Workers
│  │  (port 8765)  │  │      fetch test files
│  └───────────────┘  │
└─────────────────────┘
```

- **Generates** realistic insurance claim events for XACT and ClaimX domains
- **Serves** downloadable test files (valid JPEGs and PDFs)
- **Produces** to the same Kafka topics as production sources

## Generated Data Examples

| Data Type | Example |
|-----------|---------|
| Claim ID | `CLM-A8F3K2M1` |
| Policy | `POL-847293-42` |
| Policyholder | `Sarah Thompson` |
| Address | `4521 Oak Ave, Austin, TX 78701` |
| Damage Type | `Water Damage`, `Fire Damage`, `Hail Damage` |
| Files | `exterior_damage_1.jpg`, `estimate_1.pdf` |

## Configuration

Edit `src/config.yaml`:

```yaml
# Minimal config
dummy:
  events_per_minute: 10.0    # Events per minute per domain
  domains: ["xact", "claimx"]

# For load testing
dummy:
  burst_mode: true
  burst_size: 100            # Events per burst
  burst_interval_seconds: 30

# For reproducible testing
dummy:
  generator:
    seed: 12345              # Same seed = same data
  max_events: 500            # Stop after 500 events
```

## Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `events_per_minute` | 10.0 | Generation rate per domain |
| `domains` | xact, claimx | Which event types to generate |
| `burst_mode` | false | Generate in batches vs steady stream |
| `max_events` | null | Stop after N events (null = unlimited) |
| `max_runtime_seconds` | null | Stop after N seconds |
| `file_server.port` | 8765 | Port for test file downloads |
| `generator.seed` | null | For reproducible data |

## ClaimX Event Types Generated

Events are generated with realistic frequency distribution:

| Event Type | Frequency |
|------------|-----------|
| `PROJECT_FILE_ADDED` | Very Common |
| `CUSTOM_TASK_ASSIGNED` | Common |
| `CUSTOM_TASK_COMPLETED` | Common |
| `PROJECT_CREATED` | Occasional |
| `POLICYHOLDER_INVITED` | Occasional |
| `POLICYHOLDER_JOINED` | Occasional |
| `VIDEO_COLLABORATION_*` | Rare |
| `PROJECT_MFN_ADDED` | Rare |

## Testing Scenarios

### Basic Pipeline Test
```bash
# Terminal 1: Generate events
python -m kafka_pipeline --worker dummy-source --dev

# Terminal 2: Process XACT events
python -m kafka_pipeline --worker xact-download --dev
```

### Load Test
```yaml
# config.yaml
dummy:
  burst_mode: true
  burst_size: 200
  burst_interval_seconds: 10
```

### Reproducible Test
```yaml
dummy:
  generator:
    seed: 42
  max_events: 100
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Connection refused" on download | Dummy source not running, or wrong port |
| No events appearing | Check Kafka is running, check logs |
| Files not downloading | File server runs on port 8765 by default |

## File Server Endpoints

The dummy file server serves test files at:
```
GET http://localhost:8765/files/{project_id}/{media_id}/{filename}
GET http://localhost:8765/health
```

Files are generated on-demand based on the URL - same URL always returns same content.
