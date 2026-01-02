# Kafka Pipeline Commands Cheat-Sheet

Quick reference for managing and debugging the Kafka-based pipeline.

## Docker Compose Management

```bash
# Start Kafka (with topic initialization)
docker-compose -f docker-compose.kafka.yml up -d

# Start Kafka + Web UI (http://localhost:8080)
docker-compose -f docker-compose.kafka.yml --profile ui up -d

# Stop everything
docker-compose -f docker-compose.kafka.yml down

# Stop and remove volumes (fresh start)
docker-compose -f docker-compose.kafka.yml down -v
```

## Watching Messages in Real-Time

```bash
# Watch incoming pending downloads
docker exec -it kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.pending \
  --from-beginning

# Watch cached (downloaded, awaiting upload)
docker exec -it kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.cached

# Watch results (completed uploads)
docker exec -it kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.results

# Watch dead-letter queue (failures)
docker exec -it kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.dlq

# Watch raw events
docker exec -it kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.events.raw
```

> **Tip:** Remove `--from-beginning` to see only new messages.

### Pretty-Print JSON Messages

```bash
# Pipe through jq for readable output
docker exec -it kafka-pipeline-local kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.pending | jq .
```

## Topic Management

```bash
# List all topics
docker exec -it kafka-pipeline-local kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Describe a topic (partitions, replicas, configs)
docker exec -it kafka-pipeline-local kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic xact.downloads.pending

# Get message count per partition
docker exec -it kafka-pipeline-local kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic xact.downloads.pending
```

## Consumer Group Monitoring

```bash
# List consumer groups
docker exec -it kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# Check consumer lag (how far behind)
docker exec -it kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group xact-download-worker

# Reset consumer to beginning (use with caution!)
docker exec -it kafka-pipeline-local kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group xact-download-worker \
  --topic xact.downloads.pending \
  --reset-offsets --to-earliest --execute
```

## Producing Test Messages

```bash
# Interactive producer (type JSON, press Enter)
docker exec -it kafka-pipeline-local kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.pending

# Produce from file
cat test_messages.json | docker exec -i kafka-pipeline-local \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic xact.downloads.pending
```

## Topics Reference

| Topic | Purpose |
|-------|---------|
| `xact.events.raw` | Raw events from Eventhouse poller |
| `xact.downloads.pending` | Main work queue for download worker |
| `xact.downloads.cached` | Downloaded files awaiting upload |
| `xact.downloads.results` | Completed uploads |
| `xact.downloads.dlq` | Failed messages after all retries |
| `xact.downloads.pending.retry.{5,10,20,40}m` | Delayed retry topics |

## Health Checks

```bash
# Quick connectivity test
docker exec -it kafka-pipeline-local kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# Check if Kafka is healthy
docker exec -it kafka-pipeline-local kafka-topics.sh \
  --bootstrap-server localhost:9092 --list > /dev/null && echo "OK" || echo "FAIL"
```

## Web UI

When started with `--profile ui`, Kafka UI is available at **http://localhost:8080**

Features:
- Browse topics and messages
- View consumer group lag
- Produce test messages
- Monitor broker health
