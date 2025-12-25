"""
Structured logging module.

Provides JSON logging with correlation IDs and context propagation.

Components to extract and review from verisk_pipeline.common.logging:
    - Structured JSON formatter
    - Context managers for log context (cycle, stage, phase)
    - Memory checkpoint logging
    - Log rotation and file management

New functionality to add:
    - Kafka offset context (topic, partition, offset)
    - Consumer group context
    - OpenTelemetry trace ID propagation

Review checklist:
    [ ] JSON output is valid and parseable
    [ ] Context propagation works across async boundaries
    [ ] Sensitive data is not logged (PII, tokens, secrets)
    [ ] Log levels are appropriate
    [ ] Performance impact of structured logging
"""
