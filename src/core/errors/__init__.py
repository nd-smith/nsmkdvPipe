"""
Error classification and exception hierarchy.

Provides consistent error handling across the pipeline.

Components to extract and review from verisk_pipeline:
    - ErrorCategory enum (TRANSIENT, PERMANENT, AUTH, CIRCUIT_OPEN)
    - PipelineError exception hierarchy
    - StorageErrorClassifier (Azure error code mapping)

New functionality to add:
    - KafkaErrorClassifier (Kafka-specific error codes)
    - Error context preservation through retry chain

Review checklist:
    [ ] All Azure error codes are correctly classified
    [ ] Transient vs permanent classification is accurate
    [ ] Exception hierarchy is not overly complex
    [ ] Error messages are actionable
    [ ] Sensitive information is not included in error messages
"""
