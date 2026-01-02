# Kafka Pipeline Schema Mappings

This document provides a comprehensive reference of all schema mappings and columns used by the kafka_pipeline writers, aligned with verisk_pipeline for production compatibility.

---

## 1. Eventhouse Source Schema

Raw data from Microsoft Fabric Eventhouse.

| Column | Type | Description |
|--------|------|-------------|
| `type` | string | Full event type (e.g., `verisk.claims.property.xn.documentsReceived`) |
| `version` | string | Event version number |
| `utcDateTime` | string | Event timestamp in ISO format |
| `traceId` | string | Unique trace identifier (UUID) |
| `data` | string (JSON) | Nested JSON containing event payload, attachments, etc. |

### Nested `data` Field Structure

| JSON Path | Type | Description |
|-----------|------|-------------|
| `data.assignmentId` | string | Assignment identifier |
| `data.description` | string | Event description |
| `data.attachments` | array[string] | List of attachment URLs |
| `data.estimateVersion` | string | Estimate version (optional) |
| `data.note` | string | Event note (optional) |
| `data.author` | string | Event author (optional) |
| `data.adm.coverageLoss.claimNumber` | string | Claim number (nested) |
| `data.contact.*` | object | Contact information fields |

---

## 2. EventMessage Schema

Pydantic model for raw events consumed from Kafka topics. Matches `verisk_pipeline.xact.xact_models.EventRecord`.

| Field | Type | Alias | Source | Description |
|-------|------|-------|--------|-------------|
| `type` | str | - | `type` | Full event type string |
| `version` | str | - | `version` | Event version |
| `utc_datetime` | str | `utcDateTime` | `utcDateTime` | Event timestamp |
| `trace_id` | str | `traceId` | `traceId` | Unique event identifier |
| `data` | str | - | `data` | Raw JSON string with nested data |

### Computed Properties

| Property | Type | Derived From | Description |
|----------|------|--------------|-------------|
| `status_subtype` | str | `type.split(".")[-1]` | Last part of event type (e.g., `documentsReceived`) |
| `data_dict` | dict | `json.loads(data)` | Parsed data as dictionary |
| `attachments` | list[str] | `data_dict["attachments"]` | List of attachment URLs |
| `assignment_id` | str | `data_dict["assignmentId"]` | Assignment ID |
| `estimate_version` | str | `data_dict["estimateVersion"]` | Estimate version |

---

## 3. DownloadTaskMessage Schema

Pydantic model for download work items. Matches `verisk_pipeline.xact.xact_models.Task`.

| Field | Type | Required | Source | Description |
|-------|------|----------|--------|-------------|
| `trace_id` | str | Yes | `EventMessage.trace_id` | Unique event identifier |
| `attachment_url` | str | Yes | `EventMessage.attachments[i]` | URL to download |
| `blob_path` | str | Yes | `generate_blob_path()` | Target path in OneLake |
| `status_subtype` | str | Yes | `EventMessage.status_subtype` | Event status subtype |
| `file_type` | str | Yes | `generate_blob_path()` | File extension (pdf, esx, jpg) |
| `assignment_id` | str | Yes | `EventMessage.assignment_id` | Assignment ID |
| `estimate_version` | str | No | `EventMessage.estimate_version` | Estimate version |
| `retry_count` | int | No (default: 0) | Worker tracking | Retry attempt count |

---

## 4. DownloadResultMessage Schema

Pydantic model for download outcomes. Matches `verisk_pipeline.xact.xact_models.Task.to_tracking_row()`.

| Field | Type | Required | Source | Description |
|-------|------|----------|--------|-------------|
| `trace_id` | str | Yes | From task | Unique event identifier |
| `attachment_url` | str | Yes | From task | URL that was processed |
| `blob_path` | str | Yes | From task | Target path in OneLake |
| `status_subtype` | str | Yes | From task | Event status subtype |
| `file_type` | str | Yes | From task | File extension |
| `assignment_id` | str | Yes | From task | Assignment ID |
| `status` | str | Yes | Worker result | `completed`, `failed`, `failed_permanent` |
| `http_status` | int | No | HTTP response | Response status code |
| `bytes_downloaded` | int | No (default: 0) | Worker result | Downloaded file size |
| `retry_count` | int | No (default: 0) | From task | Retry attempts made |
| `error_message` | str | No | Worker result | Error description (max 500 chars) |
| `created_at` | datetime | Yes | Worker result | Result creation timestamp |
| `expires_at` | datetime | No | URL parsing | URL expiration time |
| `expired_at_ingest` | bool | No | URL parsing | Whether URL was expired at ingest |

---

## 5. DeltaEventsWriter Output Schema (xact_events)

Uses `flatten_events()` from `verisk_pipeline.xact.stages.transform`. Writes 28 columns to xact_events Delta table.

### Core Fields

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `type` | Utf8 | `type` | Full event type string |
| `status_subtype` | Utf8 | `type.split(".")[-1]` | Event status subtype |
| `version` | Utf8 | `version` | Event version |
| `trace_id` | Utf8 | `traceId` | Unique trace identifier |
| `ingested_at` | Datetime | Pipeline timestamp | When event was ingested |
| `event_date` | Date | `utcDateTime` | Event date (partitioning) |
| `event_datetime_mdt` | Datetime | `utcDateTime` (converted) | Event datetime in MDT |

### Assignment & Claim Fields

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `assignment_id` | Utf8 | `data.assignmentId` | Assignment identifier |
| `original_assignment_id` | Utf8 | `data.originalAssignmentId` | Original assignment ID |
| `claim_number` | Utf8 | `data.adm.coverageLoss.claimNumber` | Claim number |
| `carrier_id` | Utf8 | `data.carrierId` | Carrier identifier |
| `xn_address` | Utf8 | `data.xnAddress` | XN address |

### Content Fields

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `description` | Utf8 | `data.description` | Event description |
| `note` | Utf8 | `data.note` | Event note |
| `author` | Utf8 | `data.author` | Event author |
| `estimate_version` | Utf8 | `data.estimateVersion` | Estimate version |
| `attachments` | Utf8 | `data.attachments` (comma-joined) | Attachment URLs |
| `raw_json` | Utf8 | `data` | Full raw JSON string |

### Reviewer Fields

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `sender_reviewer_name` | Utf8 | `data.senderReviewerName` | Sender reviewer name |
| `sender_reviewer_email` | Utf8 | `data.senderReviewerEmail` | Sender reviewer email |
| `carrier_reviewer_name` | Utf8 | `data.carrierReviewerName` | Carrier reviewer name |
| `carrier_reviewer_email` | Utf8 | `data.carrierReviewerEmail` | Carrier reviewer email |

### Contact Fields

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `contact_name` | Utf8 | `data.contact.name` | Contact name |
| `contact_address` | Utf8 | `data.contact.address` | Contact address |
| `contact_city` | Utf8 | `data.contact.city` | Contact city |
| `contact_state` | Utf8 | `data.contact.state` | Contact state |
| `contact_zip` | Utf8 | `data.contact.zip` | Contact ZIP code |
| `contact_phone` | Utf8 | `data.contact.phone` | Contact phone |
| `contact_email` | Utf8 | `data.contact.email` | Contact email |

### Pipeline Metadata

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `created_at` | Datetime | Pipeline timestamp | When record was created |

---

## 6. DeltaInventoryWriter Output Schema (xact_attachments)

Matches `verisk_pipeline.xact.xact_models.Task.to_tracking_row()` output.

| Column | Polars Type | Source | Description |
|--------|-------------|--------|-------------|
| `trace_id` | Utf8 | `DownloadResultMessage.trace_id` | Unique event identifier (PK) |
| `attachment_url` | Utf8 | `DownloadResultMessage.attachment_url` | Attachment URL (PK) |
| `blob_path` | Utf8 | `DownloadResultMessage.blob_path` | Path in OneLake |
| `status_subtype` | Utf8 | `DownloadResultMessage.status_subtype` | Event status subtype |
| `file_type` | Utf8 | `DownloadResultMessage.file_type` | File extension |
| `assignment_id` | Utf8 | `DownloadResultMessage.assignment_id` | Assignment ID |
| `status` | Utf8 | `DownloadResultMessage.status` | Download status |
| `http_status` | Int64 | `DownloadResultMessage.http_status` | HTTP response code |
| `bytes_downloaded` | Int64 | `DownloadResultMessage.bytes_downloaded` | File size |
| `retry_count` | Int64 | `DownloadResultMessage.retry_count` | Retry attempts |
| `error_message` | Utf8 | `DownloadResultMessage.error_message` | Error description |
| `created_at` | Utf8 | `DownloadResultMessage.created_at` (ISO) | Creation timestamp |
| `expires_at` | Datetime(UTC) | `DownloadResultMessage.expires_at` | URL expiration |
| `expired_at_ingest` | Boolean | `DownloadResultMessage.expired_at_ingest` | Expired at ingest flag |

---

## 7. Data Flow Summary

```
Eventhouse (KQL)
    |
    | type, version, utcDateTime, traceId, data
    v
EventMessage (Pydantic)
    |
    +---> DeltaEventsWriter ---> xact_events (28 columns via flatten_events())
    |
    +---> EventIngester
              |
              | Generates blob_path, file_type from URL
              v
         DownloadTaskMessage (Pydantic)
              |
              | trace_id, attachment_url, blob_path, status_subtype,
              | file_type, assignment_id, estimate_version, retry_count
              v
         Download Worker
              |
              v
         DownloadResultMessage (Pydantic)
              |
              | + status, http_status, bytes_downloaded, error_message,
              | created_at, expires_at, expired_at_ingest
              v
         DeltaInventoryWriter ---> xact_attachments (14 columns)
```

---

## 8. Field Name Mapping Reference

### Eventhouse to EventMessage

| Eventhouse Column | EventMessage Field | Notes |
|-------------------|-------------------|-------|
| `type` | `type` | Direct mapping |
| `version` | `version` | Direct mapping |
| `utcDateTime` | `utc_datetime` | Alias: `utcDateTime` |
| `traceId` | `trace_id` | Alias: `traceId` |
| `data` | `data` | JSON string, parsed via `data_dict` |

### EventMessage to DownloadTaskMessage

| EventMessage Field | DownloadTaskMessage Field | Notes |
|-------------------|--------------------------|-------|
| `trace_id` | `trace_id` | Direct mapping |
| `attachments[i]` | `attachment_url` | One task per attachment |
| - | `blob_path` | Generated by `generate_blob_path()` |
| `status_subtype` | `status_subtype` | Computed from `type` |
| - | `file_type` | Extracted from URL extension |
| `assignment_id` | `assignment_id` | From `data.assignmentId` |
| `estimate_version` | `estimate_version` | From `data.estimateVersion` |

### DownloadTaskMessage to DownloadResultMessage

| DownloadTaskMessage Field | DownloadResultMessage Field | Notes |
|--------------------------|----------------------------|-------|
| `trace_id` | `trace_id` | Preserved |
| `attachment_url` | `attachment_url` | Preserved |
| `blob_path` | `blob_path` | Preserved |
| `status_subtype` | `status_subtype` | Preserved |
| `file_type` | `file_type` | Preserved |
| `assignment_id` | `assignment_id` | Preserved |
| `retry_count` | `retry_count` | Updated by worker |
| - | `status` | Set by worker |
| - | `http_status` | From HTTP response |
| - | `bytes_downloaded` | From download |
| - | `error_message` | On failure |
| - | `created_at` | Worker timestamp |

---

## 9. Version History

| Date | Change |
|------|--------|
| 2026-01-02 | Initial schema alignment with verisk_pipeline |
