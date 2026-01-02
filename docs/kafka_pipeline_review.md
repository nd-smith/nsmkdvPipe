# Kafka Pipeline Schema Review

## Overview
This document captures findings from comparing `kafka_pipeline` against `verisk_pipeline` (the reference implementation).

**Review Date:** 2026-01-02
**Status:** ✅ FIXES APPLIED

---

## Fixes Applied (2026-01-02)

All critical schema issues have been resolved:

1. **DeltaEventsWriter** - Now uses `flatten_events()` from verisk_pipeline
   - Writes all 28 columns to xact_events table
   - Uses `write_raw_events()` API for proper transformation

2. **EventMessage** - Now matches verisk_pipeline EventRecord
   - Fields: type, version, utc_datetime, trace_id, data
   - Computed properties: status_subtype, attachments, assignment_id

3. **DownloadTaskMessage** - Now matches verisk_pipeline Task
   - Fields: trace_id, attachment_url, blob_path, status_subtype, file_type, assignment_id
   - Added `to_verisk_task()` and `from_verisk_task()` conversion methods

4. **DownloadResultMessage** - Now matches Task.to_tracking_row()
   - All required fields: status_subtype, file_type, assignment_id, http_status, etc.
   - Added `to_tracking_row()` method for inventory writes

5. **DeltaInventoryWriter** - Now uses correct schema
   - Schema matches verisk_pipeline xact_attachments table
   - Uses INVENTORY_SCHEMA constant for consistency

6. **EventIngester** - Updated to use new schemas
   - Creates DownloadTaskMessage with all required fields
   - Uses flatten_events() for Delta writes

7. **Poller** - Updated _row_to_event() for proper structure
   - Creates EventMessage matching EventRecord schema
   - Uses to_eventhouse_row() for Delta writes

---

## Original Findings (for reference)

---

## 1. Source Schema (Eventhouse)

### Actual Eventhouse Columns:
| Column | Type | Description |
|--------|------|-------------|
| `type` | string | Full event type (e.g., `verisk.claims.property.xn.documentsReceived`) |
| `version` | number | Event version |
| `utcDateTime` | datetime | Event timestamp |
| `traceId` | UUID | Unique trace identifier (camelCase!) |
| `data` | dynamic/JSON | Nested payload containing all event data |

### Nested `data` Structure (varies by event type):
```json
{
  "description": "string",
  "assignmentId": "string",
  "originalAssignmentId": "string",
  "xnAddress": "string",
  "carrierId": "string",
  "estimateVersion": "string",
  "note": "string",
  "author": "string",
  "dateTime": "string (MDT timestamp)",
  "attachments": ["url1", "url2"],
  "adm": {
    "coverageLoss": {
      "claimNumber": "string"
    }
  },
  "contact": {
    "type": "string",
    "name": "string",
    "contactMethods": {
      "phone": { "type": "", "number": "", "extension": "" },
      "email": { "address": "" }
    }
  }
}
```

---

## 2. EventMessage Schema Comparison

### kafka_pipeline EventMessage (current):
**File:** `src/kafka_pipeline/schemas/events.py`

```python
class EventMessage(BaseModel):
    trace_id: str           # OK
    event_type: str         # MISMATCH - should be full "type" string
    event_subtype: str      # MISMATCH - should be parsed from type
    timestamp: datetime     # OK (from utcDateTime)
    source_system: str      # MISMATCH - not in source, should derive from type
    payload: Dict[str, Any] # OK (from data)
    attachments: List[str]  # OK (from data.attachments)
    metadata: Dict[str, Any] # NOT IN SOURCE
```

### verisk_pipeline EventRecord (reference):
**File:** `src/verisk_pipeline/xact/xact_models.py`

```python
@dataclass
class EventRecord:
    type: str           # Full event type string
    version: str        # Event version
    utc_datetime: str   # Timestamp
    trace_id: str       # Trace ID
    data: str           # JSON string (NOT parsed dict!)

    @property
    def status_subtype(self) -> str:
        return self.type.split(".")[-1]
```

### FINDINGS - EventMessage:
| Issue | Severity | Description |
|-------|----------|-------------|
| **Missing `type` field** | HIGH | Should preserve full type string like verisk_pipeline |
| **Missing `version` field** | MEDIUM | verisk_pipeline preserves this |
| **event_type vs type** | HIGH | kafka_pipeline parses into event_type, but xact_events stores full `type` |
| **payload as Dict vs String** | MEDIUM | verisk_pipeline keeps `data` as JSON string, kafka parses it |
| **Validation too strict** | HIGH | min_length=1 validators fail on empty source values |

---

## 3. xact_events Delta Table Schema

### Actual Table Schema (from user):
| Column | Type | Source Mapping (verisk_pipeline) |
|--------|------|----------------------------------|
| `type` | varchar | `type` (full string) |
| `status_subtype` | varchar | `type.split(".")[-1]` |
| `version` | varchar | `version` |
| `trace_id` | varchar | `traceId` |
| `description` | varchar | `data.description` |
| `assignment_id` | varchar | `data.assignmentId` |
| `original_assignment_id` | varchar | `data.originalAssignmentId` |
| `xn_address` | varchar | `data.xnAddress` |
| `carrier_id` | varchar | `data.carrierId` |
| `claim_number` | varchar | `data.adm.coverageLoss.claimNumber` |
| `note` | varchar | `data.note` |
| `author` | varchar | `data.author` |
| `sender_reviewer_name` | varchar | `data.senderReviewerName` |
| `sender_reviewer_email` | varchar | `data.senderReviewerEmail` |
| `carrier_reviewer_name` | varchar | `data.carrierReviewerName` |
| `carrier_reviewer_email` | varchar | `data.carrierReviewerEmail` |
| `contact_type` | varchar | `data.contact.type` |
| `contact_name` | varchar | `data.contact.name` |
| `contact_phone_type` | varchar | `data.contact.contactMethods.phone.type` |
| `contact_phone_number` | varchar | `data.contact.contactMethods.phone.number` |
| `contact_phone_extension` | varchar | `data.contact.contactMethods.phone.extension` |
| `contact_email_address` | varchar | `data.contact.contactMethods.email.address` |
| `raw_json` | varchar | Full row as JSON string |
| `attachments` | varchar | `data.attachments` (comma-joined) |
| `estimate_version` | varchar | `data.estimateVersion` |
| `event_datetime_mdt` | varchar | `data.dateTime` |
| `event_date` | date | `utcDateTime.date()` |
| `ingested_at` | datetime2 | `utcDateTime` |
| `created_at` | datetime2 | Pipeline processing timestamp |

### kafka_pipeline DeltaEventsWriter (current):
**File:** `src/kafka_pipeline/writers/delta_events.py`

```python
# ONLY WRITES 9 FIELDS - MISSING 19 FIELDS!
schema = {
    "trace_id": pl.Utf8,
    "event_type": pl.Utf8,      # Wrong column name (should be "type")
    "event_subtype": pl.Utf8,   # Wrong column name (should be "status_subtype")
    "source_system": pl.Utf8,   # NOT IN TABLE SCHEMA
    "timestamp": pl.Datetime,   # Wrong column name (should be "ingested_at")
    "ingested_at": pl.Datetime, # Duplicate of timestamp?
    "attachments": pl.List,     # Wrong type (should be Utf8, comma-joined)
    "payload": pl.Struct,       # NOT IN TABLE - should be flattened
    "metadata": pl.Struct,      # NOT IN TABLE SCHEMA
}
```

### FINDINGS - xact_events Writer:
| Issue | Severity | Description |
|-------|----------|-------------|
| **Missing 19 columns** | CRITICAL | Only writes 9 of 28 required columns |
| **Wrong column names** | CRITICAL | `event_type` should be `type`, `event_subtype` should be `status_subtype` |
| **No field flattening** | CRITICAL | Writes `payload` as struct instead of flattening to individual columns |
| **Wrong attachments type** | HIGH | Writes as List instead of comma-joined varchar |
| **Missing raw_json** | HIGH | Full row JSON not preserved |
| **Extra columns** | MEDIUM | `source_system`, `metadata` not in table |
| **Should use transform.py** | CRITICAL | Should reuse `flatten_events()` from verisk_pipeline |

---

## 4. Download Task Schema

### verisk_pipeline Task (reference):
**File:** `src/verisk_pipeline/xact/xact_models.py`

```python
@dataclass
class Task:
    trace_id: str
    attachment_url: str
    blob_path: str          # Destination in OneLake
    status_subtype: str     # From event type
    file_type: str          # Extracted from URL
    assignment_id: str      # From data.assignmentId
    estimate_version: Optional[str] = None
    retry_count: int = 0
```

### kafka_pipeline DownloadTaskMessage (current):
**File:** `src/kafka_pipeline/schemas/tasks.py`

```python
class DownloadTaskMessage(BaseModel):
    trace_id: str
    attachment_url: str
    destination_path: str       # OK (same as blob_path)
    event_type: str             # DIFFERENT - verisk uses status_subtype
    event_subtype: str          # DIFFERENT - verisk uses status_subtype only
    retry_count: int = 0        # OK
    original_timestamp: datetime # NOT IN verisk_pipeline
    metadata: Dict[str, Any]    # NOT IN verisk_pipeline
```

### FINDINGS - Download Task:
| Issue | Severity | Description |
|-------|----------|-------------|
| **Missing `file_type`** | HIGH | Required for inventory table and path generation |
| **Missing `assignment_id`** | HIGH | Required for inventory table and path generation |
| **Missing `estimate_version`** | MEDIUM | Used in inventory table |
| **Missing `status_subtype`** | HIGH | verisk_pipeline uses this, not event_type/event_subtype |
| **Extra `event_type`** | LOW | Not needed, status_subtype is sufficient |
| **Extra `original_timestamp`** | LOW | Not in verisk_pipeline |
| **Extra `metadata`** | LOW | Not in verisk_pipeline |

---

## 5. Inventory Table Schema (xact_attachments)

### verisk_pipeline Task.to_tracking_row() output:
**File:** `src/verisk_pipeline/xact/xact_models.py`

```python
{
    "trace_id": str,
    "attachment_url": str,
    "blob_path": str,
    "status_subtype": str,
    "file_type": str,
    "assignment_id": str,
    "status": str,              # completed/failed/failed_permanent
    "http_status": int,
    "bytes_downloaded": int,
    "retry_count": int,
    "error_message": str,
    "created_at": str,
    "expires_at": datetime,
    "expired_at_ingest": bool,
}
```

### kafka_pipeline DeltaInventoryWriter (current):
**File:** `src/kafka_pipeline/writers/delta_inventory.py`

```python
schema = {
    "trace_id": pl.Utf8,
    "attachment_url": pl.Utf8,
    "blob_path": pl.Utf8,
    "bytes_downloaded": pl.Int64,
    "downloaded_at": pl.Datetime,       # Different name from verisk
    "processing_time_ms": pl.Int64,     # NOT IN verisk_pipeline
    "created_at": pl.Datetime,
    "created_date": pl.Date,
}
```

### FINDINGS - Inventory Writer:
| Issue | Severity | Description |
|-------|----------|-------------|
| **Missing `status_subtype`** | HIGH | Required field in verisk_pipeline |
| **Missing `file_type`** | HIGH | Required field in verisk_pipeline |
| **Missing `assignment_id`** | HIGH | Required field in verisk_pipeline |
| **Missing `estimate_version`** | MEDIUM | Used in verisk_pipeline |
| **Missing `http_status`** | MEDIUM | Useful for debugging |
| **Different column name** | LOW | `downloaded_at` vs `created_at` timing |
| **Extra `processing_time_ms`** | LOW | Not in verisk_pipeline but useful |

---

## 6. Path Generation

### verisk_pipeline path_resolver.py:
Need to verify kafka_pipeline generates paths the same way.

**Expected pattern:** `{domain}/{assignment_id}/{file_type}/{filename}`

### FINDINGS - Path Generation:
- [ ] TO BE VERIFIED - check how kafka_pipeline generates blob paths

---

## 7. Summary of Critical Issues

### CRITICAL (Must Fix):
1. **xact_events writer missing 19 columns** - Table writes will fail or be incomplete
2. **xact_events writer wrong column names** - `event_type` should be `type`, etc.
3. **No flatten_events transformation** - Should reuse `verisk_pipeline/xact/stages/transform.py`
4. **EventMessage schema mismatch** - Missing required fields, wrong structure

### HIGH (Should Fix):
5. **DownloadTaskMessage missing fields** - `file_type`, `assignment_id`, `status_subtype`
6. **Inventory writer missing fields** - `status_subtype`, `file_type`, `assignment_id`
7. **Attachments stored as List not varchar** - Schema mismatch
8. **Eventhouse column names** - `traceId` (camelCase) not `trace_id`

### MEDIUM (Nice to Fix):
9. **Missing version field** in EventMessage
10. **Missing estimate_version** in task/inventory
11. **Extra fields** that aren't used (metadata, source_system)

---

## 8. Recommended Approach

### Option A: Refactor kafka_pipeline to reuse verisk_pipeline
1. Import and use `flatten_events()` from `verisk_pipeline.xact.stages.transform`
2. Import and use `Task` dataclass from `verisk_pipeline.xact.xact_models`
3. Import and use `InventoryTableWriter` from `verisk_pipeline.storage.inventory_writer`
4. Remove duplicate schema definitions in kafka_pipeline

### Option B: Update kafka_pipeline schemas to match
1. Update `EventMessage` to match `EventRecord` + include raw fields
2. Update `DeltaEventsWriter` to use `flatten_events()` transformation
3. Update `DownloadTaskMessage` to match `Task` fields
4. Update `DeltaInventoryWriter` to match inventory schema

**Recommendation: Option A** - Reuse existing verisk_pipeline code to ensure consistency.

---

## 9. Files to Modify

| File | Action |
|------|--------|
| `kafka_pipeline/schemas/events.py` | Update EventMessage or remove in favor of verisk_pipeline |
| `kafka_pipeline/schemas/tasks.py` | Update DownloadTaskMessage to match Task |
| `kafka_pipeline/writers/delta_events.py` | Rewrite to use flatten_events() |
| `kafka_pipeline/writers/delta_inventory.py` | Update schema to match verisk_pipeline |
| `kafka_pipeline/eventhouse/poller.py` | Update _row_to_event() to create proper structure |
| `kafka_pipeline/workers/event_ingester.py` | Update to generate proper Task objects |
| `kafka_pipeline/workers/download_worker.py` | Update to work with correct Task schema |
| `kafka_pipeline/workers/result_processor.py` | Update to write correct inventory schema |
