# iTel Cabinet Form Parsing Guide

**Date:** 2026-01-10
**Status:** Ready for Testing

---

## Overview

The iTel Cabinet plugin parses complex form responses from task 32513 and extracts:

1. **Form Submissions** → `claimx_itel_forms` table (cabinet damage details)
2. **Media Attachments** → `claimx_itel_attachments` table (photos with question context)

This mirrors how the main pipeline handles media with `PROJECT_FILE_ADDED` events and `MediaTransformer`.

---

## Architecture

```
ClaimX Task 32513 Event
         ↓
   Plugin Trigger
         ↓
   Kafka Topic
         ↓
   Tracking Worker Pipeline:
         ↓
   1. Transform (extract event fields)
   2. Validate (check task_id = 32513)
   3. ClaimX API Lookup (fetch full task)
         ↓
   4. ItelFormParser
      - Parses form response
      - Extracts submission data
      - Extracts media attachments (claim_media_ids)
         ↓
   5. ItelMediaDownloader
      - Downloads media files from ClaimX
      - Stores to OneLake blob storage
      - Updates blob_path in attachments
         ↓
   6. ItelDualTableWriter
      - Writes to claimx_itel_forms
      - Writes to claimx_itel_attachments (with blob_path)
```

---

## Data Flow

### Input: ClaimX Task Response

```json
{
  "assignmentId": 5423723,
  "taskId": 32513,
  "projectId": 4895140,
  "formResponseId": "6962d069f8c664c2706b0bd0",
  "status": "COMPLETED",
  "externalLinkData": {
    "firstName": "NICK",
    "lastName": "Customer",
    "email": "customer@example.com"
  },
  "response": {
    "groups": [
      {
        "name": "Cabinet Types Damaged",
        "questionAndAnswers": [
          {
            "questionText": "Lower Cabinets Damaged?",
            "formControl": {"id": "control-847167460"},
            "responseAnswerExport": {
              "type": "option",
              "optionAnswer": {"name": "Yes"}
            }
          },
          {
            "questionText": "Upload Overview Photo(s)",
            "formControl": {"id": "control-110496963"},
            "responseAnswerExport": {
              "type": "image",
              "claimMediaIds": [96264202, 96264203]
            }
          }
        ]
      }
    ]
  }
}
```

### Output: Two Delta Tables

**Table 1: claimx_itel_forms** (1 row per submission)
```
assignment_id | project_id | form_response_id | status    | customer_email | lower_cabinets_damaged | ...
5423723       | 4895140    | 6962d069...      | COMPLETED | customer@...   | true                   | ...
```

**Table 2: claimx_itel_attachments** (N rows per submission)
```
id | assignment_id | question_key     | question_text            | claim_media_id | display_order
1  | 5423723       | overview_photos  | Upload Overview Photo(s) | 96264202       | 0
2  | 5423723       | overview_photos  | Upload Overview Photo(s) | 96264203       | 1
3  | 5423723       | lower_cabinet_box| Upload Lower Cabinet...  | 96264210       | 2
```

---

## Components

### 1. ItelFormTransformer

**File:** `kafka_pipeline/plugins/itel_cabinet_api/handlers/form_transformer.py`

**Purpose:** Pure transformation logic (no I/O)

**Methods:**
- `to_submission_row(task_data, event_id)` - Parses form to submission dict
- `to_attachment_rows(task_data, assignment_id, event_id)` - Extracts media attachments
- `_parse_form_response(response)` - Flattens nested form structure
- `_question_to_field_name(question_text)` - Maps questions to field names

**Key Logic:**
```python
# Maps control IDs to question keys
MEDIA_QUESTION_MAPPING = {
    "control-110496963": "overview_photos",
    "control-989806632": "lower_cabinet_box",
    "control-374047064": "lower_cabinet_end_panels",
    # ... etc
}
```

### 2. ItelFormParser

**File:** `kafka_pipeline/plugins/itel_cabinet_api/handlers/form_parser.py`

**Purpose:** Enrichment handler that uses transformer

**Flow:**
1. Receives enriched data with `claimx_task_details` from previous lookup handler
2. Calls `ItelFormTransformer.to_submission_row()`
3. Calls `ItelFormTransformer.to_attachment_rows()`
4. Stores results in `data['itel_submission']` and `data['itel_attachments']`
5. Passes data to next handler

**Configuration:**
```yaml
- type: kafka_pipeline.plugins.itel_cabinet_api.handlers.form_parser:ItelFormParser
  config: {}  # No config needed
```

### 3. ItelMediaDownloader

**File:** `kafka_pipeline/plugins/itel_cabinet_api/handlers/media_downloader.py`

**Purpose:** Downloads media files and stores to OneLake blob storage

**Flow:**
1. Reads `data['itel_attachments']` from previous handler
2. For each attachment with `claim_media_id`:
   - Calls ClaimX API to get download URL
   - Downloads media file
   - Uploads to OneLake at blob path pattern
   - Updates `blob_path` in attachment data
3. Processes downloads concurrently (default: 5 concurrent)
4. Passes updated attachments to next handler

**Configuration:**
```yaml
- type: kafka_pipeline.plugins.itel_cabinet_api.handlers.media_downloader:ItelMediaDownloader
  config:
    blob_path_template: "abfss://.../itel_cabinet_form_attachments/{project_id}/{task_id}/"
    claimx_connection: claimx_api
    concurrent_downloads: 5
    skip_download: false  # Set to true for testing
```

**Blob Path Pattern:**
```
{blob_path_template}{media_id}_{file_name}

Example:
abfss://...@onelake.../itel_cabinet_form_attachments/4895140/32513/96264202_overview.jpg
```

### 4. ItelDualTableWriter

**File:** `kafka_pipeline/plugins/itel_cabinet_api/handlers/dual_table_writer.py`

**Purpose:** Writes to both Delta tables

**Flow:**
1. Reads `data['itel_submission']`
2. Reads `data['itel_attachments']`
3. Creates Spark DataFrames
4. Writes to `claimx_itel_forms` table
5. Writes to `claimx_itel_attachments` table

**Configuration:**
```yaml
- type: kafka_pipeline.plugins.itel_cabinet_api.handlers.dual_table_writer:ItelDualTableWriter
  config:
    submissions_table: claimx_itel_forms
    attachments_table: claimx_itel_attachments
    mode: append
```

---

## Delta Table Schemas

### claimx_itel_forms

```python
StructType([
    # Identifiers
    StructField("assignment_id", LongType(), False),
    StructField("project_id", LongType(), False),
    StructField("form_id", StringType(), False),
    StructField("form_response_id", StringType(), False),

    # Status & Dates
    StructField("status", StringType(), False),
    StructField("date_assigned", TimestampType(), False),
    StructField("date_completed", TimestampType(), True),

    # Customer Info
    StructField("customer_first_name", StringType(), True),
    StructField("customer_last_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_phone", StringType(), True),

    # Lower Cabinets (9 fields)
    StructField("lower_cabinets_damaged", BooleanType(), True),
    StructField("lower_cabinets_lf", IntegerType(), True),
    # ... etc

    # Upper Cabinets (8 fields)
    # Full Height Cabinets (7 fields)
    # Island Cabinets (9 fields)

    # Metadata
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])
```

**Total:** 47 fields

### claimx_itel_attachments

```python
StructType([
    StructField("id", LongType(), False),           # Auto-increment
    StructField("assignment_id", LongType(), False),
    StructField("question_key", StringType(), False),
    StructField("question_text", StringType(), False),
    StructField("claim_media_id", LongType(), False),
    StructField("blob_path", StringType(), True),   # Populated later by download worker
    StructField("display_order", IntegerType(), False),
    StructField("created_at", TimestampType(), True),
])
```

**Total:** 8 fields

---

## Question Key Mapping

The `question_key` field standardizes question identification across form versions:

| Control ID | Question Text | question_key |
|------------|---------------|--------------|
| control-110496963 | Upload Overview Photo(s) | overview_photos |
| control-989806632 | Upload Lower Cabinet Box Photo(s) | lower_cabinet_box |
| control-374047064 | Upload Lower Cabinet End Panel Photo(s) | lower_cabinet_end_panels |
| control-767670035 | Upload Upper Cabinet Box Photo(s) | upper_cabinet_box |
| control-488956803 | Upload Upper Cabinet End Panel Photo(s) | upper_cabinet_end_panels |
| control-734859955 | Upload Full Height Cabinet Box Photo(s) | full_height_cabinet_box |
| control-959393098 | Upload Full Height End Panel Photo(s) | full_height_end_panels |
| control-393858054 | Upload Island Cabinet Box Photo(s) | island_cabinet_box |
| control-532071774 | Upload Island Cabinet End Panel Photo(s) | island_cabinet_end_panels |

---

## Setup Instructions

### 1. Create Delta Tables

```bash
# Dry run to see schemas
python scripts/delta_tables/create_itel_forms_tables.py --dry-run

# Create tables
python scripts/delta_tables/create_itel_forms_tables.py
```

### 2. Verify Tables

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Check submissions table
spark.sql("DESCRIBE TABLE claimx_itel_forms").show(50, False)

# Check attachments table
spark.sql("DESCRIBE TABLE claimx_itel_attachments").show(20, False)
```

### 3. Start Tracking Worker

```bash
python -m kafka_pipeline.workers.itel_cabinet_tracking_worker
```

### 4. Trigger Test Event

Complete a task 32513 in ClaimX and verify:

```sql
-- Check submission was written
SELECT * FROM claimx_itel_forms
WHERE assignment_id = <your_assignment_id>;

-- Check attachments were extracted
SELECT * FROM claimx_itel_attachments
WHERE assignment_id = <your_assignment_id>
ORDER BY display_order;
```

---

## Querying Form Data

### Get Submission with Attachments

```sql
SELECT
    s.assignment_id,
    s.customer_email,
    s.status,
    s.lower_cabinets_damaged,
    s.lower_cabinets_lf,
    COUNT(a.id) as attachment_count
FROM claimx_itel_forms s
LEFT JOIN claimx_itel_attachments a
    ON s.assignment_id = a.assignment_id
GROUP BY
    s.assignment_id,
    s.customer_email,
    s.status,
    s.lower_cabinets_damaged,
    s.lower_cabinets_lf;
```

### Find Submissions with Overview Photos

```sql
SELECT
    s.assignment_id,
    s.project_id,
    s.customer_email,
    COUNT(a.id) as overview_photo_count
FROM claimx_itel_forms s
INNER JOIN claimx_itel_attachments a
    ON s.assignment_id = a.assignment_id
WHERE a.question_key = 'overview_photos'
GROUP BY s.assignment_id, s.project_id, s.customer_email;
```

### Analyze Cabinet Damage by Type

```sql
SELECT
    COUNT(*) as total_submissions,
    SUM(CASE WHEN lower_cabinets_damaged THEN 1 ELSE 0 END) as lower_damaged,
    SUM(CASE WHEN upper_cabinets_damaged THEN 1 ELSE 0 END) as upper_damaged,
    SUM(CASE WHEN full_height_cabinets_damaged THEN 1 ELSE 0 END) as full_height_damaged,
    SUM(CASE WHEN island_cabinets_damaged THEN 1 ELSE 0 END) as island_damaged
FROM claimx_itel_forms
WHERE status = 'COMPLETED';
```

### Get Attachments by Question

```sql
SELECT
    question_key,
    question_text,
    COUNT(*) as photo_count,
    COUNT(DISTINCT assignment_id) as submission_count
FROM claimx_itel_attachments
GROUP BY question_key, question_text
ORDER BY photo_count DESC;
```

---

## Troubleshooting

### No Attachments Found

**Problem:** Submissions written but no attachments

**Check:**
1. Form response contains `claimMediaIds`: Look for `responseAnswerExport.claimMediaIds` in ClaimX API response
2. Control IDs match mapping: Check if question `formControl.id` exists in `MEDIA_QUESTION_MAPPING`
3. Worker logs: Search for "Form parsed successfully" with `attachment_count`

### Wrong Field Values

**Problem:** Fields in `claimx_itel_forms` are NULL or incorrect

**Check:**
1. Question text matches mapping: Update `_question_to_field_name()` in transformer
2. Answer type parsing: Check if answer type (`option`, `number`, `text`) is handled correctly
3. Worker logs: Enable DEBUG logging to see parsed form data

### Duplicate Submissions

**Problem:** Same submission written multiple times

**Check:**
1. Worker not filtering on `COMPLETED` status: Plugin triggers on all status changes
2. Add deduplication: Use `assignment_id` + `form_response_id` as unique key
3. Change mode to `merge` instead of `append`

---

## Extending the Parser

### Add New Question

1. **Update Schema** (if new field):
```python
# In create_itel_forms_tables.py
StructField("new_field_name", StringType(), True),
```

2. **Update Mapping**:
```python
# In form_transformer.py _question_to_field_name()
"New Question Text?": "new_field_name",
```

3. **Recreate Table** (if schema changed):
```bash
python scripts/delta_tables/create_itel_forms_tables.py
```

### Add New Media Question

1. **Find Control ID** in form response JSON
2. **Update Mapping**:
```python
# In form_transformer.py MEDIA_QUESTION_MAPPING
"control-123456789": "new_question_key",
```

3. **Done** - No schema changes needed for attachments table

---

## Performance

### Expected Throughput

- **Form Parsing:** <50ms per submission
- **Delta Writes:** 100-500ms per submission (depends on attachment count)
- **Total Latency:** 150-600ms per event

### Optimization

1. **Batch Writes:** If high volume, modify `ItelDualTableWriter` to buffer and write in batches
2. **Partition Tables:** Add partitioning by date for faster queries
3. **Cache Spark Session:** Reuse Spark session across writes

---

## Summary

**What We Built:**
- ✅ Form transformer to parse nested ClaimX responses
- ✅ Form parser enrichment handler for pipeline integration
- ✅ Media downloader to fetch and store files to OneLake
- ✅ Dual table writer for submissions + attachments
- ✅ Delta table creation scripts (tables already created)
- ✅ Updated tracking worker configuration

**What's Ready:**
- ✅ Parse all cabinet damage fields (47 total)
- ✅ Extract media attachments with question context
- ✅ Download media files from ClaimX
- ✅ Store media to OneLake blob storage (iTel-specific path)
- ✅ Write to two normalized Delta tables
- ✅ Query submissions with attachments and blob paths

**Next Steps:**
1. ~~Create tables~~ (already created)
2. Start worker: `python -m kafka_pipeline.workers.itel_cabinet_tracking_worker`
3. Test with task 32513 completion
4. Verify data in Delta tables and OneLake blob storage

---

**Created:** 2026-01-10
**Status:** ✅ Ready for Testing
