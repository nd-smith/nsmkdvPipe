# ClaimX Delta Table Schema Reference

This document provides a comprehensive reference of all ClaimX Delta table schemas in the Fabric lakehouse.

---

## 1. claimx_events

Raw ClaimX webhook events. Partitioned by `event_date`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | string | No | Unique event identifier |
| `event_type` | string | No | Event type (e.g., PROJECT_CREATED, PROJECT_FILE_ADDED) |
| `project_id` | string | Yes | ClaimX project identifier |
| `media_id` | string | Yes | Media file identifier (for file events) |
| `task_assignment_id` | string | Yes | Task assignment identifier (for task events) |
| `video_collaboration_id` | string | Yes | Video collaboration identifier (for video events) |
| `master_file_name` | string | Yes | Master file name (for MFN events) |
| `ingested_at` | timestamp | No | When event was ingested from webhook |
| `created_at` | timestamp | No | Pipeline processing timestamp |
| `event_date` | date | Yes | Date partition column (derived from ingested_at) |

---

## 2. claimx_projects

ClaimX project records with claim and policyholder information.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `project_id` | string | No | Unique project identifier |
| `project_number` | string | Yes | Project number |
| `master_file_name` | string | Yes | Master file name (MFN) |
| `secondary_number` | string | Yes | Secondary reference number |
| `status` | string | Yes | Project status |
| `created_date` | string | Yes | Project creation date |
| `date_of_loss` | string | Yes | Date of loss |
| `type_of_loss` | string | Yes | Type of loss |
| `cause_of_loss` | string | Yes | Cause of loss |
| `loss_description` | string | Yes | Loss description |
| `customer_first_name` | string | Yes | Policyholder first name |
| `customer_last_name` | string | Yes | Policyholder last name |
| `street1` | string | Yes | Property street address |
| `city` | string | Yes | Property city |
| `state_province` | string | Yes | Property state/province |
| `zip_postcode` | string | Yes | Property ZIP/postal code |
| `primary_email` | string | Yes | Primary contact email |
| `primary_phone` | string | Yes | Primary contact phone |
| `custom_attribute1` | string | Yes | Custom attribute 1 |
| `custom_attribute2` | string | Yes | Custom attribute 2 |
| `custom_attribute3` | string | Yes | Custom attribute 3 |
| `coverages` | string | Yes | Coverage information |
| `contents_task_sent` | boolean | Yes | Whether contents task was sent |
| `contents_task_at` | timestamp | Yes | When contents task was sent |
| `xa_autolink_fail` | boolean | Yes | Whether XactAnalysis autolink failed |
| `xa_autolink_fail_at` | timestamp | Yes | When autolink failure occurred |
| `source_event_id` | string | Yes | Source event that created/updated record |
| `created_at` | timestamp | No | Pipeline creation timestamp |
| `updated_at` | timestamp | Yes | Last update timestamp |
| `last_enriched_at` | timestamp | Yes | Last enrichment timestamp |

---

## 3. claimx_attachments

Downloaded attachment files stored in OneLake.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `media_id` | string | Yes | Media file identifier |
| `project_id` | string | Yes | Associated project ID |
| `blob_path` | string | Yes | OneLake blob storage path |
| `file_type` | string | Yes | File extension (jpg, pdf, etc.) |
| `file_name` | string | Yes | Original file name |
| `source_event_id` | string | Yes | Source event that triggered download |
| `bytes_downloaded` | long | Yes | File size in bytes |
| `downloaded_at` | timestamp | Yes | When file was downloaded |
| `created_at` | timestamp | Yes | Pipeline creation timestamp |
| `media_type` | string | Yes | Media MIME type |
| `media_name` | string | Yes | Media display name |
| `created_date` | date | Yes | Creation date |
| `download_url` | string | Yes | Original download URL |

---

## 4. claimx_attachment_metadata

Metadata for attachment files including GPS and description.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `media_id` | string | Yes | Media file identifier |
| `project_id` | string | Yes | Associated project ID |
| `file_type` | string | Yes | File extension |
| `file_name` | string | Yes | Original file name |
| `media_description` | string | Yes | User-provided description |
| `media_comment` | string | Yes | User-provided comment |
| `latitude` | double | Yes | GPS latitude |
| `longitude` | double | Yes | GPS longitude |
| `gps_source` | string | Yes | GPS data source |
| `taken_date` | string | Yes | When photo/video was taken |
| `full_download_link` | string | Yes | Full resolution download link |
| `source_event_id` | string | Yes | Source event ID |
| `created_at` | string | Yes | Pipeline creation timestamp |
| `updated_at` | string | Yes | Last update timestamp |
| `last_enriched_at` | string | Yes | Last enrichment timestamp |

---

## 5. claimx_tasks

Custom task assignments for projects.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `assignment_id` | long | No | Unique task assignment identifier |
| `task_id` | long | Yes | Task template identifier |
| `project_id` | string | Yes | Associated project ID |
| `assignee_id` | long | Yes | User ID of assignee |
| `assignor_id` | long | Yes | User ID of assignor |
| `date_assigned` | string | Yes | Assignment date |
| `date_completed` | string | Yes | Completion date |
| `status` | string | Yes | Task status |
| `stp_enabled` | boolean | Yes | Whether STP is enabled |
| `mfn` | string | Yes | Master file name |
| `source_event_id` | string | Yes | Source event ID |
| `created_at` | timestamp | No | Pipeline creation timestamp |
| `updated_at` | timestamp | Yes | Last update timestamp |
| `last_enriched_at` | timestamp | Yes | Last enrichment timestamp |

---

## 6. claimx_task_templates

Task template definitions.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `task_id` | long | No | Unique task template identifier |
| `comp_id` | long | Yes | Company identifier |
| `name` | string | Yes | Task template name |
| `description` | string | Yes | Task description |
| `form_id` | string | Yes | Associated form ID |
| `form_name` | string | Yes | Form name |
| `enabled` | boolean | Yes | Whether template is enabled |
| `is_default` | boolean | Yes | Whether this is the default template |
| `is_manual_delivery` | boolean | Yes | Manual delivery flag |
| `is_external_link_delivery` | boolean | Yes | External link delivery flag |
| `provide_portal_access` | boolean | Yes | Whether to provide portal access |
| `notify_assigned_send_recipient` | boolean | Yes | Email notification on assignment |
| `notify_assigned_send_recipient_sms` | boolean | Yes | SMS notification on assignment |
| `notify_assigned_subject` | string | Yes | Assignment notification subject |
| `notify_task_completed` | boolean | Yes | Notification on completion |
| `notify_completed_subject` | string | Yes | Completion notification subject |
| `allow_resubmit` | boolean | Yes | Whether resubmission is allowed |
| `auto_generate_pdf` | boolean | Yes | Auto-generate PDF flag |
| `modified_by` | string | Yes | Last modifier name |
| `modified_by_id` | long | Yes | Last modifier user ID |
| `modified_date` | string | Yes | Last modification date |
| `source_event_id` | string | Yes | Source event ID |
| `created_at` | timestamp | No | Pipeline creation timestamp |
| `updated_at` | timestamp | Yes | Last update timestamp |
| `last_enriched_at` | timestamp | Yes | Last enrichment timestamp |

---

## 7. claimx_contacts

Contact records associated with projects.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `project_id` | string | Yes | Associated project ID |
| `contact_email` | string | Yes | Contact email address |
| `contact_type` | string | Yes | Contact type (policyholder, adjuster, etc.) |
| `first_name` | string | Yes | Contact first name |
| `last_name` | string | Yes | Contact last name |
| `phone_number` | string | Yes | Phone number |
| `is_primary_contact` | boolean | Yes | Whether this is the primary contact |
| `master_file_name` | string | Yes | Master file name |
| `task_assignment_id` | integer | Yes | Associated task assignment |
| `video_collaboration_id` | string | Yes | Associated video collaboration |
| `source_event_id` | string | Yes | Source event ID |
| `created_at` | timestamp | Yes | Pipeline creation timestamp |
| `last_enriched_at` | timestamp | Yes | Last enrichment timestamp |
| `created_date` | date | Yes | Creation date |
| `phone_country_code` | long | Yes | Phone country code |
| `updated_at` | string | Yes | Last update timestamp |

---

## 8. claimx_video_collab

Video collaboration session records.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `video_collaboration_id` | long | Yes | Unique video collaboration identifier |
| `claim_id` | long | Yes | Associated claim ID |
| `mfn` | string | Yes | Master file name |
| `claim_number` | string | Yes | Claim number |
| `policy_number` | string | Yes | Policy number |
| `email_user_name` | string | Yes | Email username |
| `claim_rep_first_name` | string | Yes | Claim rep first name |
| `claim_rep_last_name` | string | Yes | Claim rep last name |
| `claim_rep_full_name` | string | Yes | Claim rep full name |
| `number_of_videos` | long | Yes | Count of videos captured |
| `number_of_photos` | long | Yes | Count of photos captured |
| `number_of_viewers` | long | Yes | Count of viewers |
| `session_count` | long | Yes | Number of sessions |
| `total_time_seconds` | string | Yes | Total session time in seconds |
| `total_time` | string | Yes | Total session time formatted |
| `created_date` | string | Yes | Creation date |
| `live_call_first_session` | string | Yes | First live call session timestamp |
| `live_call_last_session` | string | Yes | Last live call session timestamp |
| `company_id` | long | Yes | Company identifier |
| `company_name` | string | Yes | Company name |
| `guid` | string | Yes | Unique GUID |
| `source_event_id` | string | Yes | Source event ID |
| `created_at` | timestamp | Yes | Pipeline creation timestamp |
| `updated_at` | timestamp | Yes | Last update timestamp |
| `last_enriched_at` | timestamp | Yes | Last enrichment timestamp |

---

## 9. claimx_external_links

External sharing links for tasks.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `link_id` | long | Yes | Unique link identifier |
| `assignment_id` | long | Yes | Associated task assignment |
| `project_id` | string | Yes | Associated project ID |
| `link_code` | string | Yes | Unique link code |
| `url` | string | Yes | Full external URL |
| `notification_access_method` | string | Yes | How notification was sent |
| `country_id` | long | Yes | Country identifier |
| `state_id` | long | Yes | State identifier |
| `created_date` | string | Yes | Link creation date |
| `accessed_count` | long | Yes | Number of times accessed |
| `last_accessed` | string | Yes | Last access timestamp |
| `source_event_id` | string | Yes | Source event ID |
| `created_at` | string | Yes | Pipeline creation timestamp |
| `updated_at` | string | Yes | Last update timestamp |

---

## 10. claimx_event_log

Pipeline processing log for event tracking and debugging.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | string | No | Event identifier being processed |
| `event_type` | string | Yes | Event type |
| `project_id` | string | Yes | Associated project ID |
| `status` | string | No | Processing status (success, failed, skipped) |
| `error_message` | string | Yes | Error message if failed |
| `error_category` | string | Yes | Error category classification |
| `api_calls` | integer | Yes | Number of API calls made |
| `duration_ms` | integer | Yes | Processing duration in milliseconds |
| `retry_count` | integer | Yes | Number of retry attempts |
| `processed_at` | timestamp | No | When event was processed |
| `processed_date` | date | No | Processing date (partition column) |

---

## Version History

| Date | Change |
|------|--------|
| 2026-01-07 | Initial ClaimX schema documentation |
