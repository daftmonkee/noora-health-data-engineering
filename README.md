# Noora Health Data Engineering Assignment

## Overview

This repository contains the complete data engineering pipeline for WhatsApp chat message analytics at Noora Health.

## Data Architecture

### Raw Tables (GCP BigQuery)
- **messages_dataset.Messages**: 14,746 records
  - Contains: uuid, content, direction (inbound/outbound), message_type, masked addresses, timestamps
  - Append-only table with _airbyte_generation_id for change tracking

- **messages_dataset.Statuses**: 32,946 records  
  - Contains: status (failed/sent/delivered/read/deleted), timestamps, message_uuid reference
  - Append-only table with _airbyte_generation_id for versioning

### Transformed Table
- Combined Messages + Statuses using LEFT JOIN on uuid = message_uuid
- Result: 32,946 rows (one per message-status combination)
- Preserves complete history of status changes per message

## Key Metrics

- **Total Messages**: 1,016 unique messages
- **Message Status Records**: 32,946 total (avg 3.2 statuses per message)
- **Direction Split**: 
  - Inbound: 1,409 (12.2%)
  - Outbound: 13,337 (87.8%)
- **Status Distribution**:
  - Delivered: 11,032 (33.5%)
  - Read: 10,832 (32.9%)
  - Sent: 10,324 (31.4%)
  - Failed: 491 (1.5%)
  - Deleted: 7 (0.02%)
- **Data Quality**: 100% (zero NULL values in key fields)

## SQL Queries

### Step 2: Data Transformation

**Query**: Step2_Transform_Messages_and_Statuses.sql

Merges Messages and Statuses tables into one consolidated view:
```sql
CREATE OR REPLACE TABLE `noora-health-de.messages_dataset.combineddataset` AS
WITH message_status_combined AS (
  -- Join messages with all their statuses (one row per message-status combination)
  SELECT
    m.id as message_id,
    m.uuid as message_uuid,
    m.content,
    m.direction,
    m.message_type,
    m.masked_author,
    m.masked_from_addr,
    m.masked_addressees,
    m.rendered_content,
    m.external_id,
    m.external_timestamp,
    m.is_deleted,
    m.author_type,
    m.last_status as last_known_status,
    m.last_status_timestamp,
    m.inserted_at as message_inserted_at,
    m.updated_at as message_updated_at,
    -- Status information
    s.status,
    s.timestamp as status_timestamp,
    s.uuid as status_uuid,
    s.id as status_id,
    s.inserted_at as status_inserted_at,
    s.updated_at as status_updated_at
  FROM `noora-health-de.messages_dataset.Messages` m
  LEFT JOIN `noora-health-de.messages_dataset.Statuses` s
    ON m.uuid = s.message_uuid
WHERE m._airbyte_generation_id = (
  SELECT MAX(_airbyte_generation_id)
  FROM `noora-health-de.messages_dataset.Messages`)
)
SELECT
  message_id,
  message_uuid,
  content,
  direction,
  message_type,
  masked_author,
  masked_from_addr,
  masked_addressees,
  rendered_content,
  external_id,
  external_timestamp,
  is_deleted,
  author_type,
  last_known_status,
  last_status_timestamp,
  message_inserted_at,
  message_updated_at,
  status,
  status_timestamp,
  status_uuid,
  status_id,
  status_inserted_at,
  status_updated_at,
  -- Rank each status for each message by timestamp (descending)
  ROW_NUMBER() OVER (PARTITION BY message_uuid ORDER BY status_timestamp DESC) as status_recency_rank
FROM message_status_combined
ORDER BY message_inserted_at DESC, status_timestamp DESC;
```

**Result**: 32,946 rows combining all message and status information

### Step 3: Data Validation

#### Validation Queries

```sql
-- NOORA HEALTH DATA ENGINEERING ASSIGNMENT - VALIDATION PIPELINE
-- Project: WhatsApp Chat Data Analysis Pipeline
-- Purpose: Validate combined dataset table and data quality

-- VALIDATION QUERY 1: Data Completeness - Check NULL values and row counts
 
SELECT
  'Combined Dataset' as table_name,
  COUNT(*) as total_rows,
  COUNTIF(message_uuid IS NULL) as null_message_uuid,
  COUNTIF(message_id IS NULL) as null_message_id,
  COUNTIF(content IS NULL) as null_content, # null content is expected.
  COUNTIF(direction IS NULL) as null_direction,
  COUNTIF(message_type IS NULL) as null_message_type,
  COUNTIF(message_inserted_at IS NULL) as null_message_inserted_at,
  COUNTIF(message_updated_at IS NULL) as null_message_updated_at,
  COUNTIF(status IS NULL) as null_status,
  COUNTIF(status_inserted_at IS NULL) as null_status_inserted_at,
  COUNTIF(status_updated_at IS NULL) as null_status_updated_at,
  COUNT(DISTINCT message_uuid) as distinct_message_uuids,
  COUNT(DISTINCT status) as distinct_statuses,
  MIN(message_inserted_at) as earliest_message_date,
  MAX(message_inserted_at) as latest_message_date,
  MIN(status_timestamp) as earliest_status_timestamp,
  MAX(status_timestamp) as latest_status_timestamp
FROM `noora-health-de.messages_dataset.combineddataset`;


-- VALIDATION QUERY 2: Duplicate Detection - Identifies duplicate records
 
SELECT DISTINCT
  message_uuid,
  status,
  COUNT(*) as occurrence_count,
  message_id,
  MIN(message_inserted_at) as first_occurrence,
  MAX(message_inserted_at) as last_occurrence
FROM `noora-health-de.messages_dataset.combineddataset`
GROUP BY ALL
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- VALIDATION QUERY 3: Data Quality Metrics - Distribution and consistency
-- Data broken across various metrics to check oddities, if any.

SELECT
  'Message Direction Distribution' as metric_type,
  direction as metric_value,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `noora-health-de.messages_dataset.combineddataset`
WHERE direction IS NOT NULL
GROUP BY direction
UNION ALL
SELECT
  'Message Type Distribution' as metric_type,
  message_type as metric_value,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `noora-health-de.messages_dataset.combineddataset`
WHERE message_type IS NOT NULL
GROUP BY message_type
UNION ALL
SELECT
  'Status Distribution' as metric_type,
  status as metric_value,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `noora-health-de.messages_dataset.combineddataset`
WHERE status IS NOT NULL
GROUP BY status
ORDER BY metric_type, count DESC;
```

### Step 4: Visualizations

Looker Studio Report: https://lookerstudio.google.com/s/m2_NfIPHysY

## How to Run the Pipeline

### Prerequisites
- GCP BigQuery access with "noora-health-de" project
- Proper IAM permissions for data warehouse (asia-south1 region)
  
### Execution Steps

1. **Extract & Load**
   - Data is pre-ingested in BigQuery using Airbyte instance (Google Sheets to BigQuery) (Manual Sync Enabled)
   - Tables: `messages_dataset.Messages` and `messages_dataset.Statuses`
   - Airbyte Instance: https://cloud.airbyte.com/workspaces/55a15196-e48e-4aba-9e25-2edff8457545/connections/8bea517e-a556-4a22-ae07-29524ee9c1fe/status

2. **Transform**
   - Execute Saved Query: `sql/Step2_Transform_Messages_and_Statuses.sql`
   - Creates `noora-health-de.messages_dataset.combineddataset` table which is used downstream in all visualisations and validations (consolidated view with all message-status combinations)

3. **Validate**
   - Execute Saved Queries: `Step3_Validation_Data_Quality_Check.sql`
   - Review results for data quality assurance

4. **Visualize**
   - Looker Studio is linked with BQ combineddataset table and is updated live
   - Export results for dashboard creation or further analysis

5. **Documentation**
   - This README provides complete documentation
   - All SQL queries include inline comments

## Key Insights

1. **High Message Volume**: 1,016 unique messages with 32,946 status records
2. **Append-Only Pattern**: Expected behavior for message and status tables confirmed
3. **Data Quality**: 100% - no NULL values in critical fields
4. **Status Progression**: Messages typically progress through multiple states (sent → delivered → read)
5. **Outbound Dominant**: 87.8% of messages are outbound (12.2% inbound)
6. **Read Engagement**: High read rate indicates good message engagement

## Repository
- **URL**: https://github.com/daftmonkee/noora-health-data-engineering
- **Visibility**: Public
- **License**: MIT
