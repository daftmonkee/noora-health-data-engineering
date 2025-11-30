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