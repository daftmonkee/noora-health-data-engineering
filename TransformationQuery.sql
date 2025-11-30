-- Step 2: Transform and merge Messages and Statuses tables
-- This query combines messages with all their status history
-- to preserve all information from both tables

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