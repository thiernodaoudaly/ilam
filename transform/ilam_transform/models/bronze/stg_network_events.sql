-- =============================================================================
-- Bronze — Staging network events
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    event_id,
    UPPER(event_type)                           AS event_type,
    UPPER(severity)                             AS severity,
    cell_id,
    site_id,
    UPPER(region)                               AS region,
    UPPER(technology)                           AS technology,
    metric_name,
    COALESCE(metric_value, 0.0)                 AS metric_value,
    COALESCE(threshold_value, 0.0)              AS threshold_value,
    vendor,
    source_system,
    event_ts,
    ingested_at,
    event_date
FROM iceberg.bronze.network_events
WHERE event_id IS NOT NULL
