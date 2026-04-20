-- =============================================================================
-- Bronze — Staging CDR data sessions
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    session_id,
    msisdn,
    UPPER(technology)                           AS technology,
    cell_id,
    UPPER(region)                               AS region,
    COALESCE(bytes_uploaded, 0)                 AS bytes_uploaded,
    COALESCE(bytes_downloaded, 0)               AS bytes_downloaded,
    duration_seconds,
    apn,
    roaming_flag,
    roaming_country,
    COALESCE(charge_amount, 0.0)                AS charge_amount,
    currency,
    offer_code,
    start_ts,
    end_ts,
    ingested_at,
    session_date
FROM iceberg.bronze.cdr_data
WHERE session_id IS NOT NULL
  AND msisdn IS NOT NULL
  AND duration_seconds > 0
