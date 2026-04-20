-- =============================================================================
-- Bronze — Staging CDR voice
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    cdr_id,
    calling_msisdn,
    called_msisdn,
    UPPER(call_direction)                       AS call_direction,
    UPPER(call_type)                            AS call_type,
    duration_seconds,
    UPPER(call_status)                          AS call_status,
    origin_cell_id,
    UPPER(origin_region)                        AS origin_region,
    UPPER(destination_type)                     AS destination_type,
    roaming_flag,
    roaming_country,
    COALESCE(charge_amount, 0.0)                AS charge_amount,
    currency,
    offer_code,
    start_ts,
    end_ts,
    ingested_at,
    call_date
FROM iceberg.bronze.cdr_voice
WHERE cdr_id IS NOT NULL
  AND calling_msisdn IS NOT NULL
  AND duration_seconds > 0
