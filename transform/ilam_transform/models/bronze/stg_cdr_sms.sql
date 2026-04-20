-- =============================================================================
-- Bronze — Staging CDR SMS
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    cdr_id,
    sender_msisdn,
    receiver_msisdn,
    UPPER(sms_type)                             AS sms_type,
    UPPER(delivery_status)                      AS delivery_status,
    origin_cell_id,
    UPPER(origin_region)                        AS origin_region,
    roaming_flag,
    COALESCE(charge_amount, 0.0)                AS charge_amount,
    currency,
    offer_code,
    sent_ts,
    delivered_ts,
    ingested_at,
    sms_date
FROM iceberg.bronze.cdr_sms
WHERE cdr_id IS NOT NULL
  AND sender_msisdn IS NOT NULL
