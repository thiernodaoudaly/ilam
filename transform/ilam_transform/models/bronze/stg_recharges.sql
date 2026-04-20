-- =============================================================================
-- Bronze — Staging recharges
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    recharge_id,
    msisdn,
    COALESCE(amount, 0.0)                       AS amount,
    currency,
    UPPER(channel)                              AS channel,
    voucher_code,
    COALESCE(bonus_amount, 0.0)                 AS bonus_amount,
    UPPER(region)                               AS region,
    UPPER(status)                               AS status,
    recharged_at,
    ingested_at,
    recharge_date
FROM iceberg.bronze.recharges
WHERE recharge_id IS NOT NULL
  AND msisdn IS NOT NULL
  AND amount > 0
