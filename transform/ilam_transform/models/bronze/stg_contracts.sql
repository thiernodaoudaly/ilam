-- =============================================================================
-- Bronze — Staging contracts
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    contract_id,
    subscriber_id,
    msisdn,
    UPPER(offer_code)                           AS offer_code,
    offer_name,
    UPPER(product_type)                         AS product_type,
    UPPER(plan_type)                            AS plan_type,
    COALESCE(monthly_fee, 0.0)                  AS monthly_fee,
    currency,
    UPPER(status)                               AS status,
    activation_date,
    expiry_date,
    termination_date,
    termination_reason,
    ingested_at,
    ingestion_date
FROM iceberg.bronze.contracts
WHERE contract_id IS NOT NULL
  AND msisdn IS NOT NULL
