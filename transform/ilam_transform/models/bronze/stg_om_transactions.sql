-- =============================================================================
-- Bronze — Staging Orange Money transactions
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    transaction_id,
    msisdn,
    UPPER(transaction_type)                     AS transaction_type,
    COALESCE(amount, 0.0)                       AS amount,
    currency,
    COALESCE(fee_amount, 0.0)                   AS fee_amount,
    counterpart_msisdn,
    UPPER(counterpart_type)                     AS counterpart_type,
    agent_id,
    UPPER(agent_region)                         AS agent_region,
    UPPER(channel)                              AS channel,
    UPPER(status)                               AS status,
    failure_reason,
    COALESCE(balance_before, 0.0)               AS balance_before,
    COALESCE(balance_after, 0.0)                AS balance_after,
    transaction_ts,
    ingested_at,
    transaction_date
FROM iceberg.bronze.om_transactions
WHERE transaction_id IS NOT NULL
  AND msisdn IS NOT NULL
  AND amount > 0
