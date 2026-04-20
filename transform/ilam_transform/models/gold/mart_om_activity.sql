-- =============================================================================
-- Gold — mart_om_activity
-- Orange Money daily activity by region and transaction category
-- =============================================================================
{{ config(materialized='table', schema='gold') }}

SELECT
    transaction_date                                AS activity_date,
    agent_region                                    AS region,
    transaction_category,
    channel,
    COUNT(*)                                        AS total_transactions,
    COUNT(*) FILTER (WHERE is_successful = TRUE)    AS successful_tx,
    COUNT(*) FILTER (WHERE is_successful = FALSE)   AS failed_tx,
    ROUND(
        CAST(COUNT(*) FILTER (WHERE is_successful = TRUE) AS DOUBLE) /
        NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS success_rate_pct,
    ROUND(SUM(amount_xof) FILTER (
        WHERE is_successful = TRUE), 0)             AS total_volume_xof,
    ROUND(AVG(amount_xof) FILTER (
        WHERE is_successful = TRUE), 0)             AS avg_transaction_xof,
    ROUND(SUM(fee_amount_xof) FILTER (
        WHERE is_successful = TRUE), 0)             AS total_fees_xof,
    COUNT(DISTINCT msisdn)                          AS unique_users,
    COUNT(DISTINCT agent_id)                        AS unique_agents,
    CURRENT_TIMESTAMP                               AS updated_at
FROM {{ ref('om_transactions') }}
GROUP BY 1,2,3,4
