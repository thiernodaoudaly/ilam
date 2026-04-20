-- =============================================================================
-- Gold — mart_churn
-- Churn indicators by region, product type and segment
-- =============================================================================
{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT
        CURRENT_DATE                                AS snapshot_date,
        region,
        account_type,
        customer_segment,
        COUNT(*)                                    AS total_base,
        COUNT(*) FILTER (WHERE status = 'INACTIVE') AS churned_count,
        COUNT(*) FILTER (WHERE status = 'ACTIVE'
            AND tenure_days <= 30)                  AS new_activations,
        AVG(tenure_days)                            AS avg_tenure_days
    FROM {{ ref('subscribers') }}
    GROUP BY 2,3,4
)

SELECT
    snapshot_date,
    region,
    account_type                                    AS product_type,
    customer_segment,
    total_base,
    churned_count,
    new_activations,
    ROUND(
        CAST(churned_count AS DOUBLE) /
        NULLIF(total_base, 0) * 100, 2
    )                                               AS churn_rate_pct,
    total_base - churned_count + new_activations    AS net_adds,
    ROUND(avg_tenure_days, 0)                       AS avg_tenure_days,
    CURRENT_TIMESTAMP                               AS updated_at
FROM base
