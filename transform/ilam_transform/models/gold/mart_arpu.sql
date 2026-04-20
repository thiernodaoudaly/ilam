-- =============================================================================
-- Gold — mart_arpu
-- Average Revenue Per User by month, region and segment
-- =============================================================================
{{ config(materialized='table', schema='gold') }}

WITH monthly_rev AS (
    SELECT
        DATE_FORMAT(revenue_date, '%Y-%m')          AS arpu_month,
        region,
        product_type,
        customer_segment,
        account_type,
        SUM(active_subscribers)                     AS active_subscribers,
        SUM(total_revenue_xof)                      AS total_revenue_xof
    FROM {{ ref('mart_revenue') }}
    GROUP BY 1,2,3,4,5
)

SELECT
    arpu_month,
    region,
    product_type,
    customer_segment,
    account_type,
    active_subscribers,
    ROUND(total_revenue_xof, 0)                     AS total_revenue_xof,
    ROUND(
        total_revenue_xof / NULLIF(active_subscribers, 0),
        2
    )                                               AS arpu_xof,
    CURRENT_TIMESTAMP                               AS updated_at
FROM monthly_rev
