-- =============================================================================
-- Gold — mart_revenue
-- Daily revenue by product type, region and customer segment
-- =============================================================================
{{ config(materialized='table', schema='gold') }}

WITH voice_rev AS (
    SELECT
        cdr_date                                    AS revenue_date,
        region,
        'VOICE'                                     AS product_type,
        customer_segment,
        account_type,
        COUNT(DISTINCT msisdn)                      AS active_subscribers,
        SUM(charge_amount)                          AS revenue_xof
    FROM {{ ref('cdr_enriched') }}
    WHERE cdr_type = 'VOICE'
    GROUP BY 1,2,3,4,5
),

sms_rev AS (
    SELECT
        cdr_date, region, 'SMS' AS product_type,
        customer_segment, account_type,
        COUNT(DISTINCT msisdn)                      AS active_subscribers,
        SUM(charge_amount)                          AS revenue_xof
    FROM {{ ref('cdr_enriched') }}
    WHERE cdr_type = 'SMS'
    GROUP BY 1,2,3,4,5
),

data_rev AS (
    SELECT
        cdr_date, region, 'DATA' AS product_type,
        customer_segment, account_type,
        COUNT(DISTINCT msisdn)                      AS active_subscribers,
        SUM(charge_amount)                          AS revenue_xof
    FROM {{ ref('cdr_enriched') }}
    WHERE cdr_type = 'DATA'
    GROUP BY 1,2,3,4,5
),

recharge_rev AS (
    SELECT
        recharge_date                               AS revenue_date,
        region,
        'RECHARGE'                                  AS product_type,
        'ALL'                                       AS customer_segment,
        'PREPAID'                                   AS account_type,
        COUNT(DISTINCT msisdn)                      AS active_subscribers,
        SUM(amount)                                 AS revenue_xof
    FROM {{ ref('stg_recharges') }}
    WHERE status = 'SUCCESS'
    GROUP BY 1,2,3,4,5
),

all_rev AS (
    SELECT * FROM voice_rev
    UNION ALL SELECT * FROM sms_rev
    UNION ALL SELECT * FROM data_rev
    UNION ALL SELECT * FROM recharge_rev
)

SELECT
    revenue_date,
    region,
    product_type,
    customer_segment,
    account_type,
    SUM(active_subscribers)                         AS active_subscribers,
    SUM(revenue_xof)                                AS total_revenue_xof,
    ROUND(SUM(revenue_xof) / NULLIF(SUM(active_subscribers), 0), 2) AS avg_revenue_per_user,
    CURRENT_TIMESTAMP                               AS updated_at
FROM all_rev
GROUP BY 1,2,3,4,5
