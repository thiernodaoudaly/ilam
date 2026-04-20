-- =============================================================================
-- Gold — mart_usage
-- Voice/SMS/Data consumption by segment, region and technology
-- =============================================================================
{{ config(materialized='table', schema='gold') }}

SELECT
    cdr_date                                        AS usage_date,
    region,
    COALESCE(technology, 'N/A')                     AS technology,
    customer_segment,
    account_type,
    cdr_type                                        AS product_type,
    COUNT(DISTINCT msisdn)                          AS active_users,
    ROUND(SUM(
        CASE WHEN cdr_type = 'VOICE'
        THEN duration_seconds / 60.0 ELSE 0 END
    ), 2)                                           AS voice_minutes,
    SUM(
        CASE WHEN cdr_type = 'SMS' THEN 1 ELSE 0 END
    )                                               AS sms_count,
    ROUND(SUM(
        CASE WHEN cdr_type = 'DATA'
        THEN bytes_total / 1073741824.0 ELSE 0 END
    ), 4)                                           AS data_gb,
    ROUND(SUM(
        CASE WHEN cdr_type = 'VOICE'
        THEN duration_seconds / 60.0 ELSE 0 END
    ) / NULLIF(COUNT(DISTINCT
        CASE WHEN cdr_type = 'VOICE' THEN msisdn END
    ), 0), 2)                                       AS avg_voice_min_per_user,
    ROUND(SUM(
        CASE WHEN cdr_type = 'DATA'
        THEN bytes_total / 1073741824.0 ELSE 0 END
    ) / NULLIF(COUNT(DISTINCT
        CASE WHEN cdr_type = 'DATA' THEN msisdn END
    ), 0), 4)                                       AS avg_data_gb_per_user,
    COUNT(DISTINCT
        CASE WHEN roaming_flag = TRUE THEN msisdn END
    )                                               AS roaming_users,
    ROUND(SUM(
        CASE WHEN roaming_flag = TRUE AND cdr_type = 'VOICE'
        THEN duration_seconds / 60.0 ELSE 0 END
    ), 2)                                           AS roaming_minutes,
    CURRENT_TIMESTAMP                               AS updated_at
FROM {{ ref('cdr_enriched') }}
GROUP BY 1,2,3,4,5,6
