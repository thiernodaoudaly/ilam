-- =============================================================================
-- Silver — Subscribers enriched
-- Deduplication, RFM segmentation, tenure calculation
-- =============================================================================
{{ config(materialized='table', schema='silver') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_subscribers') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY msisdn
            ORDER BY activated_at DESC
        ) AS rn
    FROM base
),

enriched AS (
    SELECT
        subscriber_id,
        msisdn,
        account_type,
        status,
        gender,
        CASE
            WHEN birth_date IS NULL THEN 'UNKNOWN'
            WHEN DATE_DIFF('year', birth_date, CURRENT_DATE) < 25 THEN '18-24'
            WHEN DATE_DIFF('year', birth_date, CURRENT_DATE) < 35 THEN '25-34'
            WHEN DATE_DIFF('year', birth_date, CURRENT_DATE) < 45 THEN '35-44'
            WHEN DATE_DIFF('year', birth_date, CURRENT_DATE) < 55 THEN '45-54'
            ELSE '55+'
        END                                         AS age_band,
        region,
        city,
        nationality,
        channel,
        CASE
            WHEN account_type = 'POSTPAID' THEN 'PREMIUM'
            WHEN DATE_DIFF('day', activated_at, CURRENT_TIMESTAMP) > 730 THEN 'LOYAL'
            WHEN DATE_DIFF('day', activated_at, CURRENT_TIMESTAMP) > 365 THEN 'ESTABLISHED'
            ELSE 'NEW'
        END                                         AS customer_segment,
        status = 'ACTIVE'                           AS is_active,
        DATE_DIFF('day', activated_at, CURRENT_TIMESTAMP) AS tenure_days,
        CAST(activated_at AS DATE)                  AS first_seen_date,
        CURRENT_DATE                                AS last_seen_date,
        activated_at,
        CURRENT_TIMESTAMP                           AS processed_at
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM enriched
