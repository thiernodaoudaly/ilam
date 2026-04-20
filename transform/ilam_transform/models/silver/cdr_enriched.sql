-- =============================================================================
-- Silver — CDR enriched
-- Unifies voice, SMS and data CDRs with subscriber profile
-- =============================================================================
{{ config(materialized='table', schema='silver') }}

WITH voice AS (
    SELECT
        cdr_id,
        'VOICE'                                     AS cdr_type,
        calling_msisdn                              AS msisdn,
        origin_region                               AS region,
        offer_code,
        duration_seconds,
        CAST(NULL AS BIGINT)                        AS bytes_total,
        charge_amount,
        roaming_flag,
        roaming_country,
        origin_cell_id                              AS cell_id,
        CAST(NULL AS VARCHAR)                       AS technology,
        call_status                                 AS status,
        start_ts                                    AS cdr_ts,
        HOUR(start_ts)                              AS cdr_hour,
        call_date                                   AS cdr_date
    FROM {{ ref('stg_cdr_voice') }}
    WHERE call_status = 'SUCCESS'
),

sms AS (
    SELECT
        cdr_id,
        'SMS'                                       AS cdr_type,
        sender_msisdn                               AS msisdn,
        origin_region                               AS region,
        offer_code,
        CAST(NULL AS INTEGER)                       AS duration_seconds,
        CAST(NULL AS BIGINT)                        AS bytes_total,
        charge_amount,
        roaming_flag,
        CAST(NULL AS VARCHAR)                       AS roaming_country,
        origin_cell_id                              AS cell_id,
        CAST(NULL AS VARCHAR)                       AS technology,
        delivery_status                             AS status,
        sent_ts                                     AS cdr_ts,
        HOUR(sent_ts)                               AS cdr_hour,
        sms_date                                    AS cdr_date
    FROM {{ ref('stg_cdr_sms') }}
    WHERE delivery_status = 'DELIVERED'
),

data_sessions AS (
    SELECT
        session_id                                  AS cdr_id,
        'DATA'                                      AS cdr_type,
        msisdn,
        region,
        offer_code,
        duration_seconds,
        CAST(bytes_uploaded + bytes_downloaded AS BIGINT) AS bytes_total,
        charge_amount,
        roaming_flag,
        roaming_country,
        cell_id,
        technology,
        'SUCCESS'                                   AS status,
        start_ts                                    AS cdr_ts,
        HOUR(start_ts)                              AS cdr_hour,
        session_date                                AS cdr_date
    FROM {{ ref('stg_cdr_data') }}
),

all_cdr AS (
    SELECT * FROM voice
    UNION ALL
    SELECT * FROM sms
    UNION ALL
    SELECT * FROM data_sessions
),

enriched AS (
    SELECT
        c.cdr_id,
        c.cdr_type,
        c.msisdn,
        s.subscriber_id,
        s.account_type,
        s.customer_segment,
        c.region,
        c.offer_code,
        c.duration_seconds,
        c.bytes_total,
        c.charge_amount,
        c.roaming_flag,
        c.roaming_country,
        c.cell_id,
        c.technology,
        c.cdr_date,
        c.cdr_hour,
        c.cdr_ts,
        CURRENT_TIMESTAMP                           AS processed_at
    FROM all_cdr c
    LEFT JOIN {{ ref('subscribers') }} s
        ON c.msisdn = s.msisdn
)

SELECT * FROM enriched
