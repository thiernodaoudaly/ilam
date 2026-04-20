-- =============================================================================
-- Silver — Orange Money transactions enriched
-- Categorization, subscriber join, validation
-- =============================================================================
{{ config(materialized='table', schema='silver') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_om_transactions') }}
),

enriched AS (
    SELECT
        t.transaction_id,
        t.msisdn,
        s.subscriber_id,
        s.customer_segment,
        t.transaction_type,
        CASE
            WHEN t.transaction_type IN ('DEPOT','RETRAIT')   THEN 'CASH_MANAGEMENT'
            WHEN t.transaction_type IN ('TRANSFERT')         THEN 'TRANSFER'
            WHEN t.transaction_type IN ('PAIEMENT','FACTURE') THEN 'PAYMENT'
            WHEN t.transaction_type = 'ACHAT_CREDIT'         THEN 'AIRTIME'
            ELSE 'OTHER'
        END                                         AS transaction_category,
        t.amount                                    AS amount_xof,
        t.fee_amount                                AS fee_amount_xof,
        t.amount - t.fee_amount                     AS net_amount_xof,
        t.counterpart_type,
        t.agent_id,
        t.agent_region,
        t.channel,
        t.status,
        t.status = 'SUCCESS'                        AS is_successful,
        CAST(t.transaction_ts AS DATE)              AS transaction_date,
        HOUR(t.transaction_ts)                      AS transaction_hour,
        t.transaction_ts,
        CURRENT_TIMESTAMP                           AS processed_at
    FROM base t
    LEFT JOIN {{ ref('subscribers') }} s
        ON t.msisdn = s.msisdn
)

SELECT * FROM enriched
