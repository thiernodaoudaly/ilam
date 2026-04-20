-- =============================================================================
-- Bronze — Staging subscribers
-- Simple view over raw bronze table with type casting and basic cleaning
-- =============================================================================
{{ config(materialized='table', schema='bronze') }}

SELECT
    subscriber_id,
    msisdn,
    UPPER(account_type)                         AS account_type,
    UPPER(status)                               AS status,
    UPPER(id_type)                              AS id_type,
    id_number,
    TRIM(first_name)                            AS first_name,
    TRIM(last_name)                             AS last_name,
    birth_date,
    UPPER(gender)                               AS gender,
    UPPER(region)                               AS region,
    TRIM(city)                                  AS city,
    UPPER(channel)                              AS channel,
    UPPER(nationality)                          AS nationality,
    activated_at,
    ingested_at,
    ingestion_date
FROM iceberg.bronze.subscribers
WHERE subscriber_id IS NOT NULL
  AND msisdn IS NOT NULL
