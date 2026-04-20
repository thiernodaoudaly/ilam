-- =============================================================================
-- Silver — Network events qualified
-- Severity scoring, breach detection, regional classification
-- =============================================================================
{{ config(materialized='table', schema='silver') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_network_events') }}
),

qualified AS (
    SELECT
        event_id,
        event_type,
        severity,
        CASE severity
            WHEN 'CRITICAL' THEN 5
            WHEN 'MAJOR'    THEN 4
            WHEN 'MINOR'    THEN 3
            WHEN 'WARNING'  THEN 2
            ELSE 1
        END                                         AS severity_score,
        cell_id,
        site_id,
        region,
        CASE region
            WHEN 'Dakar'       THEN 'OUEST'
            WHEN 'Thiès'       THEN 'OUEST'
            WHEN 'Saint-Louis' THEN 'NORD'
            WHEN 'Louga'       THEN 'NORD'
            WHEN 'Matam'       THEN 'NORD'
            WHEN 'Tambacounda' THEN 'EST'
            WHEN 'Kédougou'    THEN 'EST'
            WHEN 'Kaffrine'    THEN 'CENTRE'
            WHEN 'Kaolack'     THEN 'CENTRE'
            WHEN 'Fatick'      THEN 'CENTRE'
            WHEN 'Diourbel'    THEN 'CENTRE'
            WHEN 'Ziguinchor'  THEN 'SUD'
            WHEN 'Sédhiou'     THEN 'SUD'
            WHEN 'Kolda'       THEN 'SUD'
            ELSE 'AUTRE'
        END                                         AS department,
        technology,
        metric_name,
        metric_value,
        threshold_value,
        CASE
            WHEN threshold_value > 0
            THEN ROUND((metric_value - threshold_value) / threshold_value * 100, 2)
            ELSE 0.0
        END                                         AS threshold_breach_pct,
        severity IN ('CRITICAL','MAJOR')            AS is_critical,
        vendor,
        source_system,
        event_ts,
        event_date,
        HOUR(event_ts)                              AS event_hour,
        CURRENT_TIMESTAMP                           AS processed_at
    FROM base
)

SELECT * FROM qualified
