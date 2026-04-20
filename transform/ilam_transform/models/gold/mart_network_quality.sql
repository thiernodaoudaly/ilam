-- =============================================================================
-- Gold — mart_network_quality
-- Daily network KPIs by region and technology
-- =============================================================================
{{ config(materialized='table', schema='gold') }}

SELECT
    event_date                                      AS kpi_date,
    region,
    department,
    technology,
    COUNT(DISTINCT cell_id)                         AS active_cells,
    COUNT(*)                                        AS total_events,
    COUNT(*) FILTER (WHERE severity = 'CRITICAL')   AS critical_alarms,
    COUNT(*) FILTER (WHERE severity = 'MAJOR')      AS major_alarms,
    COUNT(*) FILTER (WHERE severity = 'MINOR')      AS minor_alarms,
    COUNT(*) FILTER (WHERE is_critical = TRUE)      AS total_critical,
    ROUND(AVG(metric_value) FILTER (
        WHERE metric_name = 'availability'), 2)     AS avg_availability_pct,
    ROUND(AVG(metric_value) FILTER (
        WHERE metric_name = 'drop_rate'), 2)        AS avg_drop_rate_pct,
    ROUND(AVG(metric_value) FILTER (
        WHERE metric_name = 'congestion'), 2)       AS avg_congestion_pct,
    CURRENT_TIMESTAMP                               AS updated_at
FROM {{ ref('network_events') }}
GROUP BY 1,2,3,4
