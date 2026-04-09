-- =============================================================================
-- ILAM — Gold Layer DDL
-- Sonatel Data Marts — ready for BI, analytics and ML.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3://warehouse/gold/');

-- ---------------------------------------------------------------------------
-- mart_revenue — daily revenue by product, region and segment
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_revenue (
    revenue_date          DATE,
    region                VARCHAR,
    product_type          VARCHAR,
    customer_segment      VARCHAR,
    account_type          VARCHAR,
    total_subscribers     BIGINT,
    active_subscribers    BIGINT,
    total_revenue_xof     DOUBLE,
    voice_revenue_xof     DOUBLE,
    sms_revenue_xof       DOUBLE,
    data_revenue_xof      DOUBLE,
    roaming_revenue_xof   DOUBLE,
    recharge_amount_xof   DOUBLE,
    avg_revenue_per_user  DOUBLE,
    updated_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['revenue_date'],
    sorted_by    = ARRAY['region', 'product_type']
);

-- ---------------------------------------------------------------------------
-- mart_network_quality — network KPIs by cell and technology
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_network_quality (
    kpi_date              DATE,
    region                VARCHAR,
    department            VARCHAR,
    technology            VARCHAR,
    total_cells           BIGINT,
    active_cells          BIGINT,
    critical_alarms       BIGINT,
    major_alarms          BIGINT,
    minor_alarms          BIGINT,
    avg_availability_pct  DOUBLE,
    avg_drop_rate_pct     DOUBLE,
    avg_congestion_pct    DOUBLE,
    incidents_count       BIGINT,
    mttr_minutes          DOUBLE,
    updated_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['kpi_date'],
    sorted_by    = ARRAY['region', 'technology']
);

-- ---------------------------------------------------------------------------
-- mart_churn — churn indicators and retention metrics
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_churn (
    snapshot_date         DATE,
    region                VARCHAR,
    product_type          VARCHAR,
    customer_segment      VARCHAR,
    account_type          VARCHAR,
    total_base            BIGINT,
    churned_count         BIGINT,
    new_activations       BIGINT,
    reactivations         BIGINT,
    churn_rate_pct        DOUBLE,
    net_adds              BIGINT,
    avg_tenure_days       DOUBLE,
    top_churn_reason      VARCHAR,
    updated_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['snapshot_date'],
    sorted_by    = ARRAY['churn_rate_pct']
);

-- ---------------------------------------------------------------------------
-- mart_arpu — Average Revenue Per User by month and segment
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_arpu (
    arpu_month            VARCHAR,
    region                VARCHAR,
    product_type          VARCHAR,
    customer_segment      VARCHAR,
    account_type          VARCHAR,
    active_subscribers    BIGINT,
    total_revenue_xof     DOUBLE,
    arpu_xof              DOUBLE,
    arpu_voice_xof        DOUBLE,
    arpu_data_xof         DOUBLE,
    arpu_sms_xof          DOUBLE,
    arpu_om_xof           DOUBLE,
    mom_growth_pct        DOUBLE,
    updated_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['arpu_month'],
    sorted_by    = ARRAY['region', 'product_type']
);

-- ---------------------------------------------------------------------------
-- mart_om_activity — Orange Money activity by zone and type
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_om_activity (
    activity_date         DATE,
    region                VARCHAR,
    transaction_category  VARCHAR,
    channel               VARCHAR,
    total_transactions    BIGINT,
    successful_tx         BIGINT,
    failed_tx             BIGINT,
    success_rate_pct      DOUBLE,
    total_volume_xof      DOUBLE,
    avg_transaction_xof   DOUBLE,
    total_fees_xof        DOUBLE,
    unique_users          BIGINT,
    unique_agents         BIGINT,
    updated_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['activity_date', 'transaction_category'],
    sorted_by    = ARRAY['total_volume_xof']
);

-- ---------------------------------------------------------------------------
-- mart_usage — voice/SMS/data consumption by segment and region
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_usage (
    usage_date            DATE,
    region                VARCHAR,
    technology            VARCHAR,
    customer_segment      VARCHAR,
    account_type          VARCHAR,
    product_type          VARCHAR,
    active_users          BIGINT,
    voice_minutes         DOUBLE,
    sms_count             BIGINT,
    data_gb               DOUBLE,
    avg_voice_min_per_user DOUBLE,
    avg_data_gb_per_user  DOUBLE,
    roaming_users         BIGINT,
    roaming_minutes       DOUBLE,
    updated_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['usage_date'],
    sorted_by    = ARRAY['region', 'technology']
);
