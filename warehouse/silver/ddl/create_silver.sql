-- =============================================================================
-- ILAM — Silver Layer DDL
-- Cleaned, deduplicated, enriched data. Business rules applied.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3://warehouse/silver/');

-- ---------------------------------------------------------------------------
-- Network events qualified
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.network_events (
    event_id          VARCHAR,
    event_type        VARCHAR,
    severity          VARCHAR,
    severity_score    INTEGER,
    cell_id           VARCHAR,
    site_id           VARCHAR,
    region            VARCHAR,
    department        VARCHAR,
    technology        VARCHAR,
    metric_name       VARCHAR,
    metric_value      DOUBLE,
    threshold_value   DOUBLE,
    threshold_breach_pct DOUBLE,
    is_critical       BOOLEAN,
    vendor            VARCHAR,
    event_ts          TIMESTAMP(6) WITH TIME ZONE,
    event_date        DATE,
    event_hour        INTEGER,
    processed_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['event_date', 'technology'],
    sorted_by    = ARRAY['severity_score', 'event_ts']
);

-- ---------------------------------------------------------------------------
-- CDR enriched — all CDR types unified and enriched with subscriber profile
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.cdr_enriched (
    cdr_id            VARCHAR,
    cdr_type          VARCHAR,
    msisdn            VARCHAR,
    subscriber_id     VARCHAR,
    account_type      VARCHAR,
    customer_segment  VARCHAR,
    region            VARCHAR,
    offer_code        VARCHAR,
    product_type      VARCHAR,
    duration_seconds  INTEGER,
    bytes_total       BIGINT,
    charge_amount     DOUBLE,
    charge_amount_xof DOUBLE,
    is_roaming        BOOLEAN,
    roaming_country   VARCHAR,
    cell_id           VARCHAR,
    technology        VARCHAR,
    cdr_date          DATE,
    cdr_hour          INTEGER,
    cdr_ts            TIMESTAMP(6) WITH TIME ZONE,
    processed_at      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['cdr_date', 'cdr_type'],
    sorted_by    = ARRAY['msisdn', 'cdr_ts']
);

-- ---------------------------------------------------------------------------
-- Subscribers enriched — deduplicated with RFM segmentation
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.subscribers (
    subscriber_id       VARCHAR,
    msisdn              VARCHAR,
    account_type        VARCHAR,
    status              VARCHAR,
    gender              VARCHAR,
    age_band            VARCHAR,
    region              VARCHAR,
    city                VARCHAR,
    nationality         VARCHAR,
    channel             VARCHAR,
    customer_segment    VARCHAR,
    rfm_score           INTEGER,
    tenure_days         INTEGER,
    is_active           BOOLEAN,
    has_om_account      BOOLEAN,
    activated_at        TIMESTAMP(6) WITH TIME ZONE,
    first_seen_date     DATE,
    last_seen_date      DATE,
    processed_at        TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['region'],
    sorted_by    = ARRAY['subscriber_id']
);

-- ---------------------------------------------------------------------------
-- Contracts enriched — with duration and lifecycle status
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.contracts (
    contract_id         VARCHAR,
    subscriber_id       VARCHAR,
    msisdn              VARCHAR,
    offer_code          VARCHAR,
    offer_name          VARCHAR,
    product_type        VARCHAR,
    plan_type           VARCHAR,
    monthly_fee_xof     DOUBLE,
    status              VARCHAR,
    is_active           BOOLEAN,
    activation_date     DATE,
    expiry_date         DATE,
    termination_date    DATE,
    termination_reason  VARCHAR,
    contract_duration_days INTEGER,
    processed_at        TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['product_type'],
    sorted_by    = ARRAY['subscriber_id', 'activation_date']
);

-- ---------------------------------------------------------------------------
-- Orange Money transactions enriched
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.om_transactions (
    transaction_id      VARCHAR,
    msisdn              VARCHAR,
    subscriber_id       VARCHAR,
    customer_segment    VARCHAR,
    transaction_type    VARCHAR,
    transaction_category VARCHAR,
    amount_xof          DOUBLE,
    fee_amount_xof      DOUBLE,
    net_amount_xof      DOUBLE,
    counterpart_type    VARCHAR,
    agent_id            VARCHAR,
    agent_region        VARCHAR,
    channel             VARCHAR,
    status              VARCHAR,
    is_successful       BOOLEAN,
    transaction_date    DATE,
    transaction_hour    INTEGER,
    transaction_ts      TIMESTAMP(6) WITH TIME ZONE,
    processed_at        TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['transaction_date', 'transaction_category'],
    sorted_by    = ARRAY['msisdn', 'transaction_ts']
);
