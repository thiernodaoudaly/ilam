-- =============================================================================
-- ILAM — Bronze Layer DDL
-- Sonatel telecom raw data. No business logic. Technical casting only.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3://warehouse/bronze/');

CREATE TABLE IF NOT EXISTS iceberg.bronze.network_events (
    event_id          VARCHAR,
    event_type        VARCHAR,
    severity          VARCHAR,
    cell_id           VARCHAR,
    site_id           VARCHAR,
    region            VARCHAR,
    technology        VARCHAR,
    metric_name       VARCHAR,
    metric_value      DOUBLE,
    threshold_value   DOUBLE,
    vendor            VARCHAR,
    raw_payload       VARCHAR,
    source_system     VARCHAR,
    event_ts          TIMESTAMP(6) WITH TIME ZONE,
    ingested_at       TIMESTAMP(6) WITH TIME ZONE,
    event_date        DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['event_date'],
    sorted_by    = ARRAY['event_ts']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.cdr_voice (
    cdr_id            VARCHAR,
    calling_msisdn    VARCHAR,
    called_msisdn     VARCHAR,
    call_direction    VARCHAR,
    call_type         VARCHAR,
    duration_seconds  INTEGER,
    call_status       VARCHAR,
    disconnect_cause  VARCHAR,
    origin_cell_id    VARCHAR,
    origin_region     VARCHAR,
    destination_type  VARCHAR,
    roaming_flag      BOOLEAN,
    roaming_country   VARCHAR,
    charge_amount     DOUBLE,
    currency          VARCHAR,
    offer_code        VARCHAR,
    start_ts          TIMESTAMP(6) WITH TIME ZONE,
    end_ts            TIMESTAMP(6) WITH TIME ZONE,
    ingested_at       TIMESTAMP(6) WITH TIME ZONE,
    call_date         DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['call_date'],
    sorted_by    = ARRAY['start_ts']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.cdr_sms (
    cdr_id            VARCHAR,
    sender_msisdn     VARCHAR,
    receiver_msisdn   VARCHAR,
    sms_type          VARCHAR,
    delivery_status   VARCHAR,
    origin_cell_id    VARCHAR,
    origin_region     VARCHAR,
    roaming_flag      BOOLEAN,
    charge_amount     DOUBLE,
    currency          VARCHAR,
    offer_code        VARCHAR,
    sent_ts           TIMESTAMP(6) WITH TIME ZONE,
    delivered_ts      TIMESTAMP(6) WITH TIME ZONE,
    ingested_at       TIMESTAMP(6) WITH TIME ZONE,
    sms_date          DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['sms_date'],
    sorted_by    = ARRAY['sent_ts']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.cdr_data (
    session_id        VARCHAR,
    msisdn            VARCHAR,
    technology        VARCHAR,
    cell_id           VARCHAR,
    region            VARCHAR,
    bytes_uploaded    BIGINT,
    bytes_downloaded  BIGINT,
    duration_seconds  INTEGER,
    apn               VARCHAR,
    roaming_flag      BOOLEAN,
    roaming_country   VARCHAR,
    charge_amount     DOUBLE,
    currency          VARCHAR,
    offer_code        VARCHAR,
    start_ts          TIMESTAMP(6) WITH TIME ZONE,
    end_ts            TIMESTAMP(6) WITH TIME ZONE,
    ingested_at       TIMESTAMP(6) WITH TIME ZONE,
    session_date      DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['session_date', 'technology'],
    sorted_by    = ARRAY['start_ts']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.subscribers (
    subscriber_id     VARCHAR,
    msisdn            VARCHAR,
    account_type      VARCHAR,
    status            VARCHAR,
    id_type           VARCHAR,
    id_number         VARCHAR,
    first_name        VARCHAR,
    last_name         VARCHAR,
    birth_date        DATE,
    gender            VARCHAR,
    region            VARCHAR,
    city              VARCHAR,
    channel           VARCHAR,
    nationality       VARCHAR,
    raw_metadata      VARCHAR,
    activated_at      TIMESTAMP(6) WITH TIME ZONE,
    ingested_at       TIMESTAMP(6) WITH TIME ZONE,
    ingestion_date    DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['ingestion_date'],
    sorted_by    = ARRAY['activated_at']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.contracts (
    contract_id        VARCHAR,
    subscriber_id      VARCHAR,
    msisdn             VARCHAR,
    offer_code         VARCHAR,
    offer_name         VARCHAR,
    product_type       VARCHAR,
    plan_type          VARCHAR,
    monthly_fee        DOUBLE,
    currency           VARCHAR,
    status             VARCHAR,
    activation_date    DATE,
    expiry_date        DATE,
    termination_date   DATE,
    termination_reason VARCHAR,
    ingested_at        TIMESTAMP(6) WITH TIME ZONE,
    ingestion_date     DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['ingestion_date'],
    sorted_by    = ARRAY['activation_date']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.recharges (
    recharge_id    VARCHAR,
    msisdn         VARCHAR,
    amount         DOUBLE,
    currency       VARCHAR,
    channel        VARCHAR,
    voucher_code   VARCHAR,
    bonus_amount   DOUBLE,
    region         VARCHAR,
    status         VARCHAR,
    recharged_at   TIMESTAMP(6) WITH TIME ZONE,
    ingested_at    TIMESTAMP(6) WITH TIME ZONE,
    recharge_date  DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['recharge_date'],
    sorted_by    = ARRAY['recharged_at']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.complaints (
    complaint_id   VARCHAR,
    subscriber_id  VARCHAR,
    msisdn         VARCHAR,
    channel        VARCHAR,
    category       VARCHAR,
    sub_category   VARCHAR,
    priority       VARCHAR,
    status         VARCHAR,
    description    VARCHAR,
    resolution     VARCHAR,
    agent_id       VARCHAR,
    region         VARCHAR,
    opened_at      TIMESTAMP(6) WITH TIME ZONE,
    resolved_at    TIMESTAMP(6) WITH TIME ZONE,
    ingested_at    TIMESTAMP(6) WITH TIME ZONE,
    complaint_date DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['complaint_date'],
    sorted_by    = ARRAY['opened_at']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.om_transactions (
    transaction_id     VARCHAR,
    msisdn             VARCHAR,
    transaction_type   VARCHAR,
    amount             DOUBLE,
    currency           VARCHAR,
    fee_amount         DOUBLE,
    counterpart_msisdn VARCHAR,
    counterpart_type   VARCHAR,
    agent_id           VARCHAR,
    agent_region       VARCHAR,
    channel            VARCHAR,
    status             VARCHAR,
    failure_reason     VARCHAR,
    balance_before     DOUBLE,
    balance_after      DOUBLE,
    raw_metadata       VARCHAR,
    transaction_ts     TIMESTAMP(6) WITH TIME ZONE,
    ingested_at        TIMESTAMP(6) WITH TIME ZONE,
    transaction_date   DATE
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['transaction_date', 'transaction_type'],
    sorted_by    = ARRAY['transaction_ts']
);
