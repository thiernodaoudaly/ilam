# Ilam — Open-Source Data Lakehouse Platform

> *Ilam* means **"the lake", "the flow", "the water current", or "the stream"** in Pulaar — data flowing from raw sources to insight, continuously and autonomously.

<p align="center">
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" />
  </a>
  <img src="https://img.shields.io/badge/stack-Iceberg%20%7C%20Trino%20%7C%20Flink%20%7C%20dbt%20%7C%20Airflow-orange" />
  <img src="https://img.shields.io/badge/status-WIP-yellow" />
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" />
</p>

Ilam is a fully open-source, end-to-end Data Lakehouse platform designed to unify
high-performance storage, real-time stream processing, semantic transformation, and
intelligent orchestration — with zero proprietary dependencies.

At its core, Ilam introduces an **agentic intelligence layer** that transcends static
pipeline orchestration toward a dynamic, self-adaptive data flow management system,
optimizing resilience and observability from end to end.

Built on real-world telecom data from the **Sonatel / Orange Senegal** context,
covering CDR (voice, SMS, data), network events, CRM, and Orange Money transactions.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Agentic Layer         LangGraph · ReAct · autonomous agents    │
├─────────────────────────────────────────────────────────────────┤
│  Orchestration         Apache Airflow · DAGs · Sensors          │
├───────────────────────────┬─────────────────────────────────────┤
│  Transformation           │  Query Engine                       │
│  dbt Core                 │  Trino · MPP · federation           │
├───────────────────────────┴─────────────────────────────────────┤
│  Medallion Architecture   Bronze → Silver → Gold                │
├─────────────────────────────────────────────────────────────────┤
│  Storage                  Apache Iceberg · MinIO · Parquet      │
├─────────────────────────────────────────────────────────────────┤
│  Ingestion                Apache Flink · stream · exactly-once  │
├─────────────────────────────────────────────────────────────────┤
│  Security                 Keycloak · Apache Ranger · mTLS       │
│  Observability            Prometheus · Grafana · OpenLineage    │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Tool | Role |
|-------|------|------|
| Ingestion | Apache Flink 1.18 | Stream processing, exactly-once guarantees |
| Storage | MinIO | S3-compatible object storage, Erasure Coding |
| Table format | Apache Iceberg | ACID transactions, MVCC, time travel |
| Catalog | tabulario/iceberg-rest + PostgreSQL | Persistent REST Iceberg catalog |
| Query engine | Trino 435 | Distributed SQL, multi-source federation |
| Transformation | dbt Core | ELT pipelines, semantic layer |
| Orchestration | Apache Airflow 2.8 | DAG-based pipeline orchestration |
| Data quality | Great Expectations | Automated data quality checks |
| Monitoring | Prometheus + Grafana | Metrics collection and dashboards |
| Lineage | OpenLineage + Marquez | End-to-end data lineage |
| Security | Keycloak + Apache Ranger | Authentication and fine-grained authorization |
| Agents | LangGraph | Agentic intelligence layer |

## Data Model — Sonatel Telecom

The platform is built around a realistic Senegalese telecom data model:

| Layer | Tables | Description |
|-------|--------|-------------|
| Bronze | network_events, cdr_voice, cdr_sms, cdr_data | Raw network and CDR data |
| Bronze | subscribers, contracts, recharges, complaints, om_transactions | Raw CRM and Orange Money |
| Silver | network_events, cdr_enriched, subscribers, contracts, om_transactions | Enriched business entities |
| Gold | mart_revenue, mart_network_quality, mart_churn, mart_arpu, mart_om_activity, mart_usage | Analytical Data Marts |

## Prerequisites

- Docker >= 24.0
- Docker Compose >= 2.20
- Make
- Python 3.10+ with venv
- 8 GB RAM minimum (16 GB recommended)
- WSL2 + Ubuntu (on Windows)

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/thiernodaoudaly/ilam.git
cd ilam
cp .env.example .env
# Edit .env — generate a Fernet key for Airflow:
# python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Start all services
make start

# 4. Check services health
make health

# 5. Generate simulated Sonatel data (first time only)
make generate-data

# 6. Verify data was loaded
make verify-data
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | See `.env` |
| Trino UI | http://localhost:8080 | None (dev) |
| Flink UI | http://localhost:8081 | None (dev) |
| Airflow UI | http://localhost:8082 | See `.env` |
| Prometheus | http://localhost:9090 | None |
| Grafana | http://localhost:3000 | See `.env` |
| Iceberg REST | http://localhost:8181 | None |

## Makefile Reference

```bash
# Infrastructure lifecycle
make start            # Start all services
make stop             # Stop all services
make restart          # Restart all services
make status           # Show containers status
make health           # Check all services health
make logs             # Tail all service logs
make clean            # Remove containers and volumes

# Warehouse management
make init-warehouse   # Create Iceberg schemas and tables (first time only)
make show-tables      # List all Iceberg tables (Bronze / Silver / Gold)
make trino-cli        # Open interactive Trino SQL shell

# Data
make generate-data    # Generate and insert simulated Sonatel telecom data
make verify-data      # Check row counts in all Bronze tables
```

## Project Structure

```
ilam/
├── docs/
│   └── decisions/          Architecture Decision Records (ADR-001 to ADR-004)
├── infra/
│   ├── trino/              Trino catalog and server configuration
│   └── monitoring/         Prometheus and Grafana configuration
├── ingestion/
│   └── generators/         Sonatel simulated data generator (Python)
├── warehouse/
│   ├── bronze/ddl/         Raw layer DDL (9 tables)
│   ├── silver/ddl/         Enriched layer DDL (5 tables)
│   └── gold/ddl/           Analytical layer DDL (6 tables)
├── transform/              dbt project (coming)
├── orchestration/          Airflow DAGs (coming)
├── agents/                 Agentic intelligence layer (coming)
├── quality/                Great Expectations suites (coming)
├── scripts/                Utility scripts
├── .env.example            Environment variables template
├── docker-compose.yml      Full infrastructure definition
├── Makefile                Infrastructure and data lifecycle commands
└── requirements.txt        Python dependencies
```

## Architecture Decision Records

| ADR | Decision | Status |
|-----|----------|--------|
| [ADR-001](docs/decisions/ADR-001-iceberg-vs-delta.md) | Apache Iceberg over Delta Lake | Accepted |
| [ADR-002](docs/decisions/ADR-002-trino-vs-spark.md) | Trino over Spark SQL | Accepted |
| [ADR-003](docs/decisions/ADR-003-minio-vs-s3.md) | MinIO for data sovereignty | Accepted |
| [ADR-004](docs/decisions/ADR-004-iceberg-rest-vs-nessie.md) | iceberg-rest over Nessie (dev) | Accepted |

## Roadmap

- [x] Step 1 — Base infrastructure (Docker Compose, MinIO, Iceberg REST, Trino, Flink, Airflow)
- [x] Step 2 — Iceberg tables DDL (Bronze / Silver / Gold) — 20 tables
- [x] Step 3 — Simulated Sonatel data generator — 2 901 rows across 9 Bronze tables
- [x] Step 3.1 — Persistent Iceberg REST catalog backed by PostgreSQL
- [ ] Step 4 — dbt transformation models (Bronze → Silver → Gold)
- [ ] Step 5 — Airflow orchestration DAGs
- [ ] Step 6 — Data quality (Great Expectations)
- [ ] Step 7 — Observability (Prometheus, Grafana dashboards)
- [ ] Step 8 — Security (Keycloak, Apache Ranger)
- [ ] Step 9 — Agentic intelligence layer (LangGraph, ReAct)

## License

Apache License 2.0 — see [LICENSE](LICENSE)

*Built with purpose. Named after the flow.*
