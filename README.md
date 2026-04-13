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

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Agentic Layer      LangGraph agents · ReAct · autonomous   │
├─────────────────────────────────────────────────────────────┤
│  Orchestration      Apache Airflow · DAGs · Sensors         │
├──────────────────────────┬──────────────────────────────────┤
│  Transformation          │  Query                           │
│  dbt Core                │  Trino (MPP · federation)        │
├─────────────────┬────────┴──────────────────────────────────┤
│  Medallion      │  Bronze → Silver → Gold                   │
├─────────────────┴──────────────────────────────────────────-┤
│  Storage        Apache Iceberg · MinIO · Parquet            │
├─────────────────────────────────────────────────────────────┤
│  Ingestion      Apache Flink · stream · exactly-once        │
├─────────────────────────────────────────────────────────────┤
│  Security       Keycloak · Apache Ranger · Vault · mTLS     │
│  Observability  Prometheus · Grafana · OpenLineage          │
└─────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Tool | Role |
|-------|------|------|
| Ingestion | Apache Flink 1.18 | Stream processing, exactly-once guarantees |
| Storage | MinIO | S3-compatible object storage |
| Table format | Apache Iceberg | ACID transactions, MVCC, time travel |
| Catalog | Project Nessie | Git-like Iceberg catalog |
| Query engine | Trino 435 | Distributed SQL, multi-source federation |
| Transformation | dbt Core | ELT pipelines, semantic layer |
| Orchestration | Apache Airflow 2.8 | DAG-based pipeline orchestration |
| Data quality | Great Expectations | Automated data quality checks |
| Monitoring | Prometheus + Grafana | Metrics collection and dashboards |
| Lineage | OpenLineage + Marquez | End-to-end data lineage |
| Security | Keycloak + Apache Ranger | Authentication and fine-grained authorization |
| Agents | LangGraph | Agentic intelligence layer |

## Quick Start

**Prerequisites**

- Docker >= 24.0
- Docker Compose >= 2.20
- Make
- 8 GB RAM minimum (16 GB recommended)
- WSL2 + Ubuntu (on Windows)

**1. Clone and configure**

```bash
git clone https://github.com/thiernodaoudaly/ilam.git
cd ilam
cp .env.example .env
# Generate a Fernet key for Airflow:
# python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste it into .env as AIRFLOW__CORE__FERNET_KEY
```

**2. Start the platform**

```bash
make start
```

**3. Check services health**

```bash
make health
```

**4. Access the UIs**

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | See `.env` |
| Trino UI | http://localhost:8080 | None (dev) |
| Flink UI | http://localhost:8081 | None (dev) |
| Airflow UI | http://localhost:8082 | See `.env` |
| Prometheus | http://localhost:9090 | None |
| Grafana | http://localhost:3000 | See `.env` |

## Project Structure

```
ilam/
├── docs/               Architecture docs, ADRs
├── infra/              Docker configs (Trino, Flink, Monitoring)
├── ingestion/          Flink jobs
├── warehouse/          Iceberg DDL (Bronze / Silver / Gold)
├── transform/          dbt project
├── orchestration/      Airflow DAGs
├── agents/             Agentic intelligence layer
├── quality/            Great Expectations suites
└── scripts/            Utility scripts
```

## Architecture Decision Records

| ADR | Decision | Status |
|-----|----------|--------|
| [ADR-001](docs/decisions/ADR-001.md) | Apache Iceberg over Delta Lake | Accepted |
| [ADR-002](docs/decisions/ADR-002.md) | Trino over Spark SQL | Accepted |
| [ADR-003](docs/decisions/ADR-003.md) | MinIO for local sovereignty | Accepted |

## Roadmap

- [x] Step 1 — Base infrastructure (Docker Compose, MinIO, Nessie, Trino)
- [x] Step 2 — Iceberg tables DDL (Bronze / Silver / Gold)
- [ ] Step 3 — Flink ingestion jobs
- [ ] Step 4 — dbt transformation models
- [ ] Step 5 — Airflow orchestration DAGs
- [ ] Step 6 — Data Quality (Great Expectations)
- [ ] Step 7 — Observability (Prometheus, Grafana)
- [ ] Step 8 — Security (Keycloak, Ranger)
- [ ] Step 9 — Agentic intelligence layer

## License

Apache License 2.0 — see [LICENSE](LICENSE)

---

*Built with purpose. Named after the flow.*
