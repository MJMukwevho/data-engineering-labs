# Data Engineering Labs

A hands-on lab for designing and building production-style data platforms: locally first, then on AWS.

The goal is to build skills as an **AWS data solutions architect**. That means writing pipelines, but also documenting the *why* behind each design: trade-offs, cost, security and how the same architecture maps onto managed AWS services.

---

## Architecture

The target architecture is a medallion-style lakehouse with batch and streaming ingestion:

```mermaid
flowchart LR
    subgraph Sources
        A[FRED API<br/>market indices & rates]
        B[Event streams]
    end

    subgraph Ingestion
        C[Batch ingestion<br/>Java / PySpark]
        D[Streaming ingestion<br/>Kafka]
    end

    subgraph Lake["Object storage (MinIO → S3)"]
        E[(Raw / Bronze)]
        F[(Cleaned / Silver)]
        G[(Curated / Gold)]
    end

    subgraph Serving
        H[(Postgres → RDS / Redshift)]
        I[SQL queries<br/>Athena]
    end

    A --> C --> E
    B --> D --> E
    E -- Spark --> F -- Spark --> G
    G --> H
    G --> I
```

1. **Ingest:** data lands unchanged in the raw layer.
2. **Process:** Spark jobs clean, validate and model the data into the silver and gold layers (Parquet / Apache Iceberg).
3. **Serve:** curated data is loaded into a relational store or queried in place with SQL.

> This diagram shows the **target** design. See [Project Status](#project-status) for what is built today.

---

## Data Source

All data comes from **[FRED](https://fred.stlouisfed.org/)** (Federal Reserve Economic Data, St. Louis Fed). It's free with an API key and is updated daily.

| Series ID   | Description                         | Frequency |
|-------------|-------------------------------------|-----------|
| `SP500`     | S&P 500 index                       | Daily     |
| `DJIA`      | Dow Jones Industrial Average        | Daily     |
| `NASDAQCOM` | NASDAQ Composite index              | Daily     |
| `VIXCLS`    | CBOE Volatility Index (VIX)         | Daily     |
| `DGS10`     | 10-year US Treasury yield           | Daily     |

Mixing equity indices with interest rates allows gold-layer models such as index performance versus rate changes.

> FRED republishes some series under licence (for example, `SP500` only covers roughly the last 10 years). Check each series' notes on FRED before relying on its history.

---

## Tech Stack

Each local component has an AWS equivalent, so the same design can be moved to the cloud.

| Concern            | Local (Docker)      | AWS equivalent                     |
|--------------------|---------------------|------------------------------------|
| Data source        | FRED API            | FRED API (scheduled by EventBridge)|
| Object storage     | MinIO               | Amazon S3                          |
| Batch processing   | Apache Spark        | AWS Glue / EMR Serverless          |
| Streaming          | Kafka / Redpanda    | Amazon Kinesis / Amazon MSK        |
| Relational storage | PostgreSQL          | Amazon RDS / Aurora                |
| Warehouse          | PostgreSQL          | Amazon Redshift Serverless         |
| Catalog & query    | Spark SQL           | AWS Glue Data Catalog + Athena     |
| Orchestration      | Apache Airflow      | Amazon MWAA / AWS Step Functions   |
| Governance         | —                   | AWS Lake Formation + IAM           |
| Infrastructure     | Docker / Compose    | Terraform or AWS CDK               |

**Languages:** Java (ingestion), Python / PySpark and SQL (processing).

---

## Project Status

### Phase 1: Local foundation 🚧
- [x] Base container image with Java 17 (`infrastructure/dockerfile`)
- [x] Batch ingestion job: FRED → raw layer (`jobs/fred-ingestion`)
- [ ] Docker Compose stack: Spark, PostgreSQL, MinIO
- [ ] Write raw data to MinIO instead of the local filesystem
- [ ] Spark transformations: raw → silver → gold
- [ ] Data quality checks and tests
- [ ] Orchestration with Airflow
- [ ] Streaming ingestion with Kafka / Redpanda

### Phase 2: AWS deployment
- [ ] Infrastructure as code (Terraform or CDK)
- [ ] S3 data lake + Glue Data Catalog + Athena
- [ ] Spark jobs on Glue / EMR Serverless
- [ ] Budget alerts and one-command teardown

### Phase 3: Architecture & operations
- [ ] Architecture Decision Records (`docs/adr/`)
- [ ] Cost estimates for each architecture
- [ ] Security: least-privilege IAM, KMS encryption, Secrets Manager
- [ ] Well-Architected review (Data Analytics Lens)
- [ ] CI/CD with GitHub Actions

---

## Getting Started

### Prerequisites
- Java 17+ and [Maven](https://maven.apache.org/)
- [Docker](https://docs.docker.com/get-docker/) (for the base image)
- Git
- A free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html)

### Run the FRED ingestion job

```bash
git clone https://github.com/MJMukwevho/data-engineering-labs.git
cd data-engineering-labs/jobs/fred-ingestion

mvn package                               # builds and runs the tests
export FRED_API_KEY=your_key_here
java -jar target/fred-ingestion-0.1.0.jar
```

Optional settings:

| Variable                 | Default                              | Purpose                                  |
|--------------------------|--------------------------------------|------------------------------------------|
| `FRED_SERIES`            | `SP500,DJIA,NASDAQCOM,VIXCLS,DGS10`  | Comma-separated series to pull           |
| `FRED_OBSERVATION_START` | full history                         | Only fetch observations from this date   |
| `RAW_DATA_DIR`           | `data/raw`                           | Root of the raw layer                    |

Output is partitioned so it can move to S3 unchanged:

```
data/raw/fred/series_id=SP500/ingest_date=2026-10-22/observations.json
data/raw/fred/series_id=SP500/ingest_date=2026-10-22/_metadata.json
```

Re-running on the same day overwrites that day's partition, so the job is safe to retry.

### Build the base image

From the repository root:

```bash
docker build -t data-eng-labs-base -f infrastructure/dockerfile infrastructure
```

### Verify

```bash
docker run --rm data-eng-labs-base java -version
```

> More services (Spark, Postgres, MinIO) will be added through Docker Compose as Phase 1 progresses.

---

## Repository Layout

```
data-engineering-labs/
├── infrastructure/
│   └── dockerfile      # Base image: Arch Linux + OpenJDK 17
├── jobs/
│   └── fred-ingestion/ # Java job: FRED API → raw layer
├── .gitignore
└── README.md
```

Planned additions:

| Folder        | Purpose                                         |
|---------------|-------------------------------------------------|
| `jobs/`       | More jobs, including Spark transformations      |
| `pipelines/`  | Batch and streaming pipeline definitions        |
| `data/`       | Small sample datasets (large data stays out of Git) |
| `docs/`       | ADRs, architecture diagrams and cost notes      |

---

## Design Decisions

Key decisions will be recorded as Architecture Decision Records in `docs/adr/`. Planned topics:

- Table format: Apache Iceberg vs Delta Lake vs plain Parquet
- Batch compute on AWS: Glue vs EMR Serverless
- Orchestration: Airflow (MWAA) vs Step Functions
- Streaming: Kinesis vs MSK

---

## Philosophy

> Learn by building production-like systems locally before scaling to the cloud.

Running everything locally keeps experiments fast and free. Mapping each component to a managed AWS service makes the move to the cloud a deliberate design exercise rather than a rewrite.

---

## Learning Notes

Short notes on what each lab taught, including mistakes, surprises and trade-offs, will be added here as the project grows.
