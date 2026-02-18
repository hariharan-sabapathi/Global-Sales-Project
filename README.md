# Global Sales ELT Pipeline
**Snowflake · Snowpark (Python) · GitHub Actions · Power BI**

---

## Overview

This project demonstrates a production-style, end-to-end ELT data pipeline built on **Snowflake**, using **Snowpark (Python)** for in-warehouse transformations, **GitHub Actions** for CI/CD-style orchestration, and **Power BI** for analytics and reporting.

The pipeline ingests heterogeneous global sales data from multiple countries, standardizes and transforms it through a layered warehouse architecture, and exposes analytics-ready curated tables directly to Power BI dashboards.

---

## Architecture

### Data Sources
- India sales data (CSV)
- USA sales data (Parquet)
- UK sales data (CSV)

### Storage
- Amazon S3 (Snowflake external stage)

### Processing & Warehousing
- Snowflake
- Snowpark (Python)

### Orchestration / CI-CD
- GitHub Actions

### Analytics & Visualization
- Power BI (connected to curated Snowflake tables)

---

## Data Warehouse Design

The pipeline follows a **layered warehouse architecture** commonly used in production systems.

### STAGING (Landing Layer)
- Raw files loaded from S3
- Minimal validation
- Schema matches source systems

### RAW (Cleaned & Typed Layer)
- Data type normalization
- Basic data cleaning
- Insert timestamps added
- Source-specific schemas preserved

### TRANSFORMED (Business-Standardized Layer)
- Country-level schema standardization
- Join of India order and order-detail datasets
- Union of India, USA, and UK datasets
- Creation of a unified `GLOBAL_SALES_ORDER` table

### CURATED (Analytics-Ready Layer)
- Aggregated, business-facing tables
- Optimized for BI tools
- Directly consumed by Power BI dashboards

---

## Repository Structure

```
Global-Sales-ELT-Pipeline/
├── data/
│   ├── india/
│   │   ├── order_details.csv
│   │   ├── orders.csv
│   │   └── sales_targets.csv
│   ├── uk/
│   │   └── uk_orders.csv
│   └── usa/
│       ├── Superstore_Orders_q1.csv
│       ├── Superstore_Orders_q2.csv
│       └── usa_orders.parquet
├── snowpark/
│   ├── raw_load.py
│   ├── transformed_load.py
│   └── curated_load.py
├── sql/
│   └── setup.sql
├── powerbi/
│   ├── Global Sales Analytics Model.pbix
│   └── Global Sales Analytics Model.pdf
├── utils/
│   └── run_sql.py
├── .github/
│   └── workflows/
│       └── snowpark_pipeline.yml
└── README.md
```

---

## Core Components

### `setup.sql`
Creates all required Snowflake objects: the database, schemas, staging/raw/transformed/curated tables, and the external S3 stage. The script is **idempotent** and can be safely re-run without side effects.

### `raw_load.py`
Handles the **Extract & Load** phase. Truncates staging tables, loads raw files from S3 via `COPY INTO`, performs basic cleaning and type normalization, and writes the result into the RAW schema.

### `transformed_load.py`
Handles the **Transform** phase. Standardizes schemas across all three country datasets, joins India's order and order-detail tables, and unifies everything into `GLOBAL_SALES_ORDER`.

### `curated_load.py`
Produces **analytics-ready tables** for direct BI consumption:

- `SALES_BY_COUNTRY`
- `MONTHLY_SALES_TREND`
- `CATEGORY_PERFORMANCE`
- `INDIA_SALES_VS_TARGET`
- `TOP_PRODUCTS_BY_REVENUE`

### `verify_pipeline.sql`
A **read-only validation script** for human inspection. It verifies row counts across layers, inspects curated tables, and validates transformation outputs. This script is intentionally not automated.


---

## Power BI Integration

Power BI connects directly to the **CURATED schema** in Snowflake. The curated tables act as a read-only semantic layer. All business logic lives inside Snowflake, keeping the BI layer thin and consistent.

---

## GitHub Actions Orchestration

The pipeline is orchestrated via **GitHub Actions**, providing lightweight CI/CD without requiring Airflow or a dedicated orchestration tool.

**Triggers:** push to `main`, or manual `workflow_dispatch`.

**Execution order:**
1. `setup.sql`
2. `raw_load.py`
3. `transformed_load.py`
4. `curated_load.py`

Automation ensures reproducibility, enables unattended execution, and surfaces failures via logs.

---

## Running the Pipeline

### Option 1 — Manual (Development / Debugging)

Run the following in Snowflake:
1. `setup.sql`
2. `raw_load.py`
3. `transformed_load.py`
4. `curated_load.py`
5. `verify_pipeline.sql`

Then refresh Power BI to view updated dashboards.

### Option 2 — Automated (CI/CD)

1. Push changes to the `main` branch
2. GitHub Actions automatically executes the pipeline
3. Verify results using `verify_pipeline.sql`
4. Refresh Power BI dashboards

---

## Verifying the Pipeline

Verification is intentionally separated from execution. Run these queries manually after each pipeline run:

```sql
-- Check raw layer row counts
SELECT COUNT(*) FROM SNOWPARK_DB.RAW.INDIA_ORDERS;

-- Check unified transformed table
SELECT COUNT(*) FROM SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER;

-- Inspect curated output
SELECT * FROM SNOWPARK_DB.CURATED.SALES_BY_COUNTRY;
```

Visual verification is performed through Power BI dashboards, confirming that curated tables produce meaningful and accurate business insights.

---

## Key Takeaways

- Demonstrates modern **ELT best practices** using a cloud data warehouse
- Uses **Snowpark** for scalable, in-warehouse Python transformations
- Applies a **layered warehouse architecture**: Staging → Raw → Transformed → Curated
- Implements **practical CI/CD orchestration** with GitHub Actions — no Airflow required
- Connects **Snowflake directly to Power BI** for analytics and reporting
- Cleanly separates **execution, validation, and visualization** responsibilities

---

## Author
Hariharan Nadanasabapathi
