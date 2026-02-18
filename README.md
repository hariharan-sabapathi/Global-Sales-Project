# Global Sales ELT Pipeline  
**Snowflake • Snowpark (Python) • GitHub Actions • Power BI**

## Overview

This project demonstrates a **production-style end-to-end ELT data pipeline** built on **Snowflake**, using **Snowpark (Python)** for transformations, **GitHub Actions** for CI/CD-style orchestration, and **Power BI** for analytics and reporting.

The pipeline ingests **heterogeneous global sales data** from multiple countries, standardizes and transforms it through layered warehouse models, and exposes **analytics-ready curated tables** directly to Power BI dashboards.

This project intentionally mirrors a **real-world data platform**, covering ingestion, transformation, automation, validation, and visualization — not a one-off script.

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
- Power BI (direct connection to curated Snowflake tables)

---

## Data Warehouse Design

The pipeline follows a **layered warehouse architecture** commonly used in production systems:

### STAGING (Landing Layer)
- Raw data loaded from S3
- Minimal validation
- Schema matches source systems

### RAW (Cleaned & Typed Layer)
- Data type normalization
- Basic data cleaning
- Insert timestamps added

### TRANSFORMED (Business-Standardized Layer)
- Country-level schema standardization
- Joining India order and order-detail datasets
- Union of India, USA, and UK data
- Creation of a unified `GLOBAL_SALES_ORDER` table

### CURATED (Analytics-Ready Layer)
- Aggregated, business-facing tables
- Optimized for BI tools
- Directly consumed by Power BI dashboards

---

## Repository Structure

```text
Global-Sales-ELT-Pipeline/
├── snowpark/
│   ├── raw_load.py
│   ├── transformed_load.py
│   └── curated_load.py
├── sql/
│   └── setup.sql
├── utils/
│   └── run_sql.py
├── .github/
│   └── workflows/
│       └── snowpark_pipeline.yml
└── README.md

---


**## Core Components
**
### 1. `setup.sql`
Creates all required Snowflake objects:
- Database and schemas
- Staging tables
- External stage (Amazon S3)

This script is **idempotent** and can be safely re-run without side effects.

---

### 2. `raw_load.py`
Handles the **Extract & Load** phase of the pipeline:
- Truncates staging tables
- Loads raw files from S3 using `COPY INTO`
- Performs basic cleaning and data type normalization
- Writes cleaned data into the `RAW` schema

---

### 3. `transformed_load.py`
Handles the **Transform** phase:
- Standardizes schemas across countries
- Joins India order and order-detail datasets
- Unions India, USA, and UK sales data
- Produces a unified `GLOBAL_SALES_ORDER` table

---

### 4. `curated_load.py`
Creates **analytics-ready curated tables**, including:
- Sales by country
- Category performance
- Monthly sales trends
- India sales vs targets
- Top products by revenue

These tables are optimized for **direct BI consumption**.

---

### 5. `verify_pipeline.sql`
A **read-only validation script** used to:
- Confirm row counts at each warehouse layer
- Inspect curated tables
- Validate pipeline correctness

This script is intentionally **not automated** and is meant for human inspection.

---

## Power BI Integration

Power BI serves as the analytics and reporting layer, connected directly to the `CURATED` schema in Snowflake.

### Power BI Data Model
Power BI consumes the following curated tables:
- `SALES_BY_COUNTRY`
- `MONTHLY_SALES_TREND`
- `CATEGORY_PERFORMANCE`
- `TOP_PRODUCTS_BY_REVENUE`
- `INDIA_SALES_VS_TARGET`

These tables act as **read-only semantic layers**, ensuring that all business logic remains inside Snowflake.

---

### Power BI Dashboards

The Power BI report includes:

#### Global Sales KPIs
- Total Sales
- Total Quantity
- Profit Margin

#### Sales Performance Analysis
- Sales by Country
- Monthly Sales Trend by Country
- Sales by Category

#### Target vs Actual Analysis (India)
- Actual vs Target sales line chart
- Variance heatmap by category
- KPI indicators for goal tracking

#### Distribution & Variability Analysis
- Box plot showing variance distribution across categories

These dashboards validate that the ELT pipeline produces **business-consumable outputs**, not just transformed tables.

---

## Power BI Refresh Strategy

1. GitHub Actions executes the ELT pipeline  
2. Snowflake curated tables are updated  
3. Power BI refresh pulls the latest data from Snowflake  

This mirrors a **real-world warehouse → BI workflow**.

---

## GitHub Actions Orchestration

The pipeline is orchestrated using **GitHub Actions** to provide lightweight CI/CD without requiring Airflow.

### Workflow Behavior
- Triggered on:
  - Push to `main`
  - Manual workflow dispatch
- Executes in order:
  1. `setup.sql`
  2. `raw_load.py`
  3. `transformed_load.py`
  4. `curated_load.py`

### Purpose of Automation
- Ensures reproducibility
- Validates pipeline execution without manual intervention
- Provides execution logs and failure visibility
- Demonstrates production readiness

Automation is used for **execution and validation only**, not visualization.

---

## How to Run the Project

### Option 1: Manual (Development / Debugging)

Run the following in Snowflake:
1. `setup.sql`
2. `raw_load.py`
3. `transformed_load.py`
4. `curated_load.py`
5. `verify_pipeline.sql`

Then refresh Power BI to view updated dashboards.

---

### Option 2: Automated (CI/CD)

1. Push changes to the `main` branch
2. GitHub Actions automatically executes the pipeline
3. Verify results using `verify_pipeline.sql`
4. Refresh Power BI dashboards

---

## How to Verify the Pipeline

Verification is intentionally separated from execution.

Example checks:
```sql
SELECT COUNT(*) FROM SNOWPARK_DB.RAW.INDIA_ORDERS;
SELECT COUNT(*) FROM SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER;
SELECT * FROM SNOWPARK_DB.CURATED.SALES_BY_COUNTRY;


Visual verification is performed through Power BI dashboards, ensuring that curated tables produce meaningful and accurate business insights.

---

## Key Takeaways

- Demonstrates modern **ELT best practices** using a cloud data warehouse
- Uses **Snowpark** for scalable, in-warehouse transformations
- Applies a **layered warehouse architecture** (Staging → Raw → Transformed → Curated)
- Implements **practical CI/CD-style orchestration** without Airflow
- Integrates **Snowflake directly with Power BI** for analytics and reporting
- Clearly separates **execution, validation, and visualization** responsibilities

---

## Author Notes

This project is intentionally scoped to balance **clarity, correctness, and production realism**.  
It is designed to be suitable for:
- Technical interviews
- Portfolio demonstrations
- Learning and showcasing modern data engineering workflows
