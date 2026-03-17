# 🏪 Retail Analytics Lakehouse
### End-to-End Data Engineering Project on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-1F3864?style=for-the-badge&logo=databricks&logoColor=white)

---

## 📋 Table of Contents
- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Dataset](#-dataset)
- [Pipeline Layers](#-pipeline-layers)
- [Data Quality](#-data-quality)
- [Gold Layer KPIs](#-gold-layer-kpis)
- [Performance Optimisation](#-performance-optimisation)
- [Automated Workflow](#-automated-workflow)
- [Power BI Dashboard](#-power-bi-dashboard)
- [Audit Log Results](#-audit-log-results)
- [Key Learnings](#-key-learnings)
- [Future Improvements](#-future-improvements)
- [Setup & Usage](#-setup--usage)

---

## 🎯 Project Overview

A production-grade **Retail Analytics Lakehouse** built on **Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold). Raw retail CSV files are ingested, cleaned, modelled, and aggregated into business-ready metrics consumed by a **4-page Power BI dashboard**.

### Key Highlights
| Metric | Value |
|--------|-------|
| Raw Sales Records | 200,000 |
| Customers Processed | 10,000 |
| Products Catalogued | 1,000 |
| Delta Tables Created | 15 |
| Pipeline Tasks | 3 (Automated) |
| Dashboard Pages | 4 |
| DAX Measures | 15 |
| Data Quality Issues Resolved | 12 |

---

## 🏗️ Architecture
```
Operational Sales System
        │
        ▼
┌─────────────────────┐
│   SOURCE TABLES     │
│  Master Customers   │
│  Master Products    │
│  Sales Transactions │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  UNITY CATALOG      │
│     VOLUME          │
│  /retail_data/      │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   BRONZE LAYER      │  ← Raw Delta tables, all STRING, metadata appended
│   bronze_sales      │
│   bronze_master     │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   SILVER LAYER      │  ← Cleaned, typed, deduplicated, DQ checked
│ silver_customers    │
│ silver_products     │
│ silver_sales        │
│ dim_date            │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│    GOLD LAYER       │  ← Pre-aggregated business KPIs
│ gold_daily_sales    │
│ gold_customer_ltv   │
│ gold_category_perf  │
│ gold_repeat_rate    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│    POWER BI         │  ← 4-page enterprise dashboard
│  Executive Summary  │
│  Sales Deep Dive    │
│  Product & Category │
│  Customer Intel     │
└─────────────────────┘
```

**Orchestration:** Databricks Workflow Job → Task 1: Bronze → Task 2: Silver → Task 3: Gold
**Schedule:** Daily at 02:00 AM IST `(CRON: 0 0 2 * * ?)`

---

## 📊 Data Model

Star Schema with `fact_sales` at the centre and 3 surrounding dimension tables:
```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │  customer_id PK │
                    └────────┬────────┘
                             │ 1
                             │
┌──────────────┐      *      │      *      ┌──────────────┐
│  dim_date    ├─────────────┤─────────────┤ dim_products │
│  date_key PK │      ┌──────┴──────┐      │ product_id PK│
└──────────────┘      │ fact_sales  │      └──────────────┘
                      │  sale_id PK │
                      │ customer_id │ FK
                      │  product_id │ FK
                      │   date_key  │ FK
                      │   quantity  │
                      │  unit_price │
                      │line_revenue │
                      └─────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Platform | Databricks (Unity Catalog + SQL Warehouses) |
| Storage Format | Delta Lake (ACID, Time Travel, Schema Enforcement) |
| File Storage | Unity Catalog Volumes (replaces DBFS) |
| Query Language | Spark SQL / Databricks SQL |
| Orchestration | Databricks Workflows (3-task Job) |
| Visualisation | Microsoft Power BI (Import mode + DAX) |
| Architecture | Medallion (Bronze / Silver / Gold) |

---

## 📁 Project Structure
```
retail-analytics-lakehouse/
│
├── notebooks/
│   ├── 00_setup.sql                    # Catalog, schema, volume creation
│   ├── 01_bronze_ingestion.sql         # Raw CSV → Bronze Delta tables
│   ├── 02_silver_transformation.sql    # Cleaning, typing, deduplication
│   ├── 03_gold_aggregations.sql        # Business KPI aggregations
│   ├── 04_dim_fact_models.sql          # Star schema dimension & fact tables
│   ├── 05_optimization.sql             # OPTIMIZE, ZORDER, VACUUM, ANALYZE
│   └── 06_audit_log.sql               # Pipeline audit logging
│
├── data/
│   ├── Master_customers.csv            # 10,009 customer records
│   ├── master_products.csv             # 1,000 product records
│   └── sales.csv                      # 200,000 sales transactions
│
├── powerbi/
│   ├── RetailAnalytics_Dashboard.pbix  # Power BI dashboard file
│   └── RetailAnalytics_Theme.json      # Custom enterprise dark theme
│
├── docs/
│   ├── EDA_Report.docx
│   ├── Implementation_Report.docx
│   ├── PowerBI_Dashboard_Guide.docx
│   ├── Complete_Project_Report_v2.docx
│   └── Project_Summary.pptx
│
├── diagrams/
│   ├── Architecture_Diagram.jpg
│   └── Data_Modelling.png
│
└── README.md
```

---

## 📦 Dataset

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| `Master_customers.csv` | 10,009 | 5 | customer_id, full_name, email, city, signup_date |
| `master_products.csv` | 1,000 | 4 | product_id, product_name, category, price |
| `sales.csv` | 200,000 | 9 | sale_id, event_timestamp, customer_id, product_id, quantity, currency, channel, source_file, ingestion_timestamp |

**Cities covered:** Pune, Chennai, Mumbai, Hyderabad, Kolkata, Ahmedabad, Bengaluru, Delhi

**Product Categories:** Beauty, Electronics, Grocery, Toys, Fashion, Home, Books

**Sales Period:** January 2023 – January 2024 (366 days)

---

## 🔄 Pipeline Layers

### 🥉 Bronze Layer
- All columns ingested as **STRING** — `inferSchema = false`
- No transformations applied — raw data preserved exactly
- 3 audit columns appended: `_source_file`, `_ingestion_timestamp`, `_pipeline_version`
- Reusable ingestion framework via `sp_bronze_ingest` stored procedure
```sql
CREATE OR REPLACE TABLE retail_catalog.retail_analytics.bronze_sales
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT *, 'Volume:retail_data/sales.csv' AS _source_file,
  current_timestamp() AS _ingestion_timestamp,
  'bronze_ingestion_v1' AS _pipeline_version
FROM read_files(
  '/Volumes/retail_catalog/retail_analytics/retail_data/sales.csv',
  format => 'csv', header => true, inferSchema => false
);
```

### 🥈 Silver Layer
Key transformations applied:

| Issue | Fix |
|-------|-----|
| 9 duplicate customers | `ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingestion_timestamp DESC)` |
| Corrupted price values ('356t78', 'pp567') | `RLIKE '^[0-9]+(\.[0-9]+)?$'` guard + `TRY_CAST` |
| Alphabetic quantity ('b', 'd') | `CASE WHEN quantity RLIKE '^[0-9]+$' THEN TRY_CAST(...) ELSE NULL END` |
| NULL / zero FK values | `WHERE TRY_CAST(customer_id AS INT) IS NOT NULL AND TRY_CAST(customer_id AS INT) > 0` |
| All timestamps as STRING | `TRY_TO_TIMESTAMP(col, 'dd-MM-yyyy HH:mm')` |
| sale_date extraction | `DATE(TRY_TO_TIMESTAMP(...))` |

> ⚠️ **Key Learning:** Spark SQL's Catalyst optimiser does NOT short-circuit `CAST` inside `CASE WHEN` or `WHERE` clauses. Always use `TRY_CAST` in Silver layer to handle dirty Bronze data gracefully.

### 🥇 Gold Layer
Pre-aggregated business metrics for Power BI consumption:
```sql
-- fact_sales: Star schema with computed line_revenue
ROUND(s.quantity * p.unit_price, 2) AS line_revenue

-- gold_daily_sales: Daily aggregations joined with dim_date
COUNT(DISTINCT f.sale_id) AS total_transactions,
SUM(f.quantity)           AS total_items_sold,
ROUND(SUM(f.line_revenue), 2) AS total_revenue

-- gold_customer_lifetime_value: Per-customer LTV
ROUND(SUM(f.line_revenue) / NULLIF(COUNT(DISTINCT f.sale_id), 0), 2) AS avg_order_value

-- gold_category_performance: Category share using CROSS JOIN
ROUND(b.category_revenue / t.grand_total * 100, 2) AS category_share_pct

-- dim_date: Programmatically generated using SEQUENCE + EXPLODE
SELECT EXPLODE(SEQUENCE(DATE '2022-01-01', DATE '2024-12-31', INTERVAL 1 DAY)) AS date
```

---

## ✅ Data Quality

Rules applied after Silver layer creation:

| Rule | Table | Records Rejected |
|------|-------|-----------------|
| NULL or invalid quantity | silver_sales | 2 |
| Non-numeric price | silver_products | 2 |
| Zero price | silver_products | 3 |
| Invalid email format | silver_customers | ~5 |
| NULL signup_date | silver_customers | ~3 |
| **Total** | | **15** |

Invalid records → `retail_catalog.retail_analytics.rejected_rows` (quarantined, not deleted)

---

## 📈 Gold Layer KPIs

| Gold Table | Rows | Partition | Key Metric |
|------------|------|-----------|-----------|
| `gold_daily_sales` | 366 | year | total_revenue, total_transactions, avg_transaction_value |
| `gold_customer_lifetime_value` | 10,000 | city | lifetime_revenue, total_orders, avg_order_value |
| `gold_category_performance` | 7 | category | category_revenue, items_sold, category_share_pct |
| `gold_repeat_customer_rate` | 1 | — | repeat_customers, first_time_buyers, repeat_customer_rate_pct |

---

## ⚡ Performance Optimisation
```sql
-- Compact small files + co-locate data for fast date-range queries
OPTIMIZE retail_catalog.retail_analytics.fact_sales
  ZORDER BY (sale_date, customer_id, product_id);

-- Remove stale Delta files (retain 7 days for time travel)
VACUUM retail_catalog.retail_analytics.fact_sales RETAIN 168 HOURS;

-- Update query optimiser statistics
ANALYZE TABLE retail_catalog.retail_analytics.fact_sales
  COMPUTE STATISTICS FOR ALL COLUMNS;
```

**Partitioning Strategy:**

| Table | Partition Column | Reason |
|-------|-----------------|--------|
| `silver_sales`, `fact_sales` | `sale_year` | Year-scoped queries skip irrelevant partitions |
| `gold_daily_sales` | `year` | Power BI date slicers filter by year |
| `gold_customer_lifetime_value` | `city` | City is a common filter in customer analytics |
| `gold_category_performance` | `category` | Each category queried independently |

---

## 🤖 Automated Workflow

3-task Databricks Job with dependency enforcement:
```
Task 1: bronze_ingestion
        │
        ▼ (on success)
Task 2: silver_transformation
        │
        ▼ (on success)
Task 3: gold_aggregations
```

| Setting | Value |
|---------|-------|
| Schedule | Daily 02:00 AM IST |
| CRON | `0 0 2 * * ?` (Quartz format) |
| Cluster | Job Cluster (auto-terminate) |
| Max Retries | 2 per task |
| Retry Delay | 5 minutes |
| Notifications | Email on failure |

**Incremental Load (MERGE pattern):**
```sql
MERGE INTO silver_sales AS target
USING (SELECT ... FROM bronze_sales WHERE ...) AS source
ON target.sale_id = source.sale_id
WHEN MATCHED AND target.event_timestamp != source.event_timestamp THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## 📊 Power BI Dashboard

**4 Report Pages:**

| Page | Key Visuals |
|------|-------------|
| Executive Summary | 5 KPI cards, Daily Revenue trend, Revenue by Channel donut, Heatmap matrix |
| Sales Deep Dive | Monthly vs Prior Year, Weekend/Weekday bar, Waterfall by Quarter, Scatter |
| Product & Category | Category Revenue bar, Share donut, Treemap, Performance table |
| Customer Intelligence | LTV histogram, Top 20 Customers, Loyalty scatter, Revenue by city |

**Key DAX Measures:**
```dax
Total Revenue     = SUM(gold_daily_sales[total_revenue])
Revenue MTD       = CALCULATE([Total Revenue], DATESMTD(dim_date[date]))
Revenue YTD       = CALCULATE([Total Revenue], DATESYTD(dim_date[date]))
Revenue MoM %     = DIVIDE(CurrentMonth - PriorMonth, PriorMonth, 0)
Avg Order Value   = DIVIDE([Total Revenue], [Total Transactions], 0)
Repeat Rate %     = MAX(gold_repeat_customer_rate[repeat_customer_rate_pct])
```

**Model Relationships:**
```
fact_sales[customer_id] → dim_customers[customer_id]  Many-to-One
fact_sales[product_id]  → dim_products[product_id]    Many-to-One
fact_sales[sale_date]   → dim_date[date]              Many-to-One
gold_daily_sales[sale_date] → dim_date[date]          Many-to-One
```

---

## 📋 Audit Log Results

Final validated pipeline run — all 10 tables SUCCESS:

| Target Table | Rows Written | Rows Rejected | Status |
|-------------|-------------|--------------|--------|
| bronze_sales | 200,000 | 0 | ✅ SUCCESS |
| bronze_master | 11,009 | 0 | ✅ SUCCESS |
| silver_customers | 10,000 | 0 | ✅ SUCCESS |
| silver_products | 1,000 | 0 | ✅ SUCCESS |
| silver_sales | 199,994 | 15 | ✅ SUCCESS |
| dim_date | 1,096 | 0 | ✅ SUCCESS |
| gold_daily_sales | 366 | 0 | ✅ SUCCESS |
| gold_customer_lifetime_value | 10,000 | 0 | ✅ SUCCESS |
| gold_category_performance | 7 | 0 | ✅ SUCCESS |
| gold_repeat_customer_rate | 1 | 0 | ✅ SUCCESS |

> **Row Count Validation:**
> - 200,000 → 199,994: 6 rows removed (NULL/zero FK values)
> - 10,009 → 10,000: 9 duplicate customer rows removed
> - 1,096 dim_date rows = 365 (2022) + 365 (2023) + 366 (2024 leap year)

---

## 🧠 Key Learnings

- **TRY_CAST vs CAST** — Spark does not short-circuit CAST in CASE WHEN or WHERE clauses. Always use TRY_CAST in Silver layer
- **Unity Catalog Volumes** — Modern governed replacement for DBFS with full audit trail and lineage
- **DATE() not CAST(x AS DATE)** — Spark SQL does not support two-argument CAST for date formatting
- **Quartz CRON** — Databricks uses 6-field CRON `(0 0 2 * * ?)` not standard 5-field Unix CRON
- **Partition strategy matters** — PARTITIONED BY (year) on fact tables is critical for Power BI query performance
- **Pre-aggregate Gold tables** — Standalone Gold tables in Power BI avoid relationship complexity and improve report speed
- **CACHE TABLE not supported on SQL Warehouses** — SQL Warehouses handle disk caching automatically
- **Idempotency** — All notebooks use CREATE OR REPLACE TABLE making them fully re-runnable

---

## 🔮 Future Improvements

| Improvement | Description |
|-------------|-------------|
| Real-Time Streaming | Replace batch CSV with Kafka + Databricks Structured Streaming |
| SCD Type 2 | Track historical changes in dim_customers and dim_products |
| dbt Integration | Replace SQL notebooks with dbt for version control and testing |
| Great Expectations | Automated DQ framework with expectation suites |
| ML Churn Prediction | Databricks MLflow + Feature Store for customer churn model |
| CI/CD Pipeline | GitHub Actions for automated notebook deployment |
| Delta Live Tables | Migrate to DLT for declarative pipeline definitions |
| Multi-Currency | Extend pipeline to handle USD/EUR with live FX rate conversion |

---

## 🚀 Setup & Usage

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks SQL Warehouse
- Power BI Desktop
- CSV files: `Master_customers.csv`, `master_products.csv`, `sales.csv`

### Step 1 — Create Unity Catalog Objects
```sql
CREATE CATALOG  IF NOT EXISTS retail_catalog;
CREATE SCHEMA   IF NOT EXISTS retail_catalog.retail_analytics;
CREATE VOLUME   IF NOT EXISTS retail_catalog.retail_analytics.retail_data;
```

### Step 2 — Upload CSV Files
Upload all 3 CSV files to:
```
/Volumes/retail_catalog/retail_analytics/retail_data/
```

### Step 3 — Run Notebooks in Order
```
00_setup.sql              → Create catalog, schema, volume
01_bronze_ingestion.sql   → Ingest CSVs to Bronze Delta tables
02_silver_transformation.sql → Clean and type Bronze data
03_gold_aggregations.sql  → Build Gold KPI tables
04_dim_fact_models.sql    → Build star schema
05_optimization.sql       → OPTIMIZE, ZORDER, VACUUM
06_audit_log.sql          → Validate pipeline results
```

### Step 4 — Configure Databricks Workflow
- Create job `retail_pipeline` with 3 tasks in sequence
- Set CRON schedule: `0 0 2 * * ?`
- Set cluster: Job Cluster, DBR 14.3 LTS

### Step 5 — Connect Power BI
- Get Data → Azure Databricks
- Enter SQL Warehouse Server Hostname + HTTP Path
- Authenticate with Personal Access Token
- Import Gold layer tables
- Apply `RetailAnalytics_Theme.json`

---

## 👤 Author

**Asharafraza Desai**
Data Engineering Intern — Inorg Global
Project Duration: 04-03-2026 to 17-03-2026

---

## 🏢 Company

**Inorg Global**
Project Manager: Rajesh Kumar Gupta

---
