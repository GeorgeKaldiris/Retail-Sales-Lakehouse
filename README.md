# Retail Sales Lakehouse (Bronze → Silver → Gold) + PostgreSQL + Power BI

## Overview
End-to-end data engineering project that builds an analytics-ready retail sales layer from raw order data. The pipeline lands raw files (Bronze), cleans and standardizes them (Silver), produces KPI marts (Gold), loads a PostgreSQL star schema (Docker), and serves a Power BI dashboard connected to the warehouse.

## What the Dashboard Answers (KPIs)
- How does **net revenue** change month over month?
- Which **regions** generate the most revenue?
- How many **orders** and **items sold** per month?
- What is **Average Order Value (AOV)** per month and by region?
- Which products are **top sellers** (by revenue and by quantity)?

## Data Model Grain (Important)
The main sales fact is at **region-month grain** (one row per region per month). This matches the dashboard level (monthly trends and region comparisons) and prevents double counting.

## Tech Stack
**Current implementation**
- Python (pandas) for generation + transformations
- Parquet for Bronze/Silver/Gold layers
- PostgreSQL (Docker) as the analytics warehouse
- SQL for schema + validation
- Power BI for visualization
- Git/GitHub for version control

**Planned v2 (upgrade path)**
- Spark for scalable processing
- Airflow for orchestration (DAG scheduling, retries, monitoring)

## Architecture
**Ingest → Transform → Model → Load → Serve**
- **Bronze:** raw `orders` and `order_items` snapshots (immutable)
- **Silver:** cleaned and standardized Parquet datasets
- **Gold marts:** KPI-ready tables for BI
- **Warehouse:** PostgreSQL star schema for analytics
- **BI:** Power BI dashboard connected to PostgreSQL

## Outputs
- Bronze: raw files (immutable snapshots)
- Silver: cleaned Parquet datasets
- Gold marts:
  - `sales_monthly_by_region` (region-month KPIs)
  - `top_products_monthly` (product-month ranking)

## Warehouse Schema (PostgreSQL)
**Current**
- Dimensions: `dim_time`, `dim_region`
- Facts: `fact_sales_monthly`, `fact_top_products_monthly`

**Planned**
- Add dimensions: `dim_product`, `dim_customer`

## Folder Structure
- `data/bronze/` raw generated CSV
- `data/silver/` cleaned Parquet
- `data/gold/` marts Parquet
- `sql/` schema + analysis queries
- `src/`
  - `00_ingest_bronze/` generate raw retail data
  - `10_bronze_to_silver/` cleaning + standardization
  - `20_silver_to_gold/` marts creation
  - `30_load/` apply schema + load into Postgres
- `dashboards/powerbi/` Power BI `.pbix`

## How to Run (Local)

### 1) Start PostgreSQL with Docker
'''bash
docker compose up -d
docker compose ps

2) Generate Bronze data 
python src/00_ingest_bronze/generate_bronze.py

3) Bronze → Silver 
python src/10_bronze_to_silver/bronze_to_silver.py

4) Silver → Gold
python src/20_silver_to_gold/silver_to_gold.py

5) Apply schema
python src/30_load/apply_schema.py

6) Load Gold marts into PostgreSQL
python src/30_load/load_gold_to_postgres.py

## Power BI

Open:

- dashboards/powerbi/Retail-Sales.pbix

- Connect to PostgreSQL:

- Server:localhost

- Database:retail

- User:de_user

- Password:de_pass

#Data Quality & Validation (What I Checked)

- Duplicate order_id handling (dedup in Silver)

- Invalid quantities (<= 0) filtered or corrected

- Invalid discounts (e.g., > 0.50) handled as bad records

- Orphan order_items (items with no matching order) detected

- Null payment_method handled consistently

- Sanity checks on totals between stages (counts/aggregates)

```

```md

# Appendix — Bronze Data Contract (Raw Inputs)

## Bronze: orders (one row per order)

Columns:

- order_id (string) – unique order identifier (a few duplicates injected for testing)

- order_ts (timestamp) – order datetime (spans 2023–2024)

- customer_id (string)

- region (string) – North, South, East, West (uneven distribution)

- payment_method (string) – Card, PayPal, Cash (some nulls injected)

- order_status (string) – Completed, Cancelled, Returned

- currency (string) – EUR

- ingestion_date (date) – load date (partition key)

Intended rules:

- order_id should be unique (duplicates injected to test dedup logic)

- Small % Cancelled/Returned to simulate real behavior

- Some missing payment_method to test null handling


# Bronze: order_items (multiple rows per order)

Columns:

- order_id (string) – FK to orders (a few orphan lines injected)

- line_id (int) – line number within order

- product_id (string)

- quantity (int) – typically 1–5 (rare outliers injected)

- unit_price (decimal) – realistic price ranges

- discount_pct (decimal) – typically 0–0.30 (invalid values injected > 0.50)

- ingestion_date (date) – load date (partition key)


Intended rules:

- Each order has 1–6 items

- quantity should be > 0 (some bad records injected)

- unit_price should be > 0

- discount_pct should be between 0 and 0.30 (some invalid injected)

- order_items.order_id should exist in orders (some orphans injected to test referential checks)

```