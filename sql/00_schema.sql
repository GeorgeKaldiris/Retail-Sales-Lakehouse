-- 00_schema.sql
-- PostgreSQL star schema for Retail Sales KPIs (monthly)

CREATE SCHEMA IF NOT EXISTS retail;

-- Dimensions
CREATE TABLE IF NOT EXISTS retail.dim_time (
  time_id     SERIAL PRIMARY KEY,
  year        INT NOT NULL,
  month       INT NOT NULL,
  UNIQUE (year, month)
);

CREATE TABLE IF NOT EXISTS retail.dim_region (
  region_id   SERIAL PRIMARY KEY,
  region      TEXT NOT NULL UNIQUE
);

-- Facts (monthly KPIs by region)
CREATE TABLE IF NOT EXISTS retail.fact_sales_monthly (
  time_id       INT NOT NULL REFERENCES retail.dim_time(time_id),
  region_id     INT NOT NULL REFERENCES retail.dim_region(region_id),
  orders_count  INT NOT NULL,
  items_sold    INT NOT NULL,
  gross_revenue NUMERIC(18,2) NOT NULL,
  net_revenue   NUMERIC(18,2) NOT NULL,
  aov_net       NUMERIC(18,2) NOT NULL,
  PRIMARY KEY (time_id, region_id)
);

-- Top products per month (optional for BI)
CREATE TABLE IF NOT EXISTS retail.fact_top_products_monthly (
  time_id              INT NOT NULL REFERENCES retail.dim_time(time_id),
  product_id           TEXT NOT NULL,
  items_sold           INT NOT NULL,
  net_revenue          NUMERIC(18,2) NOT NULL,
  rank_by_net_revenue  INT NOT NULL,
  PRIMARY KEY (time_id, product_id)
);
