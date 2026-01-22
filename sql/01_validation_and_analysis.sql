/* ============================================================
   01_validation_and_analysis.sql
   Retail Sales Lakehouse - SQL checks + analysis
   ============================================================ */

-- 0) Quick sanity: what tables exist?
-- (Optional, helps if names differ)
-- \dt

/* ============================================================
   1) Row counts (basic smoke test)
   ============================================================ */

SELECT COUNT(*) AS dim_time_rows FROM retail.dim_time;
SELECT COUNT(*) AS dim_region_rows FROM retail.dim_region;
SELECT COUNT(*) AS fact_sales_monthly_rows FROM retail.fact_sales_monthly;
SELECT COUNT(*) AS fact_top_products_monthly_rows FROM retail.fact_top_products_monthly;


/* ============================================================
   2) Referential integrity checks (facts must match dims)
   ============================================================ */
-- Facts with missing time dimension
SELECT COUNT(*) AS missing_time_dim
FROM retail.fact_sales_monthly f
LEFT JOIN retail.dim_time t ON t.time_id = f.time_id
WHERE t.time_id IS NULL;

-- Facts with missing region dimension
SELECT COUNT(*) AS missing_region_dim
FROM retail.fact_sales_monthly f
LEFT JOIN retail.dim_region r ON r.region_id = f.region_id
WHERE r.region_id IS NULL;


/* ============================================================
   3) Null / range sanity checks (typical data quality checks)
   ============================================================ */
-- Revenue should not be negative
SELECT COUNT(*) AS negative_revenue_rows
FROM retail.fact_sales_monthly
WHERE net_revenue < 0;

-- AOV (Average Order Value) should not be negative
SELECT COUNT(*) AS negative_aov_rows
FROM retail.fact_sales_monthly
WHERE orders_count > 0 AND (net_revenue / orders_count) < 0;


-- items_sold should not be negative
SELECT COUNT(*) AS negative_items_sold_rows
FROM retail.fact_sales_monthly
WHERE items_sold < 0;

-- orders_count should be >= 0
SELECT COUNT(*) AS negative_orders_rows
FROM retail.fact_sales_monthly
WHERE orders_count < 0;


/* ============================================================
   4) KPI queries 
   ============================================================ */

-- 4.1 Net revenue by month-year (trend)
SELECT
  t.year,
  t.month,
  ROUND(SUM(f.net_revenue)::numeric, 2) AS net_revenue
FROM retail.fact_sales_monthly f
JOIN retail.dim_time t ON t.time_id = f.time_id
GROUP BY t.year, t.month
ORDER BY t.year, t.month;

-- 4.2 Net revenue by region (ranking)
SELECT
  r.region,
  ROUND(SUM(f.net_revenue)::numeric, 2) AS net_revenue
FROM retail.fact_sales_monthly f
JOIN retail.dim_region r ON r.region_id = f.region_id
GROUP BY r.region
ORDER BY net_revenue DESC;


-- 4.3 Orders + items sold per month
SELECT
  t.year,
  t.month,
  SUM(f.orders_count) AS orders_count,
  SUM(f.items_sold)   AS items_sold
FROM retail.fact_sales_monthly f
JOIN retail.dim_time t ON t.time_id = f.time_id
GROUP BY t.year, t.month
ORDER BY t.year, t.month;


/* ============================================================
   5) Window functions (top N / ranking)
   ============================================================ */

-- 5.1 Top 5 months by net revenue per region (ROW_NUMBER)
WITH region_month AS (
  SELECT
    r.region,
    t.year,
    t.month,
    SUM(f.net_revenue) AS net_revenue
  FROM retail.fact_sales_monthly f
  JOIN retail.dim_region r ON r.region_id = f.region_id
  JOIN retail.dim_time t   ON t.time_id = f.time_id
  GROUP BY r.region, t.year, t.month
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY net_revenue DESC) AS rn
  FROM region_month
)
SELECT
  region, year, month,
  ROUND(net_revenue::numeric, 2) AS net_revenue
FROM ranked
WHERE rn <= 5
ORDER BY region, rn;

-- 5.2 Month-over-month revenue change (LAG)
WITH monthly AS (
  SELECT
    t.year,
    t.month,
    SUM(f.net_revenue) AS net_revenue
  FROM retail.fact_sales_monthly f
  JOIN retail.dim_time t ON t.time_id = f.time_id
  GROUP BY t.year, t.month
),
with_prev AS (
  SELECT
    year,
    month,
    net_revenue,
    LAG(net_revenue) OVER (ORDER BY year, month) AS prev_month_revenue
  FROM monthly
)
SELECT
  year,
  month,
  ROUND(net_revenue::numeric, 2) AS net_revenue,
  ROUND(prev_month_revenue::numeric, 2) AS prev_month_revenue,
  CASE
    WHEN prev_month_revenue IS NULL THEN NULL
    WHEN prev_month_revenue = 0 THEN NULL
    ELSE ROUND(((net_revenue - prev_month_revenue) / prev_month_revenue)::numeric, 4)
  END AS mom_growth_rate
FROM with_prev
ORDER BY year, month;


/* ============================================================
   6) Performance awareness (EXPLAIN)
   ============================================================ */

-- Use EXPLAIN to show query plan (index scans vs seq scans)
EXPLAIN
SELECT
  r.region,
  t.year,
  t.month,
  SUM(f.net_revenue) AS net_revenue
FROM retail.fact_sales_monthly f
JOIN retail.dim_region r ON r.region_id = f.region_id
JOIN retail.dim_time t   ON t.time_id = f.time_id
GROUP BY r.region, t.year, t.month
ORDER BY t.year, t.month;

