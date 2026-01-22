-- Indexes on fact foreign keys + a composite one for common group-bys

CREATE INDEX IF NOT EXISTS idx_fact_sales_monthly_time_id
  ON retail.fact_sales_monthly(time_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_monthly_region_id
  ON retail.fact_sales_monthly(region_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_monthly_time_region
  ON retail.fact_sales_monthly(time_id, region_id);

CREATE INDEX IF NOT EXISTS idx_fact_top_products_monthly_time_id
  ON retail.fact_top_products_monthly(time_id);
