from __future__ import annotations

import os
import pandas as pd

INGESTION_DATE = "2026-01-12"

SILVER_ORDERS = os.path.join("data", "silver", "orders", f"ingestion_date={INGESTION_DATE}", "orders_silver.parquet")
SILVER_ITEMS  = os.path.join("data", "silver", "order_items", f"ingestion_date={INGESTION_DATE}", "order_items_silver.parquet")

GOLD_SALES_DIR = os.path.join("data", "gold", "sales_monthly_by_region", f"ingestion_date={INGESTION_DATE}")
GOLD_TOPP_DIR  = os.path.join("data", "gold", "top_products_monthly", f"ingestion_date={INGESTION_DATE}")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main() -> None:
    # 1) Read Silver
    orders = pd.read_parquet(SILVER_ORDERS)
    items = pd.read_parquet(SILVER_ITEMS)

    # 2) Parse datetime + derive year/month
    orders["order_ts"] = pd.to_datetime(orders["order_ts"])
    orders["year"] = orders["order_ts"].dt.year
    orders["month"] = orders["order_ts"].dt.month

    # 3) Join items -> orders to get region + year/month
    fact = items.merge(
        orders[["order_id", "region", "year", "month"]],
        on="order_id",
        how="inner"
    )

    # 4) Compute revenue fields at line level
    fact["gross_line_revenue"] = fact["quantity"] * fact["unit_price"]
    fact["net_line_revenue"] = fact["gross_line_revenue"] * (1 - fact["discount_pct"])

    # -----------------------------
    # GOLD 1: Monthly KPIs by region
    # -----------------------------
    # orders_count: count distinct order_id per region/month
    orders_count = (
        orders.groupby(["region", "year", "month"])["order_id"]
        .nunique()
        .reset_index(name="orders_count")
    )

    sales_kpis = (
        fact.groupby(["region", "year", "month"])
        .agg(
            items_sold=("quantity", "sum"),
            gross_revenue=("gross_line_revenue", "sum"),
            net_revenue=("net_line_revenue", "sum"),
        )
        .reset_index()
    )

    sales_monthly_by_region = sales_kpis.merge(
        orders_count,
        on=["region", "year", "month"],
        how="left"
    )

    sales_monthly_by_region["aov_net"] = (
        sales_monthly_by_region["net_revenue"] / sales_monthly_by_region["orders_count"]
    )

    # -----------------------------
    # GOLD 2: Top products per month
    # -----------------------------
    prod_month = (
        fact.groupby(["product_id", "year", "month"])
        .agg(
            items_sold=("quantity", "sum"),
            net_revenue=("net_line_revenue", "sum"),
        )
        .reset_index()
    )

    # rank products by net_revenue within each (year, month)
    prod_month["rank_by_net_revenue"] = (
        prod_month.groupby(["year", "month"])["net_revenue"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    top_products_monthly = prod_month[prod_month["rank_by_net_revenue"] <= 10].copy()

    # 5) Write Gold as Parquet
    ensure_dir(GOLD_SALES_DIR)
    ensure_dir(GOLD_TOPP_DIR)

    sales_out = os.path.join(GOLD_SALES_DIR, "sales_monthly_by_region.parquet")
    top_out   = os.path.join(GOLD_TOPP_DIR, "top_products_monthly.parquet")

    sales_monthly_by_region.to_parquet(sales_out, index=False)
    top_products_monthly.to_parquet(top_out, index=False)

    print(f"✅ Wrote GOLD sales_monthly_by_region: {sales_out} rows={len(sales_monthly_by_region):,}")
    print(f"✅ Wrote GOLD top_products_monthly   : {top_out} rows={len(top_products_monthly):,}")


if __name__ == "__main__":
    main()
