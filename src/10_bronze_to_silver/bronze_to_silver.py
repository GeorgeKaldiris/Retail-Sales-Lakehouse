from __future__ import annotations

import os
from datetime import date
import pandas as pd


INGESTION_DATE = "2026-01-12"


BRONZE_ORDERS = os.path.join("data", "bronze", "orders", f"ingestion_date={INGESTION_DATE}", "orders.csv")
BRONZE_ITEMS  = os.path.join("data", "bronze", "order_items", f"ingestion_date={INGESTION_DATE}", "order_items.csv")

SILVER_ORDERS_DIR = os.path.join("data", "silver", "orders", f"ingestion_date={INGESTION_DATE}")
SILVER_ITEMS_DIR  = os.path.join("data", "silver", "order_items", f"ingestion_date={INGESTION_DATE}")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main() -> None:
    # 1) Read Bronze
    orders = pd.read_csv(BRONZE_ORDERS)
    items = pd.read_csv(BRONZE_ITEMS)

    print(f"Bronze orders rows: {len(orders):,}")
    print(f"Bronze items  rows: {len(items):,}")

    # 2) Basic cleaning (Silver rules)

    # Orders: drop rows with missing critical fields
    
    orders = orders.dropna(subset=["order_id", "order_ts", "customer_id"])

    # Orders: fill missing region/payment_method with "Unknown"
    orders["region"] = orders["region"].fillna("Unknown")
    orders["payment_method"] = orders["payment_method"].fillna("Unknown")

    # Orders: deduplicate on order_id keeping first occurrence
    orders = orders.drop_duplicates(subset=["order_id"], keep="first")

    # Items: enforce numeric rules
    # quantity must be > 0
    items = items[items["quantity"] > 0]

    # unit_price must be > 0
    items = items[items["unit_price"] > 0]

    # discount_pct should be between 0 and 0.30 (clip bad values)
    items["discount_pct"] = items["discount_pct"].clip(lower=0.0, upper=0.30)

    # Items: remove orphan order_ids (must exist in orders)
    valid_order_ids = set(orders["order_id"].unique())
    items = items[items["order_id"].isin(valid_order_ids)]

    # 3) Write Silver as Parquet
    ensure_dir(SILVER_ORDERS_DIR)
    ensure_dir(SILVER_ITEMS_DIR)

    orders_out = os.path.join(SILVER_ORDERS_DIR, "orders_silver.parquet")
    items_out  = os.path.join(SILVER_ITEMS_DIR, "order_items_silver.parquet")

    orders.to_parquet(orders_out, index=False)
    items.to_parquet(items_out, index=False)

    print(f"✅ Wrote Silver orders: {orders_out} rows={len(orders):,}")
    print(f"✅ Wrote Silver items : {items_out} rows={len(items):,}")


if __name__ == "__main__":
    main()
