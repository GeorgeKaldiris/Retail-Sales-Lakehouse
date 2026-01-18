from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, date

import pandas as pd


# -----------------------------
# CONFIG 
# -----------------------------
N_ORDERS = 10_000
MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 6
N_PRODUCTS = 500

# “Today” as ingestion partition 
INGESTION_DATE = date.today().isoformat()

# Date range για orders
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

# Regions and weights (not equal to be more realistic)
REGIONS = ["North", "South", "East", "West"]
REGION_WEIGHTS = [0.40, 0.25, 0.20, 0.15]

PAYMENT_METHODS = ["Card", "PayPal", "Cash"]
ORDER_STATUSES = ["Completed", "Cancelled", "Returned"]
STATUS_WEIGHTS = [0.92, 0.06, 0.02]


def random_datetime(start: datetime, end: datetime) -> datetime:
    """Random datetime between start and end."""
    delta_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_seconds))


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def make_orders(n_orders: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n_orders + 1):
        order_id = f"ORD-{i:08d}"
        ts = random_datetime(START_DATE, END_DATE)

        region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
        payment = random.choice(PAYMENT_METHODS)
        status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

        rows.append(
            {
                "order_id": order_id,
                "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": f"CUST-{random.randint(1, 2500):05d}",
                "region": region,
                "payment_method": payment,
                "order_status": status,
                "currency": "EUR",
                "ingestion_date": INGESTION_DATE,
            }
        )

    df = pd.DataFrame(rows)

    # --- Inject realistic issues (not many) ---
    # 2% missing payment_method
    df.loc[df.sample(frac=0.02, random_state=42).index, "payment_method"] = None

    # 0.5% null region
    df.loc[df.sample(frac=0.005, random_state=7).index, "region"] = None

    # 0.5% duplicate orders: duplicate a few rows (same order_id)
    dup = df.sample(frac=0.005, random_state=99)
    df = pd.concat([df, dup], ignore_index=True)

    return df


def make_order_items(orders_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    valid_order_ids = orders_df["order_id"].dropna().unique().tolist()

    for order_id in valid_order_ids:
        n_items = random.randint(MIN_ITEMS_PER_ORDER, MAX_ITEMS_PER_ORDER)
        for line in range(1, n_items + 1):
            product_id = f"P-{random.randint(1, N_PRODUCTS):04d}"

            # quantity mostly 1-5
            quantity = random.randint(1, 5)

            # rare outliers / bad records
            r = random.random()
            if r < 0.005:
                quantity = 20  # outlier
            elif r < 0.008:
                quantity = 0   # bad
            elif r < 0.011:
                quantity = -1  # bad

            # price distribution (roughly realistic)
            p = random.random()
            if p < 0.70:
                unit_price = round(random.uniform(5, 60), 2)
            elif p < 0.95:
                unit_price = round(random.uniform(60, 250), 2)
            else:
                unit_price = round(random.uniform(250, 900), 2)

            # discount mostly <= 0.30
            discount = round(random.uniform(0, 0.30), 2)

            # rare invalid discounts
            if random.random() < 0.005:
                discount = round(random.uniform(0.51, 0.80), 2)

            rows.append(
                {
                    "order_id": order_id,
                    "line_id": line,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_pct": discount,
                    "ingestion_date": INGESTION_DATE,
                }
            )

    items_df = pd.DataFrame(rows)

    # Inject ~0.3% orphan lines (order_id not in orders)
    n_orphans = max(1, int(len(items_df) * 0.003))
    orphan_rows = items_df.sample(n=n_orphans, random_state=123).copy()
    orphan_rows["order_id"] = "ORD-99999999"
    items_df = pd.concat([items_df, orphan_rows], ignore_index=True)

    return items_df


def main() -> None:
    # Output paths
    orders_out_dir = os.path.join("data", "bronze", "orders", f"ingestion_date={INGESTION_DATE}")
    items_out_dir = os.path.join("data", "bronze", "order_items", f"ingestion_date={INGESTION_DATE}")

    ensure_dir(orders_out_dir)
    ensure_dir(items_out_dir)

    # Generate
    orders_df = make_orders(N_ORDERS)
    items_df = make_order_items(orders_df)

    # Save
    orders_path = os.path.join(orders_out_dir, "orders.csv")
    items_path = os.path.join(items_out_dir, "order_items.csv")

    orders_df.to_csv(orders_path, index=False)
    items_df.to_csv(items_path, index=False)

    # Simple prints (so you see it's working)
    print(f"✅ Wrote orders: {orders_path}  rows={len(orders_df):,}")
    print(f"✅ Wrote order_items: {items_path}  rows={len(items_df):,}")

    # Quick sanity checks
    print("\n--- Quick sanity checks ---")
    print("Orders date min/max:", orders_df["order_ts"].min(), "→", orders_df["order_ts"].max())
    print("Null region %:", round(orders_df["region"].isna().mean() * 100, 2), "%")
    print("Null payment_method %:", round(orders_df["payment_method"].isna().mean() * 100, 2), "%")
    print("Items orphan order_id count:", (items_df["order_id"] == "ORD-99999999").sum())


if __name__ == "__main__":
    main()