from __future__ import annotations

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

INGESTION_DATE = "2026-01-12"

GOLD_SALES = os.path.join(
    "data", "gold", "sales_monthly_by_region", f"ingestion_date={INGESTION_DATE}", "sales_monthly_by_region.parquet"
)
GOLD_TOPP = os.path.join(
    "data", "gold", "top_products_monthly", f"ingestion_date={INGESTION_DATE}", "top_products_monthly.parquet"
)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "retail")
DB_USER = os.getenv("DB_USER", "de_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "de_pass")


def main() -> None:
    sales = pd.read_parquet(GOLD_SALES)
    topp = pd.read_parquet(GOLD_TOPP)

    # Connect
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # ---- Load dims (time, region) ----
            # time unique pairs
            time_rows = sorted({(int(r.year), int(r.month)) for r in sales.itertuples(index=False)})
            execute_values(
                cur,
                """
                INSERT INTO retail.dim_time(year, month)
                VALUES %s
                ON CONFLICT (year, month) DO NOTHING
                """,
                time_rows,
            )

            region_rows = sorted({(str(x),) for x in sales["region"].unique()})
            execute_values(
                cur,
                """
                INSERT INTO retail.dim_region(region)
                VALUES %s
                ON CONFLICT (region) DO NOTHING
                """,
                region_rows,
            )

            # Build lookup maps
            cur.execute("SELECT time_id, year, month FROM retail.dim_time;")
            time_map = {(y, m): tid for (tid, y, m) in cur.fetchall()}

            cur.execute("SELECT region_id, region FROM retail.dim_region;")
            region_map = {r: rid for (rid, r) in cur.fetchall()}

            # ---- Load fact_sales_monthly (idempotent: delete + insert for that ingestion months) ----
            fact_rows = []
            for r in sales.itertuples(index=False):
                tid = time_map[(int(r.year), int(r.month))]
                rid = region_map[str(r.region)]
                fact_rows.append((
                    tid,
                    rid,
                    int(r.orders_count),
                    int(r.items_sold),
                    float(r.gross_revenue),
                    float(r.net_revenue),
                    float(r.aov_net),
                ))

            # Upsert via ON CONFLICT
            execute_values(
                cur,
                """
                INSERT INTO retail.fact_sales_monthly
                (time_id, region_id, orders_count, items_sold, gross_revenue, net_revenue, aov_net)
                VALUES %s
                ON CONFLICT (time_id, region_id) DO UPDATE SET
                  orders_count = EXCLUDED.orders_count,
                  items_sold = EXCLUDED.items_sold,
                  gross_revenue = EXCLUDED.gross_revenue,
                  net_revenue = EXCLUDED.net_revenue,
                  aov_net = EXCLUDED.aov_net
                """,
                fact_rows,
            )

            # ---- Load fact_top_products_monthly ----
            topp_rows = []
            for r in topp.itertuples(index=False):
                tid = time_map[(int(r.year), int(r.month))]
                topp_rows.append((
                    tid,
                    str(r.product_id),
                    int(r.items_sold),
                    float(r.net_revenue),
                    int(r.rank_by_net_revenue),
                ))

            execute_values(
                cur,
                """
                INSERT INTO retail.fact_top_products_monthly
                (time_id, product_id, items_sold, net_revenue, rank_by_net_revenue)
                VALUES %s
                ON CONFLICT (time_id, product_id) DO UPDATE SET
                  items_sold = EXCLUDED.items_sold,
                  net_revenue = EXCLUDED.net_revenue,
                  rank_by_net_revenue = EXCLUDED.rank_by_net_revenue
                """,
                topp_rows,
            )

        conn.commit()
        print("✅ Loaded GOLD marts into PostgreSQL successfully.")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
