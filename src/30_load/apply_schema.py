from __future__ import annotations

import os
import pathlib
import psycopg2


# ---- CONFIG ----
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = "retail"
DB_USER = "de_user"
DB_PASSWORD = "de_pass"


def main() -> None:
    project_root = pathlib.Path(__file__).resolve().parents[2]  # Retail-Sales-Lakehouse/
    sql_path = project_root / "sql" / "00_schema.sql"

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")

    print(f"Connecting to Postgres: host={DB_HOST} port={DB_PORT} db={DB_NAME} user={DB_USER}")
    print(f"Applying schema from: {sql_path}")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        print("✅ Schema applied successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
