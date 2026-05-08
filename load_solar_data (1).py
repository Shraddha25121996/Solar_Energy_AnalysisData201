#!/usr/bin/env python3
"""
Solar Energy Analytics — Data Loader
Columns matched exactly to your CSV file.
No pandas / No OpenBLAS — uses only built-in csv module.
"""

import csv
import mysql.connector
import os
import sys
from datetime import datetime

# ──────────────────────────────────────────
# CONFIGURATION  ← update these two values
# ──────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "Door@126612",   # ← your MySQL password
    "database": "solar_energy_db"
}

CSV_FILE   = r"E:\Course Work\DATA 201\Data 201 Project\solar-dashboard\Solar-Data.csv"
CHUNK_SIZE = 2000

# ──────────────────────────────────────────
# EXACT COLUMNS FROM YOUR CSV
# ──────────────────────────────────────────
# These match the headers in your file exactly (case-insensitive after cleaning)
EXPECTED_COLUMNS = [
    "installation_date",
    "pv_system_size_dc",
    "total_installed_price",
    "rebate_or_grant",
    "customer_segment",
    "state",
    "zip_code",
    "utility_service_territory",
    "third_party_owned",
    "installer_name",
    "tracking",
    "ground_mounted",
    "azimuth_1",
    "tilt_1",
    "module_manufacturer_1",
    "module_model_1",
    "module_quantity_1",
    "technology_module_1",
    "efficiency_module_1",
    "inverter_manufacturer_1",
    "inverter_model_1",
    "output_capacity_inverter_1",
    "inverter_loading_ratio",
    "battery_rated_capacity_kwh",
    "price_per_watt",
]

CREATE_DB_SQL = "CREATE DATABASE IF NOT EXISTS solar_energy_db;"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS solar_installations (
    id                          INT AUTO_INCREMENT PRIMARY KEY,
    installation_date           DATE,
    pv_system_size_dc           DECIMAL(10,4),
    total_installed_price       DECIMAL(14,2),
    rebate_or_grant             DECIMAL(14,2),
    customer_segment            VARCHAR(20),
    state                       VARCHAR(50),
    zip_code                    VARCHAR(10),
    utility_service_territory   VARCHAR(150),
    third_party_owned           TINYINT,
    installer_name              VARCHAR(150),
    tracking                    TINYINT,
    ground_mounted              TINYINT,
    azimuth_1                   DECIMAL(8,2),
    tilt_1                      DECIMAL(8,2),
    module_manufacturer_1       VARCHAR(150),
    module_model_1              VARCHAR(150),
    module_quantity_1           INT,
    technology_module_1         VARCHAR(50),
    efficiency_module_1         DECIMAL(10,6),
    inverter_manufacturer_1     VARCHAR(150),
    inverter_model_1            VARCHAR(150),
    output_capacity_inverter_1  DECIMAL(10,4),
    inverter_loading_ratio      DECIMAL(10,4),
    battery_rated_capacity_kwh  DECIMAL(10,4),
    price_per_watt              DECIMAL(10,6),
    INDEX idx_state (state),
    INDEX idx_date  (installation_date),
    INDEX idx_segment (customer_segment),
    INDEX idx_zip (zip_code)
);
"""


def connect(database=None):
    cfg = dict(DB_CONFIG)
    if database:
        cfg["database"] = database
    else:
        cfg.pop("database", None)
    return mysql.connector.connect(**cfg)


def clean_col(name):
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def clean_val(v):
    v = str(v).strip()
    if v in ("", "NULL", "null", "None", "NaN", "nan", "N/A", "n/a", "none"):
        return None
    return v


def parse_date(v):
    """Convert Excel serial date or date string to MySQL DATE string."""
    if v is None:
        return None
    v = str(v).strip()
    if v in ("", "None", "nan"):
        return None
    # Try common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Excel serial number (e.g. 40164 → date)
    try:
        serial = int(float(v))
        # Excel epoch: 1899-12-30
        from datetime import timedelta, date
        dt = date(1899, 12, 30) + timedelta(days=serial)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


def setup_database():
    print("STEP 1: Setting up database ...")
    conn = connect()
    cur  = conn.cursor()
    cur.execute(CREATE_DB_SQL)
    conn.commit()
    cur.close(); conn.close()

    conn = connect(database="solar_energy_db")
    cur  = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS solar_installations;")
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close(); conn.close()
    print("    Database and table [solar_installations] ready.\n")


def inspect_csv(path):
    print(f"STEP 2: Inspecting CSV ...")
    if not os.path.exists(path):
        sys.exit(f"\nERROR: File not found:\n  {path}\nCheck the CSV_FILE variable.")

    # Count rows without loading into memory
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        row_count = sum(1 for _ in f) - 1

    # Peek at header
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader  = csv.reader(f)
        raw_hdr = next(reader)
        headers = [clean_col(h) for h in raw_hdr]

    print(f"    File      : {path}")
    print(f"    Rows      : {row_count:,}")
    print(f"    Columns   : {len(headers)}")
    print(f"    Header    : {headers}")

    available = [c for c in EXPECTED_COLUMNS if c in headers]
    missing   = [c for c in EXPECTED_COLUMNS if c not in headers]
    extra     = [c for c in headers if c not in EXPECTED_COLUMNS]

    if missing:
        print(f"\n    MISSING columns (will be NULL): {missing}")
    if extra:
        print(f"    EXTRA columns in CSV (ignored): {extra}")
    print(f"\n    Matched {len(available)} / {len(EXPECTED_COLUMNS)} expected columns.\n")

    return headers, available, row_count


def load_csv_to_mysql(path, headers, available, total_rows):
    print(f"STEP 3: Loading {total_rows:,} rows in batches of {CHUNK_SIZE:,} ...")

    insert_sql = (
        f"INSERT INTO solar_installations ({', '.join(available)}) "
        f"VALUES ({', '.join(['%s'] * len(available))})"
    )

    col_index = {h: i for i, h in enumerate(headers)}

    # Which columns need date parsing
    date_cols = {"installation_date"}

    conn = connect(database="solar_energy_db")
    cur  = conn.cursor()

    batch         = []
    rows_inserted = 0
    skipped       = 0

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for line_num, row in enumerate(reader, start=2):
            if len(row) == 0:
                continue
            if len(row) < len(headers) * 0.5:   # skip badly malformed rows
                skipped += 1
                continue

            try:
                values = []
                for c in available:
                    idx = col_index.get(c)
                    raw = row[idx] if idx is not None and idx < len(row) else ""
                    if c in date_cols:
                        values.append(parse_date(raw))
                    else:
                        values.append(clean_val(raw))
                batch.append(tuple(values))
            except Exception:
                skipped += 1
                continue

            if len(batch) >= CHUNK_SIZE:
                cur.executemany(insert_sql, batch)
                conn.commit()
                rows_inserted += len(batch)
                batch.clear()
                pct = rows_inserted / total_rows * 100 if total_rows else 0
                print(f"    Progress: {rows_inserted:>10,} / {total_rows:,}  ({pct:.1f}%)", end="\r")

    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()
        rows_inserted += len(batch)

    print(f"\n    Inserted : {rows_inserted:,} rows")
    if skipped:
        print(f"    Skipped  : {skipped:,} malformed/short rows")
    print()
    cur.close(); conn.close()


def run_verification():
    print("STEP 4: Verification queries ...\n")
    conn = connect(database="solar_energy_db")
    cur  = conn.cursor()

    checks = [
        ("Total rows loaded",
         "SELECT COUNT(*) FROM solar_installations"),

        ("Distinct states",
         "SELECT COUNT(DISTINCT state) FROM solar_installations"),

        ("Date range",
         "SELECT MIN(installation_date), MAX(installation_date) FROM solar_installations"),

        ("Top 5 states by installations",
         """SELECT state, COUNT(*) AS installs
            FROM solar_installations
            GROUP BY state ORDER BY installs DESC LIMIT 5"""),

        ("Customer segment breakdown",
         """SELECT customer_segment, COUNT(*) AS cnt,
                   ROUND(AVG(pv_system_size_dc),3) AS avg_size_kw,
                   ROUND(AVG(price_per_watt),4)    AS avg_price_w
            FROM solar_installations
            GROUP BY customer_segment ORDER BY cnt DESC"""),

        ("Top 5 states by avg system size",
         """SELECT state,
                   COUNT(*) AS installs,
                   ROUND(AVG(pv_system_size_dc),3)    AS avg_size_kw,
                   ROUND(AVG(total_installed_price),0) AS avg_price,
                   ROUND(AVG(price_per_watt),4)        AS avg_price_w
            FROM solar_installations
            GROUP BY state ORDER BY avg_size_kw DESC LIMIT 5"""),
    ]

    for title, sql in checks:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            print(f"  -- {title} --")
            for r in rows:
                print(f"     {r}")
            print()
        except Exception as e:
            print(f"  WARNING: {title} failed: {e}\n")

    cur.close(); conn.close()


if __name__ == "__main__":
    print("=" * 54)
    print("  Solar Energy Analytics  |  Data Loader v3")
    print("  Columns: matched to your actual CSV file")
    print("=" * 54 + "\n")

    setup_database()
    headers, available, total = inspect_csv(CSV_FILE)
    load_csv_to_mysql(CSV_FILE, headers, available, total)
    run_verification()

    print("=" * 54)
    print("  Pipeline complete!")
    print("  Open solar_dashboard.html in your browser.")
    print("=" * 54)
