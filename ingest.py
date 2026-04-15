"""
Caspers Company - PostgreSQL ingestion script
Handles two formats:
  1. Toast "Sales by day.csv" format (from zip exports or data/ folder)
     Columns: yyyyMMdd, Net sales, Total orders, Total guests
  2. Wright's pipe-delimited POS format (item-level, aggregated by date)

Place this file in the same folder as your data/ directory and run:
  python ingest.py
"""
import os, csv, zipfile, psycopg2
from datetime import datetime, date
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL environment variable first.")

# ================================================================
# CONFIGURATION - map filenames to location names
# Add zip files or CSV files here
# ================================================================

# Toast "Sales by day" format - either zip files or extracted CSVs
TOAST_SOURCES = {
    # zip files (Toast export bundles containing "Sales by day.csv")
    "SalesSummary_oxford_exchange.zip":     "Oxford Exchange",
    "SalesSummary_mad_dogs.zip":            "Mad Dogs & Englishmen",
    "SalesSummary_predalina.zip":           "Predalina",
    "SalesSummary_the_library.zip":         "The Library",
    # plain CSV files (if already extracted from zip)
    "oxford_exchange.csv":                  "Oxford Exchange",
    "mad_dogs.csv":                         "Mad Dogs & Englishmen",
    "predalina.csv":                        "Predalina",
    "the_library.csv":                      "The Library",
}

# Wright's pipe-delimited item-level format
WRIGHTS_SOURCES = {
    "wrights_s_tampa.csv":  "Wright's S. Tampa",
}

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# ================================================================
# DB SETUP
# ================================================================

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_sales (
        location      TEXT,
        business_date DATE,
        net_sales     NUMERIC,
        total_orders  INTEGER,
        total_guests  INTEGER,
        source        TEXT DEFAULT 'csv',
        loaded_at     TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (location, business_date)
    )
""")
conn.commit()
print("Table ready.\n")

# ================================================================
# HELPER - upsert one day
# ================================================================

def upsert_day(location, business_date, net_sales, orders, guests, source="csv"):
    cur.execute("""
        INSERT INTO daily_sales
            (location, business_date, net_sales, total_orders, total_guests, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (location, business_date) DO UPDATE SET
            net_sales    = EXCLUDED.net_sales,
            total_orders = EXCLUDED.total_orders,
            total_guests = EXCLUDED.total_guests,
            source       = EXCLUDED.source,
            loaded_at    = NOW()
    """, (location, business_date, round(float(net_sales), 2), int(orders), int(guests), source))

def safe_float(v):
    try: return float(str(v).strip().replace(',','')) if str(v).strip() else 0.0
    except: return 0.0

def safe_int(v):
    try: return int(float(str(v).strip().replace(',',''))) if str(v).strip() else 0
    except: return 0

# ================================================================
# PARSER 1 - Toast "Sales by day.csv" format
# Columns: yyyyMMdd, Net sales, Total orders, Total guests
# ================================================================

def parse_toast_sales_by_day(lines, location_name, delim=','):
    upserted = 0
    for line in lines[1:]:  # skip header
        line = line.strip()
        if not line:
            continue
        parts = line.split(delim)
        if len(parts) < 4:
            continue
        try:
            dt = datetime.strptime(parts[0].strip(), "%Y%m%d").date()
            net_sales = safe_float(parts[1])
            orders = safe_int(parts[2])
            guests = safe_int(parts[3])
            upsert_day(location_name, dt, net_sales, orders, guests, "toast_csv")
            upserted += 1
        except Exception as e:
            continue
    return upserted

# ================================================================
# PARSER 2 - Wright's pipe-delimited item-level format
# ================================================================

def parse_wrights_pipe(filepath, location_name):
    daily = defaultdict(lambda: {"sales": 0.0, "orders": set(), "guests": 0})

    with open(filepath, encoding="latin-1") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)  # skip header
        row_count = 0
        for row in reader:
            try:
                if len(row) < 20: continue
                if row[16].strip() == "D": continue
                date_str = row[15].strip()
                if not date_str: continue
                dt = datetime.strptime(date_str, "%m/%d/%y").date()
                ticket = row[2].strip()
                sub = safe_float(row[13])
                disc = safe_float(row[21]) if len(row) > 21 else 0.0
                guests = safe_int(row[3])
                net = sub - disc
                daily[dt]["sales"] += net
                if ticket:
                    daily[dt]["orders"].add(ticket)
                if guests > 0:
                    daily[dt]["guests"] += guests
                row_count += 1
            except:
                continue

    upserted = 0
    for dt, data in sorted(daily.items()):
        upsert_day(location_name, dt,
                   round(data["sales"], 2),
                   len(data["orders"]),
                   data["guests"],
                   "csv")
        upserted += 1

    print(f"  Parsed {row_count:,} rows -> {upserted} days")
    return upserted

# ================================================================
# MAIN - detect format and load each source
# ================================================================

def detect_and_parse_csv(filepath, location_name):
    """Auto-detect whether a CSV is Toast format or pipe-delimited."""
    with open(filepath, encoding="latin-1") as f:
        first_line = f.readline().strip()
        second_line = f.readline().strip()

    # Toast format: first column is a date like 20260101
    if '|' in first_line:
        # Pipe-delimited Wright's format
        return parse_wrights_pipe(filepath, location_name)
    else:
        # Check if it looks like Toast sales-by-day format (comma or tab delimited)
        # Try tab first, then comma
        delim = None
        parts_tab = second_line.split('\t')
        parts_com = second_line.split(',')
        if len(parts_tab) >= 4 and len(parts_tab[0].strip()) == 8 and parts_tab[0].strip().isdigit():
            delim = '\t'
        elif len(parts_com) >= 4 and len(parts_com[0].strip()) == 8 and parts_com[0].strip().isdigit():
            delim = ','
        if delim:
            with open(filepath, encoding="latin-1") as f:
                lines = f.readlines()
            upserted = parse_toast_sales_by_day(lines, location_name, delim)
            print(f"  Parsed {len(lines)-1} rows -> {upserted} days")
            return upserted
        else:
            print(f"  WARNING: Unknown format for {filepath}, skipping.")
            return 0

total_upserted = 0

# Process Toast sources (zip or CSV)
for filename, location_name in TOAST_SOURCES.items():
    fpath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(fpath):
        continue

    print(f"Loading {filename} -> {location_name}...")

    if filename.endswith(".zip"):
        # Extract "Sales by day.csv" from zip
        try:
            with zipfile.ZipFile(fpath) as z:
                if "Sales by day.csv" in z.namelist():
                    with z.open("Sales by day.csv") as f:
                        content = f.read().decode("utf-8", errors="replace")
                        lines = content.strip().split("\n")
                    # Detect delimiter from first data line
                    delim_zip = '\t' if '\t' in lines[1] else ','
                    upserted = parse_toast_sales_by_day(lines, location_name, delim_zip)
                    conn.commit()
                    print(f"  Upserted {upserted} days into PostgreSQL")
                    total_upserted += upserted
                else:
                    print(f"  WARNING: No 'Sales by day.csv' found in {filename}")
        except Exception as e:
            print(f"  ERROR: {e}")
    else:
        # Regular CSV file - auto-detect format
        upserted = detect_and_parse_csv(fpath, location_name)
        conn.commit()
        print(f"  Upserted {upserted} days into PostgreSQL")
        total_upserted += upserted

# Process Wright's pipe-delimited sources
for filename, location_name in WRIGHTS_SOURCES.items():
    fpath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(fpath):
        print(f"  SKIP {filename} - not found")
        continue

    print(f"\nLoading {filename} -> {location_name}...")
    upserted = parse_wrights_pipe(fpath, location_name)
    conn.commit()
    print(f"  Upserted {upserted} days into PostgreSQL")
    total_upserted += upserted

cur.close()
conn.close()
print(f"\nAll done! Total days upserted: {total_upserted}")
