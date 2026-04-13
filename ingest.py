"""
Caspers Company — CSV to PostgreSQL ingestion script
Loads all CSV files in the data/ folder into the daily_sales table.
"""
import os, csv, psycopg2
from datetime import datetime
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL environment variable first.")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

LOCATIONS = {
    "oxford_exchange":    "Oxford Exchange",
    "mad_dogs":           "Mad Dogs & Englishmen",
    "predalina":          "Predalina",
    "the_library":        "The Library",
    "wrights_s_tampa":    "Wright's S. Tampa",
}

def safe_float(val, default=0.0):
    try:
        v = str(val).strip()
        return float(v) if v else default
    except:
        return default

def safe_int(val, default=0):
    try:
        v = str(val).strip()
        return int(float(v)) if v else default
    except:
        return default

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
print("Table ready.")

for fname, location_name in LOCATIONS.items():
    fpath = os.path.join(DATA_DIR, fname + ".csv")
    if not os.path.exists(fpath):
        print(f"  SKIP {fname}.csv - file not found")
        continue

    print(f"\nLoading {fname}.csv -> {location_name}...")

    daily = defaultdict(lambda: {"sales": 0.0, "orders": set(), "guests": 0})

    with open(fpath, encoding="latin-1") as f:
        reader = csv.reader(f, delimiter="|")
        next(reader)
        row_count = 0
        for row in reader:
            try:
                if len(row) < 20: continue
                status = row[16].strip() if len(row) > 16 else ""
                if status == "D": continue

                date_str = row[15].strip() if len(row) > 15 else ""
                if not date_str: continue

                dt = datetime.strptime(date_str, "%m/%d/%y").date()
                ticket = row[2].strip() if len(row) > 2 else ""
                sub = safe_float(row[13]) if len(row) > 13 else 0.0
                disc = safe_float(row[21]) if len(row) > 21 else 0.0
                guests = safe_int(row[3]) if len(row) > 3 else 0
                net = sub - disc

                daily[dt]["sales"] += net
                if ticket:
                    daily[dt]["orders"].add(ticket)
                if guests > 0:
                    daily[dt]["guests"] += guests
                row_count += 1
            except Exception as e:
                continue

    print(f"  Parsed {row_count:,} rows -> {len(daily)} days")

    upserted = 0
    for business_date, data in sorted(daily.items()):
        net_sales = round(data["sales"], 2)
        orders = len(data["orders"])
        guests = data["guests"]
        cur.execute("""
            INSERT INTO daily_sales (location, business_date, net_sales, total_orders, total_guests, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (location, business_date) DO UPDATE SET
                net_sales    = EXCLUDED.net_sales,
                total_orders = EXCLUDED.total_orders,
                total_guests = EXCLUDED.total_guests,
                source       = EXCLUDED.source,
                loaded_at    = NOW()
        """, (location_name, business_date, net_sales, orders, guests, "csv"))
        upserted += 1

    conn.commit()
    print(f"  Upserted {upserted} days into PostgreSQL")

cur.close()
conn.close()
print("\nAll done!")
