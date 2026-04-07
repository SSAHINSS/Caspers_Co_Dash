"""
Run this locally to load Toast CSV data into Railway Postgres.
Usage: python ingest.py
"""
import psycopg2, csv, os, sys
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable first")
    print('  Mac/Linux: export DATABASE_URL="postgresql://..."')
    print('  Windows:   set DATABASE_URL=postgresql://...')
    sys.exit(1)

LOCATIONS = {
    "oxford_exchange":    "Oxford Exchange",
    "mad_dogs":           "Mad Dogs & Englishmen",
    "predalina":          "Predalina",
    "the_library":        "The Library",
}

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

# Create table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_sales (
        id            SERIAL PRIMARY KEY,
        location      TEXT NOT NULL,
        business_date DATE NOT NULL,
        net_sales     NUMERIC(12,2),
        total_orders  INTEGER,
        total_guests  INTEGER,
        source        TEXT DEFAULT 'toast_csv',
        loaded_at     TIMESTAMPTZ DEFAULT now(),
        UNIQUE(location, business_date)
    );
""")
conn.commit()
print("Table ready.")

# Load each CSV from the data/ folder
data_dir = os.path.join(os.path.dirname(__file__), "data")
if not os.path.exists(data_dir):
    print(f"ERROR: No 'data/' folder found. Create it and put your CSVs in it.")
    print("  Expected files: oxford_exchange.csv, mad_dogs.csv, predalina.csv, the_library.csv")
    sys.exit(1)

total_rows = 0
for key, location_name in LOCATIONS.items():
    filepath = os.path.join(data_dir, f"{key}.csv")
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} not found")
        continue

    with open(filepath) as f:
        reader = csv.DictReader(f)
        rows_loaded = 0
        for row in reader:
            date_str   = row.get("yyyyMMdd", "").strip()
            net_sales  = row.get("Net sales", "0").strip()
            orders     = row.get("Total orders", "0").strip()
            guests     = row.get("Total guests", "0").strip()

            if not date_str or date_str == "yyyyMMdd":
                continue

            business_date = datetime.strptime(date_str, "%Y%m%d").date()

            cur.execute("""
                INSERT INTO daily_sales (location, business_date, net_sales, total_orders, total_guests)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (location, business_date) DO UPDATE SET
                    net_sales     = EXCLUDED.net_sales,
                    total_orders  = EXCLUDED.total_orders,
                    total_guests  = EXCLUDED.total_guests,
                    loaded_at     = now()
            """, (location_name, business_date, float(net_sales), int(orders), int(guests)))
            rows_loaded += 1

    conn.commit()
    print(f"  {location_name}: {rows_loaded} days loaded")
    total_rows += rows_loaded

cur.close()
conn.close()
print(f"\nDone. {total_rows} total rows upserted.")
