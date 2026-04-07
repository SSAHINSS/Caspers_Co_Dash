from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, os, json
from datetime import date, timedelta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/sales/summary")
def sales_summary():
    conn = get_conn()
    cur  = conn.cursor()
    today = date.today()
    seven_ago  = today - timedelta(days=7)
    prev_seven = seven_ago - timedelta(days=7)

    cur.execute("""
        SELECT location, 
               SUM(CASE WHEN business_date >= %s THEN net_sales ELSE 0 END) as sales_7,
               SUM(CASE WHEN business_date >= %s AND business_date < %s THEN net_sales ELSE 0 END) as sales_prev,
               SUM(CASE WHEN business_date >= %s THEN total_guests ELSE 0 END) as guests_7,
               SUM(CASE WHEN business_date >= %s THEN total_orders ELSE 0 END) as orders_7,
               SUM(net_sales) as ytd_sales
        FROM daily_sales
        WHERE business_date >= '2026-01-01'
        GROUP BY location
        ORDER BY sales_7 DESC
    """, (seven_ago, prev_seven, seven_ago, seven_ago, seven_ago))

    rows = cur.fetchall()
    result = []
    for r in rows:
        loc, s7, sp, g7, o7, ytd = r
        chg = round((s7 - sp) / sp * 100, 1) if sp else 0
        avg = round(s7 / o7, 2) if o7 else 0
        result.append({
            "location": loc,
            "sales_7":  round(float(s7), 2),
            "prev_7":   round(float(sp), 2),
            "chg_7":    chg,
            "guests_7": int(g7),
            "orders_7": int(o7),
            "avg_check": avg,
            "ytd":      round(float(ytd), 2),
        })

    cur.close(); conn.close()
    return result

@app.get("/sales/trend")
def sales_trend(location: str = None):
    conn = get_conn()
    cur  = conn.cursor()
    if location:
        cur.execute("""
            SELECT business_date, SUM(net_sales)
            FROM daily_sales
            WHERE business_date >= CURRENT_DATE - INTERVAL '30 days'
              AND location = %s
            GROUP BY business_date ORDER BY business_date
        """, (location,))
    else:
        cur.execute("""
            SELECT business_date, SUM(net_sales)
            FROM daily_sales
            WHERE business_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY business_date ORDER BY business_date
        """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"date": str(r[0]), "sales": round(float(r[1]), 2)} for r in rows]
