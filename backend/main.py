from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, os
from datetime import date, timedelta, datetime
from typing import List, Optional

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def compute_ranges(range_name: str, start_date: str = None, end_date: str = None):
    today = date.today()
    if range_name == "custom" and start_date and end_date:
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
        span = (e - s).days
        ps = s - timedelta(days=span+1)
        pe = s - timedelta(days=1)
        label_cur  = f"{s.strftime('%b %d')} – {e.strftime('%b %d, %Y')}"
        label_prev = f"{ps.strftime('%b %d')} – {pe.strftime('%b %d, %Y')}"
    elif range_name == "today":
        s = today; e = today
        ps = today - timedelta(days=1); pe = today - timedelta(days=1)
        label_cur = "Today"; label_prev = "Yesterday"
    elif range_name == "week":
        s = today - timedelta(days=6); e = today
        ps = s - timedelta(days=7); pe = s - timedelta(days=1)
        label_cur  = f"{s.strftime('%b %d')} – {e.strftime('%b %d')}"
        label_prev = f"{ps.strftime('%b %d')} – {pe.strftime('%b %d')}"
    elif range_name == "month":
        # Calendar month
        first = today.replace(day=1)
        s = first; e = today
        # Prior month
        prev_end = first - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        ps = prev_start; pe = prev_end
        label_cur  = today.strftime("%B %Y")
        label_prev = prev_end.strftime("%B %Y")
    elif range_name == "ytd":
        s = date(today.year, 1, 1); e = today
        ps = date(today.year-1, 1, 1); pe = date(today.year-1, today.month, today.day)
        label_cur  = f"YTD {today.year}"
        label_prev = f"YTD {today.year-1}"
    else:  # default week
        s = today - timedelta(days=6); e = today
        ps = s - timedelta(days=7); pe = s - timedelta(days=1)
        label_cur  = f"{s.strftime('%b %d')} – {e.strftime('%b %d')}"
        label_prev = f"{ps.strftime('%b %d')} – {pe.strftime('%b %d')}"
    return s, e, ps, pe, label_cur, label_prev

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/sales/summary")
def sales_summary(
    range: str = "week",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    locations: Optional[List[str]] = Query(None)
):
    s, e, ps, pe, label_cur, label_prev = compute_ranges(range, start_date, end_date)
    conn = get_conn(); cur = conn.cursor()
    loc_ph = ""; loc_params = []
    if locations:
        loc_ph = "AND location IN (" + ",".join(["%s"]*len(locations)) + ")"
        loc_params = list(locations)

    cur.execute(f"SELECT location,SUM(net_sales),SUM(total_guests),SUM(total_orders) FROM daily_sales WHERE business_date>=%s AND business_date<=%s {loc_ph} GROUP BY location ORDER BY SUM(net_sales) DESC", [s, e]+loc_params)
    current = {r[0]:{"sales":float(r[1]),"guests":int(r[2]),"orders":int(r[3])} for r in cur.fetchall()}

    cur.execute(f"SELECT location,SUM(net_sales) FROM daily_sales WHERE business_date>=%s AND business_date<=%s {loc_ph} GROUP BY location", [ps, pe]+loc_params)
    previous = {r[0]:float(r[1]) for r in cur.fetchall()}

    ytd_s = date(date.today().year, 1, 1)
    cur.execute(f"SELECT location,SUM(net_sales) FROM daily_sales WHERE business_date>=%s {loc_ph} GROUP BY location", [ytd_s]+loc_params)
    ytd = {r[0]:float(r[1]) for r in cur.fetchall()}

    result = []
    for loc, data in current.items():
        prev = previous.get(loc, 0)
        chg_pct = round((data["sales"]-prev)/prev*100, 1) if prev else 0
        chg_abs = round(data["sales"]-prev, 0)
        avg = round(data["sales"]/data["orders"], 2) if data["orders"] else 0
        result.append({
            "location": loc, "sales": round(data["sales"], 0), "prev": round(prev, 0),
            "chg_pct": chg_pct, "chg_abs": chg_abs,
            "guests": data["guests"], "orders": data["orders"],
            "avg_check": avg, "ytd": round(ytd.get(loc, 0), 0),
        })
    cur.close(); conn.close()
    return {"data": result, "label_cur": label_cur, "label_prev": label_prev, "range": range}

@app.get("/sales/trend")
def sales_trend(
    range: str = "week",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    locations: Optional[List[str]] = Query(None),
    metric: str = "sales"
):
    s, e, _, _, _, _ = compute_ranges(range, start_date, end_date)
    conn = get_conn(); cur = conn.cursor()
    col = "net_sales" if metric=="sales" else "total_guests" if metric=="guests" else "total_orders"
    loc_ph = ""; params = [s, e]
    if locations:
        loc_ph = "AND location IN (" + ",".join(["%s"]*len(locations)) + ")"
        params += list(locations)
    cur.execute(f"SELECT business_date,location,{col} FROM daily_sales WHERE business_date>=%s AND business_date<=%s {loc_ph} ORDER BY business_date,location", params)
    rows = cur.fetchall(); cur.close(); conn.close()
    from collections import defaultdict
    by_date = defaultdict(lambda: defaultdict(float))
    all_locs = set()
    for bdate, loc, val in rows:
        by_date[str(bdate)][loc] += float(val) if val else 0
        all_locs.add(loc)
    result = []
    for d in sorted(by_date.keys()):
        entry = {"date": d}
        for loc in sorted(all_locs): entry[loc] = round(by_date[d].get(loc, 0), 0)
        entry["total"] = round(sum(by_date[d].values()), 0)
        result.append(entry)
    return {"data": result, "locations": sorted(all_locs), "metric": metric}
