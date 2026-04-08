from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, os
from datetime import date, timedelta
from typing import List, Optional

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def date_range(range_name: str):
    today = date.today()
    if range_name == "today":
        start = today; prev_start = today - timedelta(days=1); prev_end = today
    elif range_name == "week":
        start = today - timedelta(days=7); prev_start = today - timedelta(days=14); prev_end = start
    elif range_name == "month":
        start = today - timedelta(days=30); prev_start = today - timedelta(days=60); prev_end = start
    elif range_name == "ytd":
        start = date(today.year,1,1); prev_start = date(today.year-1,1,1); prev_end = date(today.year-1,today.month,today.day)
    else:
        start = today - timedelta(days=7); prev_start = today - timedelta(days=14); prev_end = start
    return start, prev_start, prev_end, today

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/sales/summary")
def sales_summary(range: str = "week", locations: Optional[List[str]] = Query(None)):
    start, prev_start, prev_end, end = date_range(range)
    conn = get_conn(); cur = conn.cursor()
    loc_filter = ""; params_cur = [start, end]; params_prev = [prev_start, prev_end]
    if locations:
        ph = ",".join(["%s"]*len(locations))
        loc_filter = f"AND location IN ({ph})"
        params_cur += locations; params_prev += locations
    cur.execute(f"SELECT location,SUM(net_sales),SUM(total_guests),SUM(total_orders) FROM daily_sales WHERE business_date>=%s AND business_date<=%s {loc_filter} GROUP BY location ORDER BY SUM(net_sales) DESC", params_cur)
    current = {r[0]:{"sales":float(r[1]),"guests":int(r[2]),"orders":int(r[3])} for r in cur.fetchall()}
    cur.execute(f"SELECT location,SUM(net_sales) FROM daily_sales WHERE business_date>=%s AND business_date<=%s {loc_filter} GROUP BY location", params_prev)
    previous = {r[0]:float(r[1]) for r in cur.fetchall()}
    ytd_start = date(date.today().year,1,1)
    ytd_params = [ytd_start] + (locations if locations else [])
    ytd_filter = loc_filter if locations else ""
    cur.execute(f"SELECT location,SUM(net_sales) FROM daily_sales WHERE business_date>=%s {ytd_filter} GROUP BY location", ytd_params)
    ytd = {r[0]:float(r[1]) for r in cur.fetchall()}
    result = []
    for loc,data in current.items():
        prev = previous.get(loc,0)
        chg = round((data["sales"]-prev)/prev*100,1) if prev else 0
        avg = round(data["sales"]/data["orders"],2) if data["orders"] else 0
        result.append({"location":loc,"sales":round(data["sales"],2),"prev":round(prev,2),"chg":chg,"guests":data["guests"],"orders":data["orders"],"avg_check":avg,"ytd":round(ytd.get(loc,0),2)})
    cur.close(); conn.close()
    return result

@app.get("/sales/trend")
def sales_trend(range: str = "week", locations: Optional[List[str]] = Query(None), metric: str = "sales"):
    start, _, _, end = date_range(range)
    conn = get_conn(); cur = conn.cursor()
    col = "net_sales" if metric=="sales" else "total_guests" if metric=="guests" else "total_orders"
    loc_filter = ""; params = [start, end]
    if locations:
        ph = ",".join(["%s"]*len(locations))
        loc_filter = f"AND location IN ({ph})"
        params += locations
    cur.execute(f"SELECT business_date,location,{col} FROM daily_sales WHERE business_date>=%s AND business_date<=%s {loc_filter} ORDER BY business_date,location", params)
    rows = cur.fetchall(); cur.close(); conn.close()
    from collections import defaultdict
    by_date = defaultdict(lambda: defaultdict(float))
    all_locs = set()
    for bdate,loc,val in rows:
        by_date[str(bdate)][loc] += float(val) if val else 0
        all_locs.add(loc)
    result = []
    for d in sorted(by_date.keys()):
        entry = {"date":d}
        for loc in sorted(all_locs): entry[loc] = round(by_date[d].get(loc,0),2)
        entry["total"] = round(sum(by_date[d].values()),2)
        result.append(entry)
    return {"data":result,"locations":sorted(all_locs),"metric":metric}
