from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2, os
from datetime import date, timedelta, datetime
from typing import List, Optional

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def compute_ranges(range_name, start_date=None, end_date=None):
    today = date.today() - timedelta(days=1)  # always cap to yesterday
    if range_name == "custom" and start_date and end_date:
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
        span = (e - s).days
        # Prior = same length, ending day before current start
        pe = s - timedelta(days=1)
        ps = pe - timedelta(days=span)
        label_cur  = f"{s.strftime('%b %d')} \u2013 {e.strftime('%b %d, %Y')}"
        label_prev = f"{ps.strftime('%b %d')} \u2013 {pe.strftime('%b %d, %Y')}"
    elif range_name == "today":
        s = e = today
        ps = pe = today - timedelta(days=1)
        label_cur = today.strftime("%a, %b %d %Y")
        label_prev = (today - timedelta(days=1)).strftime("%a, %b %d %Y")
    elif range_name == "week":
        # Mon-Sun current week
        dow = today.weekday()
        s = today - timedelta(days=dow)
        e = today
        ps = s - timedelta(days=7)
        pe = e - timedelta(days=7)
        label_cur  = f"Week of {s.strftime('%b %d, %Y')}"
        label_prev = f"Week of {ps.strftime('%b %d, %Y')}"
    elif range_name == "month":
        s = today.replace(day=1)
        e = today
        prev_end   = s - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        ps = prev_start; pe = prev_end
        label_cur  = today.strftime("%B %Y")
        label_prev = prev_end.strftime("%B %Y")
    elif range_name == "ytd":
        s = date(today.year, 1, 1); e = today
        ps = date(today.year-1, 1, 1)
        pe = date(today.year-1, today.month, today.day)
        label_cur  = f"YTD {today.year} (Jan 1 \u2013 {today.strftime('%b %d')})"
        label_prev = f"YTD {today.year-1} (Jan 1 \u2013 {pe.strftime('%b %d')})"
    else:
        dow = today.weekday()
        s = today - timedelta(days=dow); e = today
        ps = s - timedelta(days=7); pe = e - timedelta(days=7)
        label_cur  = f"Week of {s.strftime('%b %d, %Y')}"
        label_prev = f"Week of {ps.strftime('%b %d, %Y')}"
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

    lf = ""; lp = []
    if locations:
        lf = "AND location IN (" + ",".join(["%s"]*len(locations)) + ")"
        lp = list(locations)

    cur.execute(f"SELECT location,SUM(net_sales),SUM(total_guests),SUM(total_orders),COUNT(*) FROM daily_sales WHERE business_date>=%s AND business_date<=%s {lf} GROUP BY location ORDER BY SUM(net_sales) DESC", [s,e]+lp)
    current = {r[0]:{"sales":float(r[1]),"guests":int(r[2]),"orders":int(r[3]),"days":int(r[4])} for r in cur.fetchall()}

    cur.execute(f"SELECT location,SUM(net_sales),SUM(total_guests),SUM(total_orders) FROM daily_sales WHERE business_date>=%s AND business_date<=%s {lf} GROUP BY location", [ps,pe]+lp)
    previous = {r[0]:{"sales":float(r[1]),"guests":int(r[2]),"orders":int(r[3])} for r in cur.fetchall()}

    ytd_s = date(date.today().year,1,1)
    cur.execute(f"SELECT location,SUM(net_sales) FROM daily_sales WHERE business_date>=%s {lf} GROUP BY location", [ytd_s]+lp)
    ytd = {r[0]:float(r[1]) for r in cur.fetchall()}

    result = []
    for loc, d in current.items():
        prev = previous.get(loc, {"sales":0,"guests":0,"orders":0})
        chg_sales = round((d["sales"]-prev["sales"])/prev["sales"]*100,1) if prev["sales"] else 0
        chg_guests = round((d["guests"]-prev["guests"])/prev["guests"]*100,1) if prev["guests"] else 0
        chg_orders = round((d["orders"]-prev["orders"])/prev["orders"]*100,1) if prev["orders"] else 0
        avg = round(d["sales"]/d["orders"],2) if d["orders"] else 0
        prev_avg = round(prev["sales"]/prev["orders"],2) if prev["orders"] else 0
        result.append({
            "location": loc,
            "cur_sales":  round(d["sales"],0),
            "prev_sales": round(prev["sales"],0),
            "chg_sales":  chg_sales,
            "cur_guests": d["guests"],
            "prev_guests":prev["guests"],
            "chg_guests": chg_guests,
            "cur_orders": d["orders"],
            "prev_orders":prev["orders"],
            "chg_orders": chg_orders,
            "cur_avg":    avg,
            "prev_avg":   prev_avg,
            "chg_avg":    round((avg-prev_avg)/prev_avg*100,1) if prev_avg else 0,
            "ytd":        round(ytd.get(loc,0),0),
            "days":       d["days"],
            # legacy compat
            "sales": round(d["sales"],0),
            "prev":  round(prev["sales"],0),
            "chg":   chg_sales,
            "chg_pct": chg_sales,
            "chg_abs": round(d["sales"]-prev["sales"],0),
            "guests": d["guests"],
            "orders": d["orders"],
            "avg_check": avg,
        })

    cur.close(); conn.close()
    return {
        "data": result,
        "label_cur": label_cur,
        "label_prev": label_prev,
        "date_cur": {"start": str(s), "end": str(e)},
        "date_prev": {"start": str(ps), "end": str(pe)},
        "range": range
    }

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
    lf = ""; params = [s, e]
    if locations:
        lf = "AND location IN (" + ",".join(["%s"]*len(locations)) + ")"
        params += list(locations)
    cur.execute(f"SELECT business_date,location,{col} FROM daily_sales WHERE business_date>=%s AND business_date<=%s {lf} ORDER BY business_date,location", params)
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
        for loc in sorted(all_locs): entry[loc] = round(by_date[d].get(loc,0),0)
        entry["total"] = round(sum(by_date[d].values()),0)
        result.append(entry)
    return {"data":result,"locations":sorted(all_locs),"metric":metric}
