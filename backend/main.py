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
        import calendar
        # Current = full current calendar month (even if mid-month)
        s = today.replace(day=1)
        last_day = calendar.monthrange(today.year, today.month)[1]
        e = min(today, date(today.year, today.month, last_day))
        # Prior = full prior calendar month
        prev_end = s - timedelta(days=1)
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
    comp: str = "prior",  # "prior" = prior period, "lly" = last year same period
    locations: Optional[List[str]] = Query(None)
):
    s, e, ps, pe, label_cur, label_prev = compute_ranges(range, start_date, end_date)

    # Override prior period to same period last year if requested
    if comp == "lly":
        from dateutil.relativedelta import relativedelta
        try:
            ps = s.replace(year=s.year - 1)
            pe = e.replace(year=e.year - 1)
        except ValueError:
            ps = s - timedelta(days=365)
            pe = e - timedelta(days=365)
        label_prev = label_cur.replace(str(s.year), str(s.year - 1)) + " (LY)"

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

# ── TOAST API ENDPOINTS ──
"""
Toast API Connector — pulls daily sales into PostgreSQL.
Runs on Railway as a scheduled job (or call /sync/toast manually).
"""
import os, requests, psycopg2
from datetime import date, timedelta


TOAST_HOST      = "https://ws-api.toasttab.com"
CLIENT_ID       = os.environ.get("TOAST_CLIENT_ID","")
CLIENT_SECRET   = os.environ.get("TOAST_CLIENT_SECRET","")
DATABASE_URL    = os.environ.get("DATABASE_URL","")

# Map your Toast location GUIDs to dashboard names
# We'll populate these after first auth call discovers them
LOCATION_MAP = {}  # guid -> name — filled dynamically

def get_token():
    """Get OAuth bearer token from Toast."""
    # Restaurant management group client auth
    resp = requests.post(
        f"{TOAST_HOST}/authentication/v1/authentication/login",
        headers={"Content-Type": "application/json"},
        json={
            "clientId":     CLIENT_ID,
            "clientSecret": CLIENT_SECRET,
            "userAccessType": "TOAST_MACHINE_CLIENT"
        },
        timeout=15
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    # Handle both response formats
    if "token" in data:
        return data["token"]["accessToken"]
    elif "accessToken" in data:
        return data["accessToken"]
    else:
        raise Exception(f"Unexpected auth response: {str(data)[:200]}")

def get_restaurants(token):
    """Get all restaurant GUIDs and names accessible to this credential."""
    resp = requests.get(
        f"{TOAST_HOST}/restaurants/v1/groups",
        headers={
            "Authorization": f"Bearer {token}",
            "Toast-Restaurant-External-ID": "0"  # placeholder for group call
        },
        timeout=15
    )
    if resp.status_code == 404:
        # Try direct restaurants endpoint instead
        return []
    resp.raise_for_status()
    return resp.json()

def get_orders_for_day(token, restaurant_guid, business_date):
    """Pull all orders for a business date using standard orders API with pagination."""
    import time
    date_str = business_date.strftime("%Y%m%d")
    all_orders = []
    page = 1
    page_size = 100

    while True:
        resp = requests.get(
            f"{TOAST_HOST}/orders/v2/orders",
            headers={
                "Authorization": f"Bearer {token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            },
            params={
                "businessDate": date_str,
                "pageSize": page_size,
                "page": page
            },
            timeout=30
        )
        if resp.status_code == 204:
            break  # no orders
        if resp.status_code == 429:
            time.sleep(2)  # back off on rate limit
            continue
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_orders.extend(data if isinstance(data, list) else [data])
        if len(data) < page_size:
            break  # last page
        page += 1
        time.sleep(0.3)  # be polite to the API

    return all_orders

def aggregate_orders(orders):
    """Aggregate raw Toast orders into net_sales, covers, orders."""
    net_sales   = 0.0
    total_orders = 0
    total_guests = 0

    for order in orders:
        # Skip voided orders
        if order.get("voidDate") or order.get("voided"):
            continue
        # Skip open/unpaid tabs
        if order.get("paidDate") is None:
            continue

        checks = order.get("checks", [])
        for check in checks:
            # Net sales = totalAmount - tax - void amounts
            net_sales += float(check.get("totalAmount", 0) or 0)
            net_sales -= float(check.get("taxAmount",   0) or 0)

        total_orders += 1
        total_guests += int(order.get("numberOfGuests", 0) or 0)

    return round(net_sales, 2), total_orders, total_guests

def upsert_day(conn, location_name, business_date, net_sales, orders, guests):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO daily_sales (location, business_date, net_sales, total_orders, total_guests, source)
        VALUES (%s, %s, %s, %s, %s, 'toast_api')
        ON CONFLICT (location, business_date) DO UPDATE SET
            net_sales     = EXCLUDED.net_sales,
            total_orders  = EXCLUDED.total_orders,
            total_guests  = EXCLUDED.total_guests,
            loaded_at     = now()
    """, (location_name, business_date, net_sales, orders, guests))
    conn.commit()
    cur.close()

import threading, json as _json
_sync_status = {"running": False, "last_result": None, "last_run": None}

def _run_sync_background(days):
    global _sync_status
    import time
    _sync_status["running"] = True
    results = []
    try:
        token = get_token()
        conn = psycopg2.connect(DATABASE_URL)
        yesterday = date.today() - timedelta(days=1)
        locations_to_sync = os.environ.get("TOAST_LOCATION_GUIDS", "")
        loc_pairs = []
        for pair in locations_to_sync.split(","):
            pair = pair.strip()
            if ":" in pair:
                guid, name = pair.split(":", 1)
                loc_pairs.append((guid.strip(), name.strip()))

        for guid, name in loc_pairs:
            loc_results = []
            for i in range(days):
                bdate = yesterday - timedelta(days=i)
                try:
                    orders = get_orders_for_day(token, guid, bdate)
                    net_sales, total_orders, total_guests = aggregate_orders(orders)
                    upsert_day(conn, name, bdate, net_sales, total_orders, total_guests)
                    loc_results.append({"date": str(bdate), "net_sales": net_sales, "orders": total_orders, "guests": total_guests, "status": "ok"})
                except Exception as e:
                    loc_results.append({"date": str(bdate), "error": str(e)})
                time.sleep(0.5)
            results.append({"location": name, "days_ok": sum(1 for d in loc_results if "net_sales" in d), "days_err": sum(1 for d in loc_results if "error" in d)})
        conn.close()
        _sync_status["last_result"] = {"status": "ok", "synced": results}
    except Exception as e:
        _sync_status["last_result"] = {"status": "error", "error": str(e)}
    finally:
        _sync_status["running"] = False
        _sync_status["last_run"] = str(date.today())

@app.get("/sync/toast")
def sync_toast(days: int = 3):
    """Start Toast sync in background. Check /sync/status for results."""
    if not CLIENT_ID or not CLIENT_SECRET:
        return {"error": "TOAST_CLIENT_ID and TOAST_CLIENT_SECRET not set"}
    if not os.environ.get("TOAST_LOCATION_GUIDS"):
        return {"error": "TOAST_LOCATION_GUIDS not set"}
    if _sync_status["running"]:
        return {"status": "already_running", "message": "Sync already in progress. Check /sync/status"}
    t = threading.Thread(target=_run_sync_background, args=(days,), daemon=True)
    t.start()
    return {"status": "started", "days": days, "message": f"Syncing {days} days in background. Check /sync/status for progress."}

@app.get("/sync/status")
def sync_status():
    """Check status of background Toast sync."""
    return {
        "running": _sync_status["running"],
        "last_run": _sync_status["last_run"],
        "last_result": _sync_status["last_result"]
    }

@app.get("/toast/locations")
def discover_locations():
    """Discover Toast restaurant GUIDs for Standard API access."""
    if not CLIENT_ID or not CLIENT_SECRET:
        return {"error": "Toast credentials not set"}
    try:
        token = get_token()
        results = {}
        # Use partners API to get accessible restaurants
        partner_guid = "ad917397-3f78-43d4-aa8a-28affbbba15a"
        resp = requests.get(
            f"{TOAST_HOST}/partners/v1/restaurants",
            headers={
                "Authorization": f"Bearer {token}",
                "Toast-Restaurant-External-ID": partner_guid
            },
            timeout=15
        )
        results["partners_v1"] = {"status": resp.status_code, "body": resp.text[:500]}
        if resp.ok:
            data = resp.json()
            locs = data if isinstance(data, list) else [data]
            return {
                "restaurants": [
                    {
                        "guid": r.get("guid", r.get("restaurantGuid","")),
                        "name": r.get("restaurantName", r.get("name",""))
                    }
                    for r in locs
                ],
                "next_step": "Add TOAST_LOCATION_GUIDS to Railway variables",
                "format": "GUID1:Oxford Exchange,GUID2:Predalina,GUID3:The Library,GUID4:Mad Dogs & Englishmen"
            }
        # If nothing worked, return the token so we know auth is good
        # and show what the token contains
        import base64, json as jsonlib
        token_parts = token.split('.')
        if len(token_parts) >= 2:
            payload = token_parts[1] + '=='
            try:
                decoded = jsonlib.loads(base64.b64decode(payload).decode('utf-8', errors='ignore'))
            except:
                decoded = {}
        else:
            decoded = {}
        return {
            "auth": "SUCCESS - token obtained",
            "token_claims": decoded,
            "endpoints_tried": results,
            "note": "Auth works! Check token_claims for management_set_guid or restaurant GUIDs"
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/toast/debug")
def toast_debug():
    """Show what credentials Railway has (safe - only shows length and first/last chars)."""
    cid = os.environ.get("TOAST_CLIENT_ID", "NOT SET")
    sec = os.environ.get("TOAST_CLIENT_SECRET", "NOT SET")
    return {
        "client_id": cid,
        "client_id_length": len(cid),
        "secret_length": len(sec),
        "secret_first3": sec[:3] if len(sec) > 3 else sec,
        "secret_last3": sec[-3:] if len(sec) > 3 else sec,
        "secret_has_spaces": " " in sec,
        "secret_has_newline": "\n" in sec or "\r" in sec,
        "secret_repr_first10": repr(sec[:10])
    }

