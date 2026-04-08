"""
Toast API Connector — pulls daily sales into PostgreSQL.
Runs on Railway as a scheduled job (or call /sync/toast manually).
"""
import os, requests, psycopg2
from datetime import date, timedelta
from fastapi import APIRouter

router = APIRouter()

TOAST_HOST      = "https://ws-api.toasttab.com"
CLIENT_ID       = os.environ.get("TOAST_CLIENT_ID","")
CLIENT_SECRET   = os.environ.get("TOAST_CLIENT_SECRET","")
DATABASE_URL    = os.environ.get("DATABASE_URL","")

# Map your Toast location GUIDs to dashboard names
# We'll populate these after first auth call discovers them
LOCATION_MAP = {}  # guid -> name — filled dynamically

def get_token():
    """Get OAuth bearer token from Toast."""
    resp = requests.post(
        f"{TOAST_HOST}/authentication/v1/authentication/login",
        json={
            "clientId":     CLIENT_ID,
            "clientSecret": CLIENT_SECRET,
            "userAccessType": "TOAST_MACHINE_CLIENT"
        },
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()
    return data["token"]["accessToken"]

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
    """
    Pull all orders for a specific business date at one location.
    Uses businessDate param (YYYYMMDD format).
    """
    date_str = business_date.strftime("%Y%m%d")
    resp = requests.get(
        f"{TOAST_HOST}/orders/v2/ordersBulk",
        headers={
            "Authorization": f"Bearer {token}",
            "Toast-Restaurant-External-ID": restaurant_guid
        },
        params={
            "businessDate": date_str,
            "pageSize": 500
        },
        timeout=30
    )
    if resp.status_code == 204:
        return []  # no orders that day
    resp.raise_for_status()
    return resp.json()

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

@router.get("/sync/toast")
def sync_toast(days: int = 3):
    """
    Pull last N days of Toast data for all locations.
    Call this endpoint manually or schedule it via Railway cron.
    Default: last 3 days (catches yesterday + any gaps).
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        return {"error": "TOAST_CLIENT_ID and TOAST_CLIENT_SECRET not set in Railway variables"}

    results = []
    try:
        token = get_token()
    except Exception as e:
        return {"error": f"Toast auth failed: {str(e)}"}

    conn = psycopg2.connect(DATABASE_URL)
    yesterday = date.today() - timedelta(days=1)

    # Locations to sync — name must match what's in daily_sales table
    # Format: (toast_restaurant_guid, dashboard_location_name)
    # First run: we'll try to discover GUIDs automatically
    locations_to_sync = os.environ.get("TOAST_LOCATION_GUIDS", "")

    if not locations_to_sync:
        conn.close()
        return {
            "status": "setup_required",
            "message": "Add TOAST_LOCATION_GUIDS to Railway variables.",
            "format": "GUID1:Location Name,GUID2:Location Name",
            "example": "abc-123:Oxford Exchange,def-456:Predalina",
            "next_step": "Visit /toast/locations to discover your GUIDs"
        }

    # Parse GUID:Name pairs
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
                loc_results.append({
                    "date": str(bdate),
                    "net_sales": net_sales,
                    "orders": total_orders,
                    "guests": total_guests
                })
            except Exception as e:
                loc_results.append({"date": str(bdate), "error": str(e)})

        results.append({"location": name, "guid": guid, "days": loc_results})

    conn.close()
    return {"status": "ok", "synced": results}

@router.get("/toast/locations")
def discover_locations():
    """
    Discover your Toast restaurant GUIDs.
    Run this once to find the GUIDs, then add them to TOAST_LOCATION_GUIDS.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        return {"error": "Toast credentials not set"}
    try:
        token = get_token()
        # Try management group endpoint
        resp = requests.get(
            f"{TOAST_HOST}/restaurants/v1/restaurants",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15
        )
        if resp.ok:
            restaurants = resp.json()
            return {
                "restaurants": [
                    {"guid": r.get("guid",""), "name": r.get("restaurantName", r.get("name",""))}
                    for r in (restaurants if isinstance(restaurants, list) else [restaurants])
                ],
                "next_step": "Copy the GUIDs and add TOAST_LOCATION_GUIDS to Railway variables",
                "format": "GUID1:Oxford Exchange,GUID2:Predalina,GUID3:The Library,GUID4:Mad Dogs & Englishmen"
            }
        return {"status": resp.status_code, "body": resp.text[:500]}
    except Exception as e:
        return {"error": str(e)}
