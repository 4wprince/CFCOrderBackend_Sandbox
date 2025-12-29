"""
sync_service.py
B2BWave order sync and auto-sync scheduler for CFC Order Backend.
"""

import json
import base64
import urllib.request
import urllib.error
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from psycopg2.extras import RealDictCursor

from config import (
    B2BWAVE_URL, B2BWAVE_USERNAME, B2BWAVE_API_KEY,
    AUTO_SYNC_INTERVAL_MINUTES, AUTO_SYNC_DAYS_BACK
)
from db_helpers import get_db
from email_parser import get_warehouses_for_skus

# Global state for auto-sync
last_auto_sync = None
auto_sync_running = False


class B2BWaveAPIError(Exception):
    """Custom exception for B2BWave API errors"""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"B2BWave API Error ({status_code}): {message}")


def is_configured() -> bool:
    """Check if B2BWave API is configured"""
    return bool(B2BWAVE_URL and B2BWAVE_USERNAME and B2BWAVE_API_KEY)


def b2bwave_api_request(endpoint: str, params: dict = None) -> dict:
    """Make authenticated request to B2BWave API"""
    if not is_configured():
        raise B2BWaveAPIError(500, "B2BWave API not configured")
    
    url = f"{B2BWAVE_URL}/api/{endpoint}.json"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query}"
    
    # HTTP Basic Auth
    credentials = base64.b64encode(f"{B2BWAVE_USERNAME}:{B2BWAVE_API_KEY}".encode()).decode()
    
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Basic {credentials}")
    req.add_header("Content-Type", "application/json")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        raise B2BWaveAPIError(e.code, f"HTTP Error: {e.reason}")
    except urllib.error.URLError as e:
        raise B2BWaveAPIError(500, f"Connection error: {str(e)}")


def sync_order_from_b2bwave(order_data: dict) -> dict:
    """
    Sync a single order from B2BWave API response to our database.
    Returns the order_id and status.
    """
    order = order_data.get('order', order_data)
    
    order_id = str(order.get('id'))
    
    # Extract customer info
    customer_name = order.get('customer_name', '')
    company_name = order.get('customer_company', '')
    email = order.get('customer_email', '')
    phone = order.get('customer_phone', '')
    
    # Extract address - B2BWave provides these as separate fields!
    street = order.get('address', '')
    street2 = order.get('address2', '')
    city = order.get('city', '')
    state = order.get('province', '')  # B2BWave calls it 'province'
    zip_code = order.get('postal_code', '')
    
    # Comments
    comments = order.get('comments_customer', '')
    
    # Totals
    order_total = float(order.get('gross_total', 0) or 0)
    total_weight = float(order.get('total_weight', 0) or 0)
    
    # Order date
    submitted_at = order.get('submitted_at')
    if submitted_at:
        try:
            order_date = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
        except:
            order_date = datetime.now(timezone.utc)
    else:
        order_date = datetime.now(timezone.utc)
    
    # Extract line items and SKU prefixes
    order_products = order.get('order_products', [])
    sku_prefixes = []
    line_items = []
    
    for op in order_products:
        product = op.get('order_product', op)
        product_code = product.get('product_code', '')
        product_name = product.get('product_name', '')
        quantity = float(product.get('quantity', 0) or 0)
        price = float(product.get('final_price', 0) or 0)
        
        # Extract SKU prefix
        if '-' in product_code:
            prefix = product_code.split('-')[0]
            if prefix and prefix not in sku_prefixes:
                sku_prefixes.append(prefix)
        
        line_items.append({
            'sku': product_code,
            'product_name': product_name,
            'quantity': quantity,
            'price': price
        })
    
    # Get warehouses for SKU prefixes (up to 4)
    warehouses = get_warehouses_for_skus(sku_prefixes)
    warehouse_1 = warehouses[0] if len(warehouses) > 0 else None
    warehouse_2 = warehouses[1] if len(warehouses) > 1 else None
    warehouse_3 = warehouses[2] if len(warehouses) > 2 else None
    warehouse_4 = warehouses[3] if len(warehouses) > 3 else None
    
    # Check if trusted customer
    is_trusted = False
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id FROM trusted_customers 
                WHERE LOWER(customer_name) = LOWER(%s) 
                   OR LOWER(company_name) = LOWER(%s)
                   OR LOWER(email) = LOWER(%s)
            """, (customer_name, company_name, email))
            if cur.fetchone():
                is_trusted = True
    
    # Upsert order
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO orders (
                    order_id, order_date, customer_name, company_name,
                    street, street2, city, state, zip_code, phone, email,
                    comments, order_total, total_weight, warehouse_1, warehouse_2, warehouse_3, warehouse_4,
                    is_trusted_customer
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    company_name = EXCLUDED.company_name,
                    street = EXCLUDED.street,
                    street2 = EXCLUDED.street2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email,
                    comments = EXCLUDED.comments,
                    order_total = EXCLUDED.order_total,
                    total_weight = EXCLUDED.total_weight,
                    warehouse_1 = COALESCE(orders.warehouse_1, EXCLUDED.warehouse_1),
                    warehouse_2 = COALESCE(orders.warehouse_2, EXCLUDED.warehouse_2),
                    warehouse_3 = COALESCE(orders.warehouse_3, EXCLUDED.warehouse_3),
                    warehouse_4 = COALESCE(orders.warehouse_4, EXCLUDED.warehouse_4),
                    is_trusted_customer = EXCLUDED.is_trusted_customer,
                    updated_at = NOW()
                RETURNING order_id
            """, (
                order_id, order_date, customer_name, company_name,
                street, street2, city, state, zip_code, phone, email,
                comments, order_total, total_weight, warehouse_1, warehouse_2, warehouse_3, warehouse_4,
                is_trusted
            ))
            result = cur.fetchone()
            
            # Delete existing line items and re-insert
            cur.execute("DELETE FROM order_line_items WHERE order_id = %s", (order_id,))
            
            # Insert line items with warehouse info
            for item in line_items:
                sku = item.get('sku', '')
                prefix = sku.split('-')[0] if '-' in sku else ''
                # Look up warehouse for this item
                item_warehouse = None
                if prefix:
                    cur.execute("SELECT warehouse_name FROM warehouse_mapping WHERE UPPER(sku_prefix) = UPPER(%s)", (prefix,))
                    wh_row = cur.fetchone()
                    if wh_row:
                        item_warehouse = wh_row['warehouse_name']
                
                cur.execute("""
                    INSERT INTO order_line_items (order_id, sku, sku_prefix, product_name, quantity, price, warehouse)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (order_id, sku, prefix, item.get('product_name'), item.get('quantity'), item.get('price'), item_warehouse))
            
            # Log sync event
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'b2bwave_sync', %s, 'api')
            """, (order_id, json.dumps({'sku_prefixes': sku_prefixes})))
            
            # Auto-create shipments for each warehouse
            warehouses_list = [w for w in [warehouse_1, warehouse_2, warehouse_3, warehouse_4] if w]
            for wh in warehouses_list:
                # Create shipment_id like "5307-Li"
                wh_short = wh.replace(' & ', '-').replace(' ', '-')
                shipment_id = f"{order_id}-{wh_short}"
                
                # Check if shipment already exists
                cur.execute("SELECT id FROM order_shipments WHERE shipment_id = %s", (shipment_id,))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO order_shipments (order_id, shipment_id, warehouse, status)
                        VALUES (%s, %s, %s, 'needs_order')
                    """, (order_id, shipment_id, wh))
    
    return {
        'order_id': order_id,
        'customer_name': customer_name,
        'company_name': company_name,
        'city': city,
        'state': state,
        'zip_code': zip_code,
        'warehouse_1': warehouse_1,
        'warehouse_2': warehouse_2,
        'warehouse_3': warehouse_3,
        'warehouse_4': warehouse_4,
        'line_items_count': len(line_items)
    }


def run_auto_sync(gmail_sync_func=None, square_sync_func=None):
    """
    Background sync from B2BWave - runs every 15 minutes.
    
    Args:
        gmail_sync_func: Optional function to run Gmail sync
        square_sync_func: Optional function to run Square sync
    """
    global last_auto_sync, auto_sync_running
    
    while True:
        time.sleep(AUTO_SYNC_INTERVAL_MINUTES * 60)  # Wait 15 min
        
        if not is_configured():
            print("[AUTO-SYNC] B2BWave not configured, skipping")
            continue
        
        try:
            auto_sync_running = True
            print(f"[AUTO-SYNC] Starting sync at {datetime.now()}")
            
            # Calculate date range
            since_date = (datetime.now(timezone.utc) - timedelta(days=AUTO_SYNC_DAYS_BACK)).strftime("%Y-%m-%d")
            
            # Fetch from B2BWave
            data = b2bwave_api_request("orders", {"submitted_at_gteq": since_date})
            orders_list = data if isinstance(data, list) else [data]
            
            synced = 0
            for order_data in orders_list:
                try:
                    sync_order_from_b2bwave(order_data)
                    synced += 1
                except Exception as e:
                    print(f"[AUTO-SYNC] Error syncing order: {e}")
            
            last_auto_sync = datetime.now(timezone.utc)
            print(f"[AUTO-SYNC] Completed: {synced} orders synced")
            
            # Run Gmail email sync if provided
            if gmail_sync_func:
                try:
                    with get_db() as conn:
                        gmail_results = gmail_sync_func(conn, hours_back=2)
                        print(f"[AUTO-SYNC] Gmail sync: {gmail_results}")
                except Exception as e:
                    print(f"[AUTO-SYNC] Gmail sync error: {e}")
            
            # Run Square payment sync if provided
            if square_sync_func:
                try:
                    with get_db() as conn:
                        square_results = square_sync_func(conn, hours_back=24)
                        print(f"[AUTO-SYNC] Square sync: {square_results}")
                except Exception as e:
                    print(f"[AUTO-SYNC] Square sync error: {e}")
            
        except Exception as e:
            print(f"[AUTO-SYNC] Error: {e}")
        finally:
            auto_sync_running = False


def start_auto_sync_thread(gmail_sync_func=None, square_sync_func=None):
    """Start background sync thread"""
    if is_configured():
        thread = threading.Thread(
            target=run_auto_sync, 
            args=(gmail_sync_func, square_sync_func),
            daemon=True
        )
        thread.start()
        print(f"[AUTO-SYNC] Started - will sync every {AUTO_SYNC_INTERVAL_MINUTES} minutes")
        return True
    else:
        print("[AUTO-SYNC] B2BWave not configured, auto-sync disabled")
        return False


def get_sync_status() -> Dict:
    """Get current sync status"""
    return {
        "configured": is_configured(),
        "last_sync": last_auto_sync.isoformat() if last_auto_sync else None,
        "running": auto_sync_running,
        "interval_minutes": AUTO_SYNC_INTERVAL_MINUTES
    }
