"""
db_helpers.py
Database connection and common database operations for CFC Order Backend.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

from config import DATABASE_URL

# =============================================================================
# CONNECTION MANAGEMENT
# =============================================================================

@contextmanager
def get_db():
    """Get database connection with automatic commit/rollback"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_cursor(dict_cursor: bool = True):
    """Get database cursor directly (convenience wrapper)"""
    with get_db() as conn:
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur


# =============================================================================
# COMMON QUERIES
# =============================================================================

def get_order_by_id(order_id: str) -> Optional[Dict]:
    """Fetch a single order by ID"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_orders(
    status_filter: str = None,
    include_complete: bool = False,
    limit: int = 100,
    offset: int = 0
) -> List[Dict]:
    """Fetch orders with optional filtering"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM orders WHERE 1=1"
            params = []
            
            if not include_complete:
                query += " AND (is_complete = FALSE OR is_complete IS NULL)"
            
            if status_filter:
                # Add status-specific filtering
                pass
            
            query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def update_order(order_id: str, **kwargs) -> bool:
    """Update order fields dynamically"""
    if not kwargs:
        return False
    
    with get_db() as conn:
        with conn.cursor() as cur:
            # Build SET clause dynamically
            set_parts = []
            params = []
            
            for key, value in kwargs.items():
                set_parts.append(f"{key} = %s")
                params.append(value)
            
            # Always update updated_at
            set_parts.append("updated_at = NOW()")
            params.append(order_id)
            
            query = f"UPDATE orders SET {', '.join(set_parts)} WHERE order_id = %s"
            cur.execute(query, params)
            
            return cur.rowcount > 0


def get_order_line_items(order_id: str) -> List[Dict]:
    """Get line items for an order"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM order_line_items 
                WHERE order_id = %s 
                ORDER BY id
            """, (order_id,))
            return [dict(row) for row in cur.fetchall()]


def get_order_shipments(order_id: str) -> List[Dict]:
    """Get shipments for an order"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM shipments 
                WHERE order_id = %s 
                ORDER BY created_at
            """, (order_id,))
            return [dict(row) for row in cur.fetchall()]


def get_order_events(order_id: str) -> List[Dict]:
    """Get events for an order"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM order_events 
                WHERE order_id = %s 
                ORDER BY created_at DESC
            """, (order_id,))
            return [dict(row) for row in cur.fetchall()]


def add_order_event(order_id: str, event_type: str, description: str, source: str = "system") -> int:
    """Add an event to an order"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, description, source)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (order_id, event_type, description, source))
            return cur.fetchone()[0]


# =============================================================================
# WAREHOUSE MAPPING
# =============================================================================

def get_warehouse_for_sku(sku: str) -> Optional[str]:
    """Look up warehouse for a SKU prefix"""
    if not sku:
        return None
    
    # Extract prefix (before hyphen)
    prefix = sku.split('-')[0].upper() if '-' in sku else sku.upper()
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT warehouse_name FROM warehouse_mapping 
                WHERE UPPER(sku_prefix) = %s
            """, (prefix,))
            row = cur.fetchone()
            return row[0] if row else None


def get_all_warehouse_mappings() -> List[Dict]:
    """Get all warehouse mappings"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM warehouse_mapping ORDER BY sku_prefix")
            return [dict(row) for row in cur.fetchall()]


# =============================================================================
# ALERTS
# =============================================================================

def get_order_alerts(order_id: str = None, include_resolved: bool = False) -> List[Dict]:
    """Get alerts, optionally filtered by order"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM order_alerts WHERE 1=1"
            params = []
            
            if order_id:
                query += " AND order_id = %s"
                params.append(order_id)
            
            if not include_resolved:
                query += " AND (is_resolved = FALSE OR is_resolved IS NULL)"
            
            query += " ORDER BY created_at DESC"
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def create_alert(order_id: str, alert_type: str, message: str) -> int:
    """Create a new alert"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO order_alerts (order_id, alert_type, alert_message)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (order_id, alert_type, message))
            return cur.fetchone()[0]


def resolve_alert(alert_id: int) -> bool:
    """Resolve an alert"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE order_alerts 
                SET is_resolved = TRUE, resolved_at = NOW()
                WHERE id = %s
            """, (alert_id,))
            return cur.rowcount > 0


# =============================================================================
# TRUSTED CUSTOMERS
# =============================================================================

def is_trusted_customer(customer_name: str, company_name: str = None) -> bool:
    """Check if a customer is trusted"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM trusted_customers 
                WHERE LOWER(customer_name) = LOWER(%s)
                   OR LOWER(company_name) = LOWER(%s)
                   OR LOWER(company_name) = LOWER(%s)
                LIMIT 1
            """, (customer_name, customer_name, company_name or ''))
            return cur.fetchone() is not None


def get_trusted_customers() -> List[Dict]:
    """Get all trusted customers"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM trusted_customers ORDER BY customer_name")
            return [dict(row) for row in cur.fetchall()]


# =============================================================================
# PENDING CHECKOUTS
# =============================================================================

def get_pending_checkout(order_id: str) -> Optional[Dict]:
    """Get pending checkout by order ID"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM pending_checkouts WHERE order_id = %s", (order_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def upsert_pending_checkout(
    order_id: str,
    customer_email: str = None,
    checkout_token: str = None,
    payment_link: str = None,
    payment_amount: float = None
) -> bool:
    """Create or update a pending checkout"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pending_checkouts (order_id, customer_email, checkout_token, payment_link, payment_amount, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_email = COALESCE(EXCLUDED.customer_email, pending_checkouts.customer_email),
                    checkout_token = COALESCE(EXCLUDED.checkout_token, pending_checkouts.checkout_token),
                    payment_link = COALESCE(EXCLUDED.payment_link, pending_checkouts.payment_link),
                    payment_amount = COALESCE(EXCLUDED.payment_amount, pending_checkouts.payment_amount)
            """, (order_id, customer_email, checkout_token, payment_link, payment_amount))
            return True
