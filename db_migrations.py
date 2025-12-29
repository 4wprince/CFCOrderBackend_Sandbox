"""
db_migrations.py
Database migration and schema update functions for CFC Order Backend.
These are helper functions called by the migration endpoints in main.py.
"""

from db_helpers import get_db


def create_pending_checkouts_table() -> dict:
    """Create pending_checkouts table for B2BWave checkout flow"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pending_checkouts (
                    order_id VARCHAR(50) PRIMARY KEY,
                    customer_email VARCHAR(255),
                    checkout_token VARCHAR(100),
                    payment_link TEXT,
                    payment_amount DECIMAL(10, 2),
                    payment_initiated_at TIMESTAMP WITH TIME ZONE,
                    payment_completed_at TIMESTAMP WITH TIME ZONE,
                    transaction_id VARCHAR(100),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
    return {"status": "ok", "message": "pending_checkouts table created"}


def create_shipments_table() -> dict:
    """Create order_shipments table without resetting other tables"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_shipments (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
                    shipment_id VARCHAR(50) NOT NULL UNIQUE,
                    warehouse VARCHAR(100) NOT NULL,
                    status VARCHAR(50) DEFAULT 'needs_order',
                    tracking VARCHAR(100),
                    pro_number VARCHAR(50),
                    bol_sent BOOLEAN DEFAULT FALSE,
                    bol_sent_at TIMESTAMP WITH TIME ZONE,
                    weight DECIMAL(10,2),
                    ship_method VARCHAR(50),
                    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
                    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
                    shipped_at TIMESTAMP WITH TIME ZONE,
                    delivered_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_shipments_order ON order_shipments(order_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_shipments_id ON order_shipments(shipment_id)")
    return {"status": "ok", "message": "order_shipments table created"}


def add_rl_shipping_fields() -> dict:
    """Add RL Carriers shipping fields to order_shipments table"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Add RL quote fields and Li pricing fields
            fields_to_add = [
                ("origin_zip", "VARCHAR(10)"),
                ("rl_quote_number", "VARCHAR(50)"),
                ("rl_quote_price", "DECIMAL(10,2)"),
                ("rl_customer_price", "DECIMAL(10,2)"),
                ("rl_invoice_amount", "DECIMAL(10,2)"),
                ("has_oversized", "BOOLEAN DEFAULT FALSE"),
                ("li_quote_price", "DECIMAL(10,2)"),
                ("li_customer_price", "DECIMAL(10,2)"),
                ("actual_cost", "DECIMAL(10,2)"),
                ("quote_url", "TEXT"),
                ("ps_quote_url", "TEXT"),
                ("ps_quote_price", "DECIMAL(10,2)"),
                ("quote_price", "DECIMAL(10,2)"),
                ("customer_price", "DECIMAL(10,2)"),
                ("tracking_number", "VARCHAR(100)")
            ]
            
            for field_name, field_type in fields_to_add:
                try:
                    cur.execute(f"ALTER TABLE order_shipments ADD COLUMN {field_name} {field_type}")
                except Exception as e:
                    # Column might already exist
                    conn.rollback()
                    pass
            
            conn.commit()
    return {"status": "ok", "message": "Shipping fields added to order_shipments"}


def add_ps_fields() -> dict:
    """Add Pirateship fields to order_shipments table"""
    with get_db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE order_shipments ADD COLUMN ps_quote_url TEXT")
                conn.commit()
            except:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE order_shipments ADD COLUMN ps_quote_price DECIMAL(10,2)")
                conn.commit()
            except:
                conn.rollback()
    return {"status": "ok", "message": "PS fields added"}


def fix_shipment_columns() -> dict:
    """Fix column lengths in order_shipments table"""
    with get_db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE order_shipments ALTER COLUMN order_id TYPE VARCHAR(50)")
                conn.commit()
            except Exception as e:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE order_shipments ALTER COLUMN shipment_id TYPE VARCHAR(100)")
                conn.commit()
            except Exception as e:
                conn.rollback()
    return {"status": "ok", "message": "Shipment columns fixed"}


def fix_sku_columns() -> dict:
    """Fix SKU column lengths in all tables"""
    with get_db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE sku_warehouse_map ALTER COLUMN sku_prefix TYPE VARCHAR(100)")
                conn.commit()
            except:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE warehouse_mapping ALTER COLUMN sku_prefix TYPE VARCHAR(100)")
                conn.commit()
            except:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE order_items ALTER COLUMN sku_prefix TYPE VARCHAR(100)")
                conn.commit()
            except:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE order_line_items ALTER COLUMN sku_prefix TYPE VARCHAR(100)")
                conn.commit()
            except:
                conn.rollback()
    return {"status": "ok", "message": "SKU columns fixed"}


def fix_order_id_length() -> dict:
    """Increase order_id column length from VARCHAR(20) to VARCHAR(50)"""
    with get_db() as conn:
        with conn.cursor() as cur:
            results = []
            
            # First, find and drop ALL views that might depend on orders
            try:
                cur.execute("""
                    SELECT viewname FROM pg_views 
                    WHERE schemaname = 'public'
                """)
                views = cur.fetchall()
                for view in views:
                    try:
                        cur.execute(f"DROP VIEW IF EXISTS {view[0]} CASCADE")
                        results.append(f"Dropped view: {view[0]}")
                    except:
                        pass
            except Exception as e:
                results.append(f"View lookup: {str(e)}")
            
            # Also drop any rules
            try:
                cur.execute("""
                    SELECT rulename, tablename FROM pg_rules 
                    WHERE schemaname = 'public'
                """)
                rules = cur.fetchall()
                for rule in rules:
                    try:
                        cur.execute(f"DROP RULE IF EXISTS {rule[0]} ON {rule[1]} CASCADE")
                        results.append(f"Dropped rule: {rule[0]}")
                    except:
                        pass
            except Exception as e:
                results.append(f"Rule lookup: {str(e)}")
            
            conn.commit()
            
            # Now alter order_id columns in all tables
            tables = ['orders', 'order_status', 'order_line_items', 'order_events', 'order_shipments']
            for table in tables:
                try:
                    cur.execute(f"ALTER TABLE {table} ALTER COLUMN order_id TYPE VARCHAR(50)")
                    results.append(f"{table}: updated")
                except Exception as e:
                    results.append(f"{table}: {str(e)}")
            
            conn.commit()
    return {"status": "ok", "results": results}


def recreate_order_status_view() -> dict:
    """Recreate the order_status view after it was dropped"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # First drop the old view
            cur.execute("DROP VIEW IF EXISTS order_status CASCADE")
            
            # Create new view
            cur.execute("""
                CREATE VIEW order_status AS
                SELECT 
                    order_id,
                    CASE
                        WHEN is_complete THEN 'complete'
                        WHEN bol_sent AND NOT is_complete THEN 'awaiting_shipment'
                        WHEN warehouse_confirmed AND NOT bol_sent THEN 'needs_bol'
                        WHEN sent_to_warehouse AND NOT warehouse_confirmed THEN 'awaiting_warehouse'
                        WHEN payment_received AND NOT sent_to_warehouse THEN 'needs_warehouse_order'
                        WHEN payment_link_sent AND NOT payment_received THEN 'awaiting_payment'
                        ELSE 'needs_payment_link'
                    END as current_status,
                    EXTRACT(DAY FROM NOW() - order_date)::INTEGER as days_open,
                    payment_link_sent,
                    payment_received,
                    sent_to_warehouse,
                    warehouse_confirmed,
                    bol_sent,
                    is_complete,
                    updated_at
                FROM orders
            """)
            conn.commit()
    return {"status": "ok", "message": "order_status view recreated"}


def add_weight_column() -> dict:
    """Add total_weight column to orders table"""
    with get_db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE orders ADD COLUMN total_weight DECIMAL(10,2)")
                conn.commit()
                return {"status": "ok", "message": "total_weight column added"}
            except Exception as e:
                if "already exists" in str(e):
                    return {"status": "ok", "message": "total_weight column already exists"}
                return {"status": "error", "message": str(e)}
