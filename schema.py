"""
schema.py
Database schema SQL for CFC Order Backend.
Contains the full schema for initializing/resetting the database.
"""

SCHEMA_SQL = """
-- Drop view first (depends on orders)
DROP VIEW IF EXISTS order_status CASCADE;

-- Drop tables
DROP TABLE IF EXISTS order_line_items CASCADE;
DROP TABLE IF EXISTS order_events CASCADE;
DROP TABLE IF EXISTS order_alerts CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS warehouse_mapping CASCADE;
DROP TABLE IF EXISTS trusted_customers CASCADE;
DROP TABLE IF EXISTS pending_checkouts CASCADE;

-- Pending checkouts for B2BWave orders awaiting payment
CREATE TABLE pending_checkouts (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_email VARCHAR(255),
    checkout_token VARCHAR(100),
    payment_link TEXT,
    payment_amount DECIMAL(10, 2),
    payment_initiated_at TIMESTAMP WITH TIME ZONE,
    payment_completed_at TIMESTAMP WITH TIME ZONE,
    transaction_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE warehouse_mapping (
    sku_prefix VARCHAR(100) PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    warehouse_code VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Default warehouse mappings (from Supplier_Map)
INSERT INTO warehouse_mapping (sku_prefix, warehouse_name, warehouse_code) VALUES
-- LI
('GSP', 'LI', 'LI'),
('WSP', 'LI', 'LI'),
-- DL
('CS', 'DL', 'DL'),
('BNG', 'DL', 'DL'),
('UFS', 'DL', 'DL'),
('EBK', 'DL', 'DL'),
-- ROC
('EGD', 'ROC', 'ROC'),
('EMB', 'ROC', 'ROC'),
('BC', 'ROC', 'ROC'),
('DCH', 'ROC', 'ROC'),
('NJGR', 'ROC', 'ROC'),
('DCT', 'ROC', 'ROC'),
('DCW', 'ROC', 'ROC'),
('EJG', 'ROC', 'ROC'),
('SNW', 'ROC', 'ROC'),
-- Go Bravura
('HGW', 'Go Bravura', 'GB'),
('EMW', 'Go Bravura', 'GB'),
('EGG', 'Go Bravura', 'GB'),
('URC', 'Go Bravura', 'GB'),
('WWW', 'Go Bravura', 'GB'),
('NDG', 'Go Bravura', 'GB'),
('NCC', 'Go Bravura', 'GB'),
('NBW', 'Go Bravura', 'GB'),
('URW', 'Go Bravura', 'GB'),
('BX', 'Go Bravura', 'GB'),
-- Love-Milestone
('EDG', 'Love-Milestone', 'LOVE'),
('EWD', 'Love-Milestone', 'LOVE'),
('RND', 'Love-Milestone', 'LOVE'),
('RMW', 'Love-Milestone', 'LOVE'),
('NBLK', 'Love-Milestone', 'LOVE'),
('HSS', 'Love-Milestone', 'LOVE'),
('LGS', 'Love-Milestone', 'LOVE'),
('LGSS', 'Love-Milestone', 'LOVE'),
('SWO', 'Love-Milestone', 'LOVE'),
('EWT', 'Love-Milestone', 'LOVE'),
('DG', 'Love-Milestone', 'LOVE'),
('EWSCS', 'Love-Milestone', 'LOVE'),
('BGR', 'Love-Milestone', 'LOVE'),
('BESCS', 'Love-Milestone', 'LOVE'),
('EOK', 'Love-Milestone', 'LOVE'),
-- Cabinet & Stone
('CAWN', 'Cabinet & Stone', 'CS'),
('BSN', 'Cabinet & Stone', 'CS'),
('WOCS', 'Cabinet & Stone', 'CS'),
('ESCS', 'Cabinet & Stone', 'CS'),
('SIV', 'Cabinet & Stone', 'CS'),
('SAVNG', 'Cabinet & Stone', 'CS'),
('MSCS', 'Cabinet & Stone', 'CS'),
('SGCS', 'Cabinet & Stone', 'CS'),
-- DuraStone
('CMEN', 'DuraStone', 'DS'),
('NSLS', 'DuraStone', 'DS'),
('NBDS', 'DuraStone', 'DS'),
('NSN', 'DuraStone', 'DS'),
-- L&C Cabinetry
('EDD', 'L&C Cabinetry', 'LC'),
('RBLS', 'L&C Cabinetry', 'LC'),
('SWNG', 'L&C Cabinetry', 'LC'),
('MGLS', 'L&C Cabinetry', 'LC'),
('BG', 'L&C Cabinetry', 'LC'),
('SHLS', 'L&C Cabinetry', 'LC'),
-- GHI
('NOR', 'GHI', 'GHI'),
('SNS', 'GHI', 'GHI'),
('AKS', 'GHI', 'GHI'),
('APW', 'GHI', 'GHI'),
('GRSH', 'GHI', 'GHI')
ON CONFLICT (sku_prefix) DO NOTHING;

-- Trusted customers (can ship before payment)
CREATE TABLE trusted_customers (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    payment_grace_days INTEGER DEFAULT 1,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

INSERT INTO trusted_customers (customer_name, company_name, notes) VALUES
('Lou Palumbo', 'Louis And Clark Contracting', 'Long-time trusted customer'),
('Gerald Thomas', 'G & B Wood Creations', 'Trusted customer'),
('LD Stafford', 'Acute Custom Closets', 'Trusted customer'),
('James Marchant', NULL, 'Trusted customer')
ON CONFLICT DO NOTHING;

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    
    -- Customer info
    customer_name VARCHAR(255),
    company_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Address
    street VARCHAR(255),
    street2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    
    -- Order details
    order_date TIMESTAMP WITH TIME ZONE,
    order_total DECIMAL(10,2),
    total_weight DECIMAL(10,2),
    comments TEXT,
    
    -- Warehouses (extracted from SKU prefixes, up to 4)
    warehouse_1 VARCHAR(100),
    warehouse_2 VARCHAR(100),
    warehouse_3 VARCHAR(100),
    warehouse_4 VARCHAR(100),
    
    -- Payment
    payment_link_sent BOOLEAN DEFAULT FALSE,
    payment_link_sent_at TIMESTAMP WITH TIME ZONE,
    payment_received BOOLEAN DEFAULT FALSE,
    payment_received_at TIMESTAMP WITH TIME ZONE,
    payment_amount DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    
    -- Shipping quotes
    rl_quote_no VARCHAR(50),
    shipping_quote_amount DECIMAL(10,2),
    
    -- Warehouse processing
    sent_to_warehouse BOOLEAN DEFAULT FALSE,
    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
    warehouse_confirmed BOOLEAN DEFAULT FALSE,
    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
    supplier_order_no VARCHAR(100),
    
    -- Shipping
    bol_sent BOOLEAN DEFAULT FALSE,
    bol_sent_at TIMESTAMP WITH TIME ZONE,
    tracking VARCHAR(255),
    pro_number VARCHAR(50),
    
    -- Flags
    is_trusted_customer BOOLEAN DEFAULT FALSE,
    needs_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,
    
    -- Completion
    is_complete BOOLEAN DEFAULT FALSE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Meta
    email_thread_id VARCHAR(255),
    notes TEXT,
    ai_summary TEXT,
    ai_summary_updated_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Alerts/flags table (after orders so foreign key works)
CREATE TABLE order_alerts (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_alerts_order ON order_alerts(order_id);
CREATE INDEX idx_alerts_unresolved ON order_alerts(is_resolved) WHERE NOT is_resolved;

CREATE TABLE order_line_items (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    sku VARCHAR(100),
    sku_prefix VARCHAR(100),
    product_name TEXT,
    price DECIMAL(10,2),
    quantity INTEGER,
    line_total DECIMAL(10,2),
    warehouse VARCHAR(100)
);

CREATE TABLE order_events (
    event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    source VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Email snippets for AI summary
CREATE TABLE order_email_snippets (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    email_from VARCHAR(255),
    email_to VARCHAR(255),
    email_subject VARCHAR(500),
    email_snippet TEXT,
    email_date TIMESTAMP WITH TIME ZONE,
    snippet_type VARCHAR(50),  -- 'customer', 'supplier', 'internal', 'payment'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Shipments table - each warehouse in an order is a separate shipment
CREATE TABLE order_shipments (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    shipment_id VARCHAR(50) NOT NULL UNIQUE,  -- e.g., "5307-Li"
    warehouse VARCHAR(100) NOT NULL,
    status VARCHAR(50) DEFAULT 'needs_order',  -- needs_order, at_warehouse, needs_bol, ready_ship, shipped, delivered
    tracking VARCHAR(100),
    pro_number VARCHAR(50),
    bol_sent BOOLEAN DEFAULT FALSE,
    bol_sent_at TIMESTAMP WITH TIME ZONE,
    weight DECIMAL(10,2),
    ship_method VARCHAR(50),  -- LTL, Pirateship, Pickup, BoxTruck, LiDelivery
    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_complete ON orders(is_complete);
CREATE INDEX idx_orders_date ON orders(order_date DESC);
CREATE INDEX idx_line_items_order ON order_line_items(order_id);
CREATE INDEX idx_events_order ON order_events(order_id);
CREATE INDEX idx_email_snippets_order ON order_email_snippets(order_id);
CREATE INDEX idx_shipments_order ON order_shipments(order_id);
CREATE INDEX idx_shipments_id ON order_shipments(shipment_id);

-- View for current status
CREATE OR REPLACE VIEW order_status AS
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
    EXTRACT(DAY FROM NOW() - order_date)::INTEGER as days_open
FROM orders;
"""
