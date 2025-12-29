"""
load_rta_data.py
Load RTA Cabinet Database into PostgreSQL

Usage:
    python load_rta_data.py <excel_file> <database_url>

Example:
    python load_rta_data.py RTA_Cabinet_Database_42.xlsx "postgresql://user:pass@host:5432/db"
"""

import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

def requires_long_pallet(product_code: str, height: float, width: float) -> bool:
    """
    Determine if an item requires a long (8ft) pallet for shipping.
    """
    product_code_upper = (product_code or '').upper()
    height = height or 0
    width = width or 0
    
    # Boxed items - NEVER need long pallet
    boxed_keywords = ['MOLDING', 'FILLER', 'TOE', 'SCRIBE', 'FURNITURE BASE', 'CROWN']
    if any(kw in product_code_upper for kw in boxed_keywords):
        return False
    
    # Oven, Pantry, Broom cabinets - need long pallet if height >= 84"
    if ('OVEN' in product_code_upper or 'PANTRY' in product_code_upper or 'BROOM' in product_code_upper) and height >= 84:
        return True
    
    # Panels with width >= 6.5 and height >= 84 - need long pallet
    if ('PANEL' in product_code_upper or 'SKIN' in product_code_upper) and width >= 6.5 and height >= 84:
        return True
    
    # Any 96"+ item with width >= 6.5
    if height >= 96 and width >= 6.5:
        return True
    
    return False


def load_data(excel_path: str, database_url: str):
    """Load RTA data from Excel into PostgreSQL"""
    
    # Fix Heroku-style URLs
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    print(f"Reading Excel file: {excel_path}")
    df = pd.read_excel(excel_path, sheet_name='Master')
    print(f"Found {len(df)} rows")
    
    # Calculate long pallet flag
    print("Calculating long pallet flags...")
    df['requires_long_pallet'] = df.apply(
        lambda row: requires_long_pallet(
            row.get('Product_Code', ''),
            row.get('Height', 0),
            row.get('Width', 0)
        ),
        axis=1
    )
    
    long_pallet_count = df['requires_long_pallet'].sum()
    print(f"Items requiring long pallet: {long_pallet_count}")
    
    # Connect to database
    print(f"Connecting to database...")
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    
    # Prepare data for insert
    print("Inserting data...")
    
    insert_sql = """
        INSERT INTO rta_products (
            product_sku, pre_sku, post_sku, door_name, product_code,
            product_type, cabinet_type, width, height, depth,
            supplier, door_style, cogs, sales_price, weight,
            requires_long_pallet, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (product_sku) DO UPDATE SET
            pre_sku = EXCLUDED.pre_sku,
            post_sku = EXCLUDED.post_sku,
            door_name = EXCLUDED.door_name,
            product_code = EXCLUDED.product_code,
            product_type = EXCLUDED.product_type,
            cabinet_type = EXCLUDED.cabinet_type,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            depth = EXCLUDED.depth,
            supplier = EXCLUDED.supplier,
            door_style = EXCLUDED.door_style,
            cogs = EXCLUDED.cogs,
            sales_price = EXCLUDED.sales_price,
            weight = EXCLUDED.weight,
            requires_long_pallet = EXCLUDED.requires_long_pallet,
            updated_at = NOW()
    """
    
    # Convert dataframe to list of tuples
    def safe_value(val):
        if pd.isna(val):
            return None
        return val
    
    data = []
    for _, row in df.iterrows():
        data.append((
            safe_value(row.get('product_sku')),
            safe_value(row.get('pre_sku')),
            safe_value(row.get('post_sku')),
            safe_value(row.get('Door_Name')),
            safe_value(row.get('Product_Code')),
            safe_value(row.get('Product_Type')),
            safe_value(row.get('Cabinet_Type')),
            safe_value(row.get('Width')),
            safe_value(row.get('Height')),
            safe_value(row.get('Depth')),
            safe_value(row.get('Supplier')),
            safe_value(row.get('Door_Style')),
            safe_value(row.get('COGS')),
            safe_value(row.get('Sales_Price')),
            safe_value(row.get('Weight')),
            row.get('requires_long_pallet', False)
        ))
    
    # Batch insert
    execute_batch(cur, insert_sql, data, page_size=500)
    conn.commit()
    
    # Verify
    cur.execute("SELECT COUNT(*) FROM rta_products")
    count = cur.fetchone()[0]
    print(f"Total rows in database: {count}")
    
    cur.execute("SELECT COUNT(*) FROM rta_products WHERE requires_long_pallet = TRUE")
    lp_count = cur.fetchone()[0]
    print(f"Items requiring long pallet: {lp_count}")
    
    cur.close()
    conn.close()
    
    print("Done!")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python load_rta_data.py <excel_file> <database_url>")
        print("Example: python load_rta_data.py RTA_Cabinet_Database_42.xlsx \"postgresql://user:pass@host/db\"")
        sys.exit(1)
    
    excel_file = sys.argv[1]
    db_url = sys.argv[2]
    
    load_data(excel_file, db_url)
