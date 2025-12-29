"""
email_parser.py
Email parsing functions for CFC Order Backend.
Handles parsing B2BWave order emails and extracting order data.
"""

import re
from typing import Dict, List, Optional

from psycopg2.extras import RealDictCursor
from db_helpers import get_db


def parse_b2bwave_email(body: str, subject: str) -> dict:
    """
    Parse B2BWave order email and extract all fields.
    Returns dict with: order_id, name, company, street, city, state, zip, phone, email, comments, total, line_items
    """
    result = {
        'order_id': None,
        'customer_name': None,
        'company_name': None,
        'street': None,
        'city': None,
        'state': None,
        'zip_code': None,
        'phone': None,
        'email': None,
        'comments': None,
        'order_total': None,
        'line_items': []
    }
    
    # Clean up body - normalize whitespace
    clean_body = body.replace('\r\n', '\n').replace('\r', '\n')
    
    # Extract order ID from subject: "Order Legendary Home Improvements-(#5261)"
    subject_match = re.search(r'\(#(\d{4,7})\)', subject)
    if subject_match:
        result['order_id'] = subject_match.group(1)
    
    # Also try from body
    if not result['order_id']:
        order_id_match = re.search(r'Order ID:\s*(\d{4,7})', clean_body)
        if order_id_match:
            result['order_id'] = order_id_match.group(1)
    
    # Extract Name
    name_match = re.search(r'Name:\s*(.+?)(?:\n|$)', clean_body)
    if name_match:
        result['customer_name'] = name_match.group(1).strip()
    
    # Extract Company
    company_match = re.search(r'Company:\s*(.+?)(?:\n|$)', clean_body)
    if company_match:
        result['company_name'] = company_match.group(1).strip()
    
    # Extract Phone (format: "Phone 352-665-0280" or "Phone: 352-665-0280")
    phone_match = re.search(r'Phone[:\s]+(\d{3}[-.\s]?\d{3}[-.\s]?\d{4})', clean_body)
    if phone_match:
        result['phone'] = phone_match.group(1).replace('.', '-').replace(' ', '-')
    
    # Extract Email
    email_match = re.search(r'Email:\s*([\w.-]+@[\w.-]+\.\w+)', clean_body)
    if email_match:
        result['email'] = email_match.group(1).lower()
    
    # Extract Comments
    comments_match = re.search(r'Comments:\s*(.+?)(?:\n\n|\nTotal:|\nGross|$)', clean_body, re.DOTALL)
    if comments_match:
        result['comments'] = comments_match.group(1).strip()
    
    # Extract Total
    total_match = re.search(r'(?:^|\n)Total:\s*\$?([\d,]+\.?\d*)', clean_body)
    if total_match:
        result['order_total'] = float(total_match.group(1).replace(',', ''))
    
    # =========================================================================
    # IMPROVED ADDRESS PARSING
    # B2BWave format variations:
    # 1. "4943 SE 10th Place\nKeystone Heights  FL  32656"
    # 2. "4943 SE 10th Place\n\nKeystone Heights  FL  32656" (blank line between)
    # 3. Multi-space separated: "City  State  Zip"
    # =========================================================================
    
    # First, find city/state/zip pattern anywhere in email
    # Pattern: City (words)  STATE (2 letters)  ZIP (5 digits)
    csz_patterns = [
        # Double-space separated: "Keystone Heights  FL  32656"
        r'([A-Za-z][A-Za-z\s]+?)\s{2,}([A-Z]{2})\s{2,}(\d{5}(?:-\d{4})?)',
        # Single space with comma: "Keystone Heights, FL 32656"
        r'([A-Za-z][A-Za-z\s]+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
        # Single space: "Keystone Heights FL 32656"
        r'([A-Za-z][A-Za-z\s]+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
    ]
    
    for pattern in csz_patterns:
        csz_match = re.search(pattern, clean_body)
        if csz_match:
            city = csz_match.group(1).strip()
            state = csz_match.group(2)
            zip_code = csz_match.group(3)
            
            # Validate - city should not contain certain keywords
            if not any(kw in city.lower() for kw in ['total', 'order', 'email', 'phone', 'comment', 'name', 'company']):
                result['city'] = city
                result['state'] = state
                result['zip_code'] = zip_code
                break
    
    # Now find street address - look for line starting with number before the city/state/zip
    if result['city']:
        # Find all lines that start with a number (potential street addresses)
        street_pattern = r'^(\d+[^\n]+?)(?:\n|$)'
        street_matches = re.findall(street_pattern, clean_body, re.MULTILINE)
        
        for street in street_matches:
            street = street.strip()
            # Skip if it's a phone number line or contains keywords
            if 'phone' in street.lower():
                continue
            if re.match(r'^\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', street):
                continue  # This is a phone number
            if '$' in street:
                continue  # This is a price line
            
            result['street'] = street
            break
    
    # If we still don't have street, try alternative approach
    if not result['street']:
        # Look for common street patterns
        street_match = re.search(r'(\d+\s+(?:N\.?|S\.?|E\.?|W\.?|North|South|East|West)?\s*[A-Za-z0-9\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|Court|Ct|Place|Pl|Circle|Cir|Trail)[^\n]*)', clean_body, re.IGNORECASE)
        if street_match:
            result['street'] = street_match.group(1).strip()
    
    # Extract SKU codes for warehouse mapping
    # Look for patterns like HSS-3VDB15, NSN-SM8, SHLS-B09
    sku_pattern = re.findall(r'\b([A-Z]{2,5})-[A-Z0-9]+\b', clean_body)
    sku_prefixes = list(set(sku_pattern))
    result['sku_prefixes'] = sku_prefixes
    
    return result


def get_warehouses_for_skus(sku_prefixes: List[str]) -> List[str]:
    """Look up warehouse names for given SKU prefixes"""
    if not sku_prefixes:
        return []
    
    warehouses = []
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            placeholders = ','.join(['%s'] * len(sku_prefixes))
            upper_prefixes = [p.upper() for p in sku_prefixes]
            cur.execute(f"""
                SELECT DISTINCT warehouse_name
                FROM warehouse_mapping
                WHERE UPPER(sku_prefix) IN ({placeholders})
            """, upper_prefixes)
            warehouses = [row['warehouse_name'] for row in cur.fetchall()]
    
    return warehouses


def extract_order_id_from_subject(subject: str) -> Optional[str]:
    """Extract order ID from email subject line"""
    # Pattern: "Order Legendary Home Improvements-(#5261)"
    match = re.search(r'\(#(\d{4,7})\)', subject)
    if match:
        return match.group(1)
    
    # Alternative pattern: "Order #5261"
    match = re.search(r'Order\s*#?(\d{4,7})', subject)
    if match:
        return match.group(1)
    
    return None


def extract_sku_prefixes(text: str) -> List[str]:
    """Extract SKU prefixes from text (e.g., HSS, NSN, SHLS)"""
    sku_pattern = re.findall(r'\b([A-Z]{2,5})-[A-Z0-9]+\b', text)
    return list(set(sku_pattern))


def clean_phone_number(phone: str) -> str:
    """Normalize phone number to XXX-XXX-XXXX format"""
    if not phone:
        return ""
    
    # Remove all non-digits
    digits = re.sub(r'\D', '', phone)
    
    # Format as XXX-XXX-XXXX
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    
    return phone  # Return original if can't format
