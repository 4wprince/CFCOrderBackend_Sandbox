"""
detection.py
Detection functions for CFC Order Backend.
Detects payment links, payment notifications, R+L quotes, PRO numbers from emails.
"""

import re
import json
from typing import Dict, Optional, Tuple

from psycopg2.extras import RealDictCursor
from db_helpers import get_db


def detect_square_payment_link(email_body: str) -> bool:
    """Check if email contains a Square payment link"""
    return 'square.link' in email_body.lower()


def update_payment_link_sent(order_id: str) -> Dict:
    """Mark order as having payment link sent"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orders SET 
                    payment_link_sent = TRUE,
                    payment_link_sent_at = NOW(),
                    updated_at = NOW()
                WHERE order_id = %s AND NOT payment_link_sent
            """, (order_id,))
            
            if cur.rowcount > 0:
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, source)
                    VALUES (%s, 'payment_link_sent', 'email_detection')
                """, (order_id,))
                return {"status": "ok", "updated": True}
    
    return {"status": "ok", "updated": False, "message": "Already marked"}


def parse_payment_notification(email_subject: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse Square payment notification subject.
    Subject format: "$4,913.99 payment received from Dylan Gentry"
    
    Returns: (payment_amount, customer_name) or (None, None) if not a payment notification
    """
    # Extract amount from subject
    amount_match = re.search(r'\$([\d,]+\.?\d*)\s+payment received', email_subject, re.IGNORECASE)
    if not amount_match:
        return None, None
    
    payment_amount = float(amount_match.group(1).replace(',', ''))
    
    # Extract customer name
    name_match = re.search(r'payment received from (.+)$', email_subject, re.IGNORECASE)
    customer_name = name_match.group(1).strip() if name_match else None
    
    return payment_amount, customer_name


def match_payment_to_order(payment_amount: float, customer_name: Optional[str] = None) -> Optional[Dict]:
    """
    Try to match a payment to an unpaid order.
    
    Returns: Matched order dict or None
    """
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get recent unpaid orders
            cur.execute("""
                SELECT order_id, order_total, customer_name 
                FROM orders 
                WHERE NOT payment_received 
                AND order_total IS NOT NULL
                ORDER BY order_date DESC
                LIMIT 50
            """)
            orders = cur.fetchall()
            
            matched_order = None
            
            # Try to match by amount (payment should be >= order total)
            for order in orders:
                if order['order_total'] and payment_amount >= float(order['order_total']):
                    # Could be this order - check name similarity if we have it
                    if customer_name and order['customer_name']:
                        # Simple check - first name match
                        pay_first = customer_name.split()[0].lower()
                        order_first = order['customer_name'].split()[0].lower()
                        if pay_first == order_first:
                            matched_order = dict(order)
                            break
                    elif not matched_order:
                        # Take first amount match if no name match
                        matched_order = dict(order)
            
            return matched_order


def record_payment_received(order_id: str, payment_amount: float, customer_name: Optional[str] = None) -> Dict:
    """
    Record that payment was received for an order.
    """
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get order total to calculate shipping
            cur.execute("SELECT order_total FROM orders WHERE order_id = %s", (order_id,))
            row = cur.fetchone()
            order_total = float(row['order_total']) if row and row['order_total'] else 0
            shipping_cost = payment_amount - order_total if order_total else None
            
            cur.execute("""
                UPDATE orders SET 
                    payment_received = TRUE,
                    payment_received_at = NOW(),
                    payment_amount = %s,
                    shipping_cost = %s,
                    updated_at = NOW()
                WHERE order_id = %s
            """, (payment_amount, shipping_cost, order_id))
            
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'payment_received', %s, 'square_notification')
            """, (order_id, json.dumps({
                'payment_amount': payment_amount,
                'shipping_cost': shipping_cost,
                'customer_name': customer_name
            })))
            
            return {
                "status": "ok",
                "updated": True,
                "order_id": order_id,
                "payment_amount": payment_amount,
                "shipping_cost": shipping_cost
            }


def extract_rl_quote_number(email_body: str) -> Optional[str]:
    """
    Extract R+L quote number from email body.
    Pattern: "RL Quote No: 9075654" or "Quote: 9075654" or "Quote #9075654"
    """
    quote_match = re.search(r'(?:RL\s+)?Quote\s*(?:No|#)?[:\s]*(\d{6,10})', email_body, re.IGNORECASE)
    return quote_match.group(1) if quote_match else None


def record_rl_quote(order_id: str, quote_no: str) -> Dict:
    """Record R+L quote number for an order"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orders SET rl_quote_no = %s, updated_at = NOW()
                WHERE order_id = %s
            """, (quote_no, order_id))
            
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'rl_quote_captured', %s, 'email_detection')
            """, (order_id, json.dumps({'quote_no': quote_no})))
            
            return {"status": "ok", "quote_no": quote_no}


def extract_pro_number(email_body: str) -> Optional[str]:
    """
    Extract R+L PRO number from email body.
    Pattern: "PRO 74408602-5" or "PRO# 74408602-5" or "Pro Number: 74408602-5"
    """
    pro_match = re.search(r'PRO\s*(?:#|Number)?[:\s]*([A-Z]{0,2}\d{8,10}(?:-\d)?)', email_body, re.IGNORECASE)
    return pro_match.group(1).upper() if pro_match else None


def record_pro_number(order_id: str, pro_no: str) -> Dict:
    """Record R+L PRO number for an order"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orders SET pro_number = %s, tracking = %s, updated_at = NOW()
                WHERE order_id = %s
            """, (pro_no, f"R+L PRO {pro_no}", order_id))
            
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'pro_number_captured', %s, 'email_detection')
            """, (order_id, json.dumps({'pro_number': pro_no})))
            
            return {"status": "ok", "pro_number": pro_no}
