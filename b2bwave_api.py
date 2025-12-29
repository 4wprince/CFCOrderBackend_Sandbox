"""
b2bwave_api.py
B2BWave API integration for CFC Order Backend.
Handles order fetching and syncing from B2BWave.
"""

import json
import base64
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Dict, List, Optional

from config import B2BWAVE_URL, B2BWAVE_USERNAME, B2BWAVE_API_KEY


class B2BWaveAPIError(Exception):
    """Custom exception for B2BWave API errors"""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"B2BWave API Error ({status_code}): {message}")


def is_configured() -> bool:
    """Check if B2BWave API is configured"""
    return bool(B2BWAVE_URL and B2BWAVE_USERNAME and B2BWAVE_API_KEY)


def api_request(endpoint: str, params: dict = None) -> dict:
    """
    Make authenticated request to B2BWave API.
    
    Args:
        endpoint: API endpoint (e.g., 'orders', 'customers')
        params: Optional query parameters
        
    Returns:
        Parsed JSON response
        
    Raises:
        B2BWaveAPIError: On API errors
    """
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


def fetch_order(order_id: str) -> Optional[Dict]:
    """
    Fetch a single order from B2BWave by ID.
    
    Args:
        order_id: The B2BWave order ID
        
    Returns:
        Order data dict or None if not found
    """
    try:
        data = api_request("orders", {"id_eq": order_id})
        
        if isinstance(data, list) and len(data) > 0:
            return data[0].get('order', data[0])
        elif isinstance(data, dict) and 'order' in data:
            return data['order']
        
        return None
    except B2BWaveAPIError:
        return None


def fetch_orders(days_back: int = 7, status: str = None) -> List[Dict]:
    """
    Fetch orders from B2BWave.
    
    Args:
        days_back: Number of days to look back
        status: Optional status filter
        
    Returns:
        List of order dicts
    """
    params = {}
    
    if days_back:
        from_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime('%Y-%m-%d')
        params['submitted_at_gteq'] = from_date
    
    if status:
        params['status_eq'] = status
    
    data = api_request("orders", params)
    
    orders = []
    if isinstance(data, list):
        for item in data:
            order = item.get('order', item)
            orders.append(order)
    
    return orders


def parse_order_data(order: Dict) -> Dict:
    """
    Parse B2BWave order data into our standard format.
    
    Args:
        order: Raw B2BWave order dict
        
    Returns:
        Normalized order dict
    """
    order_id = str(order.get('id'))
    
    # Extract customer info
    customer_name = order.get('customer_name', '')
    company_name = order.get('customer_company', '')
    email = order.get('customer_email', '')
    phone = order.get('customer_phone', '')
    
    # Extract address
    address = {
        'street': order.get('address', ''),
        'street2': order.get('address2', ''),
        'city': order.get('city', ''),
        'state': order.get('province', ''),  # B2BWave calls it 'province'
        'zip': order.get('postal_code', ''),
        'country': order.get('country', 'US')
    }
    
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
    
    # Extract line items
    order_products = order.get('order_products', [])
    line_items = []
    sku_prefixes = []
    
    for op in order_products:
        product = op.get('order_product', op)
        product_code = product.get('product_code', '')
        product_name = product.get('product_name', '')
        quantity = int(float(product.get('quantity', 0) or 0))
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
    
    return {
        'order_id': order_id,
        'customer_name': customer_name,
        'company_name': company_name,
        'email': email,
        'phone': phone,
        'address': address,
        'comments': comments,
        'order_total': order_total,
        'total_weight': total_weight,
        'order_date': order_date,
        'line_items': line_items,
        'sku_prefixes': sku_prefixes
    }


def get_shipping_address(order: Dict) -> Dict:
    """
    Extract shipping address from B2BWave order.
    
    Args:
        order: Raw B2BWave order dict
        
    Returns:
        Address dict with standardized keys
    """
    return {
        'address': order.get('address', ''),
        'address2': order.get('address2', ''),
        'city': order.get('city', ''),
        'state': order.get('province', ''),
        'zip': order.get('postal_code', ''),
        'country': order.get('country', 'US')
    }


# Import timedelta for fetch_orders
from datetime import timedelta
