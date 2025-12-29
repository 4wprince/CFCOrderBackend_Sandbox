"""
ai_summary.py
Anthropic Claude API integration for generating order summaries.
"""

import json
import urllib.request
import urllib.error
from typing import Optional

from psycopg2.extras import RealDictCursor
from config import ANTHROPIC_API_KEY
from db_helpers import get_db


def is_configured() -> bool:
    """Check if Anthropic API is configured"""
    return bool(ANTHROPIC_API_KEY)


def call_anthropic_api(prompt: str, max_tokens: int = 1024) -> str:
    """Call Anthropic Claude API to generate summary"""
    if not ANTHROPIC_API_KEY:
        return "AI Summary not available - API key not configured"
    
    url = "https://api.anthropic.com/v1/messages"
    
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }
    
    data = json.dumps(payload).encode('utf-8')
    
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header("Content-Type", "application/json")
    req.add_header("x-api-key", ANTHROPIC_API_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode())
            if result.get('content') and len(result['content']) > 0:
                return result['content'][0].get('text', '')
            return "No summary generated"
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else str(e)
        print(f"Anthropic API Error: {e.code} - {error_body}")
        return f"AI Summary error: {e.code}"
    except Exception as e:
        print(f"Anthropic API Exception: {e}")
        return f"AI Summary error: {str(e)}"


def generate_order_summary(order_id: str) -> str:
    """Generate AI summary for an order based on all available data"""
    
    # Gather all order data
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get order details
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            
            if not order:
                return "Order not found"
            
            # Get email snippets
            cur.execute("""
                SELECT email_from, email_subject, email_snippet, email_date, snippet_type 
                FROM order_email_snippets 
                WHERE order_id = %s 
                ORDER BY email_date DESC
                LIMIT 20
            """, (order_id,))
            snippets = cur.fetchall()
            
            # Get events
            cur.execute("""
                SELECT event_type, event_data, created_at 
                FROM order_events 
                WHERE order_id = %s 
                ORDER BY created_at DESC
                LIMIT 10
            """, (order_id,))
            events = cur.fetchall()
    
    # Build context for AI
    context_parts = []
    
    # Order info
    context_parts.append(f"ORDER #{order_id}")
    context_parts.append(f"Customer: {order.get('company_name') or order.get('customer_name')}")
    context_parts.append(f"Order Total: ${order.get('order_total', 0)}")
    context_parts.append(f"Payment Received: {'Yes' if order.get('payment_received') else 'No'}")
    if order.get('tracking'):
        context_parts.append(f"Tracking: {order.get('tracking')}")
    if order.get('pro_number'):
        context_parts.append(f"PRO Number: {order.get('pro_number')}")
    if order.get('comments'):
        context_parts.append(f"Customer Comments: {order.get('comments')}")
    if order.get('notes'):
        context_parts.append(f"Internal Notes: {order.get('notes')}")
    
    # Warehouses
    warehouses = [order.get(f'warehouse_{i}') for i in range(1, 5) if order.get(f'warehouse_{i}')]
    if warehouses:
        context_parts.append(f"Warehouses: {', '.join(warehouses)}")
    
    # Email snippets
    if snippets:
        context_parts.append("\nEMAIL COMMUNICATIONS:")
        for s in snippets:
            date_str = s['email_date'].strftime('%m/%d') if s.get('email_date') else ''
            context_parts.append(f"- [{date_str}] From: {s.get('email_from', 'Unknown')}")
            context_parts.append(f"  Subject: {s.get('email_subject', '')}")
            if s.get('email_snippet'):
                context_parts.append(f"  {s['email_snippet'][:300]}")
    
    # Events (filter out sync noise)
    if events:
        important_events = [e for e in events if e.get('event_type') not in ('b2bwave_sync', 'auto_sync', 'status_check')]
        if important_events:
            context_parts.append("\nORDER EVENTS:")
            for e in important_events:
                date_str = e['created_at'].strftime('%m/%d %H:%M') if e.get('created_at') else ''
                context_parts.append(f"- [{date_str}] {e.get('event_type')}")
    
    context = "\n".join(context_parts)    

    # Create prompt
    prompt = f"""Write a brief order status summary.

Rules:
- Use simple bullet points (• symbol)
- NO headers, NO bold text, NO markdown formatting
- Only include notable information (special requests, issues, credits)
- Skip obvious info (order total, warehouse names) unless relevant to an issue
- 2-4 bullets maximum
- Plain conversational language
- Always end with "Next action:" if payment pending or action needed

Example good output:
- Customer will pay by check and pick up (no shipping needed)
- Next action: Wait for customer pickup with payment

Example bad output (too verbose):
- **Order Status:** Payment pending
- **Warehouse:** DL warehouse assigned
- **System Activity:** Multiple syncs detected

{context}"""    
    return call_anthropic_api(prompt)


def generate_simple_summary(text: str, max_length: int = 200) -> str:
    """Generate a simple summary of any text"""
    if not is_configured():
        return text[:max_length] + "..." if len(text) > max_length else text
    
    prompt = f"""Summarize this in {max_length} characters or less. Be concise:

{text}"""
    
    return call_anthropic_api(prompt, max_tokens=256)
