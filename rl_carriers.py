"""
rl_carriers.py
Direct R+L Carriers API integration for LTL freight quotes.
API Docs: https://api.rlc.com/swagger/ui/index#/RateQuote
"""

import json
import urllib.request
import urllib.error
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta

# R+L Carriers API configuration
RL_API_BASE_URL = "https://api.rlc.com"


def _get_api_key() -> str:
    """Get API key from environment (read at request time)"""
    return os.environ.get("RL_CARRIERS_API_KEY", "")


class RLCarriersError(Exception):
    """Custom exception for R+L Carriers API errors"""
    def __init__(self, message: str, errors: List[Dict] = None):
        self.message = message
        self.errors = errors or []
        super().__init__(message)


def is_configured() -> bool:
    """Check if R+L Carriers API is configured"""
    return bool(_get_api_key())


def _make_request(endpoint: str, method: str = "GET", data: dict = None) -> dict:
    """Make authenticated request to R+L Carriers API"""
    api_key = _get_api_key()
    if not api_key:
        raise RLCarriersError("R+L Carriers API key not configured")
    
    url = f"{RL_API_BASE_URL}/{endpoint}"
    
    req = urllib.request.Request(url, method=method)
    req.add_header("apiKey", api_key)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    
    if data:
        req.data = json.dumps(data).encode('utf-8')
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode())
            
            # Check for API errors
            if result.get("Code", 0) != 0 or result.get("Errors"):
                errors = result.get("Errors", [])
                error_msg = "; ".join([e.get("ErrorMessage", "Unknown error") for e in errors]) if errors else "API error"
                raise RLCarriersError(error_msg, errors)
            
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else str(e)
        raise RLCarriersError(f"HTTP {e.code}: {error_body}")
    except urllib.error.URLError as e:
        raise RLCarriersError(f"Connection error: {str(e)}")


def get_rate_quote(
    origin_zip: str,
    origin_city: str,
    origin_state: str,
    dest_zip: str,
    dest_city: str,
    dest_state: str,
    weight_lbs: int,
    freight_class: str = "70",
    pieces: int = 1,
    length: float = None,
    width: float = None,
    height: float = None,
    additional_services: List[str] = None,
    pickup_date: str = None
) -> Dict:
    """
    Get LTL freight rate quote from R+L Carriers.
    
    Args:
        origin_zip: Origin ZIP code
        origin_city: Origin city name
        origin_state: Origin state (2-letter)
        dest_zip: Destination ZIP code
        dest_city: Destination city name
        dest_state: Destination state (2-letter)
        weight_lbs: Total weight in pounds
        freight_class: NMFC freight class (default "70")
        pieces: Number of pieces/pallets
        length: Length in inches (optional)
        width: Width in inches (optional)
        height: Height in inches (optional)
        additional_services: List of accessorial codes (optional)
        pickup_date: Pickup date YYYY-MM-DD (optional, defaults to tomorrow)
    
    Returns:
        Dict with quote details including price and quote number
    """
    # Default pickup date to tomorrow if not specified (R+L wants MM/dd/yyyy format)
    if not pickup_date:
        pickup_date = (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y")
    
    # Build request payload
    payload = {
        "RateQuote": {
            "Origin": {
                "City": origin_city,
                "StateOrProvince": origin_state,
                "ZipOrPostalCode": origin_zip,
                "CountryCode": "USA"
            },
            "Destination": {
                "City": dest_city,
                "StateOrProvince": dest_state,
                "ZipOrPostalCode": dest_zip,
                "CountryCode": "USA"
            },
            "Items": [
                {
                    "Weight": int(weight_lbs),
                    "Class": freight_class
                }
            ],
            "PickupDate": pickup_date
        }
    }
    
    # Add dimensions if provided
    if length and width and height:
        payload["RateQuote"]["Items"][0]["Length"] = float(length)
        payload["RateQuote"]["Items"][0]["Width"] = float(width)
        payload["RateQuote"]["Items"][0]["Height"] = float(height)
    
    # Add additional services if provided
    if additional_services:
        payload["RateQuote"]["AdditionalServices"] = additional_services
    
    # Make API request
    result = _make_request("RateQuote", method="POST", data=payload)
    
    # Parse response
    rate_quote = result.get("RateQuote", {})
    service_levels = rate_quote.get("ServiceLevels", [])
    
    # Find standard service level (or first available)
    standard_quote = None
    for level in service_levels:
        if level.get("Code") == "STD" or level.get("Name") == "Standard":
            standard_quote = level
            break
    
    if not standard_quote and service_levels:
        standard_quote = service_levels[0]
    
    if not standard_quote:
        raise RLCarriersError("No rate quotes returned")
    
    # Extract pricing
    charge = standard_quote.get("Charge", "0")
    net_charge = standard_quote.get("NetCharge", charge)
    
    # Clean up price strings (remove $ and commas)
    def parse_price(price_str):
        if not price_str:
            return 0.0
        return float(str(price_str).replace("$", "").replace(",", ""))
    
    return {
        "quote_number": standard_quote.get("QuoteNumber"),
        "service_name": standard_quote.get("Name", "Standard"),
        "service_code": standard_quote.get("Code", "STD"),
        "service_days": standard_quote.get("ServiceDays", 0),
        "gross_charge": parse_price(charge),
        "net_charge": parse_price(net_charge),
        "customer_discounts": rate_quote.get("CustomerDiscounts", ""),
        "pickup_date": rate_quote.get("PickupDate"),
        "is_direct": rate_quote.get("IsDirect", False),
        "origin": rate_quote.get("Origin", {}),
        "destination": rate_quote.get("Destination", {}),
        "all_service_levels": service_levels,
        "charges": rate_quote.get("Charges", []),
        "messages": result.get("Messages", [])
    }


def get_simple_quote(
    origin_zip: str,
    dest_zip: str,
    weight_lbs: int,
    freight_class: str = "70"
) -> Dict:
    """
    Simplified rate quote - only requires ZIP codes and weight.
    City/state are looked up automatically by R+L.
    
    Args:
        origin_zip: Origin ZIP code
        dest_zip: Destination ZIP code
        weight_lbs: Total weight in pounds
        freight_class: NMFC freight class (default "70")
    
    Returns:
        Dict with quote details
    """
    # R+L API requires city/state, but we can use placeholder values
    # and let R+L correct them based on ZIP
    # Using generic placeholders that R+L will override
    
    payload = {
        "RateQuote": {
            "Origin": {
                "ZipOrPostalCode": origin_zip,
                "CountryCode": "USA"
            },
            "Destination": {
                "ZipOrPostalCode": dest_zip,
                "CountryCode": "USA"
            },
            "Items": [
                {
                    "Weight": int(weight_lbs),
                    "Class": freight_class
                }
            ],
            "PickupDate": (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y")
        }
    }
    
    result = _make_request("RateQuote", method="POST", data=payload)
    
    rate_quote = result.get("RateQuote", {})
    service_levels = rate_quote.get("ServiceLevels", [])
    
    # Get standard service
    standard_quote = None
    for level in service_levels:
        if level.get("Code") == "STD" or level.get("Name") == "Standard":
            standard_quote = level
            break
    
    if not standard_quote and service_levels:
        standard_quote = service_levels[0]
    
    if not standard_quote:
        raise RLCarriersError("No rate quotes returned")
    
    def parse_price(price_str):
        if not price_str:
            return 0.0
        return float(str(price_str).replace("$", "").replace(",", ""))
    
    net_charge = parse_price(standard_quote.get("NetCharge", standard_quote.get("Charge", "0")))
    
    return {
        "quote_number": standard_quote.get("QuoteNumber"),
        "net_charge": net_charge,
        "service_days": standard_quote.get("ServiceDays", 0),
        "carrier": "R+L Carriers",
        "service": standard_quote.get("Name", "Standard LTL")
    }


def get_pallet_types() -> List[Dict]:
    """Get available pallet types from R+L"""
    result = _make_request("RateQuote/GetPalletTypes", method="GET")
    return result.get("PalletTypes", [])


def track_shipment(pro_number: str) -> Dict:
    """
    Track a shipment by PRO number.
    
    Args:
        pro_number: R+L PRO number
    
    Returns:
        Dict with tracking information
    """
    result = _make_request(f"ShipmentTracing?request.traceNumbers={pro_number}&request.traceType=PRO", method="GET")
    
    shipments = result.get("Shipments", [])
    if not shipments:
        raise RLCarriersError(f"No shipment found for PRO {pro_number}")
    
    return shipments[0]


def test_connection() -> Dict:
    """Test API connection by fetching pallet types"""
    try:
        pallet_types = get_pallet_types()
        return {
            "status": "ok",
            "message": "R+L Carriers API connection successful",
            "pallet_types_count": len(pallet_types)
        }
    except RLCarriersError as e:
        return {
            "status": "error",
            "message": str(e),
            "errors": e.errors
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
