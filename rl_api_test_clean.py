"""
R+L Carriers API Test Script - CLEAN VERSION
Created: 2025-12-29
Purpose: Test API key authentication - KEY IN PAYLOAD ONLY

Based on ChatGPT recommendation:
- Remove all API key headers
- Send key ONLY in the JSON payload
"""

import requests

# Configuration
API_KEY = "gtNxNjkzU0NINIYWItMGFjZS00YzYyLWE1MzNmQ5I2MjNmZmC"
ACCOUNT_NUMBER = "C00VP1"
API_URL = "https://api.rlc.com/RateQuote"

# Validate key
print("=" * 60)
print("API Key Validation")
print("=" * 60)
print(f"Key length: {len(API_KEY)}")
print(f"Key repr: {repr(API_KEY)}")
print(f"Key stripped: {repr(API_KEY.strip())}")
print(f"Has whitespace: {API_KEY != API_KEY.strip()}")
print(f"First 10 chars: {API_KEY[:10]}")
print(f"Last 10 chars: {API_KEY[-10:]}")
print("=" * 60)

# Test payload - API key ONLY in payload, NOT in headers
payload = {
    "RateQuote": {
        "APIKey": API_KEY,
        "CustomerAccount": ACCOUNT_NUMBER,
        "QuoteType": "Domestic",
        "Origin": {
            "City": "Norcross",
            "StateOrProvince": "GA",
            "ZipOrPostalCode": "30071",
            "CountryCode": "USA"
        },
        "Destination": {
            "City": "Jacksonville",
            "StateOrProvince": "FL",
            "ZipOrPostalCode": "32256",
            "CountryCode": "USA"
        },
        "Items": [{
            "Class": "85",
            "Weight": 300
        }]
    }
}

# Headers - NO API KEY, just content type
headers = {
    "Content-Type": "application/json"
}

print("\nTest 1: API Key in PAYLOAD only (no auth headers)")
print("-" * 60)
try:
    response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text[:500]}")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 60)
print("Test 2: API Key in HEADER only (apiKey)")
print("-" * 60)

# Remove key from payload for this test
payload_no_key = {
    "RateQuote": {
        "CustomerAccount": ACCOUNT_NUMBER,
        "QuoteType": "Domestic",
        "Origin": {
            "City": "Norcross",
            "StateOrProvince": "GA",
            "ZipOrPostalCode": "30071",
            "CountryCode": "USA"
        },
        "Destination": {
            "City": "Jacksonville",
            "StateOrProvince": "FL",
            "ZipOrPostalCode": "32256",
            "CountryCode": "USA"
        },
        "Items": [{
            "Class": "85",
            "Weight": 300
        }]
    }
}

headers_with_key = {
    "Content-Type": "application/json",
    "apiKey": API_KEY
}

try:
    response = requests.post(API_URL, json=payload_no_key, headers=headers_with_key, timeout=30)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text[:500]}")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 60)
print("Test 3: Try alternate endpoint (api.rlcarriers.com)")
print("-" * 60)

ALT_URL = "https://api.rlcarriers.com/1.0.3/RateQuote"

try:
    response = requests.post(ALT_URL, json=payload, headers=headers, timeout=30)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text[:500]}")
except Exception as e:
    print(f"Error: {e}")
