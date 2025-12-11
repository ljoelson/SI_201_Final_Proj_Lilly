"""
Test script to check what data AviationStack API is returning
Run this first to debug your API connection
"""

import requests
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

api_key = os.getenv('AVIATIONSTACK_API_KEY')

print("=" * 60)
print("AVIATIONSTACK API TEST")
print("=" * 60)
print()

if not api_key:
    print("ERROR: No API key found in .env file")
    print("Make sure your .env file contains:")
    print("AVIATIONSTACK_API_KEY=your_key_here")
    exit()

print(f"API Key found: {api_key[:10]}... (showing first 10 chars)")
print()

# Test 1: Basic API call
print("TEST 1: Fetching flights from Detroit (DTW)...")
print("-" * 60)

response = requests.get(
    'http://api.aviationstack.com/v1/flights',
    params={
        'access_key': api_key,
        'dep_iata': 'DTW',
        'limit': 5  # Just get 5 for testing
    }
)

print(f"Status Code: {response.status_code}")
print()

if response.status_code == 200:
    data = response.json()
    
    # Check for errors
    if 'error' in data:
        print("API ERROR:")
        print(json.dumps(data['error'], indent=2))
        print()
        print("Common issues:")
        print("- Invalid API key")
        print("- API rate limit exceeded (500 calls/month on free tier)")
        print("- Account suspended")
    else:
        print(f"Success! API returned data.")
        print(f"Number of flights: {len(data.get('data', []))}")
        print()
        
        if data.get('data'):
            print("SAMPLE FLIGHT DATA:")
            print("-" * 60)
            
            for i, flight in enumerate(data['data'][:3]):  # Show first 3
                print(f"\nFlight {i+1}:")
                print(f"  Flight Number: {flight.get('flight', {}).get('iata', 'N/A')}")
                print(f"  Airline: {flight.get('airline', {}).get('name', 'N/A')}")
                print(f"  Status: {flight.get('flight_status', 'N/A')}")
                
                dep = flight.get('departure', {})
                arr = flight.get('arrival', {})
                
                print(f"  Departure:")
                print(f"    Airport: {dep.get('iata', 'N/A')}")
                print(f"    Scheduled: {dep.get('scheduled', 'N/A')}")
                print(f"    Actual: {dep.get('actual', 'N/A')}")
                print(f"    Delay: {dep.get('delay', 0)} minutes")
                
                print(f"  Arrival:")
                print(f"    Airport: {arr.get('iata', 'N/A')}")
                print(f"    Scheduled: {arr.get('scheduled', 'N/A')}")
            
            print()
            print("=" * 60)
            print("FULL JSON RESPONSE (first flight):")
            print("-" * 60)
            print(json.dumps(data['data'][0], indent=2))
        else:
            print("WARNING: API returned 0 flights")
            print()
            print("This could mean:")
            print("1. No flights currently departing from DTW")
            print("2. Free tier limitations on data access")
            print("3. Try a different airport code (JFK, LAX, ORD)")
            
else:
    print(f"HTTP ERROR {response.status_code}")
    print(f"Response: {response.text}")

print()
print("=" * 60)