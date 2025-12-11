#test_api.py
import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('AVIATIONSTACK_API_KEY')

response = requests.get(
    'http://api.aviationstack.com/v1/flights',
    params={'access_key': api_key, 'dep_iata': 'DTW', 'limit': 1}
)

print("Status Code:", response.status_code)
print("Response:", response.json())