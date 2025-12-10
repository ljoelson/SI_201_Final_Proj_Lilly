import requests
import os
from dotenv import load_dotenv
import sqlite3
from datetime import datetime

# Load environment variables from .env file
load_dotenv()


def get_flight_data(airport, month):
    """
    Retrieve departure/cancellation data from AviationStack API for Detroit flights
    
    Input: 
        airport (str): IATA airport code (e.g., 'DTW' for Detroit)
        month (str): Month in format 'YYYY-MM' (e.g., '2024-12')
    
    Output: 
        list of flight records (dictionaries with flight number, airline, 
        departing/arriving airports, scheduled/actual times, delays)
    """
    
    # Get API key from environment variable
    api_key = os.getenv('AVIATIONSTACK_API_KEY')
    
    if not api_key:
        raise ValueError("API key not found. Make sure AVIATIONSTACK_API_KEY is set in .env file")
    
    # Base URL for AviationStack API
    base_url = "http://api.aviationstack.com/v1/flights"
    
    flights_list = []
    
    # Limit to 25 items per execution as per project requirements
    params = {
        'access_key': api_key,
        'dep_iata': airport,  # Departure airport (DTW for Detroit)
        'limit': 25,  # IMPORTANT: Limited to 25 per run
    }
    
   