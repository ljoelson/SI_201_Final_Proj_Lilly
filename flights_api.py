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
    
    try:
        print(f"Fetching flight data for {airport} in {month}...")
        response = requests.get(base_url, params=params)
        
        #Checking if request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Checking for API errors
            if 'error' in data:
                print(f"API Error: {data['error']}")
                return flights_list
            
            #Processing each flight
            if 'data' in data:
                for flight in data['data']:
                    # Get scheduled departure time
                    scheduled_departure = flight.get('departure', {}).get('scheduled')
                    
                    # Skip flights w/o departure time
                    if not scheduled_departure:
                        continue
                    
                    # Filter by month if needed
                    flight_date = scheduled_departure[:7]
                    if flight_date != month:
                        continue
                    
                    # Relevant info
                    flight_record = {
                        'flight_number': flight.get('flight', {}).get('iata', 'N/A'),
                        'airline': flight.get('airline', {}).get('name', 'N/A'),
                        'departure_airport': flight.get('departure', {}).get('iata', 'N/A'),
                        'arrival_airport': flight.get('arrival', {}).get('iata', 'N/A'),
                        'scheduled_departure': scheduled_departure,
                        'actual_departure': flight.get('departure', {}).get('actual'),
                        'scheduled_arrival': flight.get('arrival', {}).get('scheduled'),
                        'actual_arrival': flight.get('arrival', {}).get('actual'),
                        'flight_status': flight.get('flight_status', 'unknown'),
                        'delay_minutes': flight.get('departure', {}).get('delay', 0)
                    }
                    
                    flights_list.append(flight_record)
                
                print(f"Successfully retrieved {len(flights_list)} flights for {month}")
            else:
                print("No flight data found in response")
        
        else:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response: {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    return flights_list


def store_flight_data(db_conn, flights_list):
    """
    Store flight and delay info in the database
    
    Creates two tables with shared integer key (flight_id):
    - flights table: stores flight information
    - flight_delays table: stores delay information (shares flight_id)
    
    Input: 
        db_conn: SQLite connection object
        flights_list: list of flight dictionaries
    
    Output: 
        Inserts data into flights table and flight_delays table (checks duplicates)

    """
    
    cursor = db_conn.cursor()
    
    # Create flights table if it doesn't exist to store main flight information
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flights (
            flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_number TEXT NOT NULL,
            airline TEXT NOT NULL,
            departure_airport TEXT NOT NULL,
            arrival_airport TEXT NOT NULL,
            scheduled_departure TEXT NOT NULL,
            actual_departure TEXT,
            scheduled_arrival TEXT,
            actual_arrival TEXT,
            flight_status TEXT,
            UNIQUE(flight_number, scheduled_departure)
        )
    ''')
    
    # Create flight_delays table if it doesn't exist
    # This shares the flight_id integer key with flights table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flight_delays (
            delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_id INTEGER NOT NULL,
            delay_minutes INTEGER,
            FOREIGN KEY (flight_id) REFERENCES flights (flight_id)
        )
    ''')
    
   