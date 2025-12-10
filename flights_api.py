import requests
import os
from dotenv import load_dotenv
import sqlite3
from datetime import datetime

load_dotenv()


def get_flight_data(airport, month=None):
    """
    Retrieve departure data from AviationStack API
    
    Input: 
        airport (str): IATA airport code (e.g., 'DTW', 'JFK', 'LAX')
        month (str): Optional month filter in format 'YYYY-MM' 
    
    Output: 
        list of flight records (max 25 per call)
    """
    
    api_key = os.getenv('AVIATIONSTACK_API_KEY')
    
    if not api_key:
        raise ValueError("API key not found in .env file")
    
    base_url = "http://api.aviationstack.com/v1/flights"
    flights_list = []
    
    params = {
        'access_key': api_key,
        'dep_iata': airport,
        'limit': 25,  # Max 25 items per run
    }
    
    try:
        print(f"Fetching flight data for {airport}...")
        response = requests.get(base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'error' in data:
                print(f"API Error: {data['error']}")
                return flights_list
            
            if 'data' in data:
                print(f"API returned {len(data['data'])} flights")
                
                for flight in data['data']:
                    # Get departure info
                    departure = flight.get('departure', {})
                    arrival = flight.get('arrival', {})
                    flight_info = flight.get('flight', {})
                    airline_info = flight.get('airline', {})
                    
                    scheduled_departure = departure.get('scheduled')
                    
                    # Skip if no departure time
                    if not scheduled_departure:
                        continue
                    
                    # If month filter is specified, apply it
                    if month:
                        flight_date = scheduled_departure[:7]
                        if flight_date != month:
                            continue
                    
                    # Extract flight record
                    flight_record = {
                        'flight_number': flight_info.get('iata', 'N/A'),
                        'airline': airline_info.get('name', 'N/A'),
                        'departure_airport': departure.get('iata', 'N/A'),
                        'arrival_airport': arrival.get('iata', 'N/A'),
                        'scheduled_departure': scheduled_departure,
                        'actual_departure': departure.get('actual'),
                        'scheduled_arrival': arrival.get('scheduled'),
                        'actual_arrival': arrival.get('actual'),
                        'flight_status': flight.get('flight_status', 'unknown'),
                        'delay_minutes': departure.get('delay') or 0
                    }
                    
                    flights_list.append(flight_record)
                
                print(f"Collected {len(flights_list)} flights (after filtering)")
            else:
                print("No flight data in API response")
        else:
            print(f"HTTP Error {response.status_code}: {response.text}")
    
    except Exception as e:
        print(f"Error: {e}")
    
    return flights_list


def store_flight_data(db_conn, flights_list):
    """
    Store flight and delay info in database with two tables sharing flight_id
    
    Input: 
        db_conn: SQLite connection
        flights_list: list of flight dicts
    
    Output: 
        Number of flights inserted (prevents duplicates)
    
    Responsible: Lilly
    """
    
    cursor = db_conn.cursor()
    
    # Create flights table
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
    
    # Create flight_delays table (shares flight_id key)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flight_delays (
            delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_id INTEGER NOT NULL,
            delay_minutes INTEGER,
            FOREIGN KEY (flight_id) REFERENCES flights (flight_id)
        )
    ''')
    
    inserted_count = 0
    duplicate_count = 0
    
    for flight in flights_list:
        try:
            # Insert into flights table
            cursor.execute('''
                INSERT INTO flights (
                    flight_number, airline, departure_airport, arrival_airport,
                    scheduled_departure, actual_departure, scheduled_arrival, 
                    actual_arrival, flight_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                flight['flight_number'],
                flight['airline'],
                flight['departure_airport'],
                flight['arrival_airport'],
                flight['scheduled_departure'],
                flight['actual_departure'],
                flight['scheduled_arrival'],
                flight['actual_arrival'],
                flight['flight_status']
            ))
            
            flight_id = cursor.lastrowid
            
            # Insert into flight_delays table using shared flight_id
            cursor.execute('''
                INSERT INTO flight_delays (flight_id, delay_minutes)
                VALUES (?, ?)
            ''', (flight_id, flight['delay_minutes']))
            
            inserted_count += 1
        
        except sqlite3.IntegrityError:
            duplicate_count += 1
    
    db_conn.commit()
    
    print(f"✓ Inserted {inserted_count} new flights")
    print(f"✓ Skipped {duplicate_count} duplicates")
    
    return inserted_count


if __name__ == "__main__":
    print("=" * 60)
    print("FLIGHT DATA COLLECTION")
    print("=" * 60)
    print()
    
    # Try DTW first, but if no data, suggests alternatives
    airport_code = "DTW"
    db_name = "flight_delays.db"
    
    # Don't filter by month - just get whatever is available
    flights = get_flight_data(airport_code, month=None)
    
    if not flights:
        print()
        print("⚠️  No flights found for DTW.")
        print()
        print("TROUBLESHOOTING:")
        print("1. Run: python test_aviationstack_api.py")
        print("2. Try a busier airport: JFK, LAX, or ORD")
        print("3. Check your API rate limit (500 calls/month)")
        print()
        print("To try a different airport, edit this file and change:")
        print("  airport_code = 'DTW'  -->  airport_code = 'JFK'")
    else:
        db_connection = sqlite3.connect(db_name)
        store_flight_data(db_connection, flights)
        
        # Show progress
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM flights")
        total = cursor.fetchone()[0]
        
        print()
        print(f"📊 Total flights in database: {total}")
        
        if total < 100:
            print(f"   Need {100 - total} more to reach 100")
            print(f"   Run this script again to collect more!")
        else:
            print(f"   ✓ Goal reached! (100+)")
        
        db_connection.close()
    
    print()
    print("=" * 60)