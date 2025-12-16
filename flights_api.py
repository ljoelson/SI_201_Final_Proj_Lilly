import requests
import os
from dotenv import load_dotenv
import sqlite3
from datetime import datetime

load_dotenv()
DB_NAME = "project_data.db"


def get_flight_data(airport, month=None):
    
    api_key = os.getenv('AVIATIONSTACK_API_KEY')
    
    if not api_key:
        raise ValueError("API key not found in .env file")
    
    base_url = "http://api.aviationstack.com/v1/flights"
    flights_list = []
    
    params = {
        'access_key': api_key,
        'dep_iata': airport,
        'limit': 25, 
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
                    # Departure info
                    departure = flight.get('departure', {})
                    arrival = flight.get('arrival', {})
                    flight_info = flight.get('flight', {})
                    airline_info = flight.get('airline', {})
                    
                    scheduled_departure = departure.get('scheduled')
                    
                    # Skip if no departure time
                    if not scheduled_departure:
                        continue
                    
                    if month:
                        flight_date = scheduled_departure[:7]
                        if flight_date != month:
                            continue
                    
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


def get_or_create_id(cursor, table, column, value):
    """Helper function to get or create an ID for a lookup table"""
    # Handle NULL values
    if value is None or value == 'N/A':
        return None
    
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        cursor.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))
        return cursor.lastrowid


def store_flight_data(db_conn, flights_list):

    cursor = db_conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Airlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Airports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            iata_code TEXT UNIQUE NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FlightStatuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT UNIQUE NOT NULL
        )
    ''')
    
    # New lookup table for timestamps
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Timestamps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT UNIQUE NOT NULL
        )
    ''')
    
    # Modified Flights table with timestamp foreign keys
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Flights (
            flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_number TEXT NOT NULL,
            airline_id INTEGER NOT NULL,
            departure_airport_id INTEGER NOT NULL,
            arrival_airport_id INTEGER NOT NULL,
            scheduled_departure_id INTEGER NOT NULL,
            actual_departure_id INTEGER,
            scheduled_arrival_id INTEGER,
            actual_arrival_id INTEGER,
            status_id INTEGER,
            FOREIGN KEY (airline_id) REFERENCES Airlines (id),
            FOREIGN KEY (departure_airport_id) REFERENCES Airports (id),
            FOREIGN KEY (arrival_airport_id) REFERENCES Airports (id),
            FOREIGN KEY (scheduled_departure_id) REFERENCES Timestamps (id),
            FOREIGN KEY (actual_departure_id) REFERENCES Timestamps (id),
            FOREIGN KEY (scheduled_arrival_id) REFERENCES Timestamps (id),
            FOREIGN KEY (actual_arrival_id) REFERENCES Timestamps (id),
            FOREIGN KEY (status_id) REFERENCES FlightStatuses (id),
            UNIQUE(flight_number, scheduled_departure_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FlightDelays (
            delay_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_id INTEGER NOT NULL,
            delay_minutes INTEGER,
            FOREIGN KEY (flight_id) REFERENCES Flights (flight_id)
        )
    ''')
    
    inserted_count = 0
    duplicate_count = 0
    
    for flight in flights_list:
        try:
            # Get IDs for repeating strings
            airline_id = get_or_create_id(cursor, 'Airlines', 'name', flight['airline'])
            dep_airport_id = get_or_create_id(cursor, 'Airports', 'iata_code', flight['departure_airport'])
            arr_airport_id = get_or_create_id(cursor, 'Airports', 'iata_code', flight['arrival_airport'])
            status_id = get_or_create_id(cursor, 'FlightStatuses', 'status', flight['flight_status'])
            
            # Get IDs for timestamps (handles NULL values)
            scheduled_dep_id = get_or_create_id(cursor, 'Timestamps', 'timestamp', flight['scheduled_departure'])
            actual_dep_id = get_or_create_id(cursor, 'Timestamps', 'timestamp', flight['actual_departure'])
            scheduled_arr_id = get_or_create_id(cursor, 'Timestamps', 'timestamp', flight['scheduled_arrival'])
            actual_arr_id = get_or_create_id(cursor, 'Timestamps', 'timestamp', flight['actual_arrival'])
            
            cursor.execute('''
                INSERT INTO Flights (
                    flight_number, airline_id, departure_airport_id, arrival_airport_id,
                    scheduled_departure_id, actual_departure_id, scheduled_arrival_id, 
                    actual_arrival_id, status_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                flight['flight_number'],
                airline_id,
                dep_airport_id,
                arr_airport_id,
                scheduled_dep_id,
                actual_dep_id,
                scheduled_arr_id,
                actual_arr_id,
                status_id
            ))
            
            flight_id = cursor.lastrowid
            
            cursor.execute('''
                INSERT INTO FlightDelays (flight_id, delay_minutes)
                VALUES (?, ?)
            ''', (flight_id, flight['delay_minutes']))
            
            inserted_count += 1
        
        except sqlite3.IntegrityError:
            duplicate_count += 1
    
    db_conn.commit()
    
    print(f"✓ Inserted {inserted_count} new flights")
    print(f"✓ Skipped {duplicate_count} duplicates")
    
    cursor.execute("SELECT COUNT(*) FROM Airlines")
    print(f"✓ Total unique airlines: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM Airports")
    print(f"✓ Total unique airports: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM FlightStatuses")
    print(f"✓ Total unique flight statuses: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM Timestamps")
    print(f"✓ Total unique timestamps: {cursor.fetchone()[0]}")
    
    return inserted_count


if __name__ == "__main__":
    print("FLIGHT DATA COLLECTION")
    
    airport_code = "DTW"
    
    flights = get_flight_data(airport_code, month=None)
    
    if not flights:
        print(" No flights found for DTW.")
    else:
        db_connection = sqlite3.connect(DB_NAME)
        store_flight_data(db_connection, flights)
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM Flights")
        total = cursor.fetchone()[0]
        
        print()
        print(f"Total flights in database: {total}")
        
        if total < 100:
            print(f"   Need {100 - total} more to reach 100. Run script again")
        else:
            print(f"100+ data rows reached!")
        
        db_connection.close()