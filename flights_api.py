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
                    
                    departure = flight.get('departure', {})
                    arrival = flight.get('arrival', {})
                    flight_info = flight.get('flight', {})
                    airline_info = flight.get('airline', {})
                    
                    scheduled_departure = departure.get('scheduled')
                    

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
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        cursor.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))
        return cursor.lastrowid


def migrate_old_tables(cursor):
    """Rename old tables if they exist"""

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Flights'")
    if cursor.fetchone():

        cursor.execute("PRAGMA table_info(Flights)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'airline_id' not in columns:
            print("\n Found old Flights table structure. Migrating...")
            
            cursor.execute("ALTER TABLE Flights RENAME TO Flights_old")
            print("   Renamed Flights to Flights_old")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='FlightDelays'")
            if cursor.fetchone():
                cursor.execute("ALTER TABLE FlightDelays RENAME TO FlightDelays_old")
                print("   Renamed FlightDelays to FlightDelays_old")
            
            print("   Old tables preserved with '_old' suffix")
            print()


def store_flight_data(db_conn, flights_list):

    cursor = db_conn.cursor()
    
    migrate_old_tables(cursor)
    db_conn.commit()

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
    
    # Modified Flights table with foreign keys
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Flights (
            flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_number TEXT NOT NULL,
            airline_id INTEGER NOT NULL,
            departure_airport_id INTEGER NOT NULL,
            arrival_airport_id INTEGER NOT NULL,
            scheduled_departure TEXT NOT NULL,
            actual_departure TEXT,
            scheduled_arrival TEXT,
            actual_arrival TEXT,
            status_id INTEGER,
            FOREIGN KEY (airline_id) REFERENCES Airlines (id),
            FOREIGN KEY (departure_airport_id) REFERENCES Airports (id),
            FOREIGN KEY (arrival_airport_id) REFERENCES Airports (id),
            FOREIGN KEY (status_id) REFERENCES FlightStatuses (id),
            UNIQUE(flight_number, scheduled_departure)
        )
    ''')
    
    # Flight delays table
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
            #IDs for repeating strings
            airline_id = get_or_create_id(cursor, 'Airlines', 'name', flight['airline'])
            dep_airport_id = get_or_create_id(cursor, 'Airports', 'iata_code', flight['departure_airport'])
            arr_airport_id = get_or_create_id(cursor, 'Airports', 'iata_code', flight['arrival_airport'])
            status_id = get_or_create_id(cursor, 'FlightStatuses', 'status', flight['flight_status'])
            
            cursor.execute('''
                INSERT INTO Flights (
                    flight_number, airline_id, departure_airport_id, arrival_airport_id,
                    scheduled_departure, actual_departure, scheduled_arrival, 
                    actual_arrival, status_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                flight['flight_number'],
                airline_id,
                dep_airport_id,
                arr_airport_id,
                flight['scheduled_departure'],
                flight['actual_departure'],
                flight['scheduled_arrival'],
                flight['actual_arrival'],
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
    
    return inserted_count


if __name__ == "__main__":
    print("=" * 60)
    print("FLIGHT DATA COLLECTION (NORMALIZED)")
    print("=" * 60)
    print()
    
    airport_code = "DTW"
    
    flights = get_flight_data(airport_code, month=None)
    
    if not flights:
        print()
        print(" No flights found for DTW.")
        print()
        print("TROUBLESHOOTING:")
        print("1. Run: python test_aviationstack_api.py")
        print("2. Try a busier airport: JFK, LAX, or ORD")
        print("3. Check your API rate limit (500 calls/month)")
        print()
        print("To try a different airport, edit this file and change:")
        print("  airport_code = 'DTW'  -->  airport_code = 'JFK'")
    else:
        db_connection = sqlite3.connect(DB_NAME)
        store_flight_data(db_connection, flights)
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM Flights")
        total = cursor.fetchone()[0]
        
        print()
        print(f"Total flights in database: {total}")
        
        if total < 100:
            print(f"   Need {100 - total} more to reach 100")
            print(f"   Run this script again to collect more!")
        else:
            print(f"   Goal reached! (100+)")
        

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Flights_old'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM Flights_old")
            old_count = cursor.fetchone()[0]
            print()
            print(f"Your old Flights table still exists as 'Flights_old' ({old_count} rows)")
            print("   You can delete it later if you don't need it anymore")
        
        db_connection.close()
    
    print()
    print("=" * 60)