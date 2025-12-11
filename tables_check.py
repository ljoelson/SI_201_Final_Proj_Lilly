import sqlite3
import pandas as pd

DB_NAME = "project_data.db"

def view_database_summary():
    """
    View all tables in the project_data.db database
    Shows table structure and row counts
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    print("=" * 80)
    print(f"DATABASE: {DB_NAME}")
    print("=" * 80)
    print()
    
    # Get all table names
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    
    if not tables:
        print("No tables found in database!")
        print("Run weather_api.py and flights_api.py first to create tables.")
        conn.close()
        return
    
    print(f"Found {len(tables)} tables:")
    for i, table in enumerate(tables, 1):
        print(f"  {i}. {table[0]}")
    print()
    
    # Show details for each table
    for table in tables:
        table_name = table[0]
        
        print("=" * 80)
        print(f"TABLE: {table_name}")
        print("=" * 80)
        
        # Get table schema
        cur.execute(f"PRAGMA table_info({table_name})")
        columns = cur.fetchall()
        
        print("\nSchema:")
        print("-" * 80)
        for col in columns:
            col_id, name, data_type, not_null, default, pk = col
            pk_str = " [PRIMARY KEY]" if pk else ""
            null_str = " NOT NULL" if not_null else ""
            print(f"  • {name:<25} {data_type:<15}{pk_str}{null_str}")
        
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        
        print(f"\nTotal Rows: {count}")
        
        # Show first 5 rows
        if count > 0:
            print("\nSample Data (first 5 rows):")
            print("-" * 80)
            df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", conn)
            print(df.to_string())
        else:
            print("\nTable is empty!")
        
        print("\n")
    
    # Check requirements
    print("=" * 80)
    print("REQUIREMENTS CHECK")
    print("=" * 80)
    print()
    
    # Check 1: Two APIs (Weather + Flights)
    has_weather = any(t[0] == 'WeatherData' for t in tables)
    has_flights = any(t[0] == 'Flights' for t in tables)
    print(f"✓ Weather API data: {'YES' if has_weather else 'NO'}")
    print(f"✓ Flight API data: {'YES' if has_flights else 'NO'}")
    print()
    
    # Check 2: 100+ rows from each API
    if has_weather:
        cur.execute("SELECT COUNT(*) FROM WeatherData")
        weather_count = cur.fetchone()[0]
        print(f"✓ WeatherData rows: {weather_count} {'✓ (100+)' if weather_count >= 100 else '(Need 100)'}")
    
    if has_flights:
        cur.execute("SELECT COUNT(*) FROM Flights")
        flight_count = cur.fetchone()[0]
        print(f"✓ Flights rows: {flight_count} {'✓ (100+)' if flight_count >= 100 else '(Need 100)'}")
    print()
    
    # Check 3: Two tables sharing integer key
    has_delays = any(t[0] == 'FlightDelays' for t in tables)
    print(f"✓ Two tables with shared integer key (Flights + FlightDelays): {'YES' if (has_flights and has_delays) else 'NO'}")
    
    if has_flights and has_delays:
        # Verify the relationship works
        query = """
            SELECT COUNT(*) 
            FROM Flights f
            JOIN FlightDelays fd ON f.flight_id = fd.flight_id
        """
        cur.execute(query)
        joined_count = cur.fetchone()[0]
        print(f"  • Successfully joined records: {joined_count}")
    
    print()
    print("=" * 80)
    
    conn.close()


def check_duplicates():
    """Check for duplicate data"""
    conn = sqlite3.connect(DB_NAME)
    
    print("\n DUPLICATE CHECK")
    print("=" * 80)
    
    # Check flight duplicates
    query = """
        SELECT flight_number, scheduled_departure, COUNT(*) as count
        FROM Flights
        GROUP BY flight_number, scheduled_departure
        HAVING count > 1
    """
    df = pd.read_sql_query(query, conn)
    
    if len(df) > 0:
        print("Found duplicate flights:")
        print(df)
    else:
        print("No duplicate flights found")
    
    # Check weather duplicates
    query = """
        SELECT fetch_date, datetime, COUNT(*) as count
        FROM WeatherData
        GROUP BY fetch_date, datetime
        HAVING count > 1
    """
    df = pd.read_sql_query(query, conn)
    
    if len(df) > 0:
        print("\nFound duplicate weather records:")
        print(df)
    else:
        print("No duplicate weather records found")
    
    conn.close()


if __name__ == "__main__":
    view_database_summary()
    check_duplicates()
    
    print("\nTIP: To view this database graphically in VS Code:")
    print("   1. Install 'SQLite Viewer' extension")
    print("   2. Click on 'project_data.db' in the Explorer")
    print("   3. All 3 tables will appear in one interface!")
    print()