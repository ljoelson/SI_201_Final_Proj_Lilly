"""calculate_avg_delay_precipitation(db_conn): calculate avg delay during precipitation
Responsible: Karen
Input: SQLite connection
Output (txt): avg departure delay (float) during rainy/snowy weather
"""

def calc_avg_delay_precip(db_conn):
    cur = db_conn.cursor()
    
    # First, let's check what data we have
    print("=" * 60)
    print("DEBUGGING: Checking available data...")
    print("=" * 60)
    
    # Check flights
    cur.execute("SELECT COUNT(*) FROM Flights")
    flight_count = cur.fetchone()[0]
    print(f"Total flights: {flight_count}")
    
    # Check flight_delays
    cur.execute("SELECT COUNT(*) FROM flight_delays WHERE delay_minutes IS NOT NULL")
    delay_count = cur.fetchone()[0]
    print(f"Flights with delay data: {delay_count}")
    
    # Check weather
    cur.execute("SELECT COUNT(*) FROM WeatherData")
    weather_count = cur.fetchone()[0]
    print(f"Total weather records: {weather_count}")
    
    # Check date ranges
    cur.execute("SELECT MIN(scheduled_departure), MAX(scheduled_departure) FROM Flights WHERE scheduled_departure IS NOT NULL")
    flight_dates = cur.fetchone()
    print(f"Flight dates range: {flight_dates[0]} to {flight_dates[1]}")
    
    cur.execute("SELECT MIN(fetch_date), MAX(fetch_date) FROM WeatherData")
    weather_dates = cur.fetchone()
    print(f"Weather dates range: {weather_dates[0]} to {weather_dates[1]}")
    
    # Check for precipitation in weather data
    precip = ['rain', 'snow', 'drizzle', 'sleet', 'hail']
    precip_condition = " OR ".join([f"LOWER(description) LIKE '%{term}%'" for term in precip])
    cur.execute(f"SELECT COUNT(*) FROM WeatherData WHERE {precip_condition}")
    precip_count = cur.fetchone()[0]
    print(f"Weather records with precipitation: {precip_count}")
    
    print("\n" + "=" * 60)
    print("Attempting to join data...")
    print("=" * 60 + "\n")
    
    # Try a simpler join - just get all delays with any weather
    flight_weather_sql = """
        SELECT 
            fd.delay_minutes, 
            W.description,
            F.scheduled_departure,
            W.fetch_date
        FROM Flights F
        JOIN flight_delays fd ON F.flight_id = fd.flight_id
        JOIN WeatherData W
        WHERE fd.delay_minutes IS NOT NULL
          AND W.description IS NOT NULL
        LIMIT 10
    """
    
    cur.execute(flight_weather_sql)
    sample_rows = cur.fetchall()
    
    if sample_rows:
        print(f"Sample of joined data (first {len(sample_rows)} rows):")
        for row in sample_rows:
            print(f"  Delay: {row[0]}, Weather: {row[1]}, Flight date: {row[2]}, Weather date: {row[3]}")
    else:
        print("ERROR: No data returned from join!")
        print("\nThis means your Flights and WeatherData tables have no overlapping data.")
        print("You may need to collect weather data for the same dates as your flights.")
        return None
    
    # Now do the actual calculation with all data
    flight_weather_sql = """
        SELECT fd.delay_minutes, W.description
        FROM Flights F
        JOIN flight_delays fd ON F.flight_id = fd.flight_id
        CROSS JOIN WeatherData W
        WHERE fd.delay_minutes IS NOT NULL
          AND W.description IS NOT NULL
    """
    
    cur.execute(flight_weather_sql)
    rows = cur.fetchall()
    
    print(f"\nTotal flight-weather combinations: {len(rows)}")
    
    # Check if any precip word in weather descriptions
    delays = []
    for delay, desc in rows:
        desc_lower = desc.lower()
        if any(term in desc_lower for term in precip):
            delays.append(delay)

    if len(delays) == 0:
        print("No precipitation-related flights found.")
        return None
    else:
        avg_delay = sum(delays) / len(delays)
        print(f"\nRESULTS:")
        print(f"  Total flights during precipitation: {len(delays)}")
        print(f"  Average departure delay during precipitation: {avg_delay:.2f} minutes")
        return avg_delay
    

if __name__ == "__main__":
    import sqlite3
    
    conn = sqlite3.connect("project_data.db")
    calc_avg_delay_precip(conn)
    conn.close()