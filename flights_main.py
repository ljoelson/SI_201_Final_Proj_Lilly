import os
import sqlite3
from flights_api import get_flight_data, store_flight_data

DB_NAME = "project_data.db"

def main():
    print("Using database file:", os.path.abspath(DB_NAME))
    conn = sqlite3.connect(DB_NAME)

    airport_code = "DTW"
    
    flights = get_flight_data(airport_code, month=None)
    store_flight_data(conn, flights)

    conn.close()
    print("\nAll done!")

if __name__ == "__main__":
    main()