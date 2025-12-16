import requests
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
weatherapi_key = os.getenv("API_KEY")
DB_NAME = "project_data.db"


def get_weather_data(city_name):

    # print("debug api key:", weatherapi_key)

    # detroit
    lat, lon = 42.3314, -83.0458

    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={lat}&lon={lon}&appid={weatherapi_key}"
    )

    try:
        print(f"Fetching real-time weather data for {city_name}")
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()

            if "list" not in data:
                print("Error: API didn't include 'list'. Full response:")
                print(data)
                return []

            weather_list = []
            fetch_timestamp = datetime.now().isoformat()

            for entry in data["list"][:25]:
                main = entry["main"]
                weather = entry["weather"][0]
                wind = entry["wind"]

                weather_list.append({
                    "fetch_timestamp": fetch_timestamp,
                    "datetime": entry["dt"], # Unix timestamp
                    "temp": main["temp"],
                    "humidity": main["humidity"],
                    "wind_speed": wind["speed"],
                    "description": weather["description"]
                })

            print(f"Collected {len(weather_list)} forecast rows")
            return weather_list
    
        else:
            print(f"Error {response.status_code}: {response.text}")
            return []

    except Exception as e:
        print(f"Error: {e}")
        return []

# db for integer keys ****************************************************
def init_database(conn):
    cur = conn.cursor()
    
    # weather descriptions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherDescriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT UNIQUE NOT NULL
        )
    """)
    
    # fetch timestamps
    cur.execute("""
        CREATE TABLE IF NOT EXISTS FetchTimestamps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT UNIQUE NOT NULL
        )
    """)

    # main table w foreign keys
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_timestamp_id INTEGER NOT NULL,
            datetime INTEGER NOT NULL,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description_id INTEGER NOT NULL,
            FOREIGN KEY (fetch_timestamp_id) REFERENCES FetchTimestamps(id),
            FOREIGN KEY (description_id) REFERENCES WeatherDescriptions(id),
            UNIQUE(fetch_timestamp_id, datetime)
        )
    """)
    

def get_or_create_description_id(cur, description):
    # get existing description_id or create new
    cur.execute("SELECT id FROM WeatherDescriptions WHERE description = ?", (description,))
    row = cur.fetchone()
    
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO WeatherDescriptions (description) VALUES (?)", (description,))
        return cur.lastrowid


def get_or_create_timestamp_id(cur, timestamp):
    # get existing timestamp_id or create new
    cur.execute("SELECT id FROM FetchTimestamps WHERE timestamp = ?", (timestamp,))
    row = cur.fetchone()
    
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO FetchTimestamps (timestamp) VALUES (?)", (timestamp,))
        return cur.lastrowid
    

def store_weather_data(conn, weather_list):
    cur = conn.cursor()
    init_database(conn)

    inserted = 0 
    skipped = 0

    for w in weather_list:
        try:
            # get or create foreign key ids
            description_id = get_or_create_description_id(cur, w["description"])
            timestamp_id = get_or_create_timestamp_id(cur, w["fetch_timestamp"])
            
            # insert weather data w foreign keys
            cur.execute("""
                INSERT INTO WeatherData 
                (fetch_timestamp_id, datetime, temp, humidity, wind_speed, description_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (timestamp_id, w["datetime"], w["temp"], w["humidity"], 
                  w["wind_speed"], description_id))
            inserted += 1

        except sqlite3.IntegrityError:
            skipped += 1
            
    conn.commit()
    print(f"Weather data successfully stored")
    print(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")



if __name__ == "__main__":
    import time
    conn = sqlite3.connect(DB_NAME)

    num_fetches = 4
    delay_hours = 3

    for i in range(num_fetches):
        # print(f"Fetch {i+1}/{num_fetches}")
        weather_data = get_weather_data("Detroit")

        if weather_data:
            store_weather_data(conn, weather_data)
        
        if i < num_fetches - 1:
            print(f"Wait {delay_hours} hours before next run to fetch new data")
            time.sleep(delay_hours * 3600)  # convert hrs to s
    
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM WeatherData")
    total = cur.fetchone()[0]

    print(f"Total weather records in database: {total}")
        
    if total < 100:
        print(f"Need {100 - total} more to reach 100. Run script again")
    else:
        print(f"100 reached")
    
    conn.close()