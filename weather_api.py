import requests
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
weatherapi_key = os.getenv("API_KEY")
DB_NAME = "project_data.db"

# access data across diff days
def get_next_fetch_date(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT fetch_date FROM WeatherData 
            ORDER BY fetch_date DESC LIMIT 1
        """)
        result = cur.fetchone()
    except sqlite3.OperationalError:
        return "2024-12-01"
        
    if result is None: # table doesn't exist yet; return starting date
        return "2024-12-01"
    
    # parses last date + 1d
    last_date = datetime.strptime(result[0], "%Y-%m-%d")
    next_date = last_date + timedelta(days=1)
    return next_date.strftime("%Y-%m-%d")


def get_weather_data(city_name, conn):

    # print("debug api key:", weatherapi_key)

    # detroit
    lat, lon = 42.3314, -83.0458

    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={lat}&lon={lon}&appid={weatherapi_key}"
    )

    response = requests.get(url)
    data = response.json()

    if "list" not in data:
        print("Error: API did not include 'list'. Full response:")
        print(data)
        return []

    weather_list = []

    fetch_date = get_next_fetch_date(conn)

    for entry in data["list"][:25]:
        main = entry["main"]
        weather = entry["weather"][0]
        wind = entry["wind"]

        weather_list.append({
            "fetch_date": fetch_date,
            "datetime": entry["dt"], # Unix timestamp
            "temp": main["temp"],
            "humidity": main["humidity"],
            "wind_speed": wind["speed"],
            "description": weather["description"]
        })

    print(f"Collected {len(weather_list)} forecast rows (fetched on {fetch_date}).")
    return weather_list

def store_weather_data(conn, weather_list):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_date TEXT,
            datetime INTEGER,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT,
            UNIQUE(fetch_date, datetime)
        )
    """)

    inserted = 0 
    skipped = 0

    for w in weather_list:
        try:
            cur.execute("""INSERT OR IGNORE INTO WeatherData (fetch_date, datetime, temp, humidity, wind_speed, description)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (w["fetch_date"], w["datetime"], w["temp"], w["humidity"], w["wind_speed"], w["description"]))
            
            # # count num of skipped/inserted
            # if cur.rowcount > 0:
            #     inserted += 1
            # else:
            #     skipped += 1

        except Exception as e:
            print("Insert error:", e)

    conn.commit()
    print(f"Weather data successfully stored")
    # print(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")



if __name__ == "__main__":
    conn = sqlite3.connect(DB_NAME)
    weather_data = get_weather_data("Detroit", conn)
    store_weather_data(conn, weather_data)
    conn.close()