import sqlite3

conn = sqlite3.connect("final_project.db")
cur = conn.cursor()

# Count rows
cur.execute("SELECT COUNT(*) FROM WeatherData")
count = cur.fetchone()[0]
print("Total rows in WeatherData:", count)

# Optionally show a few rows
cur.execute("SELECT datetime, temp, humidity, wind_speed, description FROM WeatherData LIMIT 5")
rows = cur.fetchall()
for r in rows:
    print(r)

conn.close()