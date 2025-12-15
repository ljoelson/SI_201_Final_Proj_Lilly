import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

def plot_avg_delay_by_hour(db_name="project_data.db"):
    """Plot average flight delay by hour of day"""
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # Avg delay
    query = """
        SELECT 
            CAST(SUBSTR(f.scheduled_departure, 12, 2) AS INTEGER) as hour,
            AVG(fd.delay_minutes) as avg_delay,
            COUNT(*) as flight_count
        FROM Flights f
        JOIN FlightDelays fd ON f.flight_id = fd.flight_id
        WHERE f.scheduled_departure IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """
    
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    
    if not results:
        print("No flight delay data found in database")
        return
    
    hours = [row[0] for row in results]
    avg_delays = [row[1] for row in results]
    flight_counts = [row[2] for row in results]
    
    # Chart
    plt.figure(figsize=(14, 6))
    bars = plt.bar(hours, avg_delays, color='steelblue', alpha=0.8)
    plt.xlabel('Hour of Day (24-hour format)', fontsize=12)
    plt.ylabel('Average Delay (minutes)', fontsize=12)
    plt.title('Average Flight Delay by Hour of Day', fontsize=14, fontweight='bold')
    plt.xticks(range(0, 24), fontsize=10)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    # Labels
    for i, (hour, delay, count) in enumerate(zip(hours, avg_delays, flight_counts)):
        plt.text(hour, delay + 0.5, f'{delay:.1f}m\n({count})', 
                ha='center', va='bottom', fontsize=8)
    
    plt.show()
    
    print(f"\nAverage delays by hour of day:")
    for hour, delay, count in zip(hours, avg_delays, flight_counts):
        print(f"  {hour:02d}:00 - {delay:.2f} minutes ({count} flights)")


def plot_avg_precipitation_by_hour(db_name="project_data.db"):
    """Plot percentage of rainy weather by hour of day"""
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    query = """
        SELECT 
            CAST(STRFTIME('%H', datetime(datetime, 'unixepoch')) AS INTEGER) as hour,
            COUNT(*) as total_records,
            SUM(CASE 
                WHEN LOWER(description) LIKE '%rain%' 
                OR LOWER(description) LIKE '%drizzle%'
                OR LOWER(description) LIKE '%shower%'
                OR LOWER(description) LIKE '%thunder%'
                OR LOWER(description) LIKE '%snow%'
                OR LOWER(description) LIKE '%sleet%'
                OR LOWER(description) LIKE '%hail%'
                THEN 1 ELSE 0 
            END) as precip_records
        FROM WeatherData
        WHERE datetime IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """
    
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    
    if not results:
        print("No weather data found in database")
        return
    
    hours = []
    precipitation_pct = []
    record_counts = []
    
    for row in results:
        hour = row[0]
        total = row[1]
        precip = row[2]
        pct = (precip / total * 100) if total > 0 else 0
        
        hours.append(hour)
        precipitation_pct.append(pct)
        record_counts.append(total)
    
    # Chart
    plt.figure(figsize=(14, 6))
    plt.bar(hours, precipitation_pct, color='skyblue', alpha=0.8)
    plt.xlabel('Hour of Day (24-hour format)', fontsize=12)
    plt.ylabel('Precipitation Conditions (%)', fontsize=12)
    plt.title('Percentage of Precipitation (Rain/Snow) by Hour of Day', fontsize=14, fontweight='bold')
    plt.xticks(range(0, 24), fontsize=10)
    plt.grid(axis='y', alpha=0.3)
    plt.ylim(0, 100)
    plt.tight_layout()
    
    for hour, pct, count in zip(hours, precipitation_pct, record_counts):
        plt.text(hour, pct + 2, f'{pct:.1f}%\n({count})', 
                ha='center', va='bottom', fontsize=8)
    
    plt.show()
    
    print(f"\nPrecipitation conditions by hour of day:")
    for hour, pct, count in zip(hours, precipitation_pct, record_counts):
        print(f"  {hour:02d}:00 - {pct:.2f}% ({count} records)")


if __name__ == "__main__":
    print("Generating Flight Delay Chart (by hour)...")
    plot_avg_delay_by_hour()
    
    print("\nGenerating Weather Precipitation Chart (by hour)...")
    plot_avg_precipitation_by_hour()