import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

def plot_avg_delay_by_month(db_name="project_data.db"):

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # Average delay by month
    query = """
        SELECT 
            SUBSTR(f.scheduled_departure, 1, 7) as month,
            AVG(fd.delay_minutes) as avg_delay
        FROM Flights f
        JOIN FlightDelays fd ON f.flight_id = fd.flight_id
        WHERE f.scheduled_departure IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    
    if not results:
        print("No flight delay data found in database")
        return
    
    # Separate months and delays
    months = [row[0] for row in results]
    avg_delays = [row[1] for row in results]
    
    # Create bar chart
    plt.figure(figsize=(12, 6))
    plt.bar(months, avg_delays, color='steelblue', alpha=0.8)
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Average Delay (minutes)', fontsize=12)
    plt.title('Average Flight Delay by Month', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    # Adding labels
    for i, (month, delay) in enumerate(zip(months, avg_delays)):
        plt.text(i, delay + 0.5, f'{delay:.1f}', ha='center', va='bottom', fontsize=9)
    
    plt.show()
    
    print(f"\nAverage delays by month:")
    for month, delay in zip(months, avg_delays):
        print(f"  {month}: {delay:.2f} minutes")


def plot_avg_precipitation_by_month(db_name="project_data.db"):

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    query = """
        SELECT 
            STRFTIME('%Y-%m', datetime(datetime, 'unixepoch')) as month,
            COUNT(*) as total_records,
            SUM(CASE 
                WHEN LOWER(description) LIKE '%rain%' 
                OR LOWER(description) LIKE '%drizzle%'
                OR LOWER(description) LIKE '%shower%'
                OR LOWER(description) LIKE '%thunder%'
                THEN 1 ELSE 0 
            END) as rainy_records
        FROM WeatherData
        WHERE datetime IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    
    if not results:
        print("No weather data found in database")
        return
    
    # Calculate percentages
    months = []
    precipitation_pct = []
    
    for row in results:
        month = row[0]
        total = row[1]
        rainy = row[2]
        pct = (rainy / total * 100) if total > 0 else 0
        
        months.append(month)
        precipitation_pct.append(pct)
    
    # Create bar chart
    plt.figure(figsize=(12, 6))
    plt.bar(months, precipitation_pct, color='skyblue', alpha=0.8)
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Rainy Conditions (%)', fontsize=12)
    plt.title('Percentage of Rainy Weather Conditions by Month', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    plt.ylim(0, 100)
    plt.tight_layout()
    
    for i, (month, pct) in enumerate(zip(months, precipitation_pct)):
        plt.text(i, pct + 1, f'{pct:.1f}%', ha='center', va='bottom', fontsize=9)
    
    plt.show()
    
    print(f"\nRainy conditions by month:")
    for month, pct in zip(months, precipitation_pct):
        print(f"  {month}: {pct:.2f}% of records")


if __name__ == "__main__":
    print("Generating Flight Delay Chart...")
    plot_avg_delay_by_month()
    
    print("\nGenerating Weather Precipitation Chart...")
    plot_avg_precipitation_by_month()