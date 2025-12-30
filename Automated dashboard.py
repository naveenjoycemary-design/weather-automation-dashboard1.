import requests
import mysql.connector
from datetime import datetime, timedelta
import pytz

# =========================
# CONFIGURATION
# =========================

API_KEY = "efd6b4dcc0f1b762d34a167b399098a5"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

DB_CONFIG = {
    "host": "mysql.railway.internal",
    "port": 3306,
    "user": "root",
    "password": "ykAAFwsZzFztPQQSuHcczaLucwqifwqI",
    "database": "weather_dashboard"
}




CITIES = [
    "New Delhi,IN",
    "Chennai,IN",
    "Mumbai,IN",
    "Bengaluru,IN",
    "Hyderabad,IN",
    "Kolkata,IN",
    "Jaipur,IN",
    "Thiruvananthapuram,IN"
]

IST = pytz.timezone("Asia/Kolkata")

# =========================
# FETCH WEATHER DATA
# =========================

def fetch_weather(city):
    params = {
        "q": city,
        "appid": API_KEY
    }
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# =========================
# MAIN INGESTION LOGIC
# =========================

def run_ingestion():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    now_ist = datetime.now(IST)
    recorded_at = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    for city in CITIES:
        try:
            data = fetch_weather(city)

            insert_query = """
                INSERT INTO weather_data (
                    city, country, temperature_c, feels_like_c,
                    humidity_percent, pressure_hpa, wind_speed_mps,
                    weather_condition, weather_description, recorded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = (
                data["name"],
                data["sys"]["country"],
                round(data["main"]["temp"] - 273.15, 2),
                round(data["main"]["feels_like"] - 273.15, 2),
                data["main"]["humidity"],
                data["main"]["pressure"],
                data["wind"]["speed"],
                data["weather"][0]["main"],
                data["weather"][0]["description"],
                recorded_at
            )

            cursor.execute(insert_query, values)
            conn.commit()

            print(f"Inserted data for {data['name']} at {recorded_at}")

        except Exception as e:
            print(f"Error fetching/inserting data for {city}: {e}")

    # =========================
    # 7-DAY RETENTION CLEANUP
    # =========================

    cleanup_query = """
        DELETE FROM weather_data
        WHERE recorded_at < NOW() - INTERVAL 7 DAY
    """
    cursor.execute(cleanup_query)
    conn.commit()

    cursor.close()
    conn.close()

    print("Old records cleanup completed.")

# =========================
# SCRIPT ENTRY POINT
# =========================

if __name__ == "__main__":
    run_ingestion()
