from cassandra.cluster import Cluster
from datetime import datetime, timedelta

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('spark_streams')

def get_yesterdays_weather_data(city, country):
    # Calculate the timestamp range for yesterday
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_of_yesterday = yesterday.replace(hour=0, minute=0)
    end_of_yesterday = yesterday.replace(hour=23, minute=00)

    # Convert datetime to string in the format expected by Cassandra
    start_of_yesterday_str = start_of_yesterday.strftime('%Y-%m-%d %H:%M')
    end_of_yesterday_str = end_of_yesterday.strftime('%Y-%m-%d %H:%M')

    # Query Cassandra for yesterday's weather data
    query = """
    SELECT * FROM historical_weather
    WHERE name = %s AND country = %s AND time >= %s AND time <= %s
    ALLOW FILTERING;
    """
    rows = session.execute(query, (city, country, start_of_yesterday_str, end_of_yesterday_str))
    print("Cassandra Query:", query)
    print("Parameters:", city, country, start_of_yesterday_str, end_of_yesterday_str)

    weather_data = []
    for row in rows:
        weather_data.append({
            'maxtemp_c': row.maxtemp_c,
            'mintemp_c': row.mintemp_c,
            'avgtemp_c': row.avgtemp_c,
            'humidity': row.humidity,
            'wind_kph': row.wind_kph,
            'lat': row.lat,
            'lon': row.lon,
            'day_of_week': 6,
            'temp_c_lag_1': 29,
            'precip_in_lag_1': 0,
            'humidity_lag_1': 56,
            'wind_kph_lag_1': 10.1,
            'season': "autumn",
            'pressure_mb_lag_1': 1012,
            'hour': 12
        })
    return weather_data

def calculate_time_diff(last_updated):
    last_updated = datetime.strptime(last_updated, "%Y-%m-%d %H:%M")
    current_time = datetime.now()
    time_diff = current_time - last_updated
    return round(time_diff.total_seconds() / 60)
