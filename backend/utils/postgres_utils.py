from datetime import datetime, timedelta
import psycopg2

conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="127.0.0.1",
    port="5432"
)
cursor = conn.cursor()


def get_yesterdays_weather_data(city, country):
    # Calculate the timestamp range for yesterday
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_of_yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_yesterday = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Convert datetime to string in the format expected by PostgreSQL
    start_of_yesterday_str = start_of_yesterday.strftime('%Y-%m-%d %H:%M')
    end_of_yesterday_str = end_of_yesterday.strftime('%Y-%m-%d %H:%M')

    query = """
     SELECT * FROM historical_weather_
     WHERE name = %s AND country = %s AND time >= %s AND time <= %s;
     """
    # Execute the query
    cursor.execute(query, (city, country, start_of_yesterday_str, end_of_yesterday_str))

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    print("PostgreSQL Query:", query)
    print("Parameters:", city, country, start_of_yesterday_str, end_of_yesterday_str)

    weather_data = []
    for row in rows:
        weather_data.append({
            'maxtemp_c': row[14],
            'mintemp_c': row[15],
            'avgtemp_c': row[16],
            'humidity': row[17],
            'wind_kph': row[19],
            'lat': row[4],
            'lon': row[5],
            'day_of_week': row[21],
            'temp_c_lag_1': row[22],
            'precip_in_lag_1': row[23],
            'humidity_lag_1': row[24],
            'wind_kph_lag_1': row[25],
            'pressure_mb_lag_1': row[26],
            'hour': row[27],
            'season': row[29],
        })

    # If no data found, return empty
    if not weather_data:
        return None

    return weather_data  # Return raw data instead of jsonify



def calculate_time_diff(last_updated):
    last_updated = datetime.strptime(last_updated, "%Y-%m-%d %H:%M")
    current_time = datetime.now()
    time_diff = current_time - last_updated
    return round(time_diff.total_seconds() / 60)


