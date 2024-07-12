# import findspark
# findspark.init()

import aiohttp
import asyncio
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
import uuid
from cassandra.cluster import Cluster
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def data_filter(weather_data):
    location_data = {
        "name": weather_data["location"]["name"],
        "region": weather_data["location"]["region"],
        "country": weather_data["location"]["country"],
        "lat": weather_data["location"]["lat"],
        "lon": weather_data["location"]["lon"],
        "tz_id": weather_data["location"]["tz_id"],
        "localtime": weather_data["location"]["localtime"],
        "localtime_epoch": weather_data["location"]["localtime_epoch"]
    }
    forecast_data = []
    for forecast in weather_data["forecast"]["forecastday"]:
        day_data = {
            "date": forecast["date"],
            "date_epoch": forecast["date_epoch"],
            "maxtemp_c": float(forecast["day"]["maxtemp_c"]),
            "mintemp_c": float(forecast["day"]["mintemp_c"]),
            "avgtemp_c": float(forecast["day"]["avgtemp_c"]),
            "maxwind_kph": float(forecast["day"]["maxwind_kph"]),
            "totalprecip_mm": float(forecast["day"]["totalprecip_mm"]),
            "totalsnow_cm": float(forecast["day"]["totalsnow_cm"]),
            "avghumidity": float(forecast["day"]["avghumidity"]),
            "daily_will_it_rain": forecast["day"]["daily_will_it_rain"],
            "daily_chance_of_rain": forecast["day"]["daily_chance_of_rain"],
            "daily_will_it_snow": forecast["day"]["daily_will_it_snow"],
            "daily_chance_of_snow": forecast["day"]["daily_chance_of_snow"],
            "condition_text": forecast["day"]["condition"]["text"],
            "condition_icon": forecast["day"]["condition"]["icon"],
            "condition_code": forecast["day"]["condition"]["code"],
            "uv": float(forecast["day"]["uv"])
        }
        for hour in forecast["hour"]:
            hour_data = {
                "id": str(uuid.uuid4()),  # Generate a unique UUID for each record
                "time": hour["time"],
                "temp_c": float(hour["temp_c"]),
                "is_day": hour["is_day"],
                "condition_text": hour["condition"]["text"],
                "condition_icon": hour["condition"]["icon"],
                "condition_code": hour["condition"]["code"],
                "wind_kph": float(hour["wind_kph"]),
                "wind_degree": hour["wind_degree"],
                "pressure_mb": float(hour["pressure_mb"]),
                "pressure_in": float(hour["pressure_in"]),
                "precip_mm": float(hour["precip_mm"]),
                "precip_in": float(hour["precip_in"]),
                "snow_cm": float(hour["snow_cm"]),
                "humidity": hour["humidity"],
                "cloud": hour["cloud"],
                "feelslike_c": float(hour["feelslike_c"]),
                "hour_uv": float(hour["uv"])  # Avoid conflict with the daily "uv" field
            }
            forecast_data.append({**location_data, **day_data, **hour_data})
    return forecast_data

def generate_date_range(start_date, end_date):
    dates = []
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    while start_date <= end_date:
        dates.append(start_date.strftime("%Y-%m-%d"))
        start_date += delta
    return dates

def generate_date_range_even_years(start_year, end_year):
    dates = []
    for year in range(start_year, end_year + 1, 2):  # Step by 2 to get even years
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        delta = timedelta(days=1)
        while start_date <= end_date:
            dates.append(start_date.strftime("%Y-%m-%d"))
            start_date += delta
    return dates

# Fetch historical data asynchronously
async def fetch_hist_data_async():
    API_key = '97f79db0d866429c807170012240906'
    # start_date = '2024-07-06'
    start_date = '2024-07-09'
    end_date = '2024-07-09'

    # start_year = 2010
    # end_year = 2010
    cities = [
        {"name": "Hanoi", "lat": "21.0285", "lon": "105.8542"},
        # {"name": "Da Nang", "lat": "16.0471", "lon": "108.2068"},
        # {"name": "Ho Chi Minh City", "lat": "10.8231", "lon": "106.6297"},
    ]
    dates = generate_date_range(start_date, end_date)
    # dates = generate_date_range_even_years(start_year, end_year)
    all_data = []

    semaphore = asyncio.Semaphore(10)  # Limit the number of concurrent requests

    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in cities:
            for date in dates:
                url = f'https://api.weatherapi.com/v1/history.json?key={API_key}&q={city["lat"]},{city["lon"]}&dt={date}'
                tasks.append(fetch_data(session, url, city["name"], date, semaphore))
        all_data = await asyncio.gather(*tasks)
    return all_data

async def fetch_data(session, url, city_name, date, semaphore):
    async with semaphore:
        async with session.get(url) as res:
            if res.status == 200:
                data = await res.json()
                logger.info(f"Retrieved data for {city_name} on {date}")
                return data
            else:
                logger.error(f"Failed to retrieve data for {city_name} on {date}: {res.status} - {res.text}")
                return None

def load_to_cast(df):
    spark = SparkSession.builder \
        .appName("WeatherDataToCassandra") \
        .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
        .getOrCreate()

    data = data_filter(df)
    # Convert the forecast data to a Spark DataFrame
    df = spark.createDataFrame(data)
    # Write DataFrame to Cassandra
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="checkup", keyspace="weather") \
        .save()
    return logger.info("Importing data to Cassandra")

# Create keyspace and table in Cassandra
def create_keyspace_and_table():
    cluster = Cluster(['localhost'])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS weather.checkup (
            id UUID PRIMARY KEY,
            name TEXT,
            region TEXT,
            country TEXT,
            lat DOUBLE,
            lon DOUBLE,
            tz_id  TEXT,
            localtime TEXT,
            localtime_epoch BIGINT,
            date TEXT,
            date_epoch BIGINT,
            maxtemp_c DOUBLE,
            mintemp_c DOUBLE,
            avgtemp_c DOUBLE,
            maxwind_kph DOUBLE,
            totalprecip_mm DOUBLE,
            totalsnow_cm DOUBLE,
            avghumidity DOUBLE,
            daily_will_it_rain INT,
            daily_chance_of_rain INT,
            daily_will_it_snow INT,
            daily_chance_of_snow INT,
            condition_text TEXT,
            condition_icon TEXT,
            condition_code INT,
            uv DOUBLE,
            time TEXT,
            temp_c DOUBLE,
            is_day INT,
            wind_kph DOUBLE,
            wind_degree INT,
            pressure_mb DOUBLE,
            pressure_in DOUBLE,
            precip_mm DOUBLE,
            precip_in DOUBLE,
            snow_cm DOUBLE,
            humidity INT,
            cloud INT,
            feelslike_c DOUBLE,
            hour_uv DOUBLE
        )
    """)
    logger.info("Keyspace and table created successfully")


def live_weather():
    from kafka import KafkaProducer
    import json
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    try:
        res = asyncio.run(fetch_hist_data_async())
        response_data = res[0]
        # response_data = data_filter(response_data)
        # print(json.dumps(response_data, indent=4))
        producer.send('weather', json.dumps(response_data).encode('utf-8'))

    except Exception as e:
        print(f'WEATHER TOPIC ERROR: {e}')
        logging.error(f'An error occurred: {e}')



def main():
    create_keyspace_and_table()
    historical_data = asyncio.run(fetch_hist_data_async())
    for weather_data in historical_data:
        if weather_data is None:
            logger.error(f"Data is empty on {weather_data['forecast']['forecastday']['date']}")
            continue
        load_to_cast(weather_data)
        logger.info(f"-> Data for {weather_data['location']['name']} written to Cassandra successfully")


if __name__ == "__main__":
    # main()
    live_weather()
    logger.info("JOB DONE")