# import findspark
# findspark.init()

from dotenv import load_dotenv
import os
import aiohttp
import asyncio
from datetime import datetime, timedelta

import uuid
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

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
            # "condition_text": forecast["day"]["condition"]["text"],
            # "condition_icon": forecast["day"]["condition"]["icon"],
            # "condition_code": forecast["day"]["condition"]["code"],
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


async def fetch_hist_data_async(start_date: str, end_date: str):
    # API_key = os.getenv('API_key')
    API_key = '97f79db0d866429c807170012240906'
    # start_date = '2024-07-09'
    # end_date = '2024-07-11'
    cities = [
        {"name": "Hanoi", "lat": "21.0285", "lon": "105.8542"},
        {"name": "Da Nang", "lat": "16.0471", "lon": "108.2068"},
        {"name": "Ho Chi Minh City", "lat": "10.8231", "lon": "106.6297"},
    ]
    dates = generate_date_range(start_date, end_date)
    all_data = []

    semaphore = asyncio.Semaphore(10)  # Limit the number of concurrent requests

    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in cities:
            for date in dates:
                url = f'https://api.weatherapi.com/v1/history.json?key={API_key}&q={city["lat"]},{city["lon"]}&dt={date}'
                tasks.append(fetch_data(session, url, city["name"], date, semaphore))
        all_data = await asyncio.gather(*tasks)
        logger.info(f"Data fetched for dates: {dates}")
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


def historical_weather(start_date, end_date):
    from kafka import KafkaProducer
    import json

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    try:
        historical_data = asyncio.run(fetch_hist_data_async(start_date, end_date))
        for weather_data in historical_data:
            if weather_data is None:
                logger.error(f"Data is empty on {weather_data['forecast']['forecastday']['date']}")
                continue
            format_data = data_filter(weather_data)
            # print(json.dumps(format_data))
            for data in range(len(format_data)):
                producer.send('weather', json.dumps(format_data[data]).encode('utf-8'))
            logger.info(f"-> Data for {weather_data['location']['name']} written to Kafka successfully")

    except Exception as e:
        print(f'WEATHER TOPIC ERROR: {e}')
        logging.error(f'An error occurred: {e}')


if __name__ == "__main__":
    historical_weather('2024-09-20','2024-09-20')
    logger.info("JOB DONE")