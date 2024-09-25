import findspark
findspark.init()

from dotenv import load_dotenv
import os
import aiohttp
import asyncio

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

    current_weather_data = {
        "last_updated": weather_data["current"]["last_updated"],
        "temp_c": float(weather_data["current"]["temp_c"]),
        "is_day": weather_data["current"]["is_day"],
        "condition_text": weather_data["current"]["condition"]["text"],
        "condition_icon": weather_data["current"]["condition"]["icon"],
        "condition_code": weather_data["current"]["condition"]["code"],
        "wind_kph": float(weather_data["current"]["wind_kph"]),
        "wind_degree": weather_data["current"]["wind_degree"],
        "pressure_mb": float(weather_data["current"]["pressure_mb"]),
        "precip_in": float(weather_data["current"]["precip_in"]),
        "humidity": weather_data["current"]["humidity"],
        "cloud": weather_data["current"]["cloud"],
        "feelslike_c": float(weather_data["current"]["feelslike_c"]),
        "windchill_c": float(weather_data["current"]["windchill_c"]),
        "heatindex_c": float(weather_data["current"]["heatindex_c"]),
        "dewpoint_c": float(weather_data["current"]["dewpoint_c"]),
        "vis_km": float(weather_data["current"]["vis_km"]),
        "uv": float(weather_data["current"]["uv"]),
        "gust_kph": float(weather_data["current"]["gust_kph"])
    }

    complete_weather_data = {**location_data, **current_weather_data}
    return complete_weather_data


async def fetch_hist_data_async():
    API_key = os.getenv('API_key')
    cities = [
        {"name": "Hanoi", "lat": "21.0285", "lon": "105.8542"},
        {"name": "Da Nang", "lat": "16.0471", "lon": "108.2068"},
        {"name": "Ho Chi Minh City", "lat": "10.8231", "lon": "106.6297"},
    ]
    all_data = []
    semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in cities:
            url = f'https://api.weatherapi.com/v1/current.json?key={API_key}&q={city["lat"]},{city["lon"]}'
            tasks.append(fetch_data(session, url, city["name"], semaphore))
        all_data = await asyncio.gather(*tasks)
        logger.info(f"Real-time weather data fetched")
    return all_data


async def fetch_data(session, url, city_name, semaphore):
    async with semaphore:
        async with session.get(url) as res:
            if res.status == 200:
                data = await res.json()
                logger.info(f"Retrieved data for {city_name}")
                return data
            else:
                logger.error(f"Failed to retrieve data for {city_name}: {res.status} - {res.text}")
                return None


def realtime_weather():
    from kafka import KafkaProducer
    import json

    # producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    try:
        weather_data = asyncio.run(fetch_hist_data_async())
        for data in weather_data:
            if data is None:
                continue
            formatted_data = data_filter(data)
            print(json.dumps(formatted_data, indent=2))
            # producer.send("current_weather", json.dumps(formatted_data).encode("utf-8"))
            logger.info(f"Weather data for {formatted_data['name']} written to Kafka")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

realtime_weather()