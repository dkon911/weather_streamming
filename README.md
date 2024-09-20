# Realtime Data Streaming

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
- [Data Overview](#data-overview)

## Introduction

This project is about building an near real-time data pipeline. This project is focused on developing an application that can perform real-time analysis of the weather conditions
## System Architecture

![System Architecture](images/system_architecture.jpg)

The project is designed with the following components:

- **Data Source**: [Weather API](https://www.weatherapi.com/)
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **PostgreSQL**: Store metadata of Airflow, and use as a data warehouse.
- **Apache Kafka**: Used for streaming data from Cassandra to the processing engine.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**:  Where the processed data will be stored
- **Docker**: Used to containerize the services.
- **Grafana**: For visualization of the data.


## Technologies
- Python **X** Apache Spark
- Apache Airflow
- Apache Kafka
- Apache Zookeeper
- Cassandra
- Postgresql
- Docker


## Getting Started

### Start pipeline and dashboard

1. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```

2. Access airflow webserver UI (http://localhost:8080/) to start the job


3. Run spark-job
  - `<spark-master container id>`: get it in docker
  - `<spark master IP address>`: get it on the spark UI

    ```bash
      docker exec -it <spark-master container id>\
      spark-submit --master spark://<spark master IP address>:7077 \
      --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
      org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark_stream.py
    ```

    or if already have spark in machine use:

    ```bash
    python spark_stream.py
    ```

  ## Data Overview

### Raw data from the Weather API looks like this:

"location": {
        "name": "Hanoi",
        "region": "",
        "country": "Vietnam",
        "lat": 21.03,
        "lon": 105.85,
        "tz_id": "Asia/Bangkok",
        "localtime_epoch": 1726065512,
        "localtime": "2024-09-11 21:38"
    },
    "forecast": {
        "forecastday": [
            {
                "date": "2024-09-11",
                "date_epoch": 1726012800,
                "day": {
                    "maxtemp_c": 25.2,
                    "mintemp_c": 23.6,
                    "avgtemp_c": 24.3,
                    "maxwind_kph": 19.4,
                    "totalprecip_mm": 37.97,
                    "totalsnow_cm": 0.0,
                    "avghumidity": 95,
                    "daily_will_it_rain": 1,
                    "daily_chance_of_rain": 100,
                    "daily_will_it_snow": 0,
                    "daily_chance_of_snow": 0,
                    "condition": {
                        "text": "Light rain shower",
                        "icon": "//cdn.weatherapi.com/weather/64x64/day/353.png",
                        "code": 1240
                    },
                    "uv": 6.0
                },
                "hour": [
                    {
                        "time_epoch": 1725987600,
                        "time": "2024-09-11 00:00",
                        "temp_c": 24.1,
                        "is_day": 0,
                        "condition": {
                            "text": "Light rain shower",
                            "icon": "//cdn.weatherapi.com/weather/64x64/night/353.png",
                            "code": 1240
                        },
                        "wind_kph": 7.9,
                        "wind_degree": 3,
                        "pressure_mb": 1007.0,
                        "pressure_in": 29.73,
                        "precip_mm": 1.14,
                        "precip_in": 0.04,
                        "snow_cm": 0.0,
                        "humidity": 97,
                        "cloud": 100,
                        "feelslike_c": 26.9,
                        "windchill_c": 24.1,
                        "will_it_rain": 1,
                        "chance_of_rain": 100,
                        "vis_km": 10.0,
                        "uv": 0.0
                    },

### And here is the data schema after processing:

- Location data will contain the following fields:
    - name: name of the city
    - region: region of the city
    - country: country of the city
    - lat: latitute of the city
    - lon: longitude of the city
    - tz_id: timezone of the city
    - localtime: local time of the city
    - localtime_epoch: local time epoch of the city

- Day data will contain the following fields:
    - date: last updated date
    - date_epoch: last updated date in epoch format
    - maxtemp_c: Maximum temperature in celsius
    - mintemp_c: Minimum temperature in celsius
    - avgtemp_c: Average temperature in celsius
    - maxwind_kph: Maximum wind speed in kph
    - totalprecip_mm: Total precipitation in mm
    - totalsnow_cm: Total snow in cm
    - avghumidity: Average humidity
    - daily_will_it_rain:
    - daily_chance_of_rain:
    - daily_will_it_snow:
    - daily_chance_of_snow:
    - condition_text:
    - condition_icon:
    - condition_code:
    - uv: UV index of the day


- Hour data will contain the following fields:
    - id: id of the record in UUID format
    - time: time in format yyyy-mm-dd hh:mm
    - temp_c: Temperature in celsius at that hour
    - is_day:
    - condition_text: The word description of the weather condition
    - condition_icon: The icon code of the weather condition
    - condition_code: The code of the weather condition
    - wind_kph: Wind speed in kph
    - wind_degree: Wind degree
    - pressure_mb: Pressure in millibars
    - pressure_in:
    - precip_mm:
    - precip_in:
    - snow_cm:
    - humidity:
    - cloud: Cloud cover percentage
    - feelslike_c:
    - hour_uv: UV index per hour

REALTIME.json
{
    "location": {
        "name": "Atba Village",
        "region": "",
        "country": "Vietnam",
        "lat": 16.05,
        "lon": 108.2,
        "tz_id": "Asia/Ho_Chi_Minh",
        "localtime_epoch": 1726673963,
        "localtime": "2024-09-18 22:39"
    },
    "current": {
        "last_updated_epoch": 1726673400,
        "last_updated": "2024-09-18 22:30",
        "temp_c": 25.2,
        "is_day": 0,
        "condition": {
            "text": "Partly cloudy",
            "icon": "//cdn.weatherapi.com/weather/64x64/night/116.png",
            "code": 1003
        },
        "wind_kph": 14.4,
        "wind_degree": 280,
        "pressure_mb": 1001.0,
        "precip_in": 0.06,
        "humidity": 94,
        "cloud": 75,
        "feelslike_c": 28.5,
        "windchill_c": 23.3,
        "heatindex_c": 25.7,
        "dewpoint_c": 22.4,
        "vis_km": 10.0,
        "uv": 1.0,
        "gust_kph": 22.7
    }
}
