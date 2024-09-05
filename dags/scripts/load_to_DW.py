from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Cassandra to PostgreSQL") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

cassandra_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="weather", keyspace="spark_streams") \
    .load()

# Extract unique locations
location_df = cassandra_df.select("name", "region", "country", "lat", "lon", "tz_id").distinct()

# Extract unique dates
date_df = cassandra_df.select("date", "date_epoch").distinct()

# Extract unique weather conditions
condition_df = cassandra_df.select("condition_text", "condition_icon", "condition_code").distinct()

# Prepare weather data with references
weather_data_df = cassandra_df \
    .join(location_df, ["name", "region", "country", "lat", "lon", "tz_id"]) \
    .join(date_df, ["date", "date_epoch"]) \
    .join(condition_df, ["condition_text", "condition_icon", "condition_code"]) \
    .select(
        col("name").alias("location_id"),
        col("date").alias("date_id"),
        col("condition_text").alias("condition_id"),
        "maxtemp_c", "mintemp_c", "avgtemp_c", "maxwind_kph", "totalprecip_mm",
        "totalsnow_cm", "avghumidity", "daily_will_it_rain", "daily_chance_of_rain",
        "daily_will_it_snow", "daily_chance_of_snow", "uv", "time", "temp_c",
        "is_day", "wind_kph", "wind_degree", "pressure_mb", "pressure_in",
        "precip_mm", "precip_in", "snow_cm", "humidity", "cloud", "feelslike_c", "hour_uv"
    )

# PostgreSQL connection properties
url = "jdbc:postgresql://localhost:5432/your_database"
properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Write DataFrames to PostgreSQL
location_df.write.jdbc(url=url, table="Location", mode="append", properties=properties)
date_df.write.jdbc(url=url, table="Date", mode="append", properties=properties)
condition_df.write.jdbc(url=url, table="WeatherCondition", mode="append", properties=properties)
weather_data_df.write.jdbc(url=url, table="WeatherData", mode="append", properties=properties)