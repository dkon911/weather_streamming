from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lag, hour, dayofweek, month, to_date, to_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("load_to_DW")
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.jars", r"C:\Spark\spark-3.4.3-bin-hadoop3\jars\myJar\postgresql-42.7.4.jar")
    .getOrCreate())

yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

df = (spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(table="historical_weather", keyspace="spark_streams")
    .load()
    .filter(col("date") == yesterday)
    )

# Define the JDBC URL and properties
url = "jdbc:postgresql://postgres:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

def assign_season(month):
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'autumn'


assign_season_udf = udf(assign_season, StringType())
# limt the time in yyyy-mm-dd hh:mm format
df = df.withColumn("time", col("time").substr(1, 16))
df = df.withColumn("date", col("date").substr(1, 10))

df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))  # Convert to DateType
df = df.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))

# Extract `hour`, `day_of_week`, and `month` from the timestamp columns
df = df.withColumn("hour", hour(col("time")))
df = df.withColumn("day_of_week", dayofweek(col("date")))
df = df.withColumn("month", month(col("date")))
window_spec = Window.orderBy("date")

# Create lagged columns
df = df.withColumn("temp_c_lag_1", lag("temp_c", 1).over(window_spec))
df = df.withColumn("humidity_lag_1", lag("humidity", 1).over(window_spec))
df = df.withColumn("wind_kph_lag_1", lag("wind_kph", 1).over(window_spec))
df = df.withColumn("precip_in_lag_1", lag("precip_in", 1).over(window_spec))
df = df.withColumn("pressure_mb_lag_1", lag("pressure_mb", 1).over(window_spec))

# Apply the UDF to create the `season` column
df = df.withColumn("season", assign_season_udf(col("month")))

df = df.select( "country","name", "date", "time", "lat", "lon", "is_day",
                "condition_text","cloud", "uv", "hour_uv", "precip_mm",
                "temp_c","feelslike_c", "maxtemp_c", "mintemp_c", "avgtemp_c",
                "humidity", "maxwind_kph","wind_kph", "wind_degree", "day_of_week",
                "temp_c_lag_1", "precip_in_lag_1", "humidity_lag_1", "wind_kph_lag_1",
                "pressure_mb_lag_1", "hour", "month", "season")

df.write.jdbc(url=url, table="historical_weather_", mode="append", properties=properties)