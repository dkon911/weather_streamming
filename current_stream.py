import logging
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
)


def create_spark_connection():
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.appName("to DW")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",  # Add Kafka connector
            )
            .config(
                "spark.jars",
                r"C:\Spark\spark-3.4.3-bin-hadoop3\jars\myJar\postgresql-42.7.4.jar",
            )
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "current_weather")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Kafka dataframe created successfully")
        print("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
    return spark_df


def create_current_weather_df_from_kafka(spark_df):
    # Define the schema for current weather data matching the data_filter function
    current_weather_schema = StructType(
        [
            # Location data
            StructField("name", StringType()),
            StructField("region", StringType()),
            StructField("country", StringType()),
            StructField("lat", FloatType()),
            StructField("lon", FloatType()),
            StructField("tz_id", StringType()),
            StructField("localtime", StringType()),
            StructField("localtime_epoch", LongType()),
            # Current weather data
            StructField("last_updated", StringType()),
            StructField("temp_c", FloatType()),
            StructField("is_day", IntegerType()),
            StructField("condition_text", StringType()),
            StructField("condition_icon", StringType()),
            StructField("condition_code", IntegerType()),
            StructField("wind_kph", FloatType()),
            StructField("wind_degree", IntegerType()),
            StructField("pressure_mb", FloatType()),
            StructField("precip_in", FloatType()),
            StructField("humidity", IntegerType()),
            StructField("cloud", IntegerType()),
            StructField("feelslike_c", FloatType()),
            StructField("windchill_c", FloatType()),
            StructField("heatindex_c", FloatType()),
            StructField("dewpoint_c", FloatType()),
            StructField("vis_km", FloatType()),
            StructField("uv", FloatType()),
            StructField("gust_kph", FloatType()),
        ]
    )

    # Parse the JSON and create the DataFrame for current weather
    current_weather_df = (
        spark_df.selectExpr("CAST(value AS STRING)", "topic")
        .where("topic = 'current_weather'")  # Ensure to read from current_weather topic
        .select(from_json(col("value"), current_weather_schema).alias("data"))
        .select("data.*")
        .filter(col("name").isNotNull())
    )  # Filter out invalid or empty entries

    return current_weather_df


def write_postgres_batch(df, epoch_id):
    """
        Function to be called on each batch of data from the stream.
        Writes the batch of data to PostgreSQL.
    """

    print(f"Processing batch: {epoch_id}")
    url = "jdbc:postgresql://localhost:5432/airflow"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }

    df.write.jdbc(
        url=url, table="current_weather", mode="append", properties=properties
    )


def load_to_postgres(df):
    """
    Setup streaming to use foreachBatch to write each micro-batch to PostgreSQL.
    - The checkpointLocation is set to /tmp/realtime_weather_checkpoint to save the state of the stream.
        + it helps to recover the stream from where it left off in case of failure and avoid duplicate in other launch.
    """
    query = (
        df.writeStream.foreachBatch(write_postgres_batch)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/realtime_weather_checkpoint")
        .start()
    )

    return query


spark_conn = create_spark_connection()
print("spark_conn: connected")

if spark_conn is not None:
    # connect to kafka with spark connection
    spark_df = connect_to_kafka(spark_conn)

    if spark_df is not None:
        weather_df = create_current_weather_df_from_kafka(spark_df)

        if weather_df is not None:
            logging.info("Streaming is being started...")
            print("Streaming is being started...")

            # Start streaming and writing to PostgreSQL
            query = load_to_postgres(weather_df)
            query.awaitTermination()
