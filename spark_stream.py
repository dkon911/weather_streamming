import logging
import findspark
findspark.init()

from cassandra.cluster import Cluster
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


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )

    print("Keyspace created successfully!")


def create_table(session):

    # TABLE: spark_streams.weather
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.historical_weather (
            id UUID,
            name TEXT,
            date TEXT,
            time TEXT,
            region TEXT,
            country TEXT,
            lat FLOAT,
            lon FLOAT,
            tz_id  TEXT,
            localtime TEXT,
            localtime_epoch BIGINT,
            date_epoch BIGINT,
            maxtemp_c FLOAT,
            mintemp_c FLOAT,
            avgtemp_c FLOAT,
            maxwind_kph FLOAT,
            totalprecip_mm FLOAT,
            totalsnow_cm FLOAT,
            avghumidity FLOAT,
            daily_will_it_rain INT,
            daily_chance_of_rain INT,
            daily_will_it_snow INT,
            daily_chance_of_snow INT,
            condition_text TEXT,
            condition_icon TEXT,
            condition_code INT,
            uv FLOAT,
            temp_c FLOAT,
            is_day INT,
            wind_kph FLOAT,
            wind_degree INT,
            pressure_mb FLOAT,
            pressure_in FLOAT,
            precip_mm FLOAT,
            precip_in FLOAT,
            snow_cm FLOAT,
            humidity INT,
            cloud INT,
            feelslike_c FLOAT,
            hour_uv FLOAT,
            PRIMARY KEY ((name, date), time));
        """
    )

    print("Table created successfully!")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
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
            .option("subscribe", "weather")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(["localhost"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_weather_df_from_kafka(spark_df):
    # Define the schema matching the weather data structure
    weather_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType()),
            StructField("region", StringType()),
            StructField("country", StringType()),
            StructField("lat", FloatType()),
            StructField("lon", FloatType()),
            StructField("tz_id", StringType()),
            StructField("localtime", StringType()),
            StructField("localtime_epoch", LongType()),
            StructField("date", StringType()),
            StructField("date_epoch", LongType()),
            StructField("maxtemp_c", FloatType()),
            StructField("mintemp_c", FloatType()),
            StructField("avgtemp_c", FloatType()),
            StructField("maxwind_kph", FloatType()),
            StructField("totalprecip_mm", FloatType()),
            StructField("totalsnow_cm", FloatType()),
            StructField("avghumidity", FloatType()),
            StructField("daily_will_it_rain", IntegerType()),
            StructField("daily_chance_of_rain", IntegerType()),
            StructField("daily_will_it_snow", IntegerType()),
            StructField("daily_chance_of_snow", IntegerType()),
            StructField("condition_text", StringType()),
            StructField("condition_icon", StringType()),
            StructField("condition_code", IntegerType()),
            StructField("uv", FloatType()),
            StructField("time", StringType()),
            StructField("temp_c", FloatType()),
            StructField("is_day", IntegerType()),
            StructField("wind_kph", FloatType()),
            StructField("wind_degree", IntegerType()),
            StructField("pressure_mb", FloatType()),
            StructField("pressure_in", FloatType()),
            StructField("precip_mm", FloatType()),
            StructField("precip_in", FloatType()),
            StructField("snow_cm", FloatType()),
            StructField("humidity", IntegerType()),
            StructField("cloud", IntegerType()),
            StructField("feelslike_c", FloatType()),
            StructField("hour_uv", FloatType()),
        ]
    )

    # Parse the JSON and create the DataFrame
    weather_df = (
        spark_df.selectExpr("CAST(value AS STRING)", "topic")
        .where("topic = 'weather'")
        .select(from_json(col("value"), weather_schema).alias("data"))
        .select("data.*")
        .filter(col("id").isNotNull())
    )  # Filter out null IDs

    return weather_df


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    print("spark DONE")

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        # users_df = create_users_df_from_kafka(spark_df)
        weather_df = create_weather_df_from_kafka(spark_df)

        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            weather_data_stream = (
                weather_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/weather_checkpoint")
                .option("keyspace", "spark_streams")
                .option("table", "historical_weather")
                .start()
            )

            weather_data_stream.awaitTermination()
