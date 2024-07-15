import logging
import findspark
findspark.init()

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    # TABLE: spark_streams.created_users
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    # TABLE: spark_streams.weather
    session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.weather (
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

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_users_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    users_df = spark_df.selectExpr("CAST(value AS STRING)", 'topic').where("topic = 'users_created'") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(users_df)

    return users_df


def create_weather_df_from_kafka(spark_df):
    # Define the schema matching the weather data structure
    weather_schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType()),
        StructField("region", StringType()),
        StructField("country", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("tz_id", StringType()),
        StructField("localtime", StringType()),
        StructField("localtime_epoch", LongType()),
        StructField("date", StringType()),
        StructField("date_epoch", LongType()),
        StructField("maxtemp_c", DoubleType()),
        StructField("mintemp_c", DoubleType()),
        StructField("avgtemp_c", DoubleType()),
        StructField("maxwind_kph", DoubleType()),
        StructField("totalprecip_mm", DoubleType()),
        StructField("totalsnow_cm", DoubleType()),
        StructField("avghumidity", DoubleType()),
        StructField("daily_will_it_rain", IntegerType()),
        StructField("daily_chance_of_rain", IntegerType()),
        StructField("daily_will_it_snow", IntegerType()),
        StructField("daily_chance_of_snow", IntegerType()),
        StructField("condition_text", StringType()),
        StructField("condition_icon", StringType()),
        StructField("condition_code", IntegerType()),
        StructField("uv", DoubleType()),
        StructField("time", StringType()),
        StructField("temp_c", DoubleType()),
        StructField("is_day", IntegerType()),
        StructField("wind_kph", DoubleType()),
        StructField("wind_degree", IntegerType()),
        StructField("pressure_mb", DoubleType()),
        StructField("pressure_in", DoubleType()),
        StructField("precip_mm", DoubleType()),
        StructField("precip_in", DoubleType()),
        StructField("snow_cm", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("cloud", IntegerType()),
        StructField("feelslike_c", DoubleType()),
        StructField("hour_uv", DoubleType())
    ])

    # Parse the JSON and create the DataFrame
    weather_df = (spark_df
                  .selectExpr("CAST(value AS STRING)", "topic")
                  .where("topic = 'weather'")
                  .select(from_json(col('value'), weather_schema).alias('data'))
                  .select("data.*")
                  .filter(col('id').isNotNull()))  # Filter out null IDs

    return weather_df


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    print('spark DONE')

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

            # streaming_users_query = (users_df.writeStream.format("org.apache.spark.sql.cassandra")
            #                    .option('checkpointLocation', '/tmp/checkpoint')
            #                    .option('keyspace', 'spark_streams')
            #                    .option('table', 'created_users')
            #                    .start())

            streaming_weather_query = (weather_df.writeStream.format("org.apache.spark.sql.cassandra")
                                     .option('checkpointLocation', '/tmp/weather_checkpoint')
                                     .option('keyspace', 'spark_streams')
                                     .option('table', 'weather')
                                     .start())

            # streaming_users_query.awaitTermination()
            streaming_weather_query.awaitTermination()