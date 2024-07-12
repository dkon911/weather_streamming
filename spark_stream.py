import logging
import findspark
findspark.init()

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType


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

def weather_filter(weather_data):
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

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    print("___________________________________________________________________")
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
    schema = StructType([
        StructField("location", StringType()),
        StructField("forecast", StringType())
    ])

    weather_df = spark_df.selectExpr("CAST(value AS STRING)", "topic").where("topic = 'weather'") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(weather_df)

    return weather_df


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    print('spark DONE')

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        users_df = create_users_df_from_kafka(spark_df)
        # weather_df = create_weather_df_from_kafka(spark_df)

        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_users_query = (users_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            # streaming_weather_query = (weather_df.writeStream.format("org.apache.spark.sql.cassandra")
            #                          .option('checkpointLocation', '/tmp/weather_checkpoint
            #                          .option('keyspace', 'spark_streams')
            #                          .option('table', 'weather')
            #                          .start())

            streaming_users_query.awaitTermination()
            # streaming_weather_query.awaitTermination()