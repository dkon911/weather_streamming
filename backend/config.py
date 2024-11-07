import psycopg2
from cassandra.cluster import Cluster

# PostgreSQL Connection
def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow"
    )

# Cassandra Connection
def get_cassandra_session():
    cluster = Cluster(['localhost'])
    return cluster.connect('spark_streams')
