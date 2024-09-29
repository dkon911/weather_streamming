from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import datetime

# Configuration
CASSANDRA_HOSTS = ["localhost"]
CASSANDRA_KEYSPACE = "spark_streams"
CASSANDRA_TABLE = "historical_weather"
USERNAME = "cassandra"
PASSWORD = "cassandra"

# Connect to Cassandra
auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
cluster = Cluster(CASSANDRA_HOSTS, auth_provider=auth_provider)
session = cluster.connect(CASSANDRA_KEYSPACE)

# Query to get max time and count of records
query = f"SELECT max(time), count(id) FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE};"
result = session.execute(query).one()

max_time = result[0]
record_count = result[1]

# Load previous day's record count from a file
try:
    with open("previous_record_count.txt", "r") as file:
        previous_record_count = int(file.read().strip())
except FileNotFoundError:
    previous_record_count = 0

# Validate the data
expected_increase = 72
if record_count - previous_record_count == expected_increase:
    print(
        f"Data validation successful: {record_count} records found, increased by {expected_increase} as expected."
    )
else:
    print(
        f"Data validation failed: {record_count} records found, expected increase by {expected_increase}."
    )

# Save the current record count for the next validation
with open("previous_record_count.txt", "w") as file:
    file.write(str(record_count))

# Close the connection
cluster.shutdown()
