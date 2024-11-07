from cassandra.cluster import Cluster

def delete_rows_by_name_and_date(keyspace, table, name, date):
    try:
        # Connect to the Cassandra cluster
        cluster = Cluster(['localhost'])
        session = cluster.connect(keyspace)

        # Retrieve the ids for the rows where the name and date match
        select_query = f"SELECT id, time FROM {table} WHERE name = '{name}' AND date = '{date}';"
        rows = session.execute(select_query)

        # Delete rows using the retrieved ids and time (since time is part of the clustering key)
        for row in rows:
            delete_query = f"DELETE FROM {table} WHERE name = '{name}' AND date = '{date}' AND time = '{row.time}';"
            session.execute(delete_query)

        print(f"Rows with name '{name}' and date '{date}' have been deleted from {table}.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        session.shutdown()
        cluster.shutdown()

# Usage
delete_rows_by_name_and_date('spark_streams', 'historical_weather', 'Hanoi', '2024-10-20')
