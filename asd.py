from cassandra.cluster import Cluster

def delete_rows_by_date(keyspace, table, date):
    try:
        # Connect to the Cassandra cluster
        cluster = Cluster(['localhost'])
        session = cluster.connect(keyspace)

        # Retrieve the ids for the rows where the date is '2024-07-26'
        select_query = f"SELECT id FROM {table} WHERE date = '{date}' ALLOW FILTERING;"
        rows = session.execute(select_query)
        ids_to_delete = [row.id for row in rows]

        # Delete rows using the retrieved ids
        for id in ids_to_delete:
            delete_query = f"DELETE FROM {table} WHERE id = {id};"
            session.execute(delete_query)

        print(f"Rows with date '{date}' have been deleted from {table}.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        session.shutdown()
        cluster.shutdown()

# Usage
delete_rows_by_date('spark_streams', 'weather', '2024-07-26')