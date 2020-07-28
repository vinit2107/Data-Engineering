from CommonUtils.PropertyUtils import PropertyReader
from Cassandra.Cassandra import Cassandra
from CommonUtils.CommonUtility import CommonUtils


def main():
    print("Initiating the ETL procedure")
    # reading the property file
    config = PropertyReader().readPropertyFile()
    # establishing connection with the cassandra server
    cluster, session = Cassandra().create_connection(config)
    # creating keyspace
    Cassandra().create_keyspace(session, config.get('Database', 'database.keyspace'))
    session.set_keyspace('db')
    # create tables
    Cassandra().create_tables(session)
    # Get path to all the files
    paths = CommonUtils().get_file_paths(config.get('Path', 'dataset.path'))
    # Read the data from the files
    rows = CommonUtils().read_data(paths)
    # Inserting records in session_songs
    Cassandra().insert_batch_session(rows, session)
    # Inserting records in user_songs
    Cassandra().insert_batch_user(rows, session)
    # Inserting records in app_history
    Cassandra().insert_batch_app_history(rows, session)
    print("Completed inserting records in the table")
    Cassandra().end_connection(session, cluster)
    print("ETL job completed!!")


if __name__ == "__main__":
    main()
