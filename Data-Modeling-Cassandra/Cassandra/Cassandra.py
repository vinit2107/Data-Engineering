from cassandra.cluster import Cluster, Session
from configparser import RawConfigParser
from cassandra.query import BatchStatement, ConsistencyLevel
from Scripts.DDL.ddl_script import keyspace_query, table_create_queries
from Scripts.DML.dml_script import *


class Cassandra:

    def create_connection(self, config: RawConfigParser):
        """
        Function to establish a connection with the database
        :param config: config object (obtained from RawConfigParser)
        :return: cluster, session (obtained from cassandra-driver)
        """
        try:
            print("Trying to establsh a connection with the Cassandra Server")
            cluster = Cluster(config.get('Database', 'database.cluster').split(","))
            session = cluster.connect()
            return cluster, session
        except Exception as ex:
            print("Error connecting to the database")
            raise ex

    def create_keyspace(self, session: Session, keyspace: str):
        """

        :param keyspace: Name of the keyspace
        :param session: Session object (obtained from cassandra.cluster)
        :return:
        """
        print("Creating keyspace if not present: " + keyspace)
        try:
            session.execute(keyspace_query)
            print("Keyspace query executed successfully")
        except Exception as ex:
            print("Error running keyspace query")
            raise ex

    def create_tables(self, session: Session):
        """
        Function to create tables
        :param session: session object (obtained through cassandra-driver)
        :return:
        """
        print("Creating tables session_songs, user_songs, app_history")
        try:
            for query in table_create_queries:
                session.execute(query)
            print("All the table creation queries ran successfully")
        except Exception as ex:
            print("Error executing query")
            raise ex

    def insert_batch_session(self, rows: list, session: Session):
        """
        Function to insert data into cassandra in batches in session_songs
        :param rows:
        :param session:
        :return:
        """
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        statement = session.prepare(insert_session_songs)
        try:
            for row in rows:
                if row[6] == "":
                    length = 0.0
                else:
                    length = float(row[6])
                batch.add(statement, (int(row[12]), int(row[4]), row[0], row[13], length))
            session.execute(batch)
        except Exception as ex:
            print("Error inserting data into session_songs")
            raise ex

    def insert_batch_user(self, rows: list, session: Session):
        """
        Function to insert records in user_songs table
        :param rows: list of records
        :param session: session object (obtained from cassandra-driver)
        :return:
        """
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        statement = session.prepare(insert_user_songs)
        try:
            for row in rows:
                if row[16] == "":
                    userId = 0
                else:
                    userId = int(row[16])
                batch.add(statement, (userId, int(row[12]), row[0], row[13], row[2], row[5], int(row[4])))
            session.execute(batch)
        except Exception as ex:
            print("Error inserting records in user_songs")
            raise ex

    def insert_batch_app_history(self, rows: list, session: Session):
        """
        Function to insert records into app_history table
        :param rows: list of records
        :param session: session object (obtained from cassandra-driver)
        :return:
        """
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        statement = session.prepare(insert_app_history)
        try:
            for row in rows:
                if row[13] != "":
                    if row[16] == "":
                        userId = 0
                    else:
                        userId = int(row[16])
                    batch.add(statement, (row[13], row[2], row[5], userId))
            session.execute(batch)
        except Exception as ex:
            print("Error inserting records in app_history")
            raise ex

    def end_connection(self, session: Session, cluster: Cluster):
        """
        Disconnecting thr session and cluster
        :param session: session object (obtained from cassandra-driver)
        :param cluster: cluster object (obtained from cassandra-driver)
        :return:
        """
        session.shutdown()
        cluster.shutdown()
