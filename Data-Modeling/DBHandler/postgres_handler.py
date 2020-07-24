from configparser import RawConfigParser
import psycopg2
from Scripts.DDL.Postgres_script import *
from Scripts.DML.Postgres_script import *


class PostGresHandler:

    def create_connection(self, config: RawConfigParser):
        """
        Function to create a connection to the PostgreSQL server using the configuration
        provided in the configuration.properties
        :param config: Object of properties file from PropertyUtils
        :return: cursor, connection (obtained from psycopg2)
        """
        print("Trying to establish a connection")
        host = config.get('Database', 'database.url')
        dbname = config.get('Database', 'database.name')
        user = config.get('Database', 'database.user')
        password = config.get('Database', 'database.password')
        credentials = "host={0} dbname={1} user={2} password={3}"
        try:
            conn = psycopg2.connect(credentials.format(host, dbname, user, password))
            cur = conn.cursor()
            print("Connected to Postgres server!!")
            print("Setting autocommit for the session as True")
            conn.set_session(autocommit=True)
            return conn, cur
        except Exception as ex:
            print("Error connecting to the database")
            raise ex

    def drop_table(self, cur):
        """
        Function to drop tables using the scripts defined in Scripts/DDL/Postgre_scripts.py
        :param cur: cursor (obtained from psycopg2)
        :return:
        """
        for query in drop_table_queries:
            try:
                cur.execute(query)
            except Exception as ex:
                print("Error dropping the table")
                raise ex

    def create_table(self, cur):
        """
        Function to create tables using the scripts defined in Scripts/DDL/Postgre_scripts.py
        :param cur: cursor (obtained from psycopg2)
        :return:
        """
        self.drop_table(cur)
        for query in create_table_scripts:
            try:
                cur.execute(query)
            except Exception as ex:
                print("Error creating the table")
                raise ex
        print("Tables created!")

    def insert(self, cur, table: str, data: list):
        """
        Function to perform insert operations in the PostgreSQL database.
        Note: This function performs insert one row at a time
        :param data: row of data
        :param cur: cursor (obtained from psycopg2)
        :param table: name of the table in which insert has to be performed
        :return:
        """
        query = insert_map.get(table)
        try:
            cur.execute(query, data)
        except Exception as ex:
            print("Error inserting values in table {0}".format(table))
            raise ex

    def fetch(self, cur, title: str, name: str, duration: float):
        """
        Function to fetch artist_id and song_id for the given title, song name and duration.
        :param cur: cursor (obtained from psycopg2)
        :param title: Title of the song
        :param name: song name
        :param duration: duration of the song
        :return: song_id, artist_id
        """
        query = fetch_ids
        cur.execute(query, (title, name, duration))
        result = cur.fetchone()
        if result:
            song_id, artist_id = result
        else:
            song_id, artist_id = None, None
        return song_id, artist_id

    def close_connection(self, conn, cur):
        """
        Function used to close the connection
        :param conn: connection object (obtained using psycopg2)
        :param cur: cursor object (obtained using psycopg2)
        :return:
        """
        print("Closing the connection")
        cur.close()
        conn.close()
        print("Closed the connection")

