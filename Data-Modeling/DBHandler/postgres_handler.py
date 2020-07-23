from configparser import RawConfigParser
import psycopg2
from Scripts.DDL.Postgres_script import *
from Scripts.DML.Postgres_script import *


class PostGresHandler:

    def create_connection(self, config: RawConfigParser):
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
            return conn, cur
        except Exception as ex:
            print("Error connecting to the database")
            raise ex

    def drop_table(self, cur):
        for query in drop_table_queries:
            try:
                cur.execute(query)
            except Exception as ex:
                print("Error dropping the table")
                raise ex

    def create_table(self, cur):
        for query in create_table_scripts:
            try:
                cur.execute(query)
            except Exception as ex:
                print("Error creating the table")
                raise ex

    def insert(self, cur, table: str, *params):
        query = insert_map.get(table)
        query = query.format(params)
        try:
            cur.execute(query)
        except Exception as ex:
            print("Error inserting values in table {0}".format(table))
            raise ex
