import os
import glob
from CommonUtils.property_utils import *
from DBHandler.postgres_handler import *
from ETL.ETL_Handler.ETL_Postgres import ETLPostgresHandler

# Factory map having the handler name as key and the object as value
dbhandler_factory_map = {"PostGresHandler": PostGresHandler()}

# Factory map having the handler name as the key and ETL handler as the value
etlhandler_factory_map = {"PostGresHandler": ETLPostgresHandler()}


def list_files(path: str):
    """
    Function to iterate through the subdirectories to obtain paths for all the JSON files stored
    in the local system
    :param path: directory where song data is stored
    :return: consolidated list of paths
    """
    files = []
    for root, _, _ in os.walk(path):
        glob_list = glob.glob(os.path.join(root, "*.json"))
        if len(glob_list) != 0:
            files = [l for l in glob_list]
    return files


def main():
    print("Initiating the ETL process")
    config = PropertyUtils().readfile()

    # Reading configuration from the configuration.properties
    dbtype = config.get('Database', 'database.type')
    dbclass = config.get('Classname', dbtype)

    # Getting the instance of handler according to the configured database
    dbinstance = dbhandler_factory_map.get(dbclass)

    # Establishing a connection to the server
    conn, cur = dbinstance.create_connection(config)

    # Creating tables
    dbinstance.create_table(cur)

    # Identifying the paths of all the files for song and log data
    song_files = list_files(config.get('FileLocation', 'song_data'))
    log_files = list_files(config.get('FileLocation', 'log_data'))

    # ETL instance
    etl_instance = etlhandler_factory_map.get(dbclass)
    etl_instance.process_song_file(song_files, cur, dbinstance)
    etl_instance.process_log_files(log_files, cur, dbinstance)

    dbinstance.close_connection(conn=conn, cur=cur)
    print("ETL job completed successfully!")


if __name__ == '__main__':
    main()

