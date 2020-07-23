import pandas as pd
import os
import glob
from CommonUtils.property_utils import *
from DBHandler.postgres_handler import *

# Factory map having the handler name as key and the object as value
dbhandler_factory_map = {"PostGresHandler": PostGresHandler()}


def list_files(path: str):
    files = []
    for root, _, _ in os.walk(path):
        glob_list = glob.glob(os.path.join(root, "*.json"))
        if len(glob_list) != 0:
            for file in glob_list:
                files.append(file)
    return files


def process_song_file(song_files: list, cur, dbinstance):
    for file in song_files:
        dataframe = pd.read_json(file)


def main():
    print("Initiating the ETL process")
    config = PropertyUtils().readfile()

    # Reading configuration from the configuration.properties
    dbtype = config.get('Database', 'database.type')
    dbclass = config.get('Classname', dbtype)

    # Getting the instance of handler according to the configured database
    dbinstance = dbhandler_factory_map.get(dbclass)

    # Establishing a connection to the server
    # conn, cur = dbinstance.create_connection(config)

    # Identifying the paths of all the files for song and log data
    song_files = list_files(config.get('FileLocation', 'song_data'))
    log_files = list_files(config.get('FileLocation', 'log_data'))


if __name__ == '__main__':
    main()

