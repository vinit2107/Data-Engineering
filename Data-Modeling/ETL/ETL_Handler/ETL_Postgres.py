import pandas as pd
from pandas import DataFrame


class ETLPostgresHandler:
    def process_song_file(self, song_files: list, cur, dbinstance):
        """
        Function to read the files from the paths obtained from list_files, perform transformation
        and store them in the database. The function call the insert function in handler which stores the data
        one row at a time. The data can be used to populate artists and songs tables.
        :param song_files: list of paths to the JSON files
        :param cur: cursor (obtained from psycopg2)
        :param dbinstance: (object of the handler)
        :return:
        """
        for file in song_files:
            dataframe = pd.read_json(file, lines=True)
            for _, row in dataframe.iterrows():
                # Inserting data in artists table
                dbinstance.insert(cur, 'artists', [row['artist_id'], row['artist_name'], row['artist_location'],
                                  row['artist_latitude'], row['artist_longitude']])
                # Inserting data in songs table
                dbinstance.insert(cur, 'songs', [row['song_id'], row['title'], row['artist_id'], row['year'], row['duration']])

    def process_log_files(self, log_files: list, cur, dbinstance):
        """
        Function to read the files from the paths obtained from list_files, perform transformation
        and store them in the database. The function call the insert function in handler which stores the data
        one row at a time. The data can be used to populate songplays, users and time tables.
        :param log_files: list of paths to JSON log files
        :param cur: cursor (obtained from psycopg2)
        :param dbinstance: object of handler
        :return:
        """
        for path in log_files:
            dataframe = pd.read_json(path, lines=True)
            dataframe = dataframe[dataframe['page'] == 'NextSong']
            time = pd.DataFrame(pd.to_datetime(dataframe['ts'], unit='ms'))
            time = self.calculate_time(time)
            for (index, d_row), (_, t_row) in zip(dataframe.iterrows(), time.iterrows()):
                # Insert records in the user table
                dbinstance.insert(cur, "users", [int(d_row['userId']), d_row['firstName'], d_row['lastName'], d_row['gender'],
                                  d_row['level']])
                # Fetching records to obtain song_id and artist_id
                song_id, artist_id = dbinstance.fetch(cur, d_row['song'], d_row['artist'], d_row['length'])
                # Insert records in the time table
                dbinstance.insert(cur, 'time', [t_row['ts'], t_row['hour'], t_row['day'], t_row['week'], t_row['month'],
                                                t_row['year'], t_row['weekday']])
                # Insert records in the songplays table
                dbinstance.insert(cur, "songplays", [index, t_row['ts'], int(d_row['userId']), d_row['level'], song_id,
                                  artist_id, d_row['sessionId'], d_row['location'], d_row['userAgent']])

    def calculate_time(self, time: DataFrame):
        """
        Function calulates the hour, day, week, month, year, weekday for the given time
        :param time: DataFrame
        :return: DataFrame
        """
        time['hour'] = time['ts'].apply(lambda x: x.hour)
        time['day'] = time['ts'].apply(lambda x: x.day)
        time['week'] = time['ts'].apply(lambda x: x.week)
        time['month'] = time['ts'].apply(lambda x: x.month)
        time['year'] = time['ts'].apply(lambda x: x.year)
        time['weekday'] = time['ts'].apply(lambda x: x.day_name())
        return time
