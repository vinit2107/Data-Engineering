# This file contains the scripts to create and drop tables

# Drop table scripts
drop_songplays = "drop table if exists songplays"
drop_users = "drop table if exists users"
drop_songs = "drop table if exists songs"
drop_artists = "drop table if exists artists"
drop_time = "drop table if exists time"

drop_table_queries = [drop_songplays, drop_users, drop_songs, drop_artists, drop_time]

# Create table scripts
create_songplays = """create table if not exists songplays
                      (songplay_id int PRIMARY KEY,
                      start_time date REFERENCES time(start_time),
                      user_id int NOT NULL REFERENCES users(user_id),
                      level text,
                      song_id text REFERENCES songs(song_id),
                      artist_id text REFERENCES artists(artist_id),
                      session_id int,
                      location text,
                      user_agent text)"""

create_users = """create table if not exists users
                 (user_id int PRIMARY KEY,
                 first_name text NOT NULL,
                 last_name text NOT NULL,
                 gender text,
                 level text)"""

create_songs = """create table if not exists songs
                  (song_id text PRIMARY KEY,
                  title text not null,
                  artist_id text NOT NULL REFERENCES artists(artist_id),
                  year int,
                  duration float NOT NULL)"""

create_artists = """create table if not exists artists
                    (artist_id text PRIMARY KEY,
                    name text NOT NULL,
                    location text,
                    latitude float,
                    longitude float)"""

create_time = """create table if not exists time
                 (start_time date PRIMARY KEY,
                 hour int,
                 day int,
                 week int,
                 month int,
                 year int,
                 weekday int)"""

create_table_scripts = [create_songplays, create_users, create_songs, create_artists, create_time]
