# Insert statements for all the tavles

insert_songplay = """insert into songplays
                     (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                     values (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) DO NOTHING"""

insert_users = """insert into users 
                  (user_id, first_name, last_name, gender, level)
                  values (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING"""

insert_songs = """insert into songs 
                  (song_id, title, artist_id, year, duration)
                  values (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING"""

insert_artists = """insert into artists 
                    (artist_id, name, location, latitude, longitude)
                    values (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING"""

insert_time = """insert into time 
                 (start_time, hour, day, week, month, year, weekday)
                 values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING"""

insert_map = {"songplays": insert_songplay,
              "users": insert_users,
              "songs": insert_songs,
              "artists": insert_artists,
              "time": insert_time}

fetch_ids = """select song_id, artists.artist_id from 
                songs JOIN artists on songs.artist_id = artists.artist_id
                where songs.title = %s and
                artists.name = %s and
                songs.duration = %s
"""