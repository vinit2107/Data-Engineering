keyspace_query = """
                 create keyspace if not exists db
                 with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                 """

user_songs_query = """
                      create table if not exists user_songs
                      (userId int, sessionId int, artist text, song text, firstName text, lastName text, itemInSession int,
                      PRIMARY KEY ((userId, sessionId), itemInSession))
                      """

session_songs_query = """
                      create table if not exists session_songs
                      (sessionId int, itemInSession int, artist text, song_title text, song_length float,
                      PRIMARY KEY (sessionId, itemInSession))
                      """

app_history_query = """
                    create table if not exists app_history
                    (song text, firstName text, lastName text, userId int,
                    PRIMARY KEY (song, userId))
                    """

table_create_queries = [session_songs_query, user_songs_query, app_history_query]