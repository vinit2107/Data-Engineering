insert_session_songs = """
                       insert into session_songs (sessionId, itemInSession, artist, song_title, song_length)
                       values (?, ?, ?, ?, ?)
                       """

insert_user_songs = """
                    insert into user_songs (userId, sessionId, artist, song, firstName, lastName, itemInSession)
                    values (?, ?, ?, ?, ?, ?, ?)
                    """

insert_app_history = """
                     insert into app_history (song, firstName, lastName, userId)
                     values (?, ?, ?, ?)
                     """