# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS SongPlaysFact (songplay_Id SERIAL PRIMARY KEY, \
                                                                    start_time TIMESTAMP NOT NULL, \
                                                                    user_id INT NOT NULL, \
                                                                    level VARCHAR NOT NULL, \
                                                                    song_id VARCHAR, \
                                                                    artist_id VARCHAR, \
                                                                    session_id INT NOT NULL, \
                                                                    location VARCHAR, \
                                                                    user_agent VARCHAR)")

# Potential to add REFERENCES here on maybe the time and user tables? 

user_table_create = ("CREATE TABLE IF NOT EXISTS UsersDim (user_id INT PRIMARY KEY, \
                                                           first_name VARCHAR NOT NULL, \
                                                           last_name VARCHAR NOT NULL, \
                                                           gender VARCHAR NOT NULL, \
                                                           level VARCHAR NOT NULL)")

# Would assume data integrity is absolutely vital in a user table? 

song_table_create = ("CREATE TABLE IF NOT EXISTS SongsDim (song_id VARCHAR PRIMARY KEY, \
                                                           title VARCHAR NOT NULL, \
                                                           artist_id VARCHAR NOT NULL, \
                                                           year INT, \
                                                           duration decimal(10, 5) NOT NULL)")

"""
Could include contraint on year > 0 ??
There's some data integrity issues around "year", in practice I find my current business reviews the importance of the particular attribute and often concludes some data is "nice to have" and not worth the effort to populate it properly. 
What does best practice look like in this space? 
Is there any point in a NOT NULL constraint on an already faulty variable? 
I would think not, why have it be the cause of a row rejection, if it's currently not correct anyway?
"""

artist_table_create = ("CREATE TABLE IF NOT EXISTS ArtistsDim (artist_id VARCHAR PRIMARY KEY, \
                                                               name VARCHAR NOT NULL, \
                                                               location VARCHAR, \
                                                               latitude REAL, \
                                                               longitude REAL)")

time_table_create = ("CREATE TABLE IF NOT EXISTS TimeDim (start_time TIMESTAMP PRIMARY KEY, \
                                                          hour INT NOT NULL, \
                                                          day INT NOT NULL, \
                                                          week INT NOT NULL, \
                                                          month INT NOT NULL, \
                                                          year INT NOT NULL, \
                                                          weekday INT NOT NULL)")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO SongPlaysFact (start_time, \
                                                     user_id, \
                                                     level, \
                                                     song_id, \
                                                     artist_id, \
                                                     session_id, \
                                                     location, \
                                                     user_agent) \
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                          ON CONFLICT DO NOTHING")

user_table_insert = ("INSERT INTO UsersDim (user_id, \
                                            first_name, \
                                            last_name, \
                                            gender, \
                                            level) \
                      VALUES (%s, %s, %s, %s, %s) \
                      ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level")


song_table_insert = ("INSERT INTO SongsDim (song_id, \
                                            title, \
                                            artist_id, \
                                            year, \
                                            duration) \
                      VALUES (%s, %s, %s, %s, %s) \
                      ON CONFLICT (song_id) DO UPDATE SET title = EXCLUDED.title, \
                                                          artist_id = EXCLUDED.artist_id, \
                                                          year = EXCLUDED.year, \
                                                          duration = EXCLUDED.duration")

#Should fields be incorrect, data like year can be amended at a later date? 
artist_table_insert = ("INSERT INTO ArtistsDim (artist_id, \
                                                name, \
                                                location, \
                                                latitude, \
                                                longitude) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (artist_id) DO UPDATE SET name = EXCLUDED.name, \
                                                              location = EXCLUDED.location, \
                                                              latitude = EXCLUDED.latitude, \
                                                              longitude = EXCLUDED.longitude")


time_table_insert = ("INSERT INTO TimeDim (start_time, \
                                           hour, \
                                           day, \
                                           week, \
                                           month, \
                                           year, \
                                           weekday) \
                      VALUES (%s, %s, %s, %s, %s, %s, %s) \
                      ON CONFLICT DO NOTHING")

# FIND SONGS

song_select = ("SELECT DISTINCT \
                   s.song_id \
                   , a.artist_id \
                   , s.duration \
                FROM \
                    SongsDim AS s \
                    LEFT JOIN ArtistsDim AS a on a.artist_id = s.artist_id \
                WHERE \
                   s.title = %s \
                   AND a.name = %s \
                   AND s.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]