import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplayfact"
user_table_drop = "DROP TABLE IF EXISTS usersdim"
song_table_drop = "DROP TABLE IF EXISTS songdim"
artist_table_drop = "DROP TABLE IF EXISTS artistdim"
time_table_drop = "DROP TABLE IF EXISTS timedim"

# CREATE TABLES

staging_events_table_create= (""" 
CREATE TABLE staging_events (
    artist TEXT NULL,
    auth TEXT NULL,
    firstName TEXT NULL,
    gender TEXT NULL,
    itemInSession INT NULL,
    lastName TEXT NULL,
    length DECIMAL(10,5) NULL,
    level TEXT NULL,
    location TEXT NULL,
    method TEXT NULL,
    page TEXT NULL,
    registration BIGINT NULL,
    sessionId INT NULL,
    song TEXT NULL,
    status INT NULL,
    ts BIGINT NULL,
    userAgent TEXT NULL,
    userId INT NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT NULL, 
    artist_id TEXT NULL, 
    artist_latitude DECIMAL(8,6) NULL,
    artist_longitude Decimal(9,6) NULL, 
    artist_location TEXT NULL, 
    artist_name TEXT NULL, 
    song_id TEXT NULL, 
    title TEXT NULL,
    duration DECIMAL(10,5) NULL,
    year INT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE songplayfact (
    songplay_id INT IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT NOT NULL,
    level TEXT NOT NULL,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL DISTKEY,
    session_id INT NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE usersdim (
    user_Id INT PRIMARY KEY SORTKEY, 
    first_Name TEXT NOT NULL,
    last_Name TEXT NOT NULL,
    gender TEXT NOT NULL,
    level TEXT NOT NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE songdim (
    song_id TEXT PRIMARY KEY, 
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL DISTKEY,
    year INT NULL,
    duration DECIMAL(10,5) NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artistdim (
    artist_id TEXT PRIMARY KEY DISTKEY,
    name TEXT NOT NULL,
    location TEXT NULL, 
    latitude DECIMAL(8,6) NULL,
    longitude Decimal(9,6) NULL
)
""")

time_table_create = ("""
CREATE TABLE timedim (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL,
    year INT NOT NULL, 
    weekday INT NOT NULL
)
DISTSTYLE ALL;
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    FORMAT AS JSON {};
""").format(config.get('S3', 'LOG_DATA'), 
            config.get('IAM_ROLE', 'ARN'), 
             config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplayfact (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
SELECT
    timestamp 'epoch' + SE.ts/1000 * interval '1 second' as start_time,
    SE.userId,
    SE.level,
    SS.song_id,
    SS.artist_id,
    SE.sessionId AS session_id,
    SE.location,
    SE.userAgent
FROM 
    staging_events SE,
    staging_songs SS
WHERE
    SE.song = SS.title
    AND SE.artist = SS.artist_name
    AND SE.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO usersdim (
    user_Id, 
    first_Name,
    last_Name,
    gender,
    level
    )
SELECT
    T.userId, 
    T.firstName, 
    T.lastName,
    T.gender, 
    T.level
FROM (
    SELECT
        SE.userId, 
        SE.firstName, 
        SE.lastName,
        SE.gender, 
        SE.level, 
        ROW_NUMBER() OVER(PARTITION BY SE.userId ORDER BY SE.ts DESC) AS LatestRecord
    FROM 
        staging_events SE
    WHERE
        SE.page = 'NextSong'
        
) T
WHERE
    T.LatestRecord = 1
""")

song_table_insert = ("""
INSERT INTO songdim (
    song_id, 
    title,
    artist_id,
    year,
    duration
)

SELECT DISTINCT
    SS.song_id, 
    SS.title, 
    SS.artist_id, 
    SS.year, 
    SS.duration
FROM 
    staging_songs SS
""")

# Several unique artist_ids contain multiple records, with changing locations and coordinates. 
# In an ideal world, the latest record based on the file the data came from would be used to dedupe. 
# For the sake of this task I'm deduping by nothing. 

artist_table_insert = ("""
INSERT INTO artistdim (
    artist_id,
    name,
    location, 
    latitude,
    longitude
)
SELECT DISTINCT 
    T1.artist_id, 
    T1.name, 
    T1.location, 
    T1.latitude, 
    T1.longitude
FROM (
    SELECT
        SS.artist_id, 
        SS.artist_name AS name, 
        SS.artist_location AS location, 
        SS.artist_latitude AS latitude, 
        SS.artist_longitude AS longitude, 
        ROW_NUMBER() OVER(PARTITION BY SS.artist_id ORDER BY (SELECT NULL)) AS UniqueRecord
    FROM 
        staging_songs SS
    ) T1
WHERE
    T1.UniqueRecord = 1
""")

time_table_insert = ("""
INSERT INTO timedim (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    SP.start_time AS start_time,
    DATE_PART(hrs, SP.start_time) AS hour,
    DATE_PART(dayofyear, SP.start_time) AS day,
    DATE_PART(w, SP.start_time) AS week,
    DATE_PART(mons ,SP.start_time) AS month,
    DATE_PART(yrs , SP.start_time) AS year,
    DATE_PART(dow, SP.start_time) AS weekday
FROM 
    songplayfact SP
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
