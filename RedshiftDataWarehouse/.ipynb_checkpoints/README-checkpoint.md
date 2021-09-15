# Sparkify Star Schema Data Model

A start schema data model was designed to allow for analytics to provide insight into Sparkify usage and user behaviour 

The database entity relationship diagram can be seen below. The model consists of 1 Fact table and 4 dimensions. 
![](SparkifyERD.png)

The fact table contains all foreign keys to the dimensions, as well as all unique attributes such as start_time, level, session_id, location and useragent, all variables subject to change per event. 

The accompanying dimension tables either aim to reduce data redundancy - songdim, userdim and artistdim, or seperate temporal data derived from start_time for use when needed, rather than storing within the fact table or deriving at runtime. 

# ETL

Raw data is extracted from json files to staging tables within Amazon Redshift using the copy command. 

The staging tables are then queried, insert statements laod data into the 5 tables, at times removing duplicates using various methods at the load stage.

## Data 

The following is the structure of each table within the star schema

**SongPlaysFact** contains a unique serial Primary Key songplay_id, as well as all Foreign Keys to DIM tables, as well as the session_id.
Table is distributed by artist_id and sorted by timestamp. There are many artists and songs in the artist and song tables, artist_id is common to all 3 tables. 

songplay_Id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent
-----------|----------|-------|-----|-------|---------|----------|--------|---------
INT PRIMARY KEY|TIMESTAMP NOT NULL|INT NOT NULL|TEXT NOT NULL|TEXT NOT NULL|TEXT NOT NULL|INT NOT NULL|TEXT NOT NULL|TEXT NOT NULL
4108|2018-11-21 21:56:47.796000|15|paid|SOZCTXZ12AB0182364|AR5KOSW1187FB35FF4|818|Chicago-Naperville-Elgin, IL-IN-WI|"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"
   
<hr>
    
**UsersDim** contains a unique Primary Key user_id and user data including price plan.
There are currently few users so the table is copied to each node. 

user_id|first_name|last_name|gender|level
-------|----------|---------|------|-----
INT PRIMARY KEY|TEXT NOT NULL|TEXT NOT NULL|TEXT NOT NULL|TEXT NOT NULL
92|Ryann|Smith|F|free
    
<hr>
    
**SongsDim** contains a unique Primary Key song_id, a Foreign Key artist_id to the ArtistDim dimension, and details about songs.
With thousands of songs, this table is distributed by artist_id. 

song_id|title|artist_id|year|duration
-------|-----|---------|----|--------
TEXT PRIMARY KEY|TEXT NOT NULL|TEXT NOT NULL|INT|decimal(10, 5) NOT NULL
SOMZWCG12A8C13C480|I Didn't Mean To|ARD7TVE1187B99BFB1|0|218.93179
    
<hr>
    
**ArtistsDim** contains a unique Primary Key artist_id and details about artists. 
This table is distributed by artist_id.

artist_id|name|location|latitude|longitude
---------|----|--------|--------|-------
TEXT PRIMARY KEY|TEXT NOT NULL|TEXT|DECIMAL(8,6)|Decimal(9,6)
ARMJAGH1187FB546F3|The Box Tops|Memphis, TN|35.1497|-90.0489
    
<hr>
    
**TimeDim** contains a unique Primary Key start_time and temporal information.
There are few unique values in the time dimension, this table is coppied over to all nodes. 

start_time|hour|day|week|month|year|weekday
----------|----|---|----|-----|----|-------
TIMESTAMP PRIMARY KEY|INT NOT NULL|INT NOT NULL|INT NOT NULL|INT NOT NULL|INT NOT NULL|INT NOT NULL
2018-11-30 00:22:07.796000|0|30|48|11|2018|4

## Step by step

1. Run the create_table.py script
    1. This imports several SQL queries within sql_queries.py script, and creates the staging tables, the fact table and all 4 dimensional tables. 
2. Run the etl.py script. 
    1. This imports aditional SQL code from sql_queries.py, first running the copy command to pull data into the staging tables from json files in various folders, then several insert statements pulling data from the staging tables into the star schema data model. 

The file SampleAnalysis.ipynb contains some sample queries.
    
## Sample queries

Find the most active users
`SELECT 
    T1.user_id, 
    U.first_name, 
    U.last_name, 
    SongsPlayed
FROM (
    SELECT TOP 5
        S.user_id,
        COUNT(*) AS SongsPlayed --DISTINCT song_id
    FROM 
        songplayfact S
    GROUP BY
        S.user_id
    ) T1
    LEFT JOIN usersdim U ON T1.user_id = U.user_id
ORDER BY
    SongsPlayed DESC`
    
At the time of this analysis, user 97 has has played 32 non-unique songs. 

user_id|first_name|last_name|songsplayed
-------|----------|---------|-----------
97|Kate|Harrell|32
29|Jacqueline|Lynch|13
25|Jayden|Graves|10
82|Avery|Martinez|3
52|Theodore|Smith|2
    
____

    
Most listened to songs and their artists per unique user. 
`SELECT TOP 5
    S.title AS SongName, 
    A.name AS Artist, 
    UniqueListeners
FROM (
    SELECT
        SP.song_id, 
        COUNT(DISTINCT SP.user_id) AS UniqueListeners
    FROM
        songplayfact SP
    GROUP BY 
        SP.song_id
    ) T1
    LEFT JOIN songdim S ON T1.song_id = S.song_id
    LEFT JOIN artistdim A ON S.artist_id = A.artist_id
ORDER BY
    UniqueListeners DESC`
    
Dwight Yoakam was the most listened to artist at the time of this analysis. 
    
songname|artist|uniquelisteners
--------|------|---------------
You're The One|Dwight Yoakam|22
Catch You Baby (Steve Pitron & Max Sanna Radio Edit)|Lonnie Gordon|9
Nothin' On You [feat. Bruno Mars] (Album Version)|B.o.B|7
I CAN'T GET STARTED|Ron Carter|6
Make Her Say|Kid Cudi|5
    
___

Distribution of songs played.
`SELECT
    T.hour
    , COUNT(*)
FROM
    songplayfact SP
    LEFT JOIN timedim T on SP.start_time = T.start_time
GROUP BY 
    T.hour
ORDER BY 
    T.hour`
    
5pm, end of shift seems to be the most popular time to listen to music. 
    
hour|count
----|----
0|6
1|11
2|3
3|2
4|7
5|7
6|9
7|13
8|18
9|9
10|11
11|16
12|12
13|14
14|16
15|25
16|24
17|40
18|26
19|16
20|18
21|12
22|7
23|11