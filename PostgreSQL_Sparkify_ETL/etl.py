import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# method to get around psycopg2 struggling to process int64 in timestamp, https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

def process_song_file(cur, filepath):
    """
    - Reads .json from "filepath"
    - Inserts data into "songs" and "artists" dimensions
    """
    
    # open song file
    df = pd.read_json(filepath, lines = True)
    
    # Insert song data into song table. 
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist() # Convert first (and only) line of df to list. 
    cur.execute(song_table_insert, song_data)
        
    # insert artist data into artist table. 
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

        
def process_log_file(cur, filepath):
    """
    - Reads .json from "filepath"
    - Filters file to "NextSong" page
    - Partitions file into time, user and songplay dataframes, generates additional attributes and inserts data accordingly into Sparkify DB
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.DataFrame({"timestamp":df['ts'], "datetime":pd.to_datetime(df['ts'], unit='ms')})
    df.insert(df.shape[1], "datetime", t.datetime) # To be inserted into SongPlaysFact
    
    # Create time dataframe, remove duplicates. 
    time_df = pd.DataFrame.from_dict({"start_time":t["datetime"], "hour":t["datetime"].dt.hour, \
                                      "day":t["datetime"].dt.day, "week":t["datetime"].dt.week, \
                                      "month":t["datetime"].dt.month, "year":t["datetime"].dt.year, \
                                      "weekday":t["datetime"].dt.weekday})
    
    for i, row in time_df.iterrows():
        #
        cur.execute(time_table_insert, row)
            

    # load user table
    user_df = pd.DataFrame.from_dict({"user_id":df["userId"], "first_name":df["firstName"], \
                                      "last_name":df["lastName"], "gender":df["gender"], \
                                      "level":df["level"]})

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


    # insert songplay records, this is formatted differently to the previous sections, because song_id and artist_id must first be extracted. 
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row["song"], row["artist"], round(row["length"], 5))) # Round to match datatype in DB
        results = cur.fetchone()
        
        if results:
            songid, artistid, durration = results
        else:
            songid, artistid = None, None
        
        # insert songplay record
        songplay_data = [row["datetime"], row["userId"], row["level"], songid, artistid, row["sessionId"], row["location"], row["userAgent"]]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Identify filepaths for all .json files in all sub-directories within "filepath" 
    - Run the "process_song_file" or "process_log_file" functions 
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connect to Sparkify DB
    
    - Process song files and insert data into DB
    
    - Process log files and insert data into DB
    
    - Close connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Process song files. 
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    # Process log files. 
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()