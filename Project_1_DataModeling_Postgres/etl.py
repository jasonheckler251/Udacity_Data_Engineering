import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Inputs cur: Database cursor, filepath: filepath of a song file
    Reads a json song file into a pandas DataFrame
    Extracts the song data record and inserts into the song table.
    Extract the artist data record and inserts into the artist table.
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]\
        .values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location',
                    'artist_latitude', 'artist_longitude']]
    # change nans to None, so that these records are NULL in the DB table
    # https://stackoverflow.com/questions/32107558/how-do-i-convert-numpy-nan-objects-to-sql-nulls
    artist_df = artist_df.where(pd.notnull(artist_df), None)
    artist_data = artist_df.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Inputs cur: Database cursor, filepath: filepath of a log file
    Reads a json log file into a pandas DataFrame
    Filters the dataframe to only contain records where page == "NextSong"
    Pulls the timestamp and inserts the time data into the time table
    Inserts the user information into the user table.
    Inserts songplay data into the songplay table.
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week,
                 t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']

    # create dictionary where the key is the column label
    # and the value is the time data
    pandas_dict = {}
    for i in range(len(time_data)):
        pandas_dict[column_labels[i]] = time_data[i]
    # convert the dictionary to a dataframe
    time_df = time_df = pd.DataFrame(pandas_dict)[column_labels]

    # insert each df row into the time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row['ts'], unit='ms'), row['userId'],
                         row['level'], songid, artistid, row['sessionId'],
                         row['location'], row['userAgent'])

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Inputs: cur - database cursor, conn - the datebase connection,
    func - process_log_file or process_song_file
    Iterate over every file and run the process_log_file or
    process_song_file on it depending on which file it is.
    Commits changes to the database.
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                            user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
