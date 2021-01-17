import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events( \
                                  artist varchar, \
                                  auth varchar, \
                                  firstName varchar, \
                                  gender varchar, \
                                  itemInSession int, \
                                  lastName varchar, \
                                  length float, \
                                  level varchar, \
                                  location varchar, \
                                  method varchar, \
                                  page varchar, \
                                  registration float, \
                                  sessionid int, \
                                  song varchar, \
                                  status varchar, \
                                  ts bigint, \
                                  userAgent varchar, \
                                  user_id int);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs( \
                                  num_songs int, \
                                  artist_id varchar, \
                                  artist_latitude float, \
                                  artist_longitude float, \
                                  artist_location \
                                  varchar, \
                                  artist_name varchar, \
                                  song_id varchar, \
                                  title varchar, \
                                  duration float, \
                                  year int);")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplay( \
                            songplay_id int IDENTITY(1,1) PRIMARY KEY, \
                            start_time timestamp NOT NULL REFERENCES time(start_time) sortkey, \
                            user_id int NOT NULL REFERENCES users(user_id), \
                            level varchar, \
                            song_id varchar NOT NULL REFERENCES songs(song_id), \
                            artist_id varchar NOT NULL REFERENCES artists(artist_id), \
                            session_id int, \
                            location varchar, \
                            user_agent varchar) \
                        diststyle all;")

user_table_create = ("CREATE TABLE IF NOT EXISTS users ( \
                        user_id int PRIMARY KEY sortkey, \
                        first_name varchar, \
                        last_name varchar, \
                        gender varchar, \
                        level varchar) \
                    diststyle all;")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs ( \
                        song_id varchar PRIMARY KEY sortkey, \
                        title varchar, \
                        artist_id varchar NOT NULL, \
                        year int, \
                        duration float) \
                    diststyle all;")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists ( \
                            artist_id varchar PRIMARY KEY sortkey, \
                            name varchar, \
                            location varchar, \
                            latitude float, \
                            longitude float) \
                        diststyle all;")

time_table_create = ("CREATE TABLE IF NOT EXISTS time ( \
                        start_time timestamp PRIMARY KEY sortkey, \
                        hour int, \
                        day int, \
                        week int, \
                        month int, \
                        year int, \
                        weekday int) \
                    diststyle all;")

# STAGING TABLES

LOG_DATA = config['S3']['LOG_DATA']
ARN = config['IAM_ROLE']['ARN']
LOG_JSON_PATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

staging_events_copy = ("copy staging_events from {} iam_role {} region 'us-west-2' json {}").format(LOG_DATA, ARN, LOG_JSON_PATH)


staging_songs_copy = ("""copy staging_songs from {} iam_role {} region 'us-west-2' json 'auto' truncatecolumns""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay( \
                                start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                            SELECT timestamp 'epoch' + ts/1000* interval '1 second', \
                                user_id, \
                                level, \
                                song_id, \
                                artist_id, \
                                sessionid, \
                                location, \
                                userAgent \
                            FROM staging_events \
                            JOIN staging_songs \
                            ON staging_events.artist = staging_songs.artist_name \
                                AND staging_events.song = staging_songs.title \
                            WHERE staging_events.page = 'NextSong'""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                        SELECT DISTINCT user_id, \
                            firstName, \
                            lastName, \
                            gender, \
                            level \
                        FROM staging_events \
                        WHERE page='NextSong';""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)\
                        SELECT DISTINCT song_id, \
                            title, \
                            artist_id, \
                            year, \
                            duration \
                        FROM staging_songs;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                          SELECT DISTINCT artist_id, \
                              artist_name, \
                              artist_location, \
                              artist_latitude, \
                              artist_longitude \
                          FROM staging_songs;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        SELECT DISTINCT start_time,
                            EXTRACT (hour FROM start_time),
                            EXTRACT (day FROM start_time),
                            EXTRACT (week FROM start_time),
                            EXTRACT (month FROM start_time),
                            EXTRACT (year FROM start_time),
                            EXTRACT (weekday FROM start_time)
                        FROM songplay;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                         user_table_create, song_table_create, 
                        artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, 
                        artist_table_insert, time_table_insert]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, 
                        artist_table_insert, time_table_insert]