
**Purpose of Database**

Sparkify is looking to consolidate their song and log files that are in json format into a format that will allow them query their data in a more standardized and optimized manner. The solution is to build an ETL piple that parses these json files and inserts records into a Postgres database. Sparkify's primary use of this database will be to analyze song play, so the database is optimized for this. The database is designed using a star schema, where the Fact Table is a song play table, and the Dimension Tables include data corresponding to songs, artists, users, and time which all contribute to the understanding of a song play.




**Tables in the Database**

songplays 
    - songplay_id SERIAL PRIMARY KEY, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar,        session_id int, location varchar, user_agent varchar
songs
    -song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float

artists
    -artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float

users
    -user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar

time
    -start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int


**File Descriptions**

create_tables.py : Creates the DB. Drops the database tables if they exist. Creates blank database tables.

sql_queries.py : Assigns SQL statements to strings. Those strings are used in create_tables.py to implement the table schemas.

etl.py : Reads the song and log json files and makes inserts into corresponding database tables.

EDA.ipynb : This python notebook can be used after running create_tables.py and etl.py. The table includes some basic exploratory data analysis for 4 of the tables, using SQL, to understand some of the data contained in those tables.


**Running the programs**

1. Run create_tables.py via the command line. Command: "python create_tables.py" At this point the database is created.
2. Run etl.py via the command line. Command: "python etl.py"  At this point the database tables have been populated with the json files in ./data.
3. EDA.ipynb contains some initial Exploratory Data Analysis. You can run the cells to begin understanding some of the data.