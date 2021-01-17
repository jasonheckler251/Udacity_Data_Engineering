**Puprose of the Project**

The purpose of the project is to create a data warehouse for Sparkify, optimized for queries on song play analysis.  In order to do this, we must load the raw data in json files in Amazon S3 buckets into RedShift staging tables, and then into a star schema database.

**Program Files**

dwh.cfg: This is a configuration file that contains the Redshift Database information, the IAM role, and the S3 buckets holding the project data.

sql_queries.py: This file reads in the configuration file. It holds the query strings for dropping database tables, creating tables, and inserting into tables. It also contains grouped lists of the queries that will be used to iterate through in the other program files.

create_tables.py: This file reads the configuration file. It connects to the RedShift DB. It then runs the Drop Table commands, followed by the Create Table commands.

etl.py: This file reads the configuration file. It connects to the RedShift DB. It then loads the data from the S3 buckets into the staging tables. It then runs an ETL process to insert the data from the staging tables to the fact and dimension tables.

**How to run the program

The program is run by first running create_tables.py. The next step is to run etl.py.  These steps should only be run after creating a RedShift cluster with appropriate permission and IAM roles. The RedShift cluster's information should be stored in dwh.cfg.

**Database Schema Design

songplay: songplay is the fact table. The corresponding keys to the dimension tables are start_time, user_id, artist_id, and song_id. The table places a sortkey on start_time to optimize for queries based off of time. The table uses a diststyle of 'ALL'. songplay is only 333 rows, and is much smaller than the other tables, so storing it on all cpus is not an issue.

users: The users table contains a diststyle of 'ALL'  because it is a very small table of only about 100 records. When making queries based off of user data, this will speed up queries, without much additional storage needed.

songs: songs is a dimension table including song information.  The table is small, so it uses a diststyle of 'ALL' to increase query speed, with a minor sacrifice of storage size.  The table is sorted by its primary key, song_id.

artists: artists is a dimension table including artist information.  The table is small, so it uses a diststyle of 'ALL' to increase query speed, with a minor sacrifice of storage size. The table is sorted by its primary key, artist_id.

time: time is a dimension table including time information. It contains timestamps from the songplay table.  The table is small, so it uses a diststyle of 'ALL' to increase query speed, with a minor sacrifice of storage size. The table is sorted by its primary key, start_time.

