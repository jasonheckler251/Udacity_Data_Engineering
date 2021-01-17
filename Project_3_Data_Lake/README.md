**Puprose of the Project**

The purpose of the project is to build an ETL piple to create a data lake for Sparkify, optimized for queries on song play analysis.  In order to do this, we must load the raw data in json files from Amazon S3 buckets into a Spark program, and then saves the output tables in parquet files, in S3.

**Program Files**

dl.cfg: This is a configuration file that contains the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for accessing AWS.

etl.py: This file reads the configuration file. It creates a spark session, reads the input data from an S3 bucket, parses the data into 5 different tables representing songs, artists, users, time, and songplays. It then saves those tables to parquet files.

**How to run the program

The program is run with the command "python etl.py". The next step is to run etl.py.  The user should fill in their
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in dl.cfg, as well as the "output_data" field with the location where they'd
like the files to be saved to. This field is currently populated with my own S3 bucket.


