import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, StructField, LongType, TimestampType
from datetime import datetime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create the spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """Imports the song data. Generates the song table and saves it to parquet files. \
        Generates the artists table and saves it to parquest files."""
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    #define song_data schema
    song_data_schema = StructType([
        StructField('num_songs', IntegerType(), True),
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', FloatType(), True),
        StructField('artist_longitude', FloatType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('duration', FloatType(), True),
        StructField('year', IntegerType(), True)
    ])
    
    
    # read song data file
    df = spark.read.json(song_data, song_data_schema)
    
    # create a view of the sond_data
    df.createOrReplaceTempView("df_song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("SELECT DISTINCT song_id, title, artist_id, \
                                             year, duration FROM df_song_data")
    
      
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id")\
                .mode('overwrite').parquet(os.path.join(output_data, "songtable"))
    
    

    # extract columns to create artists table
    artists_table =spark.sql("SELECT DISTINCT artist_id, artist_name, artist_location,\
                                  artist_latitude, artist_longitude FROM df_song_data")
    #rename the columns
    artists_table = artists_table.withColumnRenamed("artist_name", "name")\
                                 .withColumnRenamed("artist_location", "location")\
                                 .withColumnRenamed("artist_latitude", "latitude")\
                                 .withColumnRenamed("artist_longitude", "longitude")
    
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite')\
                 .parquet(os.path.join(output_data, "artists_table"))
    

def process_log_data(spark, input_data, output_data):
    
    """Imports the log data. Generates user table, time table, and songplay table and
        saves them to parquest files."""
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # define log_data_schema
    log_data_schema = StructType([
        StructField('artist', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', IntegerType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', FloatType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', FloatType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('song', StringType(), True),
        StructField('status', StringType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('user_id', IntegerType(), True)
    ])    
    
    
    # read log data file
    df = spark.read.json(log_data, log_data_schema)
    
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    #create a spark sql view of the log data
    df.createOrReplaceTempView("df_log_data")
    
    # extract columns for users table    
    users_table = spark.sql("SELECT DISTINCT user_id, firstName, lastName, \
                                             gender, level FROM df_log_data")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, "users_table"))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000.0), TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))

    # set df_log_data table to new newly modified version
    df.createOrReplaceTempView("df_log_data")
    
    # extract columns to create time table using pyspark sql functions
    time_table = spark.sql("SELECT DISTINCT start_time FROM df_log_data")
    time_table = time_table.withColumn("hour", hour("start_time")) \
                           .withColumn("day", dayofmonth("start_time")) \
                           .withColumn("week", weekofyear("start_time")) \
                           .withColumn("month", month("start_time")) \
                           .withColumn("year", year("start_time")) \
                           .withColumn("weekday", date_format('start_time','E'))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month")\
              .mode('overwrite').parquet(os.path.join(output_data, "timetable"))
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songtable"))
    song_df.createOrReplaceTempView("songs_table")
    
    artist_df = spark.read.parquet(os.path.join(output_data, "artists_table"))
    artist_df.createOrReplaceTempView("artists_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("SELECT ts, user_id, level, songs_table.song_id, \
                                songs_table.artist_id, sessionid, df_log_data.location, userAgent\
                                FROM df_log_data \
                                JOIN songs_table \
                                ON df_log_data.song = songs_table.title\
                                JOIN artists_table \
                                ON df_log_data.artist = artists_table.name")
                                
    songplays_table = songplays_table.withColumn("start_time", get_timestamp('ts'))
    songplays_table = songplays_table.withColumn("month", month("start_time")) \
                                     .withColumn("year", year("start_time"))
    
    # drop ts column
    songplays_table.drop("ts")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite')\
                         .parquet(os.path.join(output_data, "songplays_table"))

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jph-bucket-2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
