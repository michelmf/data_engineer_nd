import os
import configparser

from datetime              import datetime
from pyspark.sql           import SparkSession
from pyspark.sql.types     import StructType, StructField, FloatType, IntegerType, StringType, TimestampType, DateType 
from pyspark.sql.functions import udf, col, year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp, from_unixtime, monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']     = config.get('CREDENTIALS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('CREDENTIALS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    ''' this function only creates a spark session. Basic building block '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Description: process_song_data function loads data from a s3 bucket from input_data, process all the data and
    stores the results in another s3 bucket located in output_data
    
    Parameters:
    spark       : Spark Session
    input_data  : s3 Address where song_data json files are stored (in this specific scenario, a public s3 bucket)
    output_data : S3 bucket were dimensional tables in parquet format will be stored after processed   
    '''
    
    # get filepath to song data file
    song_data = input_data + 'data/song-data/*/*/*/*'
    
    # Defining the correct schema for these columns
    song_data_schema = StructType([
    
    StructField('artist_id'       ,  StringType(), True),
    StructField('artist_name'     ,  StringType(), True),
    StructField('artist_location' ,  StringType(), True),
    StructField('song_id'         ,  StringType(), True),
    StructField('title'           ,  StringType(), True),
    StructField('num_songs'       , IntegerType(), True),
    StructField('year'            , IntegerType(), True),
    StructField('artist_latitude' ,   FloatType(), True),
    StructField('artist_longitude',   FloatType(), True),
    StructField('duration'        ,   FloatType(), True)])

    
    # read song data file
    df = spark.read.json(song_data, song_data_schema)

    # extract columns to create songs table (There are many ways to do this)
    songs_table = df.selectExpr('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name AS name', 'artist_location AS location', 'artist_latitude AS latitude', 'artist_longitude AS longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    '''
    Description: process_song_data function loads data from a s3 bucket from input_data, process all the data and
    stores the results in another s3 bucket located in output_data
    
    Parameters:
    spark       : Spark Session
    input_data  : s3 Address where song_data json files are stored (in this specific scenario, a public s3 bucket)
    output_data : S3 bucket were dimensional tables in parquet format will be stored after processed   
    '''
    
    # get filepath to log data file
    log_data = input_data + 'data/log-data/*'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId AS user_id', 'firstName AS first_name', 'lastName AS last_name', 'gender', 'level').drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    
    df = df.withColumn('start_time',
                       to_timestamp (from_unixtime((col("ts") / 1000) , 
                       'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp"))
    
    # extract columns to create time table
    time_table = df.select('start_time',
                           hour('start_time').alias('hour'),
                           dayofmonth('start_time').alias('day'),
                           weekofyear('start_time').alias('week'),
                           month('start_time').alias('month'),
                           year('start_time').alias('year'),
                           dayofweek('start_time').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table (reading the parquet files stored before)
    song_df   = spark.read.parquet(output_data + 'songs/*/*/*') # Partitioned by year and artist_id, only two levels to read 

    # extract columns from joined song and log datasets to create songplays table 
    # Final Columns - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # Creating an id for songplays_table -> 
    songplays_table =  df.withColumn('songplay_id', monotonically_increasing_id()) \
                         .join(song_df, song_df.title == df.song) \
                         .select('songplay_id',
                                 'start_time',
                                 col('userId').alias('user_id'),
                                 'level',
                                 'song_id',
                                 col('artist').alias('artist_id'),
                                 col('sessionID').alias('session_id'),
                                 'location',
                                 col('userAgent').alias('user_agent'),
                                month('start_time').alias('month'),
                                 year('start_time').alias('year'))

    # write songplays table to parquet files
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data  = ''#"s3a://udacity-dend/"
    output_data = 'teste/'#"s3://spark-project-deng-michel"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark,  input_data, output_data)

if __name__ == "__main__":
    main()
