import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    
    """
    This function uses the spark instance, reads the data from the s3 bucket and convert it into a spark dataframe.
    
    input: json file path for the song files from s3 bucket
    
    output: 
    
    1. parquet files for songs table partitioned by 'year' and 'artist_id' 
    
    2. parquet files for artists table 
    
    """
    
    
    
    # get filepath to song data file-- instead of reading all the files, files from one folder is read for test purpose
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
   
    
    # read song data file
    df_song = spark.read.json(song_data)
    
    
    # extract columns to create songs table
    songs_table = df_song['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data,'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df_song['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    
    """
    This function uses the spark instance, reads the data from the s3 bucket and convert it into a spark dataframe.
    
    input: json file path for the log files from s3 bucket
    
    output: 
        
    1. parquet files for users table 
    
    2. parquet files for time table partitioned by 'year' and 'month' 
    
    3. parquet files for songplays table partitioned by 'year' and 'month' 
    
    """
    
    #get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log[df_log['page'] =='NextSong']
    
    
    #extract columns for users table    
    columns = df_log['userId', 'firstName', 'lastName', 'gender', 'ts', 'level']
    users_table = columns.selectExpr("userId as user_id", "firstName as frist_name", "lastName as last_name", 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'),'overwrite')
    
    #converting the time variable ts into timestamp
    df_log = df_log.withColumn("timestamp", (col("ts").cast('bigint')/1000).cast("timestamp"))
    
    # extract columns to create time table
    time_table = df_log.select('timestamp', 
                             hour('timestamp').alias('hour'),
                             dayofmonth('timestamp').alias('day'),
                             weekofyear('timestamp').alias('week'),
                             month('timestamp').alias('month'),
                             year('timestamp').alias('year'),
                             date_format('timestamp', 'EEEE').alias('day_of_week')) 
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'time_df'),'overwrite')
    

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    df_song = spark.read.json(song_data)
    
    
    #extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log.join(df_song, (df_log.length == df_song.duration) &
                                  (df_log.artist == df_song.artist_name) & 
                                  (df_log.song == df_song.title), 'left')\
                                  .select(col('userId').alias('user_Id'),
                                          df_log.location,
                                          col('userAgent').alias('user_agent'),
                                          col('sessionId').alias('session_id'),
                                          df_song.artist_id,
                                          df_song.song_id,
                                          df_log.level,
                                          df_log.timestamp,
                                          year('timestamp').alias('year'),
                                          month('timestamp').alias('month'))
                                          
                                                               
                             
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays'),'overwrite')
    
   

def main():
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    os.environ['AWS_ACCESS_KEY_ID']= config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']= config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-bucket-jahid/"
    #output_data ="output"
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
