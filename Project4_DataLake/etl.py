import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Function that creates a Spark Session to be used 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: function that reads the initial json files composing the songs datasets 
        and writes them back to properly patitioned .parquet files. Output are songs and artists tables.
    
        Parameters:
            * `input_data` is the path to the folder where the data to be read are stored.
            * `output_data` is where the parquet files will be written.
    """
    
    # get filepath to song data file
    songs_data = input_data + "song_data/*/*/*/*"
    
    # read song data file and create temp view to query it
    df = spark.read.json(songs_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data + "songs/songs.parquet")

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists/artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
        Description: function that processes the log files, reading them and writing out 
        the users, time and songplays tables in parquet format.
        
        Parameters:
            * same exactly as the above function (`process_song_data`).
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(col('page') == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, "users/users.parquet"))
    
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x/1000)), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",    hour("start_time"))\
                   .withColumn("day",     dayofmonth("start_time"))\
                   .withColumn("week",    weekofyear("start_time"))\
                   .withColumn("month",   month("start_time"))\
                   .withColumn("year" ,   year("start_time"))\
                   .withColumn("weekday", dayofweek("start_time"))\
                   .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data + "time/time.parquet")

    # read in song data to use for songplays table
#     song_df = spark.read.parquet(output_data + "songs/")
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/songs.parquet"))\
                .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how = 'inner')\
                        .withColumn("songplay_id", monotonically_increasing_id())
                        
    
    songplays_table = songplays_table.join(time_table, songplays_table.star_time == time_table.start_time)\
                                     .select(col('songplay_id'), col('start_time'), col('userId'), col('level'),
                                             col('song_id'), col('artist_id'), col('sessionId'), col('location'),
                                             col('userAgent'), col('year'), col('month'))
        
    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.mode('overwrite')\
                                     .partitionBy("year","month").parquet(output_data + "songplays/songplays.parquet")
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udacity-datalake/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
