import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('DEFAULT', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('DEFAULT', 'AWS_SECRET_ACCESS_KEY')
#os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a SparkSession object, which will be used by other functions to utilize PySpark functionality.

    Args:
        None
    Returns:
        (pyspark.sql.session.SparkSession) spark - SparkSession object for processing Spark DataFrames
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("Project : Data Lake") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song datasets in JSON format by Spark DataFrames, and save the processed datasets as parquet files.

    Args:
        (pyspark.sql.session.SparkSession) spark - SparkSession object for processing Spark DataFrames
        (str) input_data - AWS S3 bucket which hosts the Udacity song dataset
        (str) output_data - AWS S3 bucket which is created in advance to save the processed datasets as parquet files
    Returns:
        None 
    """
    # get filepath to song data file
    path_song_data = "{}/{}/*/*/*/*.json".format(input_data, "song_data")
    
    # read song data file
    df_staging_songs = spark.read.json(path_song_data)
    
    # create a temporary view using the Spark DataFrame (i.e., register the DataFrame as a table)
    df_staging_songs.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
    WITH cte_song_id AS 
    (
        SELECT DISTINCT song_id 
        FROM staging_songs 
        WHERE song_id != '' 
        AND song_id IS NOT NULL
    )

    SELECT cte.song_id, ss.title, ss.artist_id, ss.year, ss.duration 
    FROM cte_song_id cte 
    INNER JOIN staging_songs ss ON cte.song_id = ss.song_id
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet("{}/{}".format(output_data, "songs_table"), mode='overwrite', partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = spark.sql("""
    WITH cte_artist_id AS 
    (
        SELECT DISTINCT artist_id 
        FROM staging_songs 
        WHERE artist_id != '' 
        AND artist_id IS NOT NULL
    )

    SELECT cte.artist_id, ss.artist_name AS name, ss.artist_location, ss.artist_latitude, ss.artist_longitude 
    FROM cte_artist_id cte 
    INNER JOIN staging_songs ss ON cte.artist_id = ss.artist_id
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet("{}/{}".format(output_data, "artists_table"), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Processes log datasets in JSON format by Spark DataFrames, and save the processed datasets as parquet files.

    Args:
        (pyspark.sql.session.SparkSession) spark - SparkSession object for processing Spark DataFrames
        (str) input_data - AWS S3 bucket which hosts the Udacity log dataset
        (str) output_data - AWS S3 bucket which is created in advance to save the processed datasets as parquet files
    Returns:
        None 
    """
    # get filepath to log data file
    path_log_data = "{}/{}/*/*/*.json".format(input_data, "log_data")

    # read log data file
    df_staging_events = spark.read.json(path_log_data)
    
    # filter by actions for song plays
    df_staging_events = df_staging_events.filter(df_staging_events.page == 'NextSong')
    
    # create a temporary view using the Spark DataFrame (i.e., register the DataFrame as a table)
    df_staging_events.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    users_table = spark.sql("""
    WITH cte_user_id AS 
    (
        SELECT DISTINCT userId 
        FROM staging_events 
        WHERE userId != '' 
        AND userId IS NOT NULL
    ),
    cte_staging_events_ranked AS 
    (
        SELECT DISTINCT userId, firstName, lastName, gender, level, ts, 
        ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS rank 
        FROM staging_events 
        WHERE userId != '' 
        AND userId IS NOT NULL 
        ORDER BY userId, rank
    )

    SELECT DISTINCT cte.userId, se.firstName, se.lastName, se.gender, se.level 
    FROM cte_user_id cte 
    INNER JOIN (SELECT * FROM cte_staging_events_ranked ser WHERE ser.rank = 1) se ON cte.userId = se.userId 
    ORDER BY userId
    """)
    
    # write users table to parquet files
    users_table.write.parquet("{}/{}".format(output_data, "users_table"), mode='overwrite')

    # create timestamp column from original timestamp column (NOT NEEDED WHEN USING SPARK SQL)
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column (NOT NEEDED WHEN USING SPARK SQL)
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
    WITH cte_ts AS 
    (
        SELECT DISTINCT ts, to_timestamp(ts/1000) AS start_time 
        FROM staging_events 
        WHERE ts IS NOT NULL
    )

    SELECT start_time, 
    EXTRACT(hour FROM start_time) AS hour, 
    EXTRACT(day FROM start_time) AS day, 
    EXTRACT(week FROM start_time) AS week, 
    EXTRACT(month FROM start_time) AS month, 
    EXTRACT(year FROM start_time) AS year, 
    EXTRACT(dayofweek FROM start_time) AS weekday 
    FROM cte_ts
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet("{}/{}".format(output_data, "time_table"), mode='overwrite', partitionBy=["year", "month"])

    # read in song data to use for songplays table
    #song_df = 
    df_par_songs = spark.read.parquet("{}/{}".format(output_data, "songs_table"))
    df_par_artists = spark.read.parquet("{}/{}".format(output_data, "artists_table"))
    
    df_par_songs.createOrReplaceTempView("parquet_songs_table")
    df_par_artists.createOrReplaceTempView("parquet_artists_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    WITH cte_song_data AS 
    (
        SELECT s.song_id, s.title, s.artist_id, a.name, s.duration 
        FROM parquet_songs_table s 
        INNER JOIN parquet_artists_table a ON s.artist_id = a.artist_id 
    )

    SELECT monotonically_increasing_id() as songplay_id, 
    to_timestamp(se.ts/1000) AS start_time, 
    se.userId, 
    se.level, 
    cte.song_id, 
    cte.artist_id, 
    se.sessionId, 
    se.location, 
    se.userAgent, 
    EXTRACT(month FROM to_timestamp(se.ts/1000)) AS month, 
    EXTRACT(year FROM to_timestamp(se.ts/1000)) AS year 
    FROM staging_events se 
    LEFT OUTER JOIN cte_song_data cte ON se.song = cte.title AND se.artist = cte.name AND se.length = cte.duration 
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet("{}/{}".format(output_data, "songplays_table"), mode='overwrite', partitionBy=["year", "month"])


def main():
    """
    - Creates a SparkSession. 
    
    - Use the SparkSession to process project datasets stored in a S3 bucket managed by Udacity.
    
    - Save the processed in-memory Spark DataFrames to a S3 bucket managed by the student. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-dend-bucket-asthlihzxhfc"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
