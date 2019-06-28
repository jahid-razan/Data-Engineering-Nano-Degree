import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songsplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = " DROP TABLE IF EXISTS time"




# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
    
        artist varchar,
        auth varchar,
        firstname varchar,
        gender varchar,
        iteminsession int,
        lastname varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        session_id int,
        song varchar,
        status int,
        ts bigint,
        useragent varchar,
        user_id int
    )
""")

staging_songs_table_create = ("""

    CREATE TABLE staging_songs (
    
        num_songs int,
        artist_id varchar,
        artist_name varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
""")


songplay_table_create = (""" 

    CREATE TABLE songplays (
    
        songplay_id int identity(0,1) PRIMARY KEY,
        start_time timestamp, 
        user_id int, 
        level varchar,  
        song_id varchar,
        artist_id varchar, 
        session_id int, 
        location varchar, 
        userAgent varchar
    )
""")


user_table_create = (""" 

    CREATE TABLE users (
        user_id integer PRIMARY KEY NOT NULL,
        firstName varchar,
        lastName varchar,
        gender varchar,
        level varchar
    )
""")



song_table_create = (""" 
    CREATE TABLE songs (
        song_id varchar primary key,
        title varchar,
        artist_id varchar,
        year integer,
        duration integer
    )
""")


artist_table_create = (""" 
    CREATE TABLE artists ( 
        artist_id varchar primary key,
        artist_name varchar,
        artist_location varchar,
        artist_latitude numeric,
        artist_longitude numeric
    )
""")


time_table_create = (""" 
    CREATE TABLE time (
        start_time timestamp NOT NULL primary key,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer
    )
""")

# STAGING TABLES

staging_events_copy = (""" 

copy staging_events
from {}
iam_role {}
json {}

""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config ['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""

copy staging_songs
from {}
iam_role {}
json 'auto'

""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
            
            
# FINAL TABLES


songplay_table_insert =  ("""   INSERT into songplays (start_time, user_id, level, song_id, 
                                                       artist_id, session_id, location, userAgent)
                                
                                SELECT  TIMESTAMP 'epoch'+ se.ts/1000*interval '1 second'     AS start_time,
                                         
                                         se.user_id      AS user_id, 
                                         se.level        AS level,
                                         ss.song_id      AS song_id,
                                         ss.artist_id    AS artist_id,
                                         se.session_id   AS session_id,
                                         se.location     AS location,
                                         se.useragent    AS userAgent
                                         
                                         FROM staging_events as se, staging_songs as ss
                                              
                                         WHERE se.page = 'NextSong' AND
                                               se.length = ss.duration AND
                                               se.artist = ss.artist_name AND
                                               se.song = ss.title
                                               
                                                

  
               """)



user_table_insert = ("""    
                            INSERT INTO users (user_id, firstName, lastName, gender, level)
                            
                            SELECT distinct user_id, firstName, lastName, gender, level
                            
                            FROM staging_events
                            
                            WHERE page = 'NextSong'
                  """)





song_table_insert = ("""    INSERT INTO songs (song_id, title, artist_id, year, duration)
                            
                            SELECT song_id, title, artist_id, year, duration
                            
                            FROM staging_songs
                            
                            WHERE song_id IS NOT NULL
                    """)


artist_table_insert = ("""    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)

                            
                              SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            
                              FROM staging_songs
                    """)


time_table_insert = ("""    INSERT INTO time (start_time, hour, day, week, month, year, weekday)

                            
                            SELECT   start_time,
                            EXTRACT (hour  FROM start_time)                            AS hour,
                            EXTRACT (day   FROM start_time)                            AS day,
                            EXTRACT (week  FROM start_time)                            AS week,
                            EXTRACT (month FROM start_time)                            AS month,
                            EXTRACT (year  FROM start_time)                            AS year,
                            EXTRACT(weekday from start_time)                           AS weekday
                            
                            
                            FROM songplays
                    """)

            
            
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]