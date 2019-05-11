# DROP TABLES if already exists as we are going to create these tables and insert data 

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

#create songplays table
songplay_table_create = ("""  CREATE TABLE IF NOT EXISTS songplays ( song_play_id serial primary key,
                                                                     song_id varchar, 
                                                                     start_time timestamp,
                                                                     userId integer,
                                                                     level varchar, 
                                                                     artist_id varchar, 
                                                                     sessionId integer, 
                                                                     location varchar, 
                                                                     userAgent varchar)
                         """)
                                                                     
                                                                     

#create users table
user_table_create = ( """ CREATE TABLE IF NOT EXISTS users ( userId integer primary key NOT NULL,
                                                                 firstName varchar,
                                                                 lastName varchar,
                                                                 gender varchar,
                                                                 level varchar)
                                                        
                  """)


#create songs table
song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key,
                                                          title varchar,
                                                          artist_id varchar,
                                                          year integer,
                                                          duration integer)
                                                          
                    """)


#create artists table
artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists ( artist_id varchar primary key,
                                                                artist_name varchar,
                                                                artist_location varchar,
                                                                artist_latitude numeric,
                                                                artist_longitude numeric)
                                                         
                        """)

#create time table
time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL primary key,
                                                          hour integer,
                                                          day integer,
                                                          week integer,
                                                          month integer,
                                                          year integer,
                                                          weekday varchar)
                    """)


# INSERT RECORDS into all the tables created
songplay_table_insert = ("""

                            INSERT INTO songplays (song_id, start_time, 
                            userId, level, artist_id, sessionId, location, userAgent)
                            
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        

                      """)

user_table_insert = ("""

                            INSERT INTO users (userId, firstName, lastName, gender, level)
                            
                            VALUES (%s, %s, %s, %s, %s)
                            
                            ON CONFLICT (userId) DO UPDATE SET level = EXCLUDED.level


                    """)


song_table_insert = ("""

                            INSERT INTO songs (song_id, title, artist_id, year, duration)
                            
                            VALUES (%s, %s, %s, %s, %s)
                            
                            ON CONFLICT (song_id) DO NOTHING

                   """)

artist_table_insert = ("""

                            INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                            
                            VALUES (%s, %s, %s, %s, %s)
                            
                            ON CONFLICT (artist_id) DO NOTHING

                      """)


time_table_insert = (""" 
                            INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                            
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            
                            ON CONFLICT (start_time) DO NOTHING
                            
                            
                            
                    """)

# FIND SONGS

song_select = (""" 
                   SELECT songs.song_id, artists.artist_id
                   FROM songs 
                   LEFT JOIN artists 
                   ON songs.artist_id = artists.artist_id
                   WHERE songs.title = (%s) AND artists.artist_name = (%s) AND songs.duration = (%s)
              """)





# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]