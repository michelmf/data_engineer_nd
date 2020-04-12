import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# name_object
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
users_table_drop          = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"


##################################################################
# CREATE TABLES
# Based on the example depict on project datasets section
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
   (artist           VARCHAR,
    auth             VARCHAR,
    firstname        VARCHAR,
    gender           VARCHAR,
    itemInSession    INTEGER,
    lastname         VARCHAR,
    length           FLOAT,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     FLOAT,
    sessionId        INTEGER,
    song             VARCHAR,
    status           INTEGER,
    ts               TIMESTAMP,
    userAgent        VARCHAR,
    userId           INTEGER)
""")

# Example of song dataset - {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", 
# "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
   (num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (user_id    INTEGER PRIMARY KEY,
     first_name VARCHAR NOT NULL,
     last_name  VARCHAR NOT NULL,
     gender     VARCHAR NOT NULL,
     level      VARCHAR NOT NULL)
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs
    (song_id   VARCHAR SORTKEY PRIMARY KEY, 
     title     VARCHAR NOT NULL,
     artist_id VARCHAR NOT NULL,
     year      INTEGER NOT NULL,
     duration  FLOAT   NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
    (artist_id VARCHAR SORTKEY PRIMARY KEY,
     name      VARCHAR NOT NULL,
     location  VARCHAR, 
     latitude  FLOAT,
     longitude FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (start_time TIMESTAMP SORTKEY PRIMARY KEY,
     weekday    VARCHAR NOT NULL,
     hour       INTEGER NOT NULL,
     day        INTEGER NOT NULL,
     week       INTEGER NOT NULL,
     month      INTEGER NOT NULL,
     year       INTEGER NOT NULL);
""")

# Using the same schema of the last projects, changing the types of each field
# Since we are dealing with a exclusive/unique id for each songplay event, we can consider using identity
# with incremental steps of 1 - https://docs.aws.amazon.com/pt_br/redshift/latest/dg/r_CREATE_TABLE_NEW.html

#In this table, it makes sense to use the start_time field as distribution and sorting keys, since it could speeds up a query based on time 
songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplays      
    (songplay_id INTEGER   IDENTITY(0,1) PRIMARY KEY, 
     user_id     INTEGER   NOT NULL, 
     artist_id   VARCHAR   NOT NULL, 
     start_time  TIMESTAMP NOT NULL, 
     song_id     VARCHAR   NOT NULL,
     session_id  INTEGER   NOT NULL, 
     level       VARCHAR   NOT NULL, 
     location    VARCHAR, 
     user_agent  VARCHAR   NOT NULL)
""")
#############################################################################
# STAGING TABLES

staging_events_copy =  (""" 
    COPY staging_events from {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    JSON {}
    COMPUPDATE OFF STATUPDATE OFF
    timeformat as 'epochmillisecs'
 """.format(config.get('S3','log_data'), config.get('IAM_ROLE','arn'), config.get('S3', 'log_jsonpath')))

staging_songs_copy = (""" 
    COPY staging_songs from {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
 """.format(config.get('S3', 'song_data'), config.get('IAM_ROLE','arn')))

#############################################################################
# Inserting distinct users from log files
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    
        SELECT DISTINCT userId   as user_id,
                        firstName as first_name,
                        lastName  as last_name,
                                     gender,
                                     level
        FROM staging_events
        WHERE user_id IS NOT NULL AND page = 'NextSong';
    
 """)

# Inserting only distinct songs from records
song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    
        SELECT DISTINCT song_id AS song_id, 
        title,
        artist_id,
        year,
        duration
    
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    

""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    
        SELECT DISTINCT artist_id AS artist_id,
                                    artist_name,
                                artist_location,
                                artist_latitude,
                               artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    
""")
# Using SQL Extract Function to get time fields, avoiding python coding
# To get time from fields https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
time_table_insert = ("""
    INSERT INTO time (start_time, weekday, hour, day, week, month, year)
    
        SELECT DISTINCT start_time               AS start_time,
               EXTRACT(dayofweek FROM start_time) AS weekday,
               EXTRACT(hour FROM start_time)      AS hour,
               EXTRACT(day  FROM start_time)      AS day,
               EXTRACT(week FROM start_time)      AS week,
               EXTRACT(month FROM start_time)     AS month,
               EXTRACT(year FROM start_time)      AS year
        
        FROM songplays;
    

""")

# songplays - records in event data associated with song plays 
# i.e. records with page NextSong
songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id, artist_id, song_id, session_id, level, location, user_agent)
    
    SELECT DISTINCT staging_events.ts   AS start_time,
            staging_events.userId       AS user_id,
            staging_songs.artist_id     AS artist_id,
            staging_songs.song_id       AS song_id,
            staging_events.sessionId    AS session_id,
            staging_events.level        AS level,
            staging_events.location     AS location,
            staging_events.userAgent    AS user_agent

        FROM staging_events

        JOIN 
            staging_songs ON (staging_events.artist = staging_songs.artist_name AND staging_events.song = staging_songs.title)
        
        AND staging_events.page = 'NextSong'; 

    
""")

# QUERY LISTS
# CREATE TABLES
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# DROP TABLES
drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# COPY TABLES
copy_table_queries   = [staging_events_copy, staging_songs_copy]

# INSERT TABLES
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

