import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length float,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
         userId INTEGER              
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_location VARCHAR,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
                         songplay_id VARCHAR(32) IDENTITY (1, 1) NOT NULL PRIMARY KEY,
                         start_time TIMESTAMP NOT NULL SORTKEY DISTKEY, 
                         user_id VARCHAR(32),
                         level INTEGER NOT NULL,
                         song_id VARCHAR(32),
                         artist_id VARCHAR(32),
                         session_id VARCHAR(32) NOT NULL,
                         location VARCHAR(32) NOT NULL,
                         user_agent VARCHAR(32) NOT NULL
)
DISTSTYLE KEY;                     
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                     users_id VARCHAR(32) NOT NULL PRIMARY KEY SORTKEY,
                     first_name VARCHAR(32) NOT NULL,
                     last_name VARCHAR(32) NOT NULL,
                     gender CHAR(1) NOT NULL,
                     level INTEGER NOT NULL 
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(32) NOT NULL PRIMARY KEY SORTKEY,
                    title VARCHAR(32) NOT NULL,
                    artist_id VARCHAR(32) NOT NULL,
                    year INTEGER NOT NULL,
                    duration FLOAT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
                       artist_id VARCHAR(32) NOT NULL PRIMARY KEY SORTKEY,
                       location VARCHAR(32) NOT NULL,
                       latitude FLOAT NOT NULL,
                       longitude FLOAT NOT NULL
                  
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                     start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
                     hour INTEGER NOT NULL,
                     day INTEGER NOT NULL,
                     week INTEGER NOT NULL,
                     month INTEGER NOT NULL,
                     year INTEGER NOT NULL,
                     weekday VARCHAR(32) NOT NULL
);
DISTSTYLE KEY;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS json {};               
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['S3'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
    SELECT DISTINCT
        CAST(se.ts as TIMESTAMP) as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        se.location,
        se.user_agent
    FROM staging_songs as ss
    INNER JOIN staging_events as se 
    ON 
        ss.title = se.song  
    AND
        ss.artist_name = se.artist 
    AND 
        se.page = 'NextSong'
""")


user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT
        se.userId as user_id,
        se.firstName as first_name,
        se.last_name,
        se.gender,
        se.level
    FROM staging_events as se
    WHERE
        se.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs as ss
    INNER JOIN staging_events as se
    ON 
        ss.title = se.song
    AND
        ss.artist_name = se.artist
    WHERE
        se.page = 'NextSong'  
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name as name,
        ss.artist_location as location,
        ss.artist_latitude as latitude,
        ss.artist_longitude as longitude
    FROM staging_songs as ss
    WHERE 
        se.page = 'NextSong'  
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT
       CAST(se.ts as TIMESTAMP) as start_time,
       EXTRACT(HOUR from CAST(se.ts as TIMESTAMP)) as hour,
       EXTRACT(DAY from CAST(se.ts as TIMESTAMP)) as day,
       EXTRACT(WEEK from CAST(se.ts as TIMESTAMP)) as week,
       EXTRACT(MONTH from CAST(se.ts as TIMESTAMP)) as month,         
       EXTRACT(YEAR from CAST(se.ts as TIMESTAMP)) as year,
       EXTRACT(WEEKDAY from CAST(se.ts as TIMESTAMP)) as weekday              
    FROM staging_events as se
    WHERE 
        se.page = 'NextSong'  
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
