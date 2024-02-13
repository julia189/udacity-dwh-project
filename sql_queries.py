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
        length FLOAT,
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
                         songplay_id INTEGER IDENTITY (100, 1) NOT NULL PRIMARY KEY,
                         start_time TIMESTAMP NOT NULL, 
                         user_id VARCHAR(32),
                         level VARCHAR(32),
                         song_id VARCHAR(32),
                         artist_id VARCHAR(32),
                         session_id VARCHAR(32),
                         location VARCHAR(32),
                         user_agent VARCHAR(32)
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);                     
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                     user_id VARCHAR(32) NOT NULL PRIMARY KEY,
                     first_name VARCHAR(32),
                     last_name VARCHAR(32),
                     gender CHAR(1),
                     level VARCHAR(32)
)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(32) NOT NULL PRIMARY KEY,
                    title VARCHAR(32),
                    artist_id VARCHAR(32),
                    year INTEGER,
                    duration FLOAT
)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
                       artist_id VARCHAR(32) NOT NULL PRIMARY KEY,
                       location VARCHAR(32),
                       latitude VARCHAR,
                       longitude VARCHAR
)
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                     start_time TIMESTAMP NOT NULL PRIMARY KEY,
                     hour INTEGER NOT NULL,
                     day INTEGER NOT NULL,
                     week INTEGER NOT NULL,
                     month INTEGER NOT NULL,
                     year INTEGER NOT NULL,
                     weekday VARCHAR(32) NOT NULL
)
DISTSTYLE KEY
DISTKEY (start_time)
SORTKEY (start_time);
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
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second') as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        se.location,
        se.userAgent as user_agent
    FROM staging_songs as ss
    INNER JOIN staging_events as se 
    ON 
        ss.title = se.song  
    AND
        ss.artist_name = se.artist 
    AND 
        se.page = 'NextSong';
""")


user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT
        se.userId as user_id,
        se.firstName as first_name,
        se.lastName as last_name,
        se.gender,
        se.level
    FROM staging_events as se
    WHERE
        se.page = 'NextSong';
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
    AND
        ss.song_id IS NOT NULL;
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
    INNER JOIN staging_events as se
    ON 
        ss.title = se.song
    AND
        ss.artist_name = se.artist
    WHERE
        se.page = 'NextSong' 
""")

time_table_insert = ("""
    INSERT INTO time
    WITH start_time_formatted as (
    SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as start_time
    FROM staging_events
    WHERE staging_events.page = 'NextSong'
    )
    SELECT DISTINCT
        start_time,
        EXTRACT(HOUR from start_time) as hour,
        EXTRACT(DAY from start_time) as day,
        EXTRACT(WEEK from start_time) as week,
        EXTRACT(MONTH from start_time) as month,         
        EXTRACT(YEAR from start_time) as year,
        EXTRACT(WEEKDAY from start_time) as weekday
    FROM start_time_formatted 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
