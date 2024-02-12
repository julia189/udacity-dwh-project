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
        iteminSession INTEGER,
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
        title VARCHAR,
        year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
                         songplay_id VARCHAR(32) NOT NULL PRIMARY KEY,
                         start_time TIMESTAMP NOT NULL SORTKEY DISTKEY, 
                         user_id VARCHAR(32),
                         level INTEGER NOT NULL,
                         song_id VARCHAR(32),
                         artist_id VARCHAR(32)  ,
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
                     gender VARCHAR(5) NOT NULL,
                     level INTEGER NOT NULL 
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(32) NOT NULL PRIMARY KEY SORTKEY,
                    title VARCHAR(32) NOT NULL,
                    artist_id VARCHAR(32) NOT NULL,
                    year INTEGER NOT NULL,
                    duration INTEGER NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
                       artist_id VARCHAR(32) NOT NULL PRIMARY KEY SORTKEY,
                       location VARCHAR(32) NOT NULL,
                       latitude float NOT NULL,
                       longitude float NOT NULL
                  
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                     start_time date NOT NULL PRIMARY KEY SORTKEY DISTKEY,
                     hour integer NOT NULL,
                     day integer NOT NULL,
                     week integer NOT NULL,
                     month integer NOT NULL,
                     year integer NOT NULL,
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
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['S3'][])

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
