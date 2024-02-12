import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events IF EXISTS;"
staging_songs_table_drop = "DROP TABLE staging_songs IF EXISTS;"
songplay_table_drop = "DROP TABLE songplays IF EXISTS;"
user_table_drop = "DROP TABLE users IF EXISTS;"
song_table_drop = "DROP TABLE songs IF EXISTS;"
artist_table_drop = "DROP TABLE artists IF EXISTS;"
time_table_drop = "DROP TABLE time IF EXISTS;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char,
        iteminSession integer,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration long,
        sessionId integer,
        song varchar,
        status integer,
        ts long,
        userAgent varchar,
        userId integer                
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
                              
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
                         songplay_id varchar(32) NOT NULL PRIMARY KEY,
                         start_time date FOREIGN KEY REFERENCES time(start_time),  -- #TODO: partition key??
                         user_id varchar(32) FOREIGN KEY REFERENCES users(user_id),
                         level integer NOT NULL,
                         song_id varchar(32) FOREIGN KEY REFERENCES songs(song_id),
                         artist_id varchar(32)  FOREIGN KEY REFERENCES artis(artist_id),
                         session_id varchar(32) NOT NULL,
                         location varchar(32) NOT NULL,
                         user_agent varchar(32) NOT NULL

);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                     users_id varchar(32) NOT NULL PRIMARY KEY,
                     first_name varchar(32) NOT NULL,
                     last_name varchar(32) NOT NULL,
                     gender varchar(5) NOT NULL,
                     level integer NOT NULL 

);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                    song_id varchar(32) NOT NULL,
                    title varchar(32) NOT NULL,
                    artist_id varchar(32) NOT NULL,
                    year integer NOT NULL,
                    duration integer NOT NULL -- #TODO: check if float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
                       artist_id varchar(32) NOT NULL PRIMARY KEY,
                       location varchar(32) NOT NULL,
                       latitude float NOT NULL,
                       longitude float NOT NULL
                  
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                     start_time date NOT NULL PRIMARY KEY,
                     hour integer NOT NULL,
                     day integer NOT NULL,
                     week integer NOT NULL,
                     month integer NOT NULL,
                     year integer NOT NULL,
                     weekday varchar(32) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

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
