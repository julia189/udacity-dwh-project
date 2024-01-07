import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP staging_events IF EXISTS;"
staging_songs_table_drop = "DROP staging_songs IF EXISTS;"
songplay_table_drop = "DROP songplays IF EXISTS;"
user_table_drop = "DROP users IF EXISTS;"
song_table_drop = "DROP songs IF EXISTS;"
artist_table_drop = "DROP artists IF EXISTS;"
time_table_drop = "DROP time IF EXISTS;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
                        
)
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
                         songplay_id varchar(32) NOT NULL PRIMARY KEY,
                         start_time date FOREIGN KEY REFERENCES time(start_time),  -- #TODO: partition key??
                         user_id varchar(32) FOREIGN KEY REFERENCES users(user_id),
                         level int NOT NULL,
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
                     level int NOT NULL 

);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                    song_id varchar(32) NOT NULL,
                    title varchar(32) NOT NULL,
                    artist_id varchar(32) NOT NULL,
                    year int NOT NULL,
                    duration int NOT NULL -- #TODO: check if float
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
                     hour int NOT NULL,
                     day int NOT NULL,
                     week int NOT NULL,
                     month int NOT NULL,
                     year int NOT NULL,
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
