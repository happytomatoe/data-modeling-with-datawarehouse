import configparser


class TableNames:
    TIME = "time"
    ARTISTS = "artists"
    SONGS = "songs"
    USERS = "users"
    SONGPLAYS = "songplays"
    staging_events = "staging_events"
    staging_songs = "staging_songs"
    ALL_TABLES = [TIME, ARTISTS, SONGPLAYS, SONGS, USERS, staging_events, staging_songs]


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create = ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE {TableNames.SONGPLAYS}(
    id uuid not null constraint songplays_pk primary key,
    start_time timestamp not null constraint songplays__time_fk references time,
    user_id bigint constraint songplays__user_fk references users,
    level varchar,
    song_id varchar constraint songplays__songs_fk references songs,
    artist_id varchar constraint songplays__artist_fk references artists,
    session_id bigint,
    location varchar,
    user_agent varchar
    );
""")

user_table_create = ("""
CREATE TABLE {TableNames.USERS} (
    user_id bigint not null constraint users_pk primary key,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    level varchar
    )
""")

song_table_create = ("""
CREATE TABLE {TableNames.SONGS} (
    song_id varchar not null constraint songs_pk primary key,
    title varchar not null unique,
    artist_id varchar constraint songs__artist_fk references artists,
    year integer,
    duration numeric not null CHECK (duration >0)
    )

""")

artist_table_create = ("""
CREATE TABLE {TableNames.ARTISTS}(
    artist_id varchar not null constraint artists_pk primary key,
    name varchar not null unique,
    location varchar,
    latitude numeric CHECK (latitude >= -90 AND latitude <= 90),
    longitude numeric CHECK (latitude >= -180 AND latitude <= 180)
    )
""")

time_table_create = ("""
CREATE TABLE {TableNames.TIME}(
    start_time timestamp not null constraint time_pk primary key,
    hour integer not null,
    day integer not null,
    week integer not null,
    month integer not null,
    year integer not null,
    weekday integer not null 
)
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

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {};"

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert]
