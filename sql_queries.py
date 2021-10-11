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


# CREATE TABLES

staging_events_table_create = (f"""
CREATE TABLE {TableNames.staging_events}(
    id bigint not null 
)
""")

staging_songs_table_create = (f"""
CREATE TABLE {TableNames.staging_songs}(
    id bigint not null 
)
""")

# TODO:
# add constraint songplays__time_fk references time
songplay_table_create = (f"""
CREATE TABLE {TableNames.SONGPLAYS}(
    id varchar not null constraint songplays_pk primary key,
    start_time timestamp not null,
    user_id bigint constraint songplays__user_fk references users,
    level varchar,
    song_id varchar constraint songplays__songs_fk references songs,
    artist_id varchar constraint songplays__artist_fk references artists,
    session_id bigint,
    location varchar,
    user_agent varchar
    );
""")

user_table_create = (f"""
CREATE TABLE {TableNames.USERS} (
    user_id bigint not null constraint users_pk primary key,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    level varchar
    )
""")

# CHECK (duration >0)
song_table_create = (f"""
CREATE TABLE {TableNames.SONGS} (
    song_id varchar not null constraint songs_pk primary key,
    title varchar not null unique,
    artist_id varchar constraint songs__artist_fk references artists,
    year integer,
    duration numeric not null 
    )

""")

# TODO:
#  CHECK (latitude >= -90 AND latitude <= 90)
# CHECK (latitude >= -180 AND latitude <= 180)
artist_table_create = (f"""
CREATE TABLE {TableNames.ARTISTS}(
    artist_id varchar not null constraint artists_pk primary key,
    name varchar not null unique,
    location varchar,
    latitude numeric,
    longitude numeric 
    )
""")

time_table_create = (f"""
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

staging_events_copy = (f"""
""").format()

staging_songs_copy = (f"""
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
                        user_table_create, artist_table_create, song_table_create,
                        time_table_create, songplay_table_create]

DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {};"

drop_table_queries = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.ALL_TABLES]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = []
