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
    -- filtering 
    page varchar,
    -- time data
    ts bigint,
    -- user data
    userAgent varchar,
    userId varchar distkey,
    firstName varchar,
    lastName varchar,
    gender char,
    level varchar,
    -- songplay data
    artist varchar,  
    song varchar,
    length varchar
)
""")
#     log_df = log_df[log_df.page == "NextSong"]
#
#     # remove quotes in User Agent field
#     log_df['userAgent'] = log_df['userAgent'].str.replace('"', '')
#
#     # convert timestamp column to datetime
#     log_df['start_time'] = pd.to_datetime(log_df["ts"], unit='ms')
#
#     time_data_df = log_df[['start_time']].copy()
#     datetime = time_data_df.start_time.dt
#     time_data_df['hour'] = datetime.hour
#     time_data_df['day'] = datetime.day
#     time_data_df['week_of_year'] = datetime.week
#     time_data_df['month'] = datetime.month
#     time_data_df['year'] = datetime.year
#     time_data_df['weekday'] = datetime.weekday
#     time_data_df = time_data_df.drop_duplicates(subset=['start_time'])
#     load_into_db(cur, time_data_df, TableNames.TIME)
#
#     user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]].copy()
#     user_df = user_df.drop_duplicates(subset=['userId'])
#     load_into_db(cur, user_df, TableNames.USERS, "user_id", ["level"])
#
#     common_columns = ['song', 'artist', 'length']
#     tuples = [tuple(x) for x in log_df[common_columns].values]
#     df2 = select_song_and_artist_ids(cur, tuples)
#     if not df2.empty:
#         log_df = log_df.merge(df2, how='left', on=common_columns)
#     else:
#         log_df["artist_id"] = np.nan
#         log_df["song_id"] = np.nan
#     log_df['id'] = [str(uuid4()) for _ in range(len(log_df.index))]
#     songplay_data = log_df[
#         ['id', 'start_time', 'userId', 'level', 'song_id', 'artist_id',
#          'sessionId', 'location', 'userAgent']]
#     load_into_db(cur, songplay_data, TableNames.SONGPLAYS)

staging_songs_table_create = (f"""
CREATE TABLE {TableNames.staging_songs}(
--     id bigint not null,
    artist_id varchar,
    artist_name varchar,
    artist_latitude real,
    artist_longitude real,
    artist_location varchar,
    duration real,
    song_id varchar distkey,
    title varchar,
    year smallint
)
""")
# artist table
# "artist_id", "artist_name", "artist_location", "artist_latitude",
#                     "artist_longitude"
# song table
# 'song_id', "title", "artist_id", "year", "duration"

# TODO:
# add constraint songplays__time_fk references time
songplays_table_create = (f"""
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
copy {TableNames.staging_events} 
from {{}} 
iam_role {{}}
COMPUPDATE OFF STATUPDATE OFF
format as json 'auto';
;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = (f"""
copy {TableNames.staging_songs} 
from {{}}
iam_role {{}}
-- from https://stackoverflow.com/questions/57196733/best-methods-for-staging-tables-you-updated-in-redshift
COMPUPDATE OFF STATUPDATE OFF
format as json 'auto';
;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

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
                        time_table_create, songplays_table_create]

DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {} CASCADE;"

drop_table_queries = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.ALL_TABLES]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = []
