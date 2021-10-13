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
# {
#   "artist": "Des'ree",
#   "auth": "Logged In",
#   "firstName": "Kaylee",
#   "gender": "F",
#   "itemInSession": 1,
#   "lastName": "Summers",
#   "length": 246.30812,
#   "level": "free",
#   "location": "Phoenix-Mesa-Scottsdale, AZ",
#   "method": "PUT",
#   "page": "NextSong",
#   "registration": 1540344794796.0,
#   "sessionId": 139,
#   "song": "You Gotta Be",
#   "status": 200,
#   "ts": 1541106106796,
#   "userAgent": "\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
#   "userId": "8"
# }
staging_events_table_create = (f"""
CREATE TABLE {TableNames.staging_events}(
    -- filtering 
    page varchar,  -- candidate for sortkey
    -- time data
    ts bigint,
    -- user data
    useragent varchar,
    userid varchar,
    firstname varchar,
    lastname varchar,
    gender char,
    level varchar,
    -- songplay data
    artist varchar,  
    song varchar,
    length real,
    sessionid bigint,
    location varchar
)
""")
# To pick sort key
# LEFT JOIN {TableNames.ARTISTS} a ON a.name=st.artist
#     LEFT JOIN {TableNames.SONGS} s ON s.title=st.song AND s.duration=st.length

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
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    duration real,
    song_id varchar,
    title varchar,
    year smallint
)
""")
# # open song file
# df = pd.read_json(path_or_buf=filepath, lines=True)

#
# # insert artist records
# artist_df = df[["artist_id", "artist_name", "artist_location", "artist_latitude",
#                 "artist_longitude"]]
# load_into_db(cur, artist_df, TableNames.ARTISTS)
#
# # insert song records
# song_df = df[['song_id', "title", "artist_id", "year", "duration"]]
# load_into_db(cur, song_df, TableNames.SONGS)


# TODO:
# add
songplays_table_create = (f"""
CREATE TABLE {TableNames.SONGPLAYS}(
    id bigint IDENTITY(1,1) distkey,
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

user_table_create = (f"""
CREATE TABLE {TableNames.USERS} (
    user_id bigint not null constraint users_pk primary key distkey,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
    )
""")

# CHECK (duration >0)
song_table_create = (f"""
CREATE TABLE {TableNames.SONGS} (
    song_id varchar not null constraint songs_pk primary key distkey,
    title varchar not null unique,
    artist_id varchar constraint songs__artist_fk references artists,
    year smallint,
    duration real not null 
    )

""")

# TODO:
#  CHECK (latitude >= -90 AND latitude <= 90)
# CHECK (latitude >= -180 AND latitude <= 180)
# TODO: change numeric type to real
artist_table_create = (f"""
CREATE TABLE {TableNames.ARTISTS}(
    artist_id varchar not null constraint artists_pk primary key distkey,
    name varchar not null unique,
    location varchar,
    latitude real,
    longitude real 
    )
""")

time_table_create = (f"""
CREATE TABLE {TableNames.TIME}(
    start_time timestamp not null constraint time_pk primary key distkey,
    hour smallint not null,
    day smallint not null,
    week smallint not null,
    month smallint not null,
    year smallint not null,
    weekday smallint not null 
)
""")

# STAGING TABLES

staging_events_copy = (f"""
copy {TableNames.staging_events} 
from {{}} 
iam_role {{}}
COMPUPDATE OFF STATUPDATE OFF
format as json 'auto ignorecase';
;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['IAM_ROLE']['ARN'])

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


#     log_df = log_df[log_df.page == "NextSong"]
#
#     # remove quotes in User Agent field
#     log_df['userAgent'] = log_df['userAgent'].str.replace('"', '')
#
#     # convert timestamp column to datetime
#     log_df['start_time'] = pd.to_datetime(log_df["ts"], unit='ms')
#
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

songplay_table_insert = (f"""
	INSERT INTO {TableNames.SONGPLAYS}( start_time, user_id, level,
	 song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
    cast(userid as bigint) as user_id, 
    level,
    s.song_id,
    a.artist_id,
    sessionid as session_id,
    st.location,
    BTRIM(useragent,'"') as user_agent
    FROM {TableNames.staging_events} st
    LEFT JOIN {TableNames.ARTISTS} a ON a.name=st.artist
    LEFT JOIN {TableNames.SONGS} s ON s.title=st.song AND s.duration=st.length 
    WHERE st.page='NextSong'
""")
# -- filtering
# page varchar,
# -- time data
# ts bigint,
# -- user data
# useragent varchar,
# userid varchar,
# firstname varchar,
# lastname varchar,
# gender char,
# level varchar,
# -- songplay data
# artist varchar,
# song varchar,
# length real,
# sessionid bigint,
# location varchar

#     log_df = log_df[log_df.page == "NextSong"]
#
#     user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]].copy()
#     user_df = user_df.drop_duplicates(subset=['userId'])
#     load_into_db(cur, user_df, TableNames.USERS, "user_id", ["level"])

# TODO: add update
# Copied the idea on how replace empty values to 0 from
# https://stackoverflow.com/questions/49372415/invalid-digits-on-redshift/52861588
user_table_insert = (f"""
   
    INSERT INTO {TableNames.USERS}(user_id,first_name,last_name,gender,level)  
    -- filter out records with user id=null       
     with cte as (
        SELECT 
        case when userid ~ '^[0-9]+$' then cast(userid as bigint)
        end as user_id,
        firstname as first_name, 
        lastname as last_name,
        gender, 
        level,
        ts FROM {TableNames.staging_events}
        WHERE page='NextSong' 
    ), 
    -- select records unique user ids with latest timestamp      
    st as (
        SELECT user_id,first_name,last_name,gender,level FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY ts DESC) FROM cte
        )
        WHERE row_number=1
    )
    
    SELECT st.* FROM st  
    LEFT OUTER JOIN {TableNames.USERS} b 
    ON st.user_id IS NOT NULL AND st.user_id=b.user_id 
    WHERE b.user_id is null;
""")

song_table_insert = (f"""
  -- INSERT NEW ROWS ONLY
    insert into {TableNames.SONGS}(song_id, title,artist_id,year,duration)  
    select  st.song_id, st.title, st.artist_id, 
    case when st.year=0 then NULL
    else st.year
    end,     
    st.duration  FROM
    {TableNames.staging_songs} st
    left outer join {TableNames.SONGS} b using(song_id)
    where b.song_id is null;
""")

artist_table_insert = (f"""
    -- INSERT NEW ROWS ONLY
    INSERT INTO {TableNames.ARTISTS}(artist_id,name,location,latitude,longitude)  
    SELECT  artist_id, artist_name, artist_location, artist_latitude,
                     artist_longitude FROM {TableNames.staging_songs}
    LEFT OUTER JOIN {TableNames.ARTISTS} b USING(artist_id)
    WHERE b.artist_id is null;
""")


# TODO: drop duplicates inside staging table

# Idea on how convert to timestamp from https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
time_table_insert = (f"""
    INSERT INTO {TableNames.TIME}(start_time,hour,day,week,month,year,weekday)  
    SELECT st.* FROM (SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time, 
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(weekday from start_time) as weekday
    FROM (SELECT DISTINCT ts FROM {TableNames.staging_events} WHERE page='NextSong')
    ) st  
    LEFT OUTER JOIN {TableNames.TIME} b 
    ON st.start_time = b.start_time
    WHERE b.start_time is null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create,
                        time_table_create, songplays_table_create]

DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {} CASCADE;"

drop_table_queries = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.ALL_TABLES]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, time_table_insert,
                        user_table_insert, songplay_table_insert]
