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
    staging_tables = [staging_events, staging_songs]
    ALL_TABLES_WITHOUT_STAGING = [TIME, ARTISTS, SONGPLAYS, SONGS, USERS]


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

songplays_table_create = (f"""
CREATE TABLE {TableNames.SONGPLAYS}(
    id bigint IDENTITY(1,1) distkey,
    start_time timestamp not null constraint songplays__time_fk references time,
    user_id char(18) constraint songplays__user_fk references users,
    level char(4),
    song_id char(18) constraint songplays__songs_fk references songs,
    artist_id char(18) constraint songplays__artist_fk references artists,
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
    level char(4)
    )
""")

# CHECK (duration >0)
song_table_create = (f"""
CREATE TABLE {TableNames.SONGS} (
    song_id char(18) not null constraint songs_pk primary key distkey,
    title varchar not null unique,
    artist_id char(18) constraint songs__artist_fk references artists,
    year smallint,
    duration real not null 
    )

""")

# TODO:
#  CHECK (latitude >= -90 AND latitude <= 90)
# CHECK (latitude >= -180 AND latitude <= 180)
# TODO: add check if artist name is unique
artist_table_create = (f"""
CREATE TABLE {TableNames.ARTISTS}(
    artist_id char(18) not null constraint artists_pk primary key distkey,
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

staging_events_table_create = (f"""
CREATE TABLE {TableNames.staging_events}(
    -- filtering 
    page varchar,  -- candidate for sortkey
    -- time data
    ts timestamp,
    -- user data
    useragent varchar(50),
    userid char(18),
    firstname varchar,
    lastname varchar,
    gender char,
    level char(4),
    -- songplay data
    artist varchar,  
    song varchar,
    length real,
    sessionid bigint,
    location varchar
)
""")

# TODO: pick sort keys for tables
staging_songs_table_create = (f"""
CREATE TABLE {TableNames.staging_songs}(
    artist_id char(18),
    artist_name varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    duration real,
    song_id varchar(18) distkey,
    title varchar,
    year smallint
)
""")

staging_events_copy = (f"""
    COPY {TableNames.staging_events} 
    FROM {{}} 
    IAM_ROLE {{}}
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT AS JSON 'auto ignorecase'
    TIMEFORMAT AS 'epochmillisecs'
    BLANKSASNULL;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['IAM_ROLE']['ARN'])

# Copied the flags to speed up copy command  from
# https://stackoverflow.com/questions/57196733/best-methods-for-staging-tables-you-updated-in-redshift
staging_songs_copy = (f"""
    COPY {TableNames.staging_songs} 
    FROM {{}}
    IAM_ROLE {{}}
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = (f"""
    INSERT INTO {TableNames.SONGPLAYS}(start_time, user_id, level,
    song_id, artist_id, session_id, location, user_agent)
    SELECT  ts AS start_time,
    cast(userid as bigint) as user_id, 
    level,
    s.song_id,
    a.artist_id,
    sessionid as session_id,
    st.location,
    btrim(useragent,'"') as user_agent
    FROM {TableNames.staging_events} st
    LEFT JOIN {TableNames.ARTISTS} a ON a.name=st.artist
    LEFT JOIN {TableNames.SONGS} s ON s.title=st.song AND s.duration=st.length 
    WHERE st.page='NextSong'
""")

# TODO: add update
user_table_insert = (f"""
    INSERT INTO {TableNames.USERS}(user_id,first_name,last_name,gender,level)  
    WITH cte AS (
        SELECT 
        userid::bigint as user_id,
        firstname as first_name, 
        lastname as last_name,
        gender, 
        level,
        ts FROM {TableNames.staging_events}
        WHERE page='NextSong' AND userid IS NOT NULL 
    ), 
    -- select records unique user ids with latest timestamp      
    st AS (
        SELECT user_id,first_name,last_name, gender, level FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY ts DESC) FROM cte
        )
        WHERE row_number=1
    )
    
    SELECT st.* FROM st  
    LEFT OUTER JOIN {TableNames.USERS} u
    ON st.user_id IS NOT NULL AND st.user_id=u.user_id 
    WHERE u.user_id IS NULL;
""")

# TODO: figure out how to optimize
song_table_insert = (f"""
  -- INSERT NEW ROWS ONLY
    INSERT INTO {TableNames.SONGS}(song_id, title,artist_id,year,duration)  
    WITH cte AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY song_id) as rn FROM {TableNames.staging_songs}
    ) 
    SELECT DISTINCT  st.song_id, st.title, st.artist_id, 
    CASE WHEN st.year=0 then NULL
    ELSE st.year
    END,     
    st.duration  FROM cte st
    LEFT OUTER JOIN {TableNames.SONGS} s USING(song_id)
    WHERE s.song_id IS NULL AND st.rn=1;
""")

artist_table_insert = (f"""
    -- INSERT NEW ROWS ONLY
    INSERT INTO {TableNames.ARTISTS}(artist_id,name,location,latitude,longitude)  
    WITH cte AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY artist_id) as rn FROM {TableNames.staging_songs}
    ) 
    SELECT artist_id, artist_name, artist_location, artist_latitude,
                     artist_longitude  FROM cte
    LEFT OUTER JOIN {TableNames.ARTISTS} a USING(artist_id)
    WHERE a.artist_id IS NULL AND cte.rn=1;
""")

time_table_insert = (f"""
    INSERT INTO {TableNames.TIME}(start_time,hour,day,week,month,year,weekday)  
    SELECT st.* FROM (SELECT ts AS start_time, 
    extract(hour from start_time) as hour,
    EXTRACT(day from start_time) as day,
    EXTRACT(week from start_time) as week,
    EXTRACT(month from start_time) as month,
    EXTRACT(year from start_time) as year,
    EXTRACT(weekday from start_time) as weekday
    FROM (SELECT DISTINCT ts FROM {TableNames.staging_events} WHERE page='NextSong')
    ) st  
    LEFT OUTER JOIN {TableNames.TIME} t 
    ON st.start_time = t.start_time
    WHERE t.start_time IS NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create,
                        time_table_create, songplays_table_create]

# DROP TABLES
DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {} CASCADE;"

drop_table_queries = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.ALL_TABLES]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, time_table_insert,
                        user_table_insert, songplay_table_insert]

# TODO: delete
create__non_staging_tables_queries = [user_table_create, artist_table_create, song_table_create,
                                      time_table_create, songplays_table_create]
drop_non_staging_tables_queries = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in
                                   TableNames.ALL_TABLES_WITHOUT_STAGING]
