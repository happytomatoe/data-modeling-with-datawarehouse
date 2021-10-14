import configparser


class TableNames:
    TIME = "time"
    ARTISTS = "artists"
    SONGS = "songs"
    USERS = "users"
    SONGPLAYS = "songplays"
    STAGING_EVENTS = "staging_events"
    STAGING_SONGS = "staging_songs"
    ALL_TABLES = [TIME, ARTISTS, SONGPLAYS, SONGS, USERS, STAGING_EVENTS, STAGING_SONGS]


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

SONGPLAYS_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.SONGPLAYS}(
    id bigint IDENTITY(1,1) primary key,
    start_time timestamp not null constraint songplays__time_fk references time,
    user_id char(18) constraint songplays__user_fk references users,
    level char(4),
    song_id char(18) constraint songplays__songs_fk references songs,
    artist_id char(18),
    session_id bigint,
    location varchar,
    user_agent varchar
    ) interleaved sortkey(start_time, user_id, song_id, artist_id)
""")

USER_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.USERS} (
    user_id bigint not null constraint users_pk primary key distkey sortkey,
    first_name varchar,
    last_name varchar,
    gender char,
    level char(4)
    )
""")

SONG_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.SONGS} (
    song_id char(18) not null constraint songs_pk primary key distkey,
    title varchar not null,
    artist_id char(18) not null,
    year smallint,
    duration real not null 
    ) interleaved sortkey(song_id, title, artist_id, duration)
""")

ARTIST_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.ARTISTS}(
    artist_id char(18) not null distkey,
    name varchar not null,
    location varchar,
    latitude real,
    longitude real
    ) sortkey(artist_id, name) 
""")

TIME_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.TIME}(
    start_time timestamp not null constraint time_pk primary key sortkey,
    hour smallint not null,
    day smallint not null,
    week smallint not null,
    month smallint not null,
    year smallint not null,
    weekday smallint not null
)
""")

# STAGING TABLES

STAGING_EVENTS_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.STAGING_EVENTS}(
    page varchar,
    ts timestamp,
    useragent varchar,
    userid char(18),
    firstname varchar,
    lastname varchar,
    gender char,
    level char(4),
    artist varchar,  
    song varchar,
    length real,
    sessionid bigint,
    location varchar
)
""")

STAGING_SONGS_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.STAGING_SONGS}(
    artist_id char(18),
    artist_name varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    duration real,
    song_id varchar(18),
    title varchar,
    year smallint
)
""")

STAGING_EVENTS_COPY = (f"""
    COPY {TableNames.STAGING_EVENTS} 
    FROM {{}} 
    IAM_ROLE {{}}
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT AS JSON 'auto ignorecase'
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS
    BLANKSASNULL;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['IAM_ROLE']['ARN'])

STAGING_SONGS_COPY = (f"""
    COPY {TableNames.STAGING_SONGS} 
    FROM {{}}
    IAM_ROLE {{}}
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# INSERTS TABLES

SONGPLAY_TABLE_INSERT = (f"""
    
    INSERT INTO {TableNames.SONGPLAYS}(start_time, user_id, level,
    song_id, artist_id, session_id, location, user_agent)
    with deduped_events AS (
     SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY ts, userid, sessionid, song, artist ) 
     as rn
      FROM {TableNames.STAGING_EVENTS} WHERE page='NextSong') WHERE rn=1
    )
    SELECT ts AS start_time,
    cast(userid as bigint) as user_id, 
    level,
    CASE WHEN a.artist_id IS NOT NULL THEN s.song_id
    END,
    a.artist_id,
    sessionid as session_id,
    st.location,
    btrim(useragent,'"') as user_agent
    FROM deduped_events as st
    LEFT JOIN {TableNames.SONGS} s ON s.title=st.song AND s.duration=st.length 
    LEFT JOIN {TableNames.ARTISTS} a ON s.artist_id=a.artist_id AND a.name=st.artist  
""")

USER_TABLE_INSERT = (f"""
    -- select records with unique user ids based on latest timestamp      
    CREATE TEMPORARY TABLE st AS (
        SELECT user_id, first_name, last_name, gender, level FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY ts DESC) FROM (
        SELECT 
        cast(userid as  bigint) as user_id,
        firstname as first_name, 
        lastname as last_name,
        gender, 
        level,
        ts FROM {TableNames.STAGING_EVENTS}
        WHERE page='NextSong' AND userid IS NOT NULL ))
        WHERE row_number=1
    );
    
    --  Update users level    
    UPDATE {TableNames.USERS}  
    SET level=st.level
    FROM st
    WHERE {TableNames.USERS}.user_id=st.user_id;
    
    --  Insert only new rows   
    INSERT INTO {TableNames.USERS}(user_id,first_name,last_name,gender,level)  
    SELECT st.* FROM st  
    LEFT OUTER JOIN {TableNames.USERS} u
    ON st.user_id IS NOT NULL AND st.user_id=u.user_id 
    WHERE u.user_id IS NULL;
""")

SONG_TABLE_INSERT = (f"""
    -- Insert new rows only
    INSERT INTO {TableNames.SONGS}(song_id, title, artist_id, year, duration)  
    WITH deduped_songs AS (
    SELECT * FROM(SELECT *, ROW_NUMBER() OVER(PARTITION BY song_id) as rn FROM
     {TableNames.STAGING_SONGS}) WHERE rn=1
    ) 
    SELECT st.song_id, st.title, st.artist_id, 
    CASE WHEN st.year=0 then NULL
    ELSE st.year
    END,     
    st.duration  FROM deduped_songs as st
    LEFT OUTER JOIN {TableNames.SONGS} s USING(song_id)
    WHERE s.song_id IS NULL;
""")

ARTIST_TABLE_INSERT = (f"""
    -- Insert new rows only
    INSERT INTO {TableNames.ARTISTS}(artist_id, name, location, latitude, longitude)  
    with deduped_artists AS (
     SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY artist_id, artist_name) 
     as rn
      FROM {TableNames.STAGING_SONGS}) WHERE rn=1
    )
    SELECT st.artist_id, artist_name, artist_location, artist_latitude,
                     artist_longitude  FROM deduped_artists as st
    LEFT OUTER JOIN {TableNames.ARTISTS} a 
    ON a.artist_id=st.artist_id AND a.name=st.artist_name
    WHERE a.artist_id IS NULL;
""")

TIME_TABLE_INSERT = (f"""
    INSERT INTO {TableNames.TIME}(start_time,hour,day,week,month,year,weekday)  
    SELECT st.* FROM (SELECT ts AS start_time, 
    extract(hour from start_time) as hour,
    EXTRACT(day from start_time) as day,
    EXTRACT(week from start_time) as week,
    EXTRACT(month from start_time) as month,
    EXTRACT(year from start_time) as year,
    EXTRACT(weekday from start_time) as weekday
    FROM (SELECT DISTINCT ts FROM {TableNames.STAGING_EVENTS} WHERE page='NextSong')
    ) st  
    LEFT OUTER JOIN {TableNames.TIME} t 
    ON st.start_time = t.start_time
    WHERE t.start_time IS NULL
""")

# QUERY LISTS

CREATE_TABLE_QUERIES = [STAGING_EVENTS_TABLE_CREATE, STAGING_SONGS_TABLE_CREATE,
                        USER_TABLE_CREATE, ARTIST_TABLE_CREATE, SONG_TABLE_CREATE,
                        TIME_TABLE_CREATE, SONGPLAYS_TABLE_CREATE]

# DROP TABLES
DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {} CASCADE;"

DROP_TABLE_QUERIES = [DROP_TABLE_QUERY_TEMPLATE.format(x) for x in TableNames.ALL_TABLES]

COPY_TABLE_QUERIES = [STAGING_EVENTS_COPY, STAGING_SONGS_COPY]
INSERT_TABLE_QUERIES = [ARTIST_TABLE_INSERT, SONG_TABLE_INSERT, TIME_TABLE_INSERT,
                        USER_TABLE_INSERT, SONGPLAY_TABLE_INSERT]
