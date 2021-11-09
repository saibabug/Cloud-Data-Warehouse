import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES


staging_events_table_drop = " DROP TABLE IF EXISTS staging_events; "
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs; "
songplay_table_drop = " DROP TABLE IF EXISTS songplays; "
user_table_drop = " DROP TABLE IF EXISTS users; "
song_table_drop = " DROP TABLE IF EXISTS songs; "
artist_table_drop = " DROP TABLE IF EXISTS artists; "
time_table_drop = " DROP TABLE IF EXISTS time; "

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
event_id INTEGER IDENTITY(0,1) NOT NULL SORTKEY DISTKEY,
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
iteminSession INTEGER,
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
ts TIMESTAMP,
userAgent VARCHAR,
userId INTEGER
);
 
""")

staging_songs_table_create = ("""
 CREATE TABLE IF NOT EXISTS staging_songs(
   num_songs INTEGER NOT NULL SORTKEY DISTKEY,
    artist_id VARCHAR NOT NULL,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR NOT NULL,
    title varchar,
    duration DECIMAL,
    year INTEGER
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
start_time TIMESTAMP, 
user_id VARCHAR,
level VARCHAR, 
song_id VARCHAR NOT NULL, 
artist_id VARCHAR NOT NULL, 
session_id INTEGER, 
location VARCHAR, 
user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER NOT NULL PRIMARY KEY DISTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR NOT NULL PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INTEGER,
    duration DECIMAL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR NOT NULL PRIMARY KEY DISTKEY, 
    name VARCHAR,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp PRIMARY KEY, 
    hour int , 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday VARCHAR);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
                       CREDENTIALS 'aws_iam_role={}'
                       TIMEFORMAT as 'epochmillisecs'
                       TRUNCATECOLUMNS
                       BLANKSASNULL
                       EMPTYASNULL
                       JSON {}
                       """).format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)




staging_songs_copy = ("""
COPY staging_songs FROM {}
                      CREDENTIALS 'aws_iam_role={}'
                      TRUNCATECOLUMNS
                      BLANKSASNULL
                      EMPTYASNULL
                      JSON 'auto';
                      """).format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO songplays (start_time,
                                                user_id,
                                                level,
                                                song_id,
                                                artist_id,
                                                session_id,
                                                location,
                                                user_agent)
                         SELECT DISTINCT to_timestamp(to_char(events.ts,'9999-99-99 99:99:99'),
                                                      'YYYY-MM-DD HH24:MI:SS'),
                                         events.userId AS user_id,
                                         events.level AS level,
                                         songs.song_id AS song_id,
                                         songs.artist_id AS artist_id,
                                         events.sessionId AS session_id,
                                         events.location AS location,
                                         events.userAgent AS user_agent
                         FROM staging_events events
                         JOIN staging_songs songs
                             ON events.song = songs.title
                             AND events.artist = songs.artist_name
                             WHERE events.page = 'NextSong';
                         """)

user_table_insert = ("""
                     INSERT INTO users (user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
                     SELECT DISTINCT userId AS user_id,
                                     firstName AS first_name,
                                     lastName AS last_name,
                                     gender AS gender,
                                     level AS level
                     FROM staging_events
                     WHERE userId IS NOT NULL;
                     """)

song_table_insert = ("""
                     INSERT INTO songs (song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
                     SELECT DISTINCT song_id AS song_id,
                                     title AS title,
                                     artist_id AS artist_id,
                                     year AS year,
                                     duration AS duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
                     """)

artist_table_insert = ("""
                       INSERT INTO artists (artist_id,
                                            name,
                                            location,
                                            latitude,
                                            longitude)
                       SELECT DISTINCT artist_id AS artist_id,
                                       artist_name AS name,
                                       artist_location AS location,
                                       artist_latitude AS latitude,
                                       artist_longitude AS longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL;
                       """)

time_table_insert = ("""
                     INSERT INTO time (start_time,
                                       hour,
                                       day,
                                       week,
                                       month,
                                       year,
                                       weekday)
                     SELECT DISTINCT ts,
                     EXTRACT(hour FROM ts),
                     EXTRACT(day FROM ts),
                     EXTRACT(week FROM ts),
                     EXTRACT(month FROM ts),
                     EXTRACT(year FROM ts),
                     EXTRACT(weekday FROM ts)
                     FROM staging_events
                     WHERE ts IS NOT NULL;
                     """)


top_ten_songs = (
  """SELECT sp.song_id, s.title, count(*) AS cnt 
    FROM songplays sp
    JOIN songs s
      ON sp.song_id = s.song_id
GROUP BY 1, 2
ORDER BY 3 DESC
   LIMIT 10;
""")

top_ten_artists=("""
     SELECT sp.artist_id, a.name AS artist_name, count(*) AS cnt
    FROM songplays sp
    JOIN artists a
      ON sp.artist_id = a.artist_id
GROUP BY 1, 2
ORDER BY 3 DESC
   LIMIT 10;
""")

listen_time=( """SELECT CASE
           WHEN t.hour BETWEEN 2 AND 8  THEN '2-8'
           WHEN t.hour BETWEEN 9 AND 12 THEN '9-12'
           WHEN t.hour BETWEEN 13 AND 18 THEN '13-18'
           WHEN t.hour BETWEEN 19 AND 22 THEN '19-22'
           ELSE '23-24, 0-2'
         END AS play_time, 
         count(*) AS cnt
    FROM songplays sp
    JOIN time t
      ON sp.start_time = t.start_time
GROUP BY 1
ORDER BY 2 DESC;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
validation_queries = [top_ten_songs,top_ten_artists,listen_time]