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
    CREATE TABLE staging_events (
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   INTEGER,
        lastName        VARCHAR,
        lenght          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    FLOAT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id         INTEGER     IDENTITY(1,1)   PRIMARY KEY,
        start_time          TIMESTAMP   NOT NULL,
        user_id             INTEGER     NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR     NOT NULL,
        artist_id           VARCHAR     NOT NULL,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
    DISTSTYLE KEY
    DISTKEY ( start_time )
    SORTKEY ( start_time );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id         INTEGER     PRIMARY KEY,
        first_name      VARCHAR(50) NOT NULL,
        last_name       VARCHAR(50) NOT NULL,
        gender          CHAR(1)     NOT NULL    ENCODE BYTEDICT,
        level           VARCHAR     NOT NULL    ENCODE BYTEDICT
    )
    SORTKEY ( user_id );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id     VARCHAR     PRIMARY KEY,
        title       VARCHAR     NOT NULL,
        artist_id   VARCHAR     NOT NULL,
        year        INTEGER     NOT NULL,
        duration    FLOAT
    )
    SORTKEY ( song_id );
    """)

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id           VARCHAR     PRIMARY KEY,
        artist_name         VARCHAR     NOT NULL,
        artist_location     VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT   
    )
    SORTKEY ( artist_id );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time      TIMESTAMP   PRIMARY KEY,
        hour            INTEGER     NOT NULL,
        day             INTEGER     NOT NULL,
        week            INTEGER     NOT NULL,
        month           INTEGER     NOT NULL,
        year            INTEGER     NOT NULL    ENCODE BYTEDICT,
        weekday         VARCHAR(10) NOT NULL    ENCODE BYTEDICT
    )
    DISTSTYLE KEY
    DISTKEY ( start_time )
    SORTKEY ( start_time );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {data_bucket}
    iam_role {iam_role_arn}
    FORMAT AS json {json_path}
    TIMEFORMAT AS 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'],
            iam_role_arn=config['IAM_ROLE']['ARN'],
            json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {data_bucket}
    iam_role {iam_role_arn}
    FORMAT AS json 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'],
            iam_role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT e.ts                  AS start_time,
                    e.userId              AS user_id,
                    e.level               AS level,
                    s.song_id             AS song_id,
                    s.artist_id           AS artist_id,
                    e.sessionId           AS session_id,
                    e.location            AS location,
                    e.userAgent           AS user_agent
    FROM staging_events e JOIN staging_songs s
    ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page == 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
           userId AS user_id,
           firstName        AS first_name,
           lastName         AS last_name,
           gender,
           level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page == 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
           song_id              AS song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
           artist_id AS artist_id,
           artist_name         AS name,
           artist_location     AS location,
           artist_latidude     AS latitude,
           artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
           ts                                   AS start_time,
           EXTRACT(HOUR from start_time)        AS hour,
           EXTRACT(DAY from start_time)         AS day,
           EXTRACT(WEEK from start_time)        AS week,
           EXTRACT(MONTH from start_time)       AS month,
           EXTRACT(YEAR from start_time)        AS year,
           EXTRACT(DAYOFWEEK from start_time)   AS day
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
