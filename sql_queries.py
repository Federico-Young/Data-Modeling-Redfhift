import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay_table"
user_table_drop = "drop table if exists user_table"
song_table_drop = "drop table if exists song_table"
artist_table_drop = "drop table if exists artist_table"
time_table_drop = "drop table if exists time_table"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
artist VARCHAR(MAX),
auth varchar(MAX),
firstName VARCHAR(MAX),
gender varchar(MAX),
IteminSession SMALLINT,
lastName VARCHAR(MAX),
length float,
level varchar(MAX),
location varchar(MAX),
method varchar(MAX),
page varchar(MAX),
registration float,
sessionId varchar,
song VARCHAR(MAX),
status float,
ts timestamp,
userAgent varchar(MAX),
userId varchar
)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
num_songs int,
artist_id varchar,
artist_latitude decimal,
artist_longitude decimal,
artist_location varchar,
artist_name VARCHAR(MAX),
song_id varchar,
title VARCHAR(MAX),
duration float,
year int
)
""")

#Fact table
songplay_table_create = ("""
create table if not exists songplay_table(
songplay_id int IDENTITY(0,1),
start_time date not null,
userId int not null,
level varchar not null,
song_id varchar not null,
artist_id varchar not null,
sessionId int not null,
location varchar not null,
userAgent varchar not null
)
""")

#Dim tables
user_table_create = ("""
create table if not exists user_table(
userId int not null primary key sortkey,
firstName varchar not null,
lastName varchar not null,
gender varchar not null,
level varchar not null
)
""")

song_table_create = ("""
create table if not exists song_table(
song_id varchar not null PRIMARY KEY sortkey,
title varchar not null,
artist_id varchar not null,
year int not null,
duration float not null)
""")

artist_table_create = ("""
create table if not exists artist_table(
artist_id varchar not null PRIMARY KEY sortkey,
artist_name varchar not null,
artist_location varchar not null,
artist_latitude decimal not null,
artist_longitude decimal not null
)
""")

time_table_create = ("""
create table if not exists time_table(
ts timestamp not null PRIMARY KEY sortkey,
hour int not null,
day int not null,
week int not null,
month int not null,
year int not null,
weekday int not null
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
FORMAT AS JSON {}
""".format(LOG_DATA , ARN, LOG_JSONPATH)
                      )

staging_songs_copy = ("""
copy staging_songs 
from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json'auto'
""".format(SONG_DATA , ARN)
                     )

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay_table (start_time ,userId, level, song_id, artist_id, sessionId, location, userAgent)
select 
ts,
userId,
level,
song_id,
artist_id,
sessionId,
location,
userAgent
FROM staging_events se
JOIN staging_songs ss ON se.artist=ss.artist_name AND
se.song=ss.title AND
se.length=ss.duration
WHERE se.page='NextSong'
""")

user_table_insert = (""""
insert into user_table (userId, firstName , lastName , gender ,level)
with uniq_staging_events as (
    select userId, firstName , lastName , gender ,level,
    ROW_NUMBER() OVER(PARTITION BY userId order by ts DESC) as rank
    from staging_events
    where UserId IS NOT NULL
    )
select userId, firstName , lastName , gender ,level
from uniq_staging_events
where rank = 1 
""")

song_table_insert = ("""insert into song_table (song_id , title , artist_id , year, duration)
select song_id,
title,
artist_id,
year,
duration
from staging_songs
""")

artist_table_insert = ("""
insert into artist_table (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
select artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
from staging_songs
""")

time_table_insert = ("""
insert into time_table (ts, hour, day, week, month, year, weekday)
with uniq_staging_events AS (
    select ts,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day,
    EXTRACT(week FROM ts) AS week,
    EXTRACT(month FROM ts) AS month,
    EXTRACT(isodow FROM ts) AS weekday,
    ROW_NUMBER() OVER(PARTITION BY ts ORDER BY UserId) AS rank
    FROM staging_events 
    WHERE ts IS NOT NULL
    )
select ts, hour, day, week, month, year, weekday
from uniq_staging_events 
where rank = 1
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
