# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"
log_table_drop = "drop table if exists log"
log_json_table_drop = "drop table if exists log_data"
song_json_table_drop = "drop table if exists song_data"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays (songplay_id int, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)
""")

user_table_create = ("""create table if not exists users (user_id int, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = ("""create table if not exists songs (song_id varchar, title varchar, artist_id varchar, year int, duration numeric)
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar, name varchar, location varchar, lattitude numeric, longitude numeric)
""")

time_table_create = ("""create table if not exists time (start_time timestamp, hour numeric, day numeric, week numeric, month numeric, year numeric, weekday numeric)
""")

log_table_create = ("""create table if not exists log (artist_name varchar, auth varchar, firstName varchar, gender varchar, itemInSession numeric, lastName varchar, length numeric, level varchar, location varchar, method varchar,\
page varchar, registration varchar, sessionId numeric, song varchar, status varchar, ts varchar, userAgent varchar, userId numeric)
""")

log_json_table_create = ("""CREATE TABLE log_data (log_data jsonb NOT NULL);
""")

song_json_table_create = ("""CREATE TABLE song_data (song_data jsonb NOT NULL);
""")



# INSERT RECORDS

songplay_table_insert = ("""insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""insert into songs (song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""insert into artists (artist_id, name, location, lattitude, longitude) values (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s)
""")

log_table_insert = ("""insert into log (artist_name,auth,firstName,gender,itemInSession,lastName,length,level,location,method,page,registration,sessionId,song,status,ts,userAgent,userId) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
""")


# FIND SONGS

song_select = ("""select songs.song_id, songs.artist_id from songs join artists on songs.artist_id = artists.artist_id where songs.title = %s and artists.name = %s and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, log_table_create, log_json_table_create, song_json_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, log_table_drop]
