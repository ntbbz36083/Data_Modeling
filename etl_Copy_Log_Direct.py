import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_data_new(cur, conn, filepath):
    # get all files matching extension from directory
    all_files = []
    file_name = "/home/workspace/merged_file.json"
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    with open(file_name, "w") as outfile:
        outfile.write('{}'.format(
        '\n'.join([open(f, "r").read() for f in all_files])))
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    total_num_rec = 0
    # This is to check that we want to load song_data data
    if filepath == 'data/song_data':
        # Direct copy data to table with copy command
        command = "COPY song_data (song_data) FROM '" + file_name + "';"
        cur.execute(command)
        conn.commit()
        # drop table since it's created in the create_tables.py
        cur.execute('drop table songs;')
        conn.commit()
        # drop table since it's created in the create_tables.py
        cur.execute('drop table artists;')
        conn.commit()
        # create table songs by query from song_data table
        cur.execute("create table songs as select song_data -> 'song_id' as song_id, song_data -> 'title' as title, song_data -> 'artist_id' as artist_id, \
                    song_data -> 'year' as year, song_data -> 'duration' as duration from song_data;")
        conn.commit()
        cur.execute('select count(*) from songs')
        songs_num_rec = str(cur.fetchone())
        print('songs table created, {} records loaded successfully.'.format(songs_num_rec[1:-2]))
        # create table artists by query from song_data table
        cur.execute("create table artists as select song_data -> 'artist_id' as artist_id, song_data -> 'artist_name' as name, \
                    song_data -> 'artist_location' as artist_location, song_data -> 'artist_latitude' as artist_latitude, \
                    song_data -> 'artist_longitude' as artist_longitude from song_data;")
        conn.commit()
        cur.execute('select count(*) from artists')
        artists_num_rec = str(cur.fetchone())
        print('artists table created, {} records loaded successfully.'.format(artists_num_rec[1:-2]))
    # This is to check that we want to load log_data data
    elif filepath == 'data/log_data':
        # Direct copy data to table with copy command
        command = "COPY log_data (log_data) FROM '" + file_name + "' csv quote e'\x01' delimiter e'\x02';"
        cur.execute(command)
        conn.commit()
        
        # drop table since it's created in the create_tables.py
        cur.execute('drop table if exists users;')
        conn.commit()
        # create table users by query from song_data table
        cur.execute("create table users as select log_data -> 'userId' as userId, log_data -> 'firstName' as firstName, \
                    log_data -> 'lastName' as lastName, log_data -> 'gender' as gender, log_data -> 'level' as level from log_data where log_data->> 'page' = 'NextSong';")
        conn.commit()
        cur.execute('select count(*) from users')
        users_num_rec = str(cur.fetchone())
        print('Users table created, {} records loaded successfully.'.format(users_num_rec[1:-2]))
        # drop table since it's created in the create_tables.py
        cur.execute('drop table if exists songplays;')
        conn.commit()
        # We need the a sequence object to geneate first column index
        cur.execute("drop sequence if exists serial;")
        conn.commit()
        # Create sequence object
        cur.execute("CREATE SEQUENCE serial START 1;")
        conn.commit()
        # create songplays table by querying log_data
        cur.execute("create table songplays as select nextval('serial') as ind, to_timestamp(to_number(b.ts,'9999G999G999g999')) as starttime, b.userId, b.level, b.sessionId, b.location, b.userAgent, b.title, b.song, b.duration \
        from (select log_data ->> 'ts' as ts, log_data -> 'userId' as userId, log_data -> 'level' as level, log_data -> 'sessionId' as sessionId, log_data -> 'location' as location, \
        log_data -> 'userAgent' as userAgent, log_data -> 'artist' as title,  log_data -> 'song' as song, log_data -> 'length' as duration from log_data where log_data->> 'page' = 'NextSong') b;")
        conn.commit()
        # Add 2 columns: song_id, artists_id
        cur.execute("alter table songplays add column song_id varchar default NULL;")
        conn.commit()
        cur.execute("alter table songplays add column artists_id varchar default NULL;")
        conn.commit()
        # Update these 2 columns to meet the requirement metioned in the project
        cur.execute("update songplays as s set song_id= t.song_id, artists_id= t.artist_id from (select songplays.ind, songs.song_id, artists.artist_id \
        from songplays, songs, artists where \
        songplays.duration = songs.duration and songs.title = songplays.song \
        and artists.name = songplays.title) as t(ind, song_id, artist_id) where s.ind = t.ind;")
        conn.commit()
        cur.execute('select count(*) from songplays')
        songplays_num_rec = str(cur.fetchone())
        print('songplays table created, {} records loaded successfully.'.format(songplays_num_rec[1:-2]))
        # drop table since it's created in the create_tables.py
        cur.execute('drop table if exists time;')
        conn.commit()
        # create time table by querying log_data
        cur.execute('create table time as select starttime, extract(hour from starttime) as hour, extract(day from starttime) as day, extract(week from starttime) as week, extract(month from starttime) as month, extract(year from starttime), extract(dow from starttime) as dow from songplays;')
        conn.commit()
        cur.execute('select count(*) from time')
        time_num_rec = str(cur.fetchone())
        print('time table created, {} records loaded successfully.'.format(time_num_rec[1:-2]))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data_new(cur, conn, 'data/song_data')
    process_data_new(cur, conn, 'data/log_data')

    conn.close()



if __name__ == "__main__":
    main()