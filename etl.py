import os
import glob
import psycopg2
import pandas as pd
from sql_queries import 


def process_song_file(cur, filepath)
    # open song file
    df = pd.read_json(filepath,typ='series')
    #if df['song_id'] == 'SOUDSGM12AC9618304'
    #    print(filepath)
    # insert song record
    song_data = df[['song_id','title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)
    return 1

def process_log_file(cur, filepath)
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = column_labels = ['starttime', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    time_data = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(data = time_data)

    for i, row in time_df.iterrows()
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows()
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows()
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        # insert all log into log table
        #cur.execute(log_table_insert, row)
        if results
            songid, artistid = results
        else
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit = 'ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    return df.shape[0]

def process_data(cur, conn, filepath, func)
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath)
        files = glob.glob(os.path.join(root,'.json'))
        for f in files 
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    total_num_rec = 0
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1)
        count = func(cur, datafile)
        conn.commit()
        total_num_rec += count
        print('{}{} files processed.'.format(i, num_files))
    if filepath == 'datasong_data'
        cur.execute(select count() from songs)
        songs_num_rec = cur.fetchone()[0]
        cur.execute(select count() from artists)
        artists_num_rec = cur.fetchone()[0]
        if total_num_rec == songs_num_rec == artists_num_rec
            print('All files processed, files have {} records and {} records loaded into songs, aritsts table successfully.'.format(total_num_rec, songs_num_rec))
        else
            print('All files processed, however, files have {} records and table has {} records.'.format(total_num_rec, songs_num_rec))
    elif filepath == 'datalog_data'
        cur.execute(select count() from songplays)
        songplays_num_rec = cur.fetchone()[0]
        cur.execute(select count() from time)
        time_num_rec = cur.fetchone()[0]
        cur.execute(select count() from users)
        users_num_rec = cur.fetchone()[0]
        if total_num_rec == songplays_num_rec == time_num_rec == users_num_rec
            print('All files processed, files have {} records and {} records loaded into songplays, time, users successfully.'.format(total_num_rec, songplays_num_rec))
        else
            print('All files processed, however, files have {} records and table has {} records.'.format(total_num_rec, songplays_num_rec))

def main()
    conn = psycopg2.connect(host=127.0.0.1 dbname=sparkifydb user=student password=student)
    cur = conn.cursor()

    process_data(cur, conn, filepath='datasong_data', func=process_song_file)
    process_data(cur, conn, filepath='datalog_data', func=process_log_file)

    conn.close()


if __name__ == __main__
    main()