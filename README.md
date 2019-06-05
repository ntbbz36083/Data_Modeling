1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
This database contains 5 tables: users, songs, artists, time and songplays. 
Users table has the information about all the Sparkify user, which can help analyze the distribution of user. 
songs and artists tables contains the information about songs and artists.
time contains the specific information on the time that user uses the app.
songplays table has the logs of users' activities. You can find what user plays which song at when and what level he/she is. You can also find what device he/she is using.
With these tables inside this database, Sparkify can analyze what kind of user that are having. 
Where are they from? the geographical distribution. What gender do they have?
What songs they listens and when they start use the app? The peak use time during the day. What device are they using?
Based on these, they can start thinking about what kind of campaign they should use.

2. State and justify your database schema design and ETL pipeline.
This database has 5 tables, songplays is the fact table and users, songs, artists, time are the dimension table.
ETL pipeline is like below:
A. create database, connection and tables, these can be found in the create_tables.py, sql_queries.py.
B. insert into tables with data from Json file, this is achieved in etl.py.
    a. we populate songs, artists tables with data from song file.
    b. we populate users, time, songplays tables with data from log file. For song_id, artist_id in songplays table, we need to extract it from songs and artists table since they are not available in the log file.
    c. Get all files and loop through it to populate the tables.

3. Provide example queries and results for song play analysis.
%sql select level,count(level),count(level) * 100 /(select count(*) as percent from songplays) as per from songplays group by level;
This query will display the percentage of paid and free users. 
level	count	per
free	1229	18
paid	5591	81


%sql SELECT a.gender, count(a.gender), count(gender) * 100 /(select count(*) from songplays)  as percent FROM songplays inner join (select distinct * from users) a on songplays.user_id = a.user_id and songplays.level = a.level group by a.gender
gender	count	percent
F	4887	71
M	1933	28
This query will display the percentage of male and female.

4. Add data quality checks
Check1: whether all data are inserted?
I add a layer of check that will check how many records that are in the files, then compare it with the number of records of the table that we populated. if they are the same, print good. else, print a warning.
Check2: whether condition insert are working?
If you llok at the songplays table, you will find most records' songid and artistid are None. Only 1 record has data. Is this true or is it an error?
To check whether this is only one record is inserted into the songplay table that song_id and artist_id are neither not None, see below command:
%sql SELECT * FROM log, songs, artists where songs.title = log.song and log.artist_name = artists.name and songs.duration = log.length;

5. Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
I have implemented this and created a function in the etl_Copy_Log_Direct.py. You can run this script and you will get the same result as the insert version.
In this script, I write a function that will copy the json data into table directly. Then i do some queries to generate the tables. You can see each query in the function. 
Comparing to insert, the bulk copy is much faster.
