# Cloud Data warehouse

### Introduction

Music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in AWS S3 including user activity logs and song metadata in JSON format.
    
### Business requirements


- Analytics team want to find insights on what songs their users are listening to using dimension tables
- analytics team want dimension model optimized for queries on song play analysis in data warehouse 

### Success criteria

* Launch a AWS redshift cluster 
  * Redshift cluster able to access s3
* Create dimension model and ETL pipeline to prepare data for analytics team
  * Explore and load JSON files in S3 to Redshift staging tables
  * Define and create FACT and DIMENSION tables for a star schema model for data analytics purposes
* Create ETL pipeline to load data from staging tables to data warehouse tables on Redshift
* Able to Connect to the Redshift cluster and complete testing utilizing test queries

### Technology

- AWS S3
- Python
- Redshift

### Data
data comprises of songs and songs playing activity

* Song Dataset

The song dataset is a subset of real data from the Million Song Dataset(https://labrosa.ee.columbia.edu/millionsong/). Data files are in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example:

song_data/A/B/C/TRABCEI128F424C983.json song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of single song file, TRAABJL12903CDCF1A.json. 

{
   "num_songs":1,
   "artist_id":"ARJIE2Y1187B994AB7",
   "artist_latitude":null,
   "artist_longitude":null,
   "artist_location":"",
   "artist_name":"Line Renaud",
   "song_id":"SOUPIRU12A6D4FA1E1",
   "title":"Der Kleine Dompfaff",
   "duration":152.92036,
   "year":0
}

* Log Dataset

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month. For example:

log_data/2018/11/2018-11-12-events.json log_data/2018/11/2018-11-13-events.json

And below is an example of single log file, 2018-11-13-events.json

{
   "artist":"Pavement",
   "auth":"Logged In",
   "firstName":"Sylvie",
   "gender":"F",
   "itemInSession":0,
   "lastName":"Cruz",
   "length":99.16036,
   "level":"free",
   "location":"Klamath Falls, OR",
   "method":"PUT",
   "page":"NextSong",
   "registration":"1.541078e+12",
   "sessionId":345,
   "song":"Mercy:The Laundromat",
   "status":200,
   "ts":1541990258796,
   "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)",
   "userId":10
}

### Schema for Song Play Analysis

A Star Schema would be required for optimized queries on song play queries

* Fact Table

| Table | Description |
|-------|-------------|
| songplays | records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time,user_id, level,song_id, artist_id, session_id, location, user_agent|

* Dimension Tables

| Table | Description |
|--------|---------------|
| users | users in the app user_id, first_name, last_name, gender, level|
| songs | songs in music database song_id, title, artist_id, year, duration|
| artists | artists in music database artist_id, name, location, lattitude, longitude|
| time | timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year,weekday|


### ETL Process

### Project inlcudes:

create_table.py - Creates the fact, dimension and staging tables schemas, staging, fact and dimension tables for the star schema on Redshift.

etl.py - Script loads data from S3 into staging tables on Redshift and then process that data into data warehouse tables(dimension and Fact) on Redshift.

sql_queries.py - Script has Sql queries to define tables, insert data , which will be imported into the two other files above.

README.md - Describes process and decisions for this ETL pipeline


### Sample Data Analysis


Top 10 popular songs

Query:

  SELECT sp.song_id, s.title, count(*) AS cnt 
    FROM songplays sp
    JOIN songs s
      ON sp.song_id = s.song_id
GROUP BY 1, 2
ORDER BY 3 DESC
   LIMIT 10;
Result: query1

* Top-10 popular artists

Query:

  SELECT sp.artist_id, a.name AS artist_name, count(*) AS cnt
    FROM songplays sp
    JOIN artists a
      ON sp.artist_id = a.artist_id
GROUP BY 1, 2
ORDER BY 3 DESC
   LIMIT 10;
   
Result: 

* When most people are listening to songs?

Query:

  SELECT CASE
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

Result: query3 We can tell from the above result that most users play songs in the afternoon between 13:00 and 18:00 and very few users play songs in (late)mid-night.


