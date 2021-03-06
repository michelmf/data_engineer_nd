# Project - Data Modeling with Postgres

This is the first project of udacity Data Engineering Nanodegree. In this project the students have the opportunity to practice the following concepts learned during the classes:

- Data modeling
- Database Schemas (snowflake/star)
- Creation of ETL pipelines
- Database CRUD

# Motivation

In this project, a fake startup called Sparkify wants to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Goals

For this the project, we have the following goals:

1. Creation of queries for table creation, data insertions, selection and table drop  
2. Creation of the ETL process to examine and extract all the data from JSON files
3. Creation of the ETL pipeline after checking the correctness of the ETL process

# Dataset

Two datasets are used in this leveling project - Song Dataset and Log Dataset. The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations. The log files in the dataset are partitioned by year and month.

## Schemas

The Schema for Song Play Analysis is created using the song and log datasets. We need to create a star schema optimized for queries on song play analysis. This includes the following tables.

* Fact Tables
    1. songplays: records in log data associated with song plays i.e. records with page NextSong
    
       - Fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

* Dimension Tables

    2. Users: users in the app
        - Fields: user_id, first_name, last_name, gender, level

    3. Songs: songs in music database
        - Fields: song_id, title, artist_id, year, duration

    4. Artists: artists in music database
        - Fields: artist_id, name, location, latitude, longitude
    
    5. Time: timestamps of records in songplays broken down into specific units
        - Fields: start_time, hour, day, week, month, year, weekday

The image below shows the relation between tables.

![](Diagram.jpg?raw=true)


## Files

Some files are included in this project.

1. sql_queries.py    - Python file where all the queries for CRUD operations are defined.
2. create_tables.py  - Script that creates the connection to PostgreSQL database and executes the queries defined in sql_queries.py file
3. etl.py            - Script which executes all the ETL process.
4. etl.ipynb         - Jupyter Notebook used to validate the ETL needed steps for correct extraction, transformation and load of data into database
5. test.ipynb        - Jupyter Notebook for validation of database operations. Used to check the correctness of performed operations

# How to Run

Besides the PostgreSQL database installed on your computer, this project needs the following packages in order to run properly.

- psycopg2
- pandas

The steps needed to create and populate the tables are:

1. Open the terminal and execute the _create_tables.py_ script (_python create_tables.py_)
2. After executing the script above, you should execute the _etl.py_ file (_python etl.py_)
3. In order to check the data inserted into PostgreSQL, you can execute the notebook called _test.ipynb_
