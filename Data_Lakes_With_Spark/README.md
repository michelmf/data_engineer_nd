# Data Lake with Apache Spark

This is the fourth project of the Data Engineer Nanodegree. In this project, students need to build an ETL pipeline for a data lake hosted on S3, loading data from S3, process the data into analytics tables using Spark, and load them back into S3, including the deployment of Spark processes on a cluster using AWS. During the development of the project, student have opportunity to practice the following concepts and technologies:

* Extract, Transform and Load concepts - ETL
* Extraction of data from Amazon S3 Buckets
* Data Wrangling using Apache Spark
* Solution Deployment using Amazon EMR

# Motivation

Continuing the progress of previous projects, a music streaming startup called Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Using the data engineer concepts learned so far, students are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Project Goals

In this project, we have to build an ETL pipeline using Spark for a database hosted on Amazon S3. To complete the project, the following tasks must be done:

1. Load Json files from S3 buckets
2. ETL process using a EMR cluster 
3. Store the processed data again into Amazon S3

# Dataset 

We use the same dataset from previous projects, now residing in a s3 bucket. Here is the S3 link for all the files

* s3a://udacity-dend/

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings. The log files in the dataset you'll be working with are partitioned by year and month. 

## Schema

The schema is the same from the previous projects. Click [here](https://github.com/michelmf/data_engineer_nd/tree/master/Data_Modelling_with_PostgreSQL) to check the schema picture.

## Files

The project follows the structure below.

* dl.cfg - File with AWS credentials
* etl.py - Script that executes the ETL process of this project, using pyspark.

# How to run

In order to run this project, you must start an EMR cluster with Apache Spark installed in it, and execute the etl.py script. Remember to store your credentials in dl.cfg.