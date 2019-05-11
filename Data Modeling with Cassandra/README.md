# Data Engineering Nano Degree- Project 2

## 1. Background & Objective of the Project

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project.

The objective of this project is to create an Apache Cassandra database for this analysis so that queries given by the analytics team from Sparkify to create the results.


## 2. Installations

This project was written in Python, using Jupyter Notebook on Anaconda. The relevant Python packages for this project are as follows:

* os
* glob
* pandas
* numpy

To follow the steps it is recommended to install Anaconda, a pre-packaged Python distribution that contains all of the necessary libraries and software for this project.

To run the notebook locally you need to install : **`! pip install cassandra-driver`**


## 3. Dataset

`event_datafile_new.csv` is given as a part of the project. 



## 4. Database schema design and ETL pipeline 

`Project_2.ipynb` has been used to create an Apache Cassandra database for this analysis and 3 queries have been performed.


## 5. Steps

1. With CREATE statements three tables have been created for three query. IF NOT EXISTS clauses have been included in the CREATE statements to create tables only if the tables do not already exist.

2. Data have been loaded with INSERT statement for each of the tables. The selection of partition and clustering keys have been included for the each table. The keys have been choosen based on the query to make sure data is inserted and retracted properly.

3. With proper SELECT statements and correct WHERE clause query have been made. The query output has been shown using pandas dataframe and pretty tables.   

4. DROP TABLE statements have been included for each table, to be able to run drop and create tables whenever needed and to reset your database and test the ETL pipeline



## 7. Run

Running the file `Project_2.ipynb` will create the tables, and perform the query. 


## 8. References

1. [CREATE TABLE in Apache Cassandra](https://docs.datastax.com/en/archived/cql/3.0/cql/cql_reference/create_table_r.html)

2. [Apache Cassandra Data Modeling and Query Best Practices](https://www.red-gate.com/simple-talk/sql/nosql-databases/apache-cassandra-data-modeling-and-query-best-practices/) 

3. [Difference between partition key, composite key and clustering key in Cassandra](https://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra)

4. [Clustering columns in Apache Cassandra](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html)