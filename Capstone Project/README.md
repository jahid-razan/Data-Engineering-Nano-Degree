# Data Engineering Nano Degree- Project 3

## 1. Background & Objective of the Project

Immigration has been a major source of population growth and cultural change throughout much of U.S. history. According to the 2016 Yearbook of Immigration Statistics, the United States admitted 1.18 million legal immigrants in 2016 [Ref-7]. The objective of this project is to build facts and dimension table to analyze the trend of immigration and its impact on the demography of US states. In this project, a simplified data model will be built for the common people with minimum training who can perform basic analysis to observe the impact of immigration on the state immigrant population. From the fact table, the percentage increase in immigration per state can be observed. 



## 2. Dataset

A. **I94 Immigration Data**:This data comes from the US National Tourism and Trade Office. The original dataset can be found [here](https://travel.trade.gov/research/reports/i94/historical/2016.html)

B. **U.S City Demographic Data**: U.S. City Demographic Data: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

At least 2 data sources
More than 1 million lines of data.
At least two data sources/formats (csv, api, json)

## 3. Fact and Dimension Tables


### Star Schema

**Dimension Table**: The dimension table contains the attributes of the immigrants of the year 2016 and the current demography of the US states. 


1. **immigration_table**:  year, month, city_of_origin, state_code, birth_year, gender

2. **demography_table**: year, month, state_code, number_of_immigrants, median_age, race, birth_year, gender

Following is a brief **data dictionary** for the immigrant data:

+ immigration_yr: contains the year of immigration, data type: integer

+ immigration_month: contains the month of immigration, data type: integer

+ country _of_origin: contains the country of origin for the immigrants, data type: string

+ state_code: contains a two-letter abbreviation of the destination state of the immigrants, data type: string

+ birth_year: contains the year of birth of the individual immigrant, data type: integer

+ gender: contains the gender of the individual immigrant, data type: string


Following is a brief **data dictionary** for us-cities-demographics:

+ city: contains the name of the city in which the demography is under analysis, data type: string

+ state: contains the name of the city where the state is located, data type: string

+ median_age: contains the median age of the population, data type: float

+ state_code: contains a two-letter abbreviation of the destination state of the immigrants, data type: string

+ foreign_born: contains the total number of immigrants coming to each city, data type: integer

+ birth_year: contains the year of birth of the individual immigrant, data type: integer

+ race: contains the race description of the immigrants of a particular city, data type: string


**Fact Table** contains the records that show the total number of immigrants per state and the number of new immigrants coming to each state. This is a simple fact table can be used to analyze the rate of immigration increase in each state for a given year. 

**immigration_impact_table:** state, new_immigrant_per_state, total_immigrants_per_state.

Following is a brief **data dictionary** for the fact table


+ state: contains the name of the city where the state is located, data type: string

+ new_immigrant_per_state : contains the number of new immigrants per state, data type: long

+ total_immigrant_per_state: contains the number of total immigrants currently exist before new migration in the state, data type: long

The fact table has 48 rows contains the data for 48 states and 3 rows indicate the impact of demography change for the 48 states

**The facts and dimension tables have been saved in the output folder in parquet file format.**

## 4. Data files

1. `Capstone Project.ipynb` is used to read the data and create the ELT pipeline. All the intermediate steps of data cleaning and conversation have been shown in the file. 

2. Data has been gathered from two different sources- `immigration_data_sample.csv` which contains data in CSV format and the parquet data from the `sas_data` folder.

3. `immigration_codes.py` contains the country code and the name of the country in the dictionary format where the key is the country code and the value is the name of the country. A user-defined function has been written and later used in the `Capstone Project.ipynb` to convert the country code into the names of the countries. 

4. The 2016 immigration data for April that has been read from the `sas_data` folder in a spark data frame. The data frame has more than 3 million data rows and the initial data contains 28 columns. For the simplicity of this project, only 6 columns have chosen. To analyze the impact of 2016 immigration the data for all the months could be merged. 


5. It has been observed that the dataset often contains a column that is not descriptive in nature and inappropriate data type. To build the fact and the dimension tables some data cleaning has been performed such as- making the column names descriptive that can be understood easily, converting the data into appropriate data type and removing the null and the duplicate values. All the cleaning steps have been recorded in the `Capstone Project.ipynb`file.



## 5. The rationale for the choice of tools and technologies

To create the ELT pipeline Apache spark has been used for the following reasons: 

+ Spark, Support multiple languages like Java, R, Scala, Python. Therefore, it is dynamic and overcomes the limitation of Hadoop that can build applications only in Java.

+ Using Apache Spark, a high data processing speed of about 100x faster in memory and 10x faster on the disk is achievable. This is made possible by reducing the number of read-write to disk. 

+ A parallel application process can be developed using Spark, as Spark provides 80 high-level operators.

+ Spark is capable of handling multiple file formats (including SAS) containing large amounts of data. 

+ We can reuse the Spark code for batch-processing, join stream against historical data or run ad-hoc queries on stream state


## 6. Scaling and updating of the data

### 6.A. What if the database needed to be accessed by 100+ people
If the database needed to be accessed by 100+ people, we can consider publishing the fact and dimension tables saved in the output folder in Amazon AWS redshift or s3 bucket for increased capacity.

### 6.B. How often the data should be updated and why?
To understand the total impact of immigration on demography, the whole year data set needs to be merged. Therefore, the data can be updated every month. However, for convenience, we propose that the data can be updated quarterly to get the most up to date data from the government organizations if monthly data is not available.

### 6.C. How often the data should be updated and why?
The data should be updated quarterly to get the most up to date data from the government organizations.

## 6.D. What if the pipelines were run daily by 7 am
If the data analysis needs to be run daily at 7 am to meet an SLA then we apache Airflow can be used as it has the scheduling feature to run the ETL pipeline overnight.


## 7. References

1. Course Content : [Udacity Data Engineerg Nanodegree](https://eu.udacity.com/course/data-engineer-nanodegree--nd027)

2. Basic Syntax for using [Markdown](https://www.markdownguide.org/basic-syntax/) 

3. For general issues related to python and dataframe: [Stackoverflow](https://stackoverflow.com/)

4. Style Guide for Python Code [PEP 8](https://www.python.org/dev/peps/pep-0008/) has been followed

5. [Features of Apache Spark – Learn the benefits of using Spark](https://data-flair.training/blogs/apache-spark-features/)

6. [What are the pros and cons of Apache Spark?](https://www.quora.com/What-are-the-pros-and-cons-of-Apache-Spark)

7. [Immigration to United States from Wikipedia](https://en.wikipedia.org/wiki/Immigration_to_the_United_States)
