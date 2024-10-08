# Covid19 Analysis using Spark

## PURPOSE
The purpose of this project is to demonstrate how Apache Spark can be used as a Big Data tool to handle and process data which can be later used by visualization tools so that the trends and patterns in the data can be analyzed. This project uses Covid19 dataset of 3 months to ingest and transform data in Spark.

## DATA INGESTION
The process of obtaining and importing data for immediate use or storage in a database is known as data ingestion. To take something in or absorb something is to ingest it. 
\
The COVID-19 dataset used here is for the months of April, May, and June 2022. Three folders named apr22, may22, and jun22 were used to save the day-wise csv files of the dataset. 
\
The segregation is as follows:

<table align = "center">
  <tr>
    <th>Dates(MM-DD-YYYY)</th>
    <th>Name</th>
    <th>Rows</th>
    <th>Columns</th>
  </tr>
  <tr>
    <td>04-01-2022 to 04-30-2022</td>
    <td>apr22</td>
    <td>120360</td>
    <td>14</td>
  </tr>
  <tr>
    <td>05-01-2022 to 05-30-2022</td>
    <td>may22</td>
    <td>124372</td>
    <td>14</td>
  </tr>
  <tr>
    <td>06-01-2022 to 06-30-2022</td>
    <td>jun22</td>
    <td>120360</td>
    <td>14</td>
  </tr>
</table>

These data sources will be loaded into spark from the local file system as well as MySQL and Hive tables.

### Loading Data into Spark from Local File

A spark dataframe needs to be given the schema of the dataset while reading a dataframe. To provide schema to the dataframe, the inferSchema option could be used while reading or schema can be manually defined and passed to the dataframe while reading the data like below.
\
\
from pyspark.sql.types import StructType,StructField, StringType,IntegerType, TimestampType,DoubleType
\
\
dfschema=StructType( \
[ \
StructField("fips", IntegerType(), True), \
StructField("admin2", StringType(), True), \
StructField("province_state", StringType(), True), \
StructField("country_region", StringType(), True), \
StructField("last_update", StringType(), True), \
StructField("lat", DoubleType(), True), \
StructField("long_", DoubleType(), True), \
StructField("confirmed", IntegerType(), True), \
StructField("deaths", IntegerType(), True), \
StructField("recovered", IntegerType(), True), \
StructField("active", IntegerType(), True), \
StructField("combined_key", StringType(), True), \
StructField("incident_rate", DoubleType(), True), \
StructField("case_fatality_ratio", DoubleType(), True) \
] \
) \
\
aprdf = spark.read.schema(dfschema).csv("file:///home/ak/covid19/apr22", header=True) \
aprdf.count()

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image9.png)


### Loading Data into MySQL
sudo mysql --local-infile=1 -u root -p

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image19.png)


create table jun22(fips int,admin2 varchar(100),province_state varchar(100),country_region varchar(100),last_update varchar(100),lat decimal(14,6),long_ decimal(14,6),confirmed int,deaths int,recovered int,active int,combined_key varchar(100),incident_rate int,case_fatality_ratio decimal(14,6));

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image17.png)


#### Shell script to load data:
#!/usr/bin/env bash \
for f in *.csv \
do \
sudo mysql --local-infile=1 --user=root -password=P@55word -e "use covid19;load data local infile '"$f"' into table jun22 fields terminated by ',' OPTIONALLY ENCLOSED BY '\"' ignore 1 lines \
\
(@vfips,@vadmin2,@vprovince_state,@vcountry_region,@vlast_update,@vlat,@vlong_,@vconfirmed,@vdeaths,@vrecovered,@vactive,@vcombined_key,@vincident_rate,@vcase_fatality_ratio) \
\
set fips = NULLIF(@vfips,''), \
admin2 = NULLIF(@vadmin2,''), \
province_state = NULLIF(@vprovince_state,''), \
country_region = NULLIF(@vcountry_region,''), \
last_update = NULLIF(@vlast_update,''), \
lat= NULLIF(@vlat,''), \
long_= NULLIF(@vlong_,''), \
confirmed= NULLIF(@vconfirmed,''), \
deaths= NULLIF(@vdeaths,''), \
recovered= NULLIF(@vrecovered,''), \
active= NULLIF(@vactive,''), \
combined_key= NULLIF(@vcombined_key,''), \
incident_rate= NULLIF(@vincident_rate,''), \
case_fatality_ratio= NULLIF(@vcase_fatality_ratio,'')" \
done 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image2.png)


### Loading Data into Hive
hadoop fs -mkdir covid19 \
hadoop fs -put covid19/may22 covid19

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image4.png)

\
hive> create external table may22(fips int,admin2 string,province_state string,country_region string,last_update string,lat double,long_ double,confirmed int,deaths int,recovered int,active int,combined_key string,incident_rate double,case_fatality_ratio double)\
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
WITH SERDEPROPERTIES ( \
"separatorChar" = ",", \
"quoteChar"="\"" \
) \
stored as textfile location '/user/ak/covid19/may22' TBLPROPERTIES ("skip.header.line.count"="1"); 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image10.png)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image5.png)




### Converting Hive table to dataframe in Spark
maydf = spark.sql("select * from covid19.may22 where country_region!='Country_Region'")
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image16.png)



### Converting MYSQL table to Spark dataframe
jundf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/covid19?useSSL=false&allowPublicKeyRetrieval=true").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "jun22").option("user","ak").option("password", "ak").load() 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image15.png)


## DATA CLEANSING
Data cleansing or data cleaning is the process of detecting and correcting corrupt or inaccurate records from a record set, table, or database and refers to identifying incomplete, incorrect, inaccurate or irrelevant parts of the data and then replacing, modifying, or deleting the dirty or coarse data.

### Reading separate data frames
aprdf = spark.read.schema(dfschema).csv("file:///home/ak/covid19/apr22", header=True) \
maydf = spark.sql("select * from covid19.may22 where country_region!='Country_Region'") \
jundf = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/covid19?useSSL=false&allowPublicKeyRetrieval=true").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "jun22").option("user","ak").option("password", "ak").load() 

### Union of all dataframes
covid_df = aprdf.union(maydf).union(jundf)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image21.png)


### Dropping rows with all null columns
covid_df = covid_df.na.drop("all")

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image25.png)


### Replacing null strings with null
covid_df = covid_df.na.replace('null', None) \
covid_df = covid_df.na.replace('Null', None) \
covid_df = covid_df.na.replace('NULL', None) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image3.png)


### Dropping duplicate rows
covid_df = covid_df.dropDuplicates() 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image18.png)


### Saving cleaned union file as csv:
covid_df.write.csv("/home/ak/covid19/covid_union", mode = “overwrite”) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image20.png)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image8.png)


## TRANSFORMATIONS
Data transformation involves converting data from one format or structure into another. This process is crucial for preparing your data for analytics or machine learning models. \
Transformations in Spark are operations on DataFrames that produce a new Dataframe from an existing one. They are generally lazy, meaning they are not executed until an action is called. The execution plan is recorded, and Spark optimizes the plan before executing it.

### Renaming column
covid_df=covid_df.withColumnRenamed('admin2','admin') \
covid_df.printSchema()

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image27.png)


### Splitting column “last_update” into two new columns
from pyspark.sql.functions import split, col \
covid_df=covid_df.withColumn('date',split(col('last_update'),' ').getItem(0)).withColumn('time',split(col('last_update'),' ').getItem(1)) \
covid_df.show(5)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image26.png)


### Changing datatype of column “last_update” to timestamp

from pyspark.sql.functions import to_timestamp \
covid_df=covid_df.withColumn('last_update',to_timestamp(col('last_update'))) \
covid_df.printSchema() 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image22.png)


### Saving dataframe as parquet
Parquet is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. \
\
covid_df.write.parquet("/home/ak/covid19/covid_union_parquet") \
pdf = spark.read.parquet("/home/ak/covid19/covid_union_parquet", header = True) \
pdf.printSchema()
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image12.png)


## GROUP BY, FILTER & AGGREGATIONS
PySpark’s groupBy,filter and aggregate operations are used to perform data aggregation and summarization on a DataFrame. They allow you to group or filter data based on one or more columns and then apply various aggregate functions to compute statistics or transformations on the grouped data.

### Filtering data for deaths more than 1000
covid_df.filter(‘deaths>1000’).show(5) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image1.png)


### Applying groupBy for average confirmed cases country wise
covid_df.groupBy(‘country_region’).avg(‘confirmed’).show(5) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image6.png)


### Top 10 countries with highest deaths 
covid_df.groupBy(‘country_region’).sum(‘deaths’).orderBy(‘sum(deaths)’, ascending = False).show(5) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image13.png)


### Top 10 countries with lowest deaths 
covid_df.groupBy(‘country_region’).sum(‘deaths’).orderBy(‘sum(deaths)’, ascending = True).show(5) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image11.png)


### Top 10 countries with highest confirmed cases 
covid_df.groupBy("country_region").sum("confirmed").orderBy("sum(confirmed)", ascending = False).show(5)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image14.png)


### Top 10 countries with lowest confirmed cases 
covid_df.groupBy("country_region").sum("confirmed").orderBy("sum(confirmed)", ascending = True).show(5)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image24.png)


### Filtering data for a country 
covid_df.filter("country_region='India'").select('country_region','province_state','confirmed','deaths','recovered','active').show(10) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image23.png)


### Sorting dataframe according to country and state 
covid_df=covid_df.sort('country_region','province_state') \
covid_df.show(10) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image7.png)
