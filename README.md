# covid19-analysis-spark

PROBLEM STATEMENT
Recent Covid-19 The pandemic has raised alarms over one of the most overlooked areas to
focus on: healthcare management. While healthcare management has various use cases for
using data, patient cases are one critical parameter to observe and predict if one wants to
improve the efficiency of healthcare management in a hospital.
This problem can be resolved by selecting precise big data tools to handle the data and data
visualization so that the trend can be analyzed precisely and, accordingly, the demand would be
satisfied.



DATA INGESTION
The process of obtaining and importing data for immediate use or storage in a database is
known as data ingestion. To take something in or absorb something is to ingest it. There are
three ways to carry out data ingestion, including real time, batches, or a combination of both in
a setup known as lambda architecture. Depending on their company objectives, IT environment,
and financial constraints, companies might choose one of these varieties.
We used the COVID-19 dataset for the months of April, May, and June 2022. We made three
folders named apr22, may22, and jun22 to save the day-wise csv files of the dataset. The
segregation is as follows:
Dates (MM-DD-YYYY) Name Rows Columns
04-01-2022 to 04-30-2022 apr22 120360 14
05-01-2022 to 05-30-2022 may22 124372 14
06-01-2022 to 06-30-2022 jun22 120360 14



LOADING DATA IN SPARK

from pyspark.sql.types import StructType,StructField, StringType,IntegerType, TimestampType,
DoubleType
dfschema=StructType(
[
StructField("fips", IntegerType(), True),
StructField("admin2", StringType(), True),
StructField("province_state", StringType(), True),
StructField("country_region", StringType(), True),
StructField("last_update", StringType(), True),
StructField("lat", DoubleType(), True),
StructField("long_", DoubleType(), True),
StructField("confirmed", IntegerType(), True),
StructField("deaths", IntegerType(), True),
StructField("recovered", IntegerType(), True),
StructField("active", IntegerType(), True),
StructField("combined_key", StringType(), True),
StructField("incident_rate", DoubleType(), True),
StructField("case_fatality_ratio", DoubleType(), True)
]
)
aprdf = spark.read.schema(dfschema).csv("file:///home/ak/covid19/apr22", header=True)
aprdf.count()

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image9.png)







LOADING DATA INTO MYSQL
sudo mysql --local-infile=1 -u root -p

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image19.png)



create table jun22(fips int,admin2 varchar(100),province_state varchar(100),country_region
varchar(100),last_update varchar(100),lat decimal(14,6),long_ decimal(14,6),confirmed
int,deaths int,recovered int,active int,combined_key varchar(100),incident_rate
int,case_fatality_ratio decimal(14,6));

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image17.png)





Shell script to load data:
#!/usr/bin/env bash
for f in *.csv
do
sudo mysql --local-infile=1 --user=root -password=P@55word -e "use covid19;load data
local infile '"$f"' into table jun22 fields terminated by ',' OPTIONALLY ENCLOSED BY '\"' ignore 1
lines
(@vfips,@vadmin2,@vprovince_state,@vcountry_region,@vlast_update,@vlat,@vlong_,@vco
nfirmed,@vdeaths,@vrecovered,@vactive,@vcombined_key,@vincident_rate,@vcase_fatality_
ratio)
set fips = NULLIF(@vfips,''),
admin2 = NULLIF(@vadmin2,''),
province_state = NULLIF(@vprovince_state,''),
country_region = NULLIF(@vcountry_region,''),
last_update = NULLIF(@vlast_update,''),
lat= NULLIF(@vlat,''),
long_= NULLIF(@vlong_,''),
confirmed= NULLIF(@vconfirmed,''),
deaths= NULLIF(@vdeaths,''),
recovered= NULLIF(@vrecovered,''),
active= NULLIF(@vactive,''),
combined_key= NULLIF(@vcombined_key,''),
incident_rate= NULLIF(@vincident_rate,''),
case_fatality_ratio= NULLIF(@vcase_fatality_ratio,'')"
done

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image2.png)








LOADING DATA INTO HIVE
hadoop fs -mkdir covid19
hadoop fs -put covid19/may22 covid19

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image4.png)


hive> create external table may22(fips int,admin2 string,province_state string,country_region
string,last_update string,lat double,long_ double,confirmed int,deaths int,recovered int,active
int,combined_key string,incident_rate double,case_fatality_ratio double)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"="\""
)
stored as textfile location '/user/ak/covid19/may22' TBLPROPERTIES
("skip.header.line.count"="1");

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image10.png)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image5.png)







Converting Hive table to dataframe in Spark:
maydf = spark.sql("select * from covid19.may22 where country_region!='Country_Region'")
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image16.png)









Converting MYSQL table to Spark dataframe:
jundf = spark.read.format("jdbc").option("url",
"jdbc:mysql://localhost:3306/covid19?useSSL=false&allowPublicKeyRetrieval=true").option("dri
ver", "com.mysql.jdbc.Driver").option("dbtable", "jun22").option("user",
"ak").option("password", "ak").load()

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image15.png)




DATA CLEANSING
Data cleansing or data cleaning is the process of detecting and correcting corrupt or inaccurate
records from a record set, table, or database and refers to identifying incomplete, incorrect,
inaccurate or irrelevant parts of the data and then replacing, modifying, or deleting the dirty or
coarse data.
Reading separate data frames
aprdf = spark.read.schema(dfschema).csv("file:///home/ak/covid19/apr22", header=True)
maydf = spark.sql("select * from covid19.may22 where country_region!='Country_Region'")
jundf = spark.read.format("jdbc").option("url",
"jdbc:mysql://localhost:3306/covid19?useSSL=false&allowPublicKeyRetrieval=true").option("dri
ver", "com.mysql.jdbc.Driver").option("dbtable", "jun22").option("user",
"ak").option("password", "ak").load()
Union of all dataframes
covid_df = aprdf.dropDuplicates().union(maydf.dropDuplicates()).union(jundf.dropDuplicates())
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image21.png)




covid_df = covid_df.na.drop("all")

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image25.png)








covid_df = covid_df.na.replace('null', None) 
covid_df = covid_df.na.replace('Null', None) 
covid_df = covid_df.na.replace('NULL', None)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image3.png)


covid_df = covid_df.dropDuplicates() 
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image18.png)




covid_df.write.csv("/home/ak/covid19/covid_union", mode = “overwrite”) 
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image20.png)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image8.png)








coviddf=coviddf.withColumnRenamed('admin2','admin') 
coviddf.printSchema()

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image27.png)

coviddf=coviddf.withColumn('date',split(col('last_update'),' ').getItem(0)).withColumn('time',split(col('last_update'),' ').getItem(1))

covid_df.show(5)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image26.png)



From pyspark.sql.functions import to_timestamp

coviddf=coviddf.withColumn('last_update',to_timestamp(col('last_update'))) 

coviddf.printSchema() 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image22.png)


parquet
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image12.png)




covid_df.filter(‘deaths>1000’).show(5) 
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image1.png)


covid_df.groupBy(‘country_region’).avg(‘confirmed’).show(5) 
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image6.png)



Top 10 countries with highest deaths covid_df.groupBy(‘country_region’).sum(‘deaths’).orderBy(‘sum(deaths)’, ascending = False).show(5) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image13.png)



Top 10 countries with highest deaths covid_df.groupBy(‘country_region’).sum(‘deaths’).orderBy(‘sum(deaths)’, ascending = True).show(5) 
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image11.png)




Top 10 countries with highest confirmed cases covid_df.groupBy("country_region").sum("confirmed").orderBy("sum(confirmed)", ascending = False).show(5)
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image14.png)




Top 10 countries with highest confirmed cases covid_df.groupBy("country_region").sum("confirmed").orderBy("sum(confirmed)", ascending = True).show(5)

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image24.png)






Filtering data for a country covid_df.filter("country_region='India'").select('country_region','province_state','confirmed','deaths','recovered','active').show(10) 

![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image23.png)



Sorting dataframe according to country and state covid_df=covid_df.sort('country_region','province_state') 
covid_df.show(10) 
![alt text](https://github.com/azeemkhot/covid19-analysis-spark/blob/main/images/image7.png)
















