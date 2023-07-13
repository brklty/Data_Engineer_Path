# Hive Commands


Checking if Hive services are working

```
pgrep -f org.apache.hive.service.server.HiveServer2
```

```
pgrep -f org.apache.hadoop.hive.metastore.HiveMetaStore
```

If the pid number comes out, we can say that Hive is working.

Hive Connection with Beeline

```
beeline -u jdbc:hive2://localhost:10000
```

Switching off log messages

```
set hive.server2.logging.operation.level=NONE;
```

How to log out of Beeline shell

```
!q
```

### Database Operations

## Create, Describe and Drop commands

Database creation

```sql
create database test1;
```

List databases

```sql
show databases;
```

Demonstrate database properties

```sql
describe database test1;
```

Removing the database

```sql
drop database test1;
```

Entering a description when creating a database

```sql
create database test1 comment 'This is for training';
```

Select a database

```sql
use test1;
```

----

### Table Operations

Creating a table

```sql
create table if not exists mytable(id int, user_name string, email string);
```

See table properties

```sql
describe mytable;
```

```
0: jdbc:hive2://localhost:10000> describe mytable;
+------------+------------+----------+
| col_name | data_type | comment |
+------------+------------+----------+
| id | int | | |
| user_name | string | | |
| email | string | |
+------------+------------+----------+

```

See detailed table properties

```sql
describe formatted mytable;
```

Table drop

```sql
drop table mytable;
```

Creating a table suitable for Hive schemas

```sql
create table if not exists mytable(id int, user_name string, email string)
row format delimited
fields terminated by ','
collection items terminated by ':'
lines terminated by '\n'
stored as textfile;
```

Get create commands of a created table

```sql
show create table mytable;
```

### Manual data entry into the Hive Table

```sql
insert into table test1.mytable values(1, "testuser1", "testuser1@example.com");
```

Entering data with select query

```sql
insert into table test1.mytable SELECT 2, "testuser2", "testuser2@example.com";
```

Manually displaying multiple data

```sql
10000> insert into table test1.mytable values(3, "testuser3", "testuser3@example.com"),
(4, "testuser4", "testuser4@example.com")

```

### Loading CSV File with table

Sample data file:

``bash

[train@10 ~]$ hdfs dfs -head /user/train/datasets/Advertising.csv
ID,TV,Radio,Newspaper,Sales
1,230.1,37.8,69.2,22.1
2,44.5,39.3,45.1,10.4
3,17.2,45.9,69.3,9.3
4,151.5,41.3,58.5,18.5
5,180.8,10.8,58.4,12.9

```

Creating a table

```sql
create table test1.advertising (ID int,TV float ,Radio float ,Newspaper float ,Sales float)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile 
tblproperties('skip.header.line.count'='1');

```

Import data from HDFS into a Hive table

```sql
load data inpath '/user/train/datasets/Advertising.csv' into table test1.advertising;
```

Control:

```sql
0: jdbc:hive2://localhost:10000> select * from advertising limit 5;
+-----------------+-----------------+--------------------+------------------------+--------------------+
| advertising.id | advertising.tv | advertising.radio | advertising.newspaper | advertising.sales |
+-----------------+-----------------+--------------------+------------------------+--------------------+
| 1 | 230.1 | 37.8 | 69.2 | 22.1 |
| 2 | 44.5 | 39.3 | 45.1 | 10.4 |
| 3 | 17.2 | 45.9 | 69.3 | 9.3 |
| 4 | 151.5 | 41.3 | 58.5 | 18.5 |
| 5 | 180.8 | 10.8 | 58.4 | 12.9 |
+-----------------+-----------------+--------------------+------------------------+--------------------+

```

The data is moved from the directory where it is located to a different location.

Let's put the data back into HDFS.

```bash
 hdfs dfs -put ~/datasets/Advertising.csv /user/train/datasets
```

Adding data to a table with `overwrite` from CSV

```sql
load data inpath '/user/train/datasets/Advertising.csv' overwrite into table test1.advertising;
```

Output

```
0: jdbc:hive2://localhost:10000> SELECT COUNT(1) from advertising;
+------+
| _c0 |
+------+
| 200 |
+------+

```

### Create a new table using existing tables

```
create table adv_sales_gt_20 as select * from test1.advertising where sales > 20;
```

Add a new record to an existing table

```
insert into table adv_sales_gt_20 select * from test1.advertising where sales > 20;
```

emptying the table

```
truncate table adv_sales_gt_20
```

### Alter Table

Table backup

```
create table test1.mytable_bckp as select * from test1.mytable;
```

Change table name

```
alter table mytable rename to test1.mytable_renamed;
```

Display table properties

```
describe test1.mytable_renamed;
```

Adding a new column

```
alter table test1.mytable_renamed add columns (added_col int, added_col2 string);
```

Change the column name in the table

```
alter table test1.mytable_renamed change user_name username string;
```

Changing table properties

```
alter table test1.mytable_renamed SET TBLPROPERTIES ('comment' = 'this edit by altertable');

```

### Creating an External Table

Create a folder on HDFS

```
hdfs dfs -mkdir /user/train/hiveExt
```

Let's send a dataset from Linux local to HDFS.

```
hdfs dfs -put ~/datasets/Advertising.csv /user/train/hiveExt
```

Let's connect to Beeline.

```
beeline -u jdbc:hive2://localhost:10000
```

Let's create an external table.

```
create external table if not exists test1.adv_ext like test1.advertising location '/user/train/hiveExt';```

Control

```select * from adv_ext limit 5;

+-------------+-------------+----------------+--------------------+----------------+
| adv_ext.id | adv_ext.tv | adv_ext.radio | adv_ext.newspaper | adv_ext.sales |
+-------------+-------------+----------------+--------------------+----------------+
| NULL | NULL | NULL | NULL | NULL | NULL | NULL || 1 | 230.1 | 37.8 | 69.2 | 22.1 || 2 | 44.5 | 39.3 | 45.1 | 10.4 |
| 3 | 17.2 | 45.9 | 69.3 | 9.3 |
| 4 | 151.5 | 41.3 | 58.5 | 18.5 |+-------------+-------------+----------------+--------------------+----------------+

``` The first row is empty because we did not say that the title information is in the first row when creating the table.Let's fix this situation with alter table

```
 alter table test1.adv_ext SET TBLPROPERTIES('skip.header.line.count' = '1');

```Let's lower the table```drop table test1.adv_ext
```

Let's check if the data has been deleted from HDFS.

```hdfs dfs -ls /user/train/hiveExt```

Output

```

[train@10 ~]$ hdfs dfs -ls /user/train/hiveExt
Found 1 items

-rw-r--r-- 1 train supergroup 4556 2022-06-12 23:04 /user/train/hiveExt/Advertising.csv


```

### Using query and query file as arguments with Beeline

```
beeline -u jdbc:hive2://localhost:10000 -e 'SELECT * FROM test1.advertising LIMIT 5;'


```

Create a file and use it with Beeline

```

vim beeline_queries.sql

```


```
use test1;show tables;SELECT * FROM advertising LIMIT 5;


```

save and exit.

```

beeline -u jdbc:hive2://localhost:10000 -f ~/beeline_queries.sql 


```

### File Formats in the Hadoop worldDownloading the relevant data set to linux locale

```
wget -P ~/datasets/ https://github.com/erkansirin78/datasets/raw/master/Hotel_Reviews.csv.gz

```

Moving the dataset to HDFS

```
 hdfs dfs -put ~/datasets/Hotel_Reviews.csv.gz /user/train/datasets
```

Controlling

```
hdfs dfs -ls /user/train/datasets
```

Since this is a difficult scheme to prepare by hand, let's open it with the help of Spark.

Spark opening

```
pyspark --master yarn
```

```
>>> df = spark.read.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.option("compression", "gzip") \
.load("/user/train/datasets/Hotel_Reviews.csv.gz")

```

Let's do a schematic check

```
df.printSchema()

root
 |-- Hotel_Address: string (nullable = true)
 |-- Additional_Number_of_Scoring: integer (nullable = true)
 |-- Review_Date: string (nullable = true)
 |-- Average_Score: double (nullable = true)
 |-- Hotel_Name: string (nullable = true)
 |-- Reviewer_Nationality: string (nullable = true)
 |-- Negative_Review: string (nullable = true)
 |-- Review_Total_Negative_Word_Counts: integer (nullable = true)
 |-- Total_Number_of_Reviews: integer (nullable = true)
 |-- Positive_Review: string (nullable = true)
 |-- Review_Total_Positive_Word_Counts: integer (nullable = true)
 |-- Total_Number_of_Reviews_Reviewer_Has_Given: integer (nullable = true)
 |-- Reviewer_Score: double (nullable = true)
 |-- Tags: string (nullable = true)
 |-- days_since_review: string (nullable = true)
 |-- lat: string (nullable = true)
 |-- lng: string (nullable = true)

```

Checking how many lines there are

```
df.count()
```

Write to Hive as a table

```
df.write.format("parquet").mode("overwrite").saveAsTable("test1.hotels_parquet")
```

Checking with Beeline:

```
beeline -u jdbc:hive2://localhost:10000 -e 'SELECT * FROM test1.hotels_parquet LIMIT 5;' 
```

### Create Tables of different file formats

select the test1 database

```
use test1;
```

Create a table in `orc` format using an existing table

```
create table hotels_orc stored as orc as select * from test1.hotels_parquet;
```

Let's check

```
describe formatted hotels_orc;
```

```

| InputFormat: | org.apache.hadoop.hive.ql.io.orc.OrcInputFormat | NULL |
| OutputFormat: | org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat | NULL |
```

Create a table in `textfile` format using an existing table

```
create table hotels_text stored as textfile as select * from test1.hotels_parquet;
```

### Using compression algorithms with file formats

Let's make the settings

```
SET hive.exec.compress.output=true;

SET io.textfile.compression.type=SNAPPY;
```

Let's compress the textfile file using SNAPPY.

```
create table hotels_text_snappy stored as textfile as select * from test1.hotels_parquet;
```

Checking their size

```

[train@10 datasets]$ hdfs dfs -du -h /user/hive/warehouse/test1.db | grep hotels
40.4 M 40.4 M /user/hive/warehouse/test1.db/hotels_orc
60.2 M 60.2 M /user/hive/warehouse/test1.db/hotels_parquet
44.2 M 44.2 M /user/hive/warehouse/test1.db/hotels_text
44.2 M 44.2 M /user/hive/warehouse/test1.db/hotels_text_snappy

```

----

### Partitioning

```
SELECT COUNT(DISTINCT review_date) FROM test1.hotels_orc; 

DESCRIBE hotels_orc;

SELECT review_date, 
MONTH(from_unixtime(unix_timestamp(review_date, 'MM/dd/yyyy'))) as review_month,
YEAR (from_unixtime(unix_timestamp(review_date, 'MM/dd/yyyy')))) as review_year
FROM hotels_orc LIMIT 20;
```

Create a new blank table with Partition;

```
-- create partitioned table 
create table if not exists test1.hotels_prt (
Hotel_Address string,
Review_Date Date,
 Additional_Number_of_Scoring int,
 Average_Score double,
 Hotel_Name string,
 Reviewer_Nationality string,
 Negative_Review string,
 Review_Total_Negative_Word_Counts int,
 Total_Number_of_Reviews int,
 Positive_Review string,
 Review_Total_Positive_Word_Counts int,
 Total_Number_of_Reviews_Reviewer_Has_Given int,
 Reviewer_Score double,
 days_since_review string,
 lat string,
 lng string,
 Tags string
 ) 
 partitioned by (review_year int, review_month int)
 stored as orc;
```

Dynamic partitioning settings

```
-- open dynamic partitioning
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
```

Inserting data

```
-- please pay attention to partition column must be at the end in select statement
INSERT into table test1.hotels_prt PARTITION(review_year, review_month) 
select Hotel_Address,
from_unixtime(unix_timestamp(review_date , 'MM/dd/yyyy')) as Review_Date,
Additional_Number_of_Scoring,
 Average_Score,
 Hotel_Name,
 Reviewer_Nationality,
 Negative_Review,
 Review_Total_Negative_Word_Counts,
 Total_Number_of_Reviews,
 Positive_Review,
 Review_Total_Positive_Word_Counts,
 Total_Number_of_Reviews_Reviewer_Has_Given,
 Reviewer_Score,
 days_since_review,
 lat,
 lng,
 Tags,


 YEAR(from_unixtime(unix_timestamp(review_date , 'MM/dd/yyyy')))) as review_year,
 MONTH(from_unixtime(unix_timestamp(review_date , 'MM/dd/yyyy')))) as review_month
from test1.hotels_parquet;
```

Control

```
SELECT COUNT(1) FROM hotels_prt LIMIT 5;
```

```
SHOW PARTITIONS hotels_prt;
```

---

### Bucketing

```
-- Bucketing 

create table if not exists test1.hotels_bucket (
Hotel_Address string,
Review_Date string,
 Additional_Number_of_Scoring int,
 Average_Score double,
 Hotel_Name string,
 Reviewer_Nationality string,
 Negative_Review string,
 Review_Total_Negative_Word_Counts int,
 Total_Number_of_Reviews int,
 Positive_Review string,
 Review_Total_Positive_Word_Counts int,
 Total_Number_of_Reviews_Reviewer_Has_Given int,
 Reviewer_Score double,
 days_since_review string,
 lat string,
 lng string,
 Tags string
 )
 clustered by (Hotel_Name) into 8 buckets 
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
stored as orc;
```

```
set hive.enforce.bucketing = true;
```

```
insert into test1.hotels_bucket select * from test1.hotels_parquet;
```

```
describe formatted test1.hotels_bucket;
```

```
SELECT COUNT (1) from test1.hotels_bucket ;
-- 515738
```