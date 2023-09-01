# Apache Hive Partition and Bucketing

1)

# Download the data set:

# -u.data
'''
wget -P datasets/ https://raw.githubusercontent.com/erkansirin78/datasets/master/ml-100k/u.data
'''
# -u.item

'''
wget -P datasets/ https://raw.githubusercontent.com/erkansirin78/datasets/master/ml-100k/u.item
'''


# Importing the datasets into HDFS

'''
hdfs dfs -put ~/datasets/u.data /user/train/datasets
hdfs dfs -put ~/datasets/u.item /user/train/datasets
'''

# Beeline connection

'''
beeline -u jdbc:hive2://localhost:10000
'''

# Create Database

'''
CREATE DATABASE movielens;
'''

# Select database

'''
use wine_db;
'''

# Schema ratings

'''
create table if not exists movielens.ratings (
user_id int,
item_id int,
rating int,
rating_time bigint)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');
'''

# Load data to table

'''
load data local inpath '/home/train/datasets/u.data' into table movielens.ratings;
'''

# Control

'''
select * from movielens.ratings limit 4;
'''

# Schema movies

create table if not exists movielens.movies (
movieid int,
movietitle string,
releasedate string,
videoreleasedate string,
IMDbURL string,
unknown tinyint,
Action tinyint,
Adventure tinyint,
Animation tinyint,
Childrens tinyint,
Comedy tinyint,
Crime tinyint,
Documentary tinyint,
Drama tinyint,
Fantasy tinyint,
FilmNoir tinyint,
Horror tinyint,
Musical tinyint,
Mystery tinyint,
Romance tinyint,
SciFi tinyint,
Thriller tinyint,
War tinyint,
Western tinyint)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');

# Load data to table

'''
load data local inpath '/home/train/datasets/u.item' into table movielens.movies;
'''

# Control

'''
select movieid, movietitle, releasedate from movielens.movies limit 5;
'''

2)
# It is desired to determine the most popular (most rated, highest average score) movies on a monthly basis. Accordingly, design the table (partition and bucketing) and create it.

'''
create table if not exists movielens.movie_ratings (
user_id int,
rating int,
rating_time bigint,
movieid int,
movietitle string,
videoreleasedate string,
imdburl string)
partitioned by (review_year int, review_month int)
clustered by (movietitle) into 4 buckets
stored as orc;
'''

# Dynamic Partitioning

'''
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;
'''

# Load the two table data into the table you designed.

'''
insert overwrite table movielens.movie_ratings PARTITION(review_year, review_month)
select user_id,
rating,
rating_time,
movieid,
movietitle,
videoreleasedate,
imdburl,
YEAR(from_unixtime(rating_time, 'yyyy-MM-dd')) as review_year,
MONTH(from_unixtime(rating_time, 'yyyy-MM-dd')) as review_month
from movielens.ratings r join movielens.movies m on r.item_id = m.movieid;
'''

# Control

'''
select count(1) from movielens.movie_ratings;
'''

# List Partitions

'''
show partitions movielens.movie_ratings;
'''

# Table properties

'''
describe movielens.movie_ratings;
'''

# How many unique values are there as Review Year and Review Month?
'''
select distinct (review_year, review_month) from movielens.movie_ratings;
'''

3)
# Find the 20 highest rated films of April 1998.

'''
select count(*) total_count, movietitle
from movielens.movie_ratings
where review_year=1998 AND review_month=4
group by movietitle order by total_count desc limit 20;
'''

# Find the 20 films with the highest average ratings from the films voted in April 1998.

'''
select avg(rating) as avg_rating, count(*) total_count, movietitle
from movielens.movie_ratings
where review_year=1998 AND review_month=4
group by movietitle order by avg_rating desc limit 20;
'''