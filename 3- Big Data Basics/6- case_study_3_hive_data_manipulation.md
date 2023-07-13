# wine.csv

1)

# Download the data set:

'''
wget -P datasets/ https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv
'''

# Importing the data set into HDFS

'''
hdfs dfs -put ~/datasets/Wine.csv /user/train/datasets
'''

# Beeline connection

'''
beeline -u jdbc:hive2://localhost:10000
'''

# Create Database

'''
create database wine_db;
'''

# Select database

'''
use wine_db;
'''

#Schema

'''
create table if not exists wine_db.wine_table(
Alcohol int,
tv float,
Ash float,
Ash_Alcanity float,
Magnesium float,
Total_Phenols float,
Flavanoids float,
Nonflavanoid_Phenols float,
Proanthocyanins float,
Color_Intensity float,
Hue float,
OD280 float,
Proline float,
Customer_Segment int)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1'); #We specify that there is a header in the csv file.
'''

# Load data to table

'''
load data inpath '/user/train/datasets/Wine.csv' into table wine_table;
'''

# Control

'''
SELECT * FROM wine_table LIMIT 5;
'''

2) 
# Write the records with Alcohol ratio greater than 13.00 in the wine table to a new table named wine_alc_gt_13.


'''
CREATE TABLE wine_db.wine_alc_gt_13 as
SELECT * FROM wine_db.wine_table
WHERE alcohol > 13.00
'''

3)
# Delete the database and the tables in it with a single command.

drop database wine_db cascade;

