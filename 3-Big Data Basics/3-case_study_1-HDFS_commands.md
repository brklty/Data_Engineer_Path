#Case Study 1 - HDFS Commands

run hdfs services

'''
start-all.sh
'''

Step 1: Import the wine.csv file into hdfs/user/train/hdfs_odev folder.

'''
wget -P ~/datasets/ https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv

hdfs dfs -mkdir /user/train/hdfs_odev

hdfs dfs -put ~/datasets/Wine.csv /user/train/hdfs_odev

'''

Step 2: Copy the /user/train/hdfs_odev/Wine.csv file from HDFS to the /tmp/hdfs_odev folder.

'''
hdfs dfs -cp /user/train/hdfs_odev /tmp/
'''

Step 3: Delete the /tmp/hdfs_odev folder in HDFS, bypassing the recycling.

'''
hdfs dfs -rm -r -skipTrash /tmp/hdfs_odev
'''

Step 4: List the file /user/train/hdfs_odev/Wine.csv from HDFS web interface.

'''
http://localhost:9870
'''