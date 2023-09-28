# Spark Streamnig with Multiple Sink

# in the iot_telemetry_data.csv dataset, data from three different sensors are logged mixed.

# Task 1: Using Spark Structured Streaming, separate the signals coming from three different sensors and
# print them to three different sinks"


# Step 1: Stream dataset to ~/data-generator/output folder with data-generator.

# -- start-all.sh
# -- rm -rm ~/data-generator/output/*
# -- python dataframe_to_log.py -i ~/datasets/iot_telemetry_data.csv -idx True

# Step 2: Import the necessary libraries and create SparkSession.

import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F


spark = (SparkSession.builder
         .appName("Case Study 1-Device Foreach Multiple Sinks")
         .master("local[2]")
         .config("spark.executor.memory", "3g")
         .config("spark.driver.memory", "1g")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .enableHiveSupport()
         .getOrCreate())


spark.sparkContext.setLogLevel('ERROR')

# Step3: Create the iot schema and create connection paths (Hive, Postgresql, Delta Table).

iot_schema = "row_id int, event_ts double,device string,co float,humidity float,light boolean, "\
            "lpg float,motion boolean,smoke float,temp float, time timestamp"

jdbcUrl = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"
deltaPathSensor4d = "hdfs://localhost:9000/user/train/deltaPathSensor4d"

# Step 4: Read the data using the IoT scheme you created.

lines = (spark.readStream
         .format('csv')
         .schema(iot_schema)
         .option("header", True)
         .option("maxFilesPerTrigger", 1)
         .load("file:///home/train/data-generator/output"))


# Step 5: Write the function that filters the data with the specified ids and writes it as a postgresql, hive and delta table.

# -Data of the sensor with id number 00:0f:00:70:91:0a postgresql traindb sensor_0a table
# -Data for sensor id b8:27:eb:bf:9d:51 hive test1 sensor_51 table
# -The data of the sensor with id number 1c:bf:ce:15:ec:4d is in the sensor_4d table as a delta table.


def write_to_multiple_sinks(df, batchId):
    df.persist()
    df.show()

    # write to postgresql
    df.filter("device = '00:0f:00:70:91:0a'")\
        .write.jdbc(url=jdbcUrl,
                    table="sensor_0a",
                    mode="append",
                    properties={"driver":'org.postgresql.Driver'})

    # write to hive

    df.filter("device = 'b8:27:eb:bf:9d:51'")\
        .write.format("parquet") \
        .mode("append")\
        .saveAsTable("test1.sensor_51")

    # write to delta

    df.filter("device = '1c:bf:ce:15:ec:4d'")\
        .write.format("delta")\
        .mode("append")\
        .save(deltaPathSensor4d)

    df.unpersist()

# Step 6: Create the checkpoint folder, save its path to the checkpointDir variable and start Streaming.


checkpointDir = "file:///home/train/checkpoint/iot_multiple_sink_foreach_batch"

# start streaming


streamingQuery = (lines
                  .writeStream
                  .foreachBatch(write_to_multiple_sinks)
                  .option("checkpointLocation", checkpointDir)
                  .start())


streamingQuery.awaitTermination()


# Step 7: Stop the flow after 250 lines.
# Check the results by going to three different sinks (hive, postgresql and delta) separately.