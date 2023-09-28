# Task: In the data set iot_telemetry_data.csv, data from three different sensors are logged mixed.
# Based on the event time, scrolling every 5 minutes in the last 10-minute window of each sensor (device).
# - Sensor(device) id
# - How many signals are generated
# - Average CO and humidity
# Write a spark streaming application (in local or yarn mode) that prints the values to the screen.

# Step1: Stream the data set to the ~/data-generator/output folder with data-generator.
# -- start-all.sh
# -- rm -rm ~/data-generator/output/*
# -- python dataframe_to_log.py -i ~/datasets/iot_telemetry_data.csv -idx True

# Step2: Import the necessary libraries and create SparkSession.
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("Case Study 2-IOT")
         .master("local[2]")
         .config("spark.executor.memory", "3g")
         .config("spark.driver.memory", "1g")
         .enableHiveSupport()
         .getOrCreate())


spark.sparkContext.setLogLevel('ERROR')


# Step3: Create the iot scheme and read the data you stream according to this scheme.

iot_schema = "row_id int, event_ts double,device string,co float,humidity float,light boolean, "\
            "lpg float,motion boolean,smoke float,temp float, time timestamp"

lines = (spark.readStream
        .format('csv')
        .schema(iot_schema)
        .option("header", True)
        .option("maxFilesPerTrigger", 1)
        .load("file:///home/train/data-generator/output"))

lines2 = (lines.withColumn("ts_long", F.col("event_ts").cast("long")) \
          .withColumn("event_ts",F.to_timestamp(F.from_unixtime(F.col("ts_long"))))
          .drop("ts_long"))


# Step4: Group the data with 10 and 5 minute windows, aggregate the required values to print
# on the screen and assign them to the ten_min_avg variable.

ten_min_avg = lines2.groupBy(
    F.window(F.col("event_ts"), "10 minutes", "5 minutes"), F.col("device")
).agg(F.count("*"), F.mean("co"), F.mean("humidity"))

# Step6: Create the Checkpoint folder and save its path to the checkpointDir variable.

checkpointDir = "file:///home/train/checkpoint/iot_count_checkpoint"

# Step7: Create the streaming query using the information provided.

streamingQuery = (ten_min_avg
                  .writeStream
                  .format("console")
                  .outputMode("complete")
                  .trigger(processingTime="1 second")
                  .option("numRows", 10)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .start())


streamingQuery.awaitTermination()