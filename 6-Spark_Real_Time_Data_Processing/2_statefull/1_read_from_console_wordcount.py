# Read From Scoket

import findspark
findspark.init("/opt/manual/spark")
from pyspark.sql import SparkSession, functions as F

# SparkSession
spark = SparkSession.builder.master("local[2]").appName("Read From Socket").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read Data
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())

# Operation
words = lines.select(F.explode(F.split(F.col("value"), "\\s+")).alias("word"))
counts = words.groupBy("word").count()

# Write results
streamingQuery = (counts
.writeStream
.format("console")
.outputMode("complete")
.trigger(processingTime="2 second")
.option("checkpointLocation", "file:///home/train/checkpoint-console")
.option("truncate", False)
.start())

streamingQuery.awaitTermination(100)